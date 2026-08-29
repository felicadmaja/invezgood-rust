//! Entry point — daftarkan semua layanan gRPC dari crate modul di sini.

use bandarmology::{BandarmologyServer, BandarmologyService};
use broker::{BrokerServer, BrokerService};
use chart::{ChartServer, ChartService};
use emiten_trending::{EmitenTrendingServer, EmitenTrendingService};
use haka_haki::{HakaHakiServer, HakaHakiService};
use hari_libur::{HariLiburServer, HariLiburService};
use ftse::{FtseServer, FtseService};
use msci::{MsciServer, MsciService};
use pending_order::{PendingOrderServer, PendingOrderService};
use portofolio::{PortofolioServer, PortofolioService};
use portofolio_equity::{PortofolioEquityServer, PortofolioEquityService};
use portofolio_history::{PortofolioHistoryServer, PortofolioHistoryService};
use stock_list::{connect, spawn_daily_notation_sync, StockListServer, StockListService};
use stockbit_browser::ReadinessPoller;
use worker_scrapping::invezgo_spike_poller::InvezgoSpikePoller;
use worker_scrapping::yahoo_spike_poller::YahooSpikePoller;
use config_fundamental::{ConfigFundamentalServer, ConfigFundamentalService};
use evtoebit::{new_shared_median_cache, new_yahoo_client, spawn_monthly_evtoebit_sync, EvToEbitServer, EvToEbitService};
use shareholder_composition::{
    ShareholderCompositionServer, ShareholderCompositionService,
};
use top_foreign_flow::{spawn_daily_top_foreign_flow_sync, TopForeignFlowServer, TopForeignFlowService};
use top_gainer_loser::{TopGainerLoserServer, TopGainerLoserService};
use user::{new_session_store, UserServer, UserService};
use wyckoff_glossary::{WyckoffGlossaryServer, WyckoffGlossaryService};
use xlbr_laporan_keuangan::{XlbrLaporanKeuanganServer, XlbrLaporanKeuanganService};

use grpc_server::apply_grpc_transport;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;
use tonic_reflection::server::Builder as ReflectionBuilder;

/// Baca ENABLE_COMPRESSION dari env. Default true jika tidak di-set.
fn enable_compression_from_env() -> bool {
    std::env::var("ENABLE_COMPRESSION")
        .map_or(true, |v| v == "1" || v.eq_ignore_ascii_case("true"))
}

macro_rules! maybe_compressed {
    ($e:expr, $enable:expr) => {
        if $enable {
            $e.send_compressed(CompressionEncoding::Gzip)
                .accept_compressed(CompressionEncoding::Gzip)
        } else {
            $e
        }
    };
}

pub mod pb {
    tonic::include_proto!("invezgood");

    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("invezgood_descriptor");
}

use pb::invezgood_server::{Invezgood, InvezgoodServer};
use pb::{PingRequest, PingResponse};

const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_PORT: &str = "50054";

#[derive(Default)]
struct InvezgoodService;

#[tonic::async_trait]
impl Invezgood for InvezgoodService {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        let message = request.into_inner().message;
        Ok(tonic::Response::new(PingResponse {
            message: format!("pong: {message}"),
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Wajib override: bila shell/PM2 masih punya INVEZGO_BEARER_TOKEN lama, dotenv() biasa
    // TIDAK menimpa → server pakai key rate-limited, Postman pakai key baru dari .env.
    if let Err(e) = dotenvy::dotenv_override() {
        eprintln!("\x1b[31m.env gagal di-load: {e} — variabel setelah baris itu TIDAK ter-load\x1b[0m");
    }

    if let Ok(token) = std::env::var("INVEZGO_BEARER_TOKEN") {
        let fp = if token.len() >= 12 {
            format!(
                "{}…{} len={}",
                &token[..8],
                &token[token.len().saturating_sub(4)..],
                token.len()
            )
        } else {
            format!("len={}", token.len())
        };
        eprintln!("INVEZGO_BEARER_TOKEN loaded fp={fp}");
    } else {
        eprintln!("\x1b[31mINVEZGO_BEARER_TOKEN tidak ter-load\x1b[0m");
    }

    let host = std::env::var("HOST").unwrap_or_else(|_| DEFAULT_HOST.into());
    let port = std::env::var("GRPC_PORT").unwrap_or_else(|_| DEFAULT_PORT.into());
    let addr = format!("{host}:{port}").parse()?;

    let session = connect().await?;
    market_holiday::init_scylla_session(session.clone());
    let auth_sessions = new_session_store();
    let stock_list = StockListService::new(session.clone(), auth_sessions.clone())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let top_gainer_loser = TopGainerLoserService::new(session.clone(), auth_sessions.clone());
    let top_foreign_flow = TopForeignFlowService::new(session.clone(), auth_sessions.clone());
    let shareholder_composition =
        ShareholderCompositionService::new(session.clone(), auth_sessions.clone());
    let bandarmology = BandarmologyService::new(session.clone(), auth_sessions.clone());
    let broker = BrokerService::new(session.clone(), auth_sessions.clone());
    let portofolio = PortofolioService::new(session.clone(), auth_sessions.clone());
    let portofolio_equity =
        PortofolioEquityService::new(session.clone(), auth_sessions.clone());
    let portofolio_history =
        PortofolioHistoryService::new(session.clone(), auth_sessions.clone());
    let pending_order = PendingOrderService::new(session.clone(), auth_sessions.clone());
    let emiten_trending = EmitenTrendingService::new(session.clone(), auth_sessions.clone());
    let chart = ChartService::new(
        chart::new_shared_cache()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
        auth_sessions.clone(),
    );
    let haka_haki = HakaHakiService::new(
        session.clone(),
        auth_sessions.clone(),
        haka_haki::new_shared_intraday_cache()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
    );
    let msci = MsciService::new(session.clone(), auth_sessions.clone());
    let ftse = FtseService::new(session.clone(), auth_sessions.clone());
    let hari_libur = HariLiburService::new(session.clone(), auth_sessions.clone());
    let wyckoff_glossary =
        WyckoffGlossaryService::new(session.clone(), auth_sessions.clone());
    let config_fundamental =
        ConfigFundamentalService::new(session.clone(), auth_sessions.clone());
    let evtoebit_yahoo = new_yahoo_client()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let evtoebit_cache = new_shared_median_cache(evtoebit_yahoo.clone());
    let evtoebit = EvToEbitService::new(
        session.clone(),
        auth_sessions.clone(),
        evtoebit_cache.clone(),
    );
    let xlbr_laporan_keuangan =
        XlbrLaporanKeuanganService::new(session.clone(), auth_sessions.clone());

    let readiness_poller = ReadinessPoller::new();
    {
        let session_hook = session.clone();
        readiness_poller
            .set_after_poll_hook(std::sync::Arc::new(move |ready| {
                let session = session_hook.clone();
                Box::pin(async move {
                    worker_scrapping::on_demand::run_poller_stockbit_scrapes(session, ready).await
                })
            }))
            .await;
        readiness_poller.ensure_loop_running().await;
    }

    let yahoo_spike = YahooSpikePoller::new();
    yahoo_spike
        .ensure_loop_running(session.clone())
        .await;

    let invezgo_spike = InvezgoSpikePoller::new();
    invezgo_spike
        .ensure_loop_running(session.clone())
        .await;

    spawn_daily_notation_sync(session.clone());
    spawn_daily_top_foreign_flow_sync(session.clone());
    spawn_monthly_evtoebit_sync(session.clone(), evtoebit_yahoo, evtoebit_cache);

    let user = UserService::new(
        session,
        auth_sessions,
        readiness_poller,
        yahoo_spike,
        invezgo_spike,
    );

    let enable_compression = enable_compression_from_env();

    let reflection = maybe_compressed!(
        ReflectionBuilder::configure()
            .register_encoded_file_descriptor_set(stock_list::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(user::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(top_gainer_loser::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(top_foreign_flow::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(shareholder_composition::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(bandarmology::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(broker::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(portofolio::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(portofolio_equity::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(portofolio_history::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(pending_order::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(emiten_trending::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(chart::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(haka_haki::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(msci::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(ftse::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(hari_libur::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(wyckoff_glossary::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(config_fundamental::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(evtoebit::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(xlbr_laporan_keuangan::FILE_DESCRIPTOR_SET)
            .register_encoded_file_descriptor_set(pb::FILE_DESCRIPTOR_SET)
            .build_v1()?,
        enable_compression
    );

    let invezgood_svc =
        maybe_compressed!(InvezgoodServer::new(InvezgoodService), enable_compression);
    let stock_list_svc = maybe_compressed!(StockListServer::new(stock_list), enable_compression);
    let user_svc = maybe_compressed!(UserServer::new(user), enable_compression);
    let top_gainer_loser_svc =
        maybe_compressed!(TopGainerLoserServer::new(top_gainer_loser), enable_compression);
    let top_foreign_flow_svc =
        maybe_compressed!(TopForeignFlowServer::new(top_foreign_flow), enable_compression);
    let shareholder_composition_svc = maybe_compressed!(
        ShareholderCompositionServer::new(shareholder_composition),
        enable_compression
    );
    let bandarmology_svc =
        maybe_compressed!(BandarmologyServer::new(bandarmology), enable_compression);
    let broker_svc = maybe_compressed!(BrokerServer::new(broker), enable_compression);
    let portofolio_svc =
        maybe_compressed!(PortofolioServer::new(portofolio), enable_compression);
    let portofolio_equity_svc = maybe_compressed!(
        PortofolioEquityServer::new(portofolio_equity),
        enable_compression
    );
    let portofolio_history_svc = maybe_compressed!(
        PortofolioHistoryServer::new(portofolio_history),
        enable_compression
    );
    let pending_order_svc =
        maybe_compressed!(PendingOrderServer::new(pending_order), enable_compression);
    let emiten_trending_svc = maybe_compressed!(
        EmitenTrendingServer::new(emiten_trending),
        enable_compression
    );
    let chart_svc = maybe_compressed!(ChartServer::new(chart), enable_compression);
    let haka_haki_svc = maybe_compressed!(HakaHakiServer::new(haka_haki), enable_compression);
    let msci_svc = maybe_compressed!(MsciServer::new(msci), enable_compression);
    let ftse_svc = maybe_compressed!(FtseServer::new(ftse), enable_compression);
    let hari_libur_svc =
        maybe_compressed!(HariLiburServer::new(hari_libur), enable_compression);
    let wyckoff_glossary_svc =
        maybe_compressed!(WyckoffGlossaryServer::new(wyckoff_glossary), enable_compression);
    let config_fundamental_svc = maybe_compressed!(
        ConfigFundamentalServer::new(config_fundamental),
        enable_compression
    );
    let evtoebit_svc =
        maybe_compressed!(EvToEbitServer::new(evtoebit), enable_compression);
    let xlbr_laporan_keuangan_svc = maybe_compressed!(
        XlbrLaporanKeuanganServer::new(xlbr_laporan_keuangan),
        enable_compression
    );

    let mut builder = apply_grpc_transport(Server::builder());

    if tls::use_tls_from_env() {
        let tls_config = tls::load_tls_config()?;
        println!("TLS enabled: gRPC server accepting HTTPS connections");
        builder = builder.tls_config(tls_config)?;
    }

    if enable_compression {
        println!("gRPC gzip compression enabled (send + accept)");
    }

    println!("invezgood gRPC listening on {addr} (reflection enabled)");

    builder
        .add_service(reflection)
        .add_service(invezgood_svc)
        .add_service(stock_list_svc)
        .add_service(user_svc)
        .add_service(top_gainer_loser_svc)
        .add_service(top_foreign_flow_svc)
        .add_service(shareholder_composition_svc)
        .add_service(bandarmology_svc)
        .add_service(broker_svc)
        .add_service(portofolio_svc)
        .add_service(portofolio_equity_svc)
        .add_service(portofolio_history_svc)
        .add_service(pending_order_svc)
        .add_service(emiten_trending_svc)
        .add_service(chart_svc)
        .add_service(haka_haki_svc)
        .add_service(msci_svc)
        .add_service(ftse_svc)
        .add_service(hari_libur_svc)
        .add_service(wyckoff_glossary_svc)
        .add_service(config_fundamental_svc)
        .add_service(evtoebit_svc)
        .add_service(xlbr_laporan_keuangan_svc)
        .serve(addr)
        .await?;

    Ok(())
}
