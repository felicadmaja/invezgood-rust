//! Auto-scrape saat poller `IsStockbitReady` = ready.
//!
//! Dipicu **setiap siklus poller** (interval acak 9–10 menit) bila `ready=true`
//! dan `poll_seq > 0` (hasil cek web nyata — bukan hydrate Redis).
//!
//! Env `NEW_BUILD_LANGSUNG_SCRAPE` / `NEW_BUILD_LANNGSUNG_SCRAPE` (default `true`):
//! bila `false`, siklus ready **pertama** setelah restart di-skip (cek web tetap jalan);
//! scrape menunggu interval poller berikutnya (9–10 menit).
//!
//! Urutan:
//! 1. GetAllPortofolioFromStockbit (holdings + equity dari satu GET portfolio/v2/list)
//! 2. GetAllPendingOrderFromStockbit
//! 3. GetLatestEmitenTrendingFromStockbit
//!
//! `GetPortofolioHistoryByEmitenNameFromStockbit` **tidak** di-auto-invoke (hanya via RPC user).
//!
//! Jam operasional Senin–Jumat, 08:45–12:15 dan 13:25–16:15 (Sabtu/Minggu tidak scrape).
//! Tiap alur memakai **rate limit RPC yang sama** (`acquire_*`):
//! - auto pakai jatah → user RPC bisa kena limit
//! - user RPC pakai jatah → auto skip alur itu (lanjut ke berikutnya)

use std::sync::Arc;

use emiten_trending::EmitenTrendingService;
use pending_order::PendingOrderService;
use portofolio::PortofolioService;
use stockbit_browser::ReadinessPoller;
use tonic::{Code, Status};
use user::require_stockbit_scrape_hours;

fn log_auto_skip(rpc: &str, err: &Status) {
    if err.code() == Code::FailedPrecondition {
        println!(
            "Readiness ready → skip auto {rpc} (jatah rate limit sudah terpakai): {}",
            err.message()
        );
    } else {
        println!(
            "Readiness ready → skip/fail auto {rpc}: {}",
            err.message()
        );
    }
}

/// `true` = setelah restart, siklus ready pertama boleh auto-scrape (default).
/// `false` = skip scrape pertama; tunggu interval poller berikutnya.
fn new_build_langsung_scrape() -> bool {
    for key in ["NEW_BUILD_LANGSUNG_SCRAPE", "NEW_BUILD_LANNGSUNG_SCRAPE"] {
        if let Ok(v) = std::env::var(key) {
            let v = v.trim();
            if v.is_empty() {
                continue;
            }
            return !(v.eq_ignore_ascii_case("false")
                || v == "0"
                || v.eq_ignore_ascii_case("no")
                || v.eq_ignore_ascii_case("off"));
        }
    }
    true
}

pub fn spawn_on_stockbit_ready(
    poller: Arc<ReadinessPoller>,
    portofolio: PortofolioService,
    pending_order: PendingOrderService,
    emiten_trending: EmitenTrendingService,
) {
    let skip_first_scrape = !new_build_langsung_scrape();
    let mut rx = poller.subscribe();
    tokio::spawn(async move {
        let mut last_poll_seq = 0u64;
        let mut first_ready_cycle = true;
        loop {
            let update = rx.borrow_and_update().clone();
            let (ready, poll_seq) = update
                .as_ref()
                .map(|u| (u.ready, u.poll_seq))
                .unwrap_or((false, 0));

            if ready && poll_seq > 0 && poll_seq != last_poll_seq {
                last_poll_seq = poll_seq;
                if first_ready_cycle {
                    first_ready_cycle = false;
                    if skip_first_scrape {
                        println!(
                            "Readiness poll_seq={poll_seq} ready → skip auto scrape (NEW_BUILD_LANGSUNG_SCRAPE=false); tunggu siklus poller berikutnya"
                        );
                        continue;
                    }
                }
                if let Err(e) = require_stockbit_scrape_hours() {
                    println!(
                        "Readiness poll_seq={poll_seq} ready → skip auto Stockbit scrapes: {}",
                        e.message()
                    );
                } else {
                    println!(
                        "Readiness poll_seq={poll_seq} ready → auto invoke 3 scrapes (gate rate limit bersama user RPC)..."
                    );
                    run_auto_scrapes(&portofolio, &pending_order, &emiten_trending).await;
                    println!("Readiness poll_seq={poll_seq} → auto scrapes selesai.");
                }
            }

            if rx.changed().await.is_err() {
                break;
            }
        }
    });
}

async fn run_auto_scrapes(
    portofolio: &PortofolioService,
    pending_order: &PendingOrderService,
    emiten_trending: &EmitenTrendingService,
) {
    println!("Readiness ready → auto GetAllPortofolioFromStockbit (holdings + equity)...");
    match portofolio.scrape_from_stockbit_if_allowed().await {
        Ok((n, codes)) => println!(
            "Auto GetAllPortofolioFromStockbit selesai: {n} baris ({} kode).",
            codes.len()
        ),
        Err(e) => log_auto_skip("GetAllPortofolioFromStockbit", &e),
    }

    println!("Readiness ready → auto GetAllPendingOrderFromStockbit...");
    match pending_order.scrape_from_stockbit_if_allowed().await {
        Ok(n) => println!("Auto GetAllPendingOrderFromStockbit selesai: {n} baris."),
        Err(e) => log_auto_skip("GetAllPendingOrderFromStockbit", &e),
    }

    println!("Readiness ready → auto GetLatestEmitenTrendingFromStockbit...");
    match emiten_trending.scrape_from_stockbit_if_allowed().await {
        Ok(()) => println!("Auto GetLatestEmitenTrendingFromStockbit selesai."),
        Err(e) => log_auto_skip("GetLatestEmitenTrendingFromStockbit", &e),
    }
}
