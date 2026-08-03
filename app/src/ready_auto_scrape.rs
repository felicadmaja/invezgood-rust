//! Auto-scrape portofolio saat poller Stockbit ready.
//! Pending order tidak di-auto-scrape — hanya on-demand via RPC GetAllPendingOrderFromStockbit.

use std::sync::Arc;

use portofolio::PortofolioService;
use stockbit_browser::ReadinessPoller;
use tonic::{Code, Status};
use user::require_stockbit_scrape_hours;

fn log_auto_skip(rpc: &str, err: &Status) {
    if err.code() == Code::FailedPrecondition {
        eprintln!(
            "Readiness ready → skip auto {rpc} (jatah rate limit sudah terpakai): {}",
            err.message()
        );
    } else {
        eprintln!(
            "Readiness ready → skip/fail auto {rpc}: {}",
            err.message()
        );
    }
}

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

pub fn spawn_on_stockbit_ready(poller: Arc<ReadinessPoller>, portofolio: PortofolioService) {
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
                        eprintln!(
                            "Readiness poll_seq={poll_seq} ready → skip auto scrape (NEW_BUILD_LANGSUNG_SCRAPE=false)"
                        );
                        continue;
                    }
                }
                if let Err(e) = require_stockbit_scrape_hours() {
                    eprintln!(
                        "Readiness poll_seq={poll_seq} ready → skip auto Stockbit scrapes: {}",
                        e.message()
                    );
                } else {
                    eprintln!(
                        "Readiness poll_seq={poll_seq} ready → auto invoke portofolio..."
                    );
                    run_auto_scrapes(&portofolio).await;
                }
            }

            if rx.changed().await.is_err() {
                break;
            }
        }
    });
}

async fn run_auto_scrapes(portofolio: &PortofolioService) {
    match portofolio.scrape_from_stockbit_if_allowed_background().await {
        Ok((n, codes)) => eprintln!(
            "Auto GetAllPortofolioFromStockbit selesai: {n} baris ({} kode).",
            codes.len()
        ),
        Err(e) => log_auto_skip("GetAllPortofolioFromStockbit", &e),
    }
}
