//! Auto-scrape saat poller `IsStockbitReady` = ready.
//!
//! Urutan: GetAllPortofolioFromStockbit → GetPortofolioHistoryByEmitenNameFromStockbit
//! (batch holdings) → GetAllPendingOrderFromStockbit → GetLatestEmitenTrendingFromStockbit.
//! Jam 07–17; rate limit tiap alur sama RPC (kecuali history batch = gaya worker).

use std::sync::Arc;

use emiten_trending::EmitenTrendingService;
use pending_order::PendingOrderService;
use portofolio::PortofolioService;
use portofolio_history::PortofolioHistoryService;
use stockbit_browser::ReadinessPoller;
use user::require_stockbit_scrape_hours;

pub fn spawn_on_stockbit_ready(
    poller: Arc<ReadinessPoller>,
    portofolio: PortofolioService,
    portofolio_history: PortofolioHistoryService,
    pending_order: PendingOrderService,
    emiten_trending: EmitenTrendingService,
) {
    let mut rx = poller.subscribe();
    tokio::spawn(async move {
        loop {
            let ready = rx
                .borrow_and_update()
                .as_ref()
                .map(|u| u.ready)
                .unwrap_or(false);

            if ready {
                if let Err(e) = require_stockbit_scrape_hours() {
                    println!(
                        "Readiness ready → skip auto Stockbit scrapes: {}",
                        e.message()
                    );
                } else {
                    run_auto_scrapes(
                        &portofolio,
                        &portofolio_history,
                        &pending_order,
                        &emiten_trending,
                    )
                    .await;
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
    portofolio_history: &PortofolioHistoryService,
    pending_order: &PendingOrderService,
    emiten_trending: &EmitenTrendingService,
) {
    println!("Readiness ready → auto GetAllPortofolioFromStockbit...");
    let holding_codes = match portofolio.scrape_from_stockbit_if_allowed().await {
        Ok((n, codes)) => {
            println!("Auto GetAllPortofolioFromStockbit selesai: {n} baris ({} kode).", codes.len());
            codes
        }
        Err(e) => {
            println!(
                "Readiness ready → skip/fail GetAllPortofolioFromStockbit: {}",
                e.message()
            );
            match portofolio.list_holding_codes().await {
                Ok(codes) => codes,
                Err(e2) => {
                    eprintln!(
                        "Auto list holding codes gagal: {}",
                        e2.message()
                    );
                    Vec::new()
                }
            }
        }
    };

    if holding_codes.is_empty() {
        println!(
            "Readiness ready → skip auto GetPortofolioHistoryByEmitenNameFromStockbit (tidak ada holding)"
        );
    } else {
        println!(
            "Readiness ready → auto GetPortofolioHistoryByEmitenNameFromStockbit ({} holding)...",
            holding_codes.len()
        );
        match portofolio_history
            .scrape_holdings_from_stockbit(&holding_codes)
            .await
        {
            Ok(n) => println!(
                "Auto GetPortofolioHistoryByEmitenNameFromStockbit selesai: {n}/{} emiten.",
                holding_codes.len()
            ),
            Err(e) => eprintln!("Auto GetPortofolioHistoryByEmitenNameFromStockbit GAGAL: {e}"),
        }
    }

    println!("Readiness ready → auto GetAllPendingOrderFromStockbit...");
    match pending_order.scrape_from_stockbit_if_allowed().await {
        Ok(n) => println!("Auto GetAllPendingOrderFromStockbit selesai: {n} baris."),
        Err(e) => println!(
            "Readiness ready → skip/fail GetAllPendingOrderFromStockbit: {}",
            e.message()
        ),
    }

    println!("Readiness ready → auto GetLatestEmitenTrendingFromStockbit...");
    match emiten_trending.scrape_from_stockbit_if_allowed().await {
        Ok(()) => println!("Auto GetLatestEmitenTrendingFromStockbit selesai."),
        Err(e) => println!(
            "Readiness ready → skip/fail GetLatestEmitenTrendingFromStockbit: {}",
            e.message()
        ),
    }
}
