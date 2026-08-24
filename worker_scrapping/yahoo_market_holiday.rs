//! Re-export deteksi market libur (Sabtu/Minggu, tanggal di Scylla `invezgood.hari_libur`,
//! atau BBCA volume=0 setelah 10:00 — dipakai poller Stockbit on-demand).

pub use market_holiday::{can_check_market_holiday, is_market_holiday, is_national_holiday, is_weekend};

/// Alias untuk poller Stockbit on-demand (termasuk cek Yahoo BBCA volume=0 setelah 10:00).
pub async fn is_poller_market_holiday() -> bool {
    is_market_holiday().await
}

/// Hari libur poller spike Yahoo & Invezgo: Sabtu/Minggu atau tanggal ada di `invezgood.hari_libur`.
pub async fn is_spike_poller_holiday() -> bool {
    if is_weekend() {
        return true;
    }
    is_national_holiday().await
}

/// Hari libur poller readiness Stockbit (`IsStockbitReady` scrape hook): alias ke cek `invezgood.hari_libur`.
pub async fn is_readiness_poller_holiday() -> bool {
    is_spike_poller_holiday().await
}
