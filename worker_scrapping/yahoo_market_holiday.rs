//! Re-export deteksi market libur (Sabtu/Minggu, tanggal di Scylla `invezgood.hari_libur`,
//! atau BBCA volume=0 setelah 10:00).

pub use market_holiday::{can_check_market_holiday, is_market_holiday};

/// Alias untuk poller Stockbit / Yahoo spike.
pub async fn is_poller_market_holiday() -> bool {
    is_market_holiday().await
}
