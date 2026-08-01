//! Jam/hari operasional realtime price (waktu server lokal).

use chrono::{Datelike, Local, Timelike, Weekday};

/// Senin–Jumat, 08:55–12:05 dan 13:25–16:05 (inclusive menit).
pub fn is_realtime_price_hours() -> bool {
    let now = Local::now();
    match now.weekday() {
        Weekday::Sat | Weekday::Sun => return false,
        _ => {}
    }
    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 8 * 60 + 55; // 08:55
    const MORNING_END: u32 = 12 * 60 + 5; // 12:05
    const AFTERNOON_START: u32 = 13 * 60 + 25; // 13:25
    const AFTERNOON_END: u32 = 16 * 60 + 5; // 16:05
    (mins >= MORNING_START && mins <= MORNING_END)
        || (mins >= AFTERNOON_START && mins <= AFTERNOON_END)
}
