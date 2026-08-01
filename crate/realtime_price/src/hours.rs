//! Jam/hari operasional realtime price (waktu server lokal).

use chrono::{Datelike, Local, NaiveDate, Timelike, Weekday};

/// Senin–Jumat (bukan Sab/Min).
pub fn is_weekday() -> bool {
    !matches!(Local::now().weekday(), Weekday::Sat | Weekday::Sun)
}

/// Senin–Jumat, 08:55–12:05 dan 13:25–16:05 (inclusive menit).
pub fn is_realtime_price_hours() -> bool {
    if !is_weekday() {
        return false;
    }
    let now = Local::now();
    let mins = now.hour() * 60 + now.minute();
    const MORNING_START: u32 = 8 * 60 + 55; // 08:55
    const MORNING_END: u32 = 12 * 60 + 5; // 12:05
    const AFTERNOON_START: u32 = 13 * 60 + 25; // 13:25
    const AFTERNOON_END: u32 = 16 * 60 + 5; // 16:05
    (mins >= MORNING_START && mins <= MORNING_END)
        || (mins >= AFTERNOON_START && mins <= AFTERNOON_END)
}

/// Mulai 09:10 (dan menit-jam berikutnya di hari yang sama) boleh deteksi libur via volume==0.
pub fn can_detect_holiday_by_volume() -> bool {
    if !is_weekday() {
        return false;
    }
    let now = Local::now();
    let mins = now.hour() * 60 + now.minute();
    const DETECT_FROM: u32 = 9 * 60 + 10; // 09:10
    mins >= DETECT_FROM
}

pub fn today_local() -> NaiveDate {
    Local::now().date_naive()
}
