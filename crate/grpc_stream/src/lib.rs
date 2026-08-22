//! Pola `tokio::sync::mpsc` + `ReceiverStream` untuk server streaming gRPC.
//!
//! Client disconnect → `ReceiverStream` drop → `rx` drop → `tx.send()` mengembalikan
//! `SendError` dan `tx.closed()` selesai. Hentikan loop/task segera agar resource dibersihkan.

use std::time::Duration;

use tokio::sync::mpsc;

/// Kirim item ke channel stream. `false` = receiver sudah drop (client disconnect).
pub async fn send_or_break<T>(tx: &mpsc::Sender<T>, item: T) -> bool {
    tx.send(item).await.is_ok()
}

/// Tunggu `duration` kecuali client disconnect lebih dulu. `false` = disconnect.
pub async fn sleep_unless_disconnected<T>(tx: &mpsc::Sender<T>, duration: Duration) -> bool {
    tokio::select! {
        biased;
        () = tx.closed() => false,
        () = tokio::time::sleep(duration) => true,
    }
}
