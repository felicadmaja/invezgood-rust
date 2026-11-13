//! Buka URL Stockbit (default Web Terminal watchlist ESSA) pakai Chrome persistent yang sudah
//! login (reuse profil browser_data, tanpa reset), lalu screenshot.
//! URL override via env STOCKBIT_SHOT_URL. Jalankan:
//!   set -a && source .env && set +a && cargo run --bin stockbit_terminal_screenshot

use std::time::Duration;

use stockbit_browser::{
    acquire_browser_session, goto_stockbit, has_app_error, launch_page, save_debug_screenshot,
    BrowserLockClass,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let url = std::env::var("STOCKBIT_SHOT_URL").unwrap_or_else(|_| {
        "https://terminal.stockbit.com/main?layout=watchlist&tabName=ESSA&symbol=ESSA".to_string()
    });
    let label = std::env::var("STOCKBIT_SHOT_LABEL")
        .unwrap_or_else(|_| "terminal_essa".to_string());

    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;

    println!("Buka URL: {url}");
    goto_stockbit(&page, &url).await.map_err(|e| e.to_string())?;

    // Beri waktu SPA Web Terminal render; reload bila crash app-error.
    tokio::time::sleep(Duration::from_secs(6)).await;
    for i in 1..=3 {
        if has_app_error(&page).await {
            println!("App error — reload ({i}/3)...");
            let _ = goto_stockbit(&page, &url).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        } else {
            break;
        }
    }
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Opsional: urutan klik via STOCKBIT_SHOT_CLICK, dipisah ";;". Prefix "all:" = klik SEMUA
    // elemen bertext sama (mis. 2 tombol "5 Years"). Contoh: "Analysis;;all:5 Years".
    if let Ok(click_spec) = std::env::var("STOCKBIT_SHOT_CLICK") {
        for raw in click_spec.split(";;") {
            let step = raw.trim();
            if step.is_empty() {
                continue;
            }
            let (click_all, want) = match step.strip_prefix("all:") {
                Some(rest) => (true, rest.trim().to_string()),
                None => (false, step.to_string()),
            };
            let js = format!(
                r#"(() => {{
                    const want = {want:?};
                    const clickAll = {click_all};
                    const vis = (el) => {{
                        const r = el.getBoundingClientRect();
                        const s = getComputedStyle(el);
                        return r.width > 0 && r.height > 0 && s.display !== 'none' && s.visibility !== 'hidden';
                    }};
                    const txt = (el) => (el.innerText || el.textContent || '').trim();
                    let cands = Array.from(document.querySelectorAll('.css-1ll1mc4, [class*="e2ln8a0"], [class*="e101i19e"], [role="tab"], button'))
                        .filter((el) => vis(el) && txt(el) === want);
                    if (cands.length === 0) {{
                        cands = Array.from(document.querySelectorAll('div, span, a, button, [role="tab"], li'))
                            .filter((el) => vis(el) && txt(el) === want);
                    }}
                    if (cands.length === 0) return 'not-found';
                    const targets = clickAll ? cands : [cands[0]];
                    const doClick = (target) => {{
                        target.scrollIntoView({{block: 'center', inline: 'center'}});
                        const r = target.getBoundingClientRect();
                        const opts = {{bubbles: true, cancelable: true, view: window,
                            clientX: r.left + r.width / 2, clientY: r.top + r.height / 2}};
                        for (const type of ['pointerdown','mousedown','pointerup','mouseup','click']) {{
                            try {{ target.dispatchEvent(new MouseEvent(type, opts)); }} catch (e) {{}}
                        }}
                    }};
                    targets.forEach(doClick);
                    return 'clicked=' + targets.length + ' (match=' + cands.length + ')';
                }})()"#
            );
            match tokio::time::timeout(Duration::from_secs(8), page.evaluate(js)).await {
                Ok(Ok(v)) => println!(
                    "Klik '{want}'{}: {}",
                    if click_all { " [all]" } else { "" },
                    v.into_value::<String>().unwrap_or_default()
                ),
                _ => println!("Klik '{want}': gagal/timeout"),
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }

    let final_url = page.url().await.ok().flatten().unwrap_or_default();
    println!("URL akhir: {final_url}");

    match save_debug_screenshot(&page, &label).await {
        Some(p) => println!("Screenshot: {}", p.display()),
        None => println!("Screenshot gagal disimpan"),
    }

    browser.close().await;
    Ok(())
}
