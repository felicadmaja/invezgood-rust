//! Capture URL Fetch/XHR saat buka Analysis + klik 5 Years di Web Terminal (untuk menemukan
//! endpoint fundamental chart PE Band / PBV Band). Pakai Chrome login (reuse profil).
//! Jalankan: set -a && source .env && set +a && cargo run --bin stockbit_terminal_netcapture

use std::time::Duration;

use stockbit_browser::{
    acquire_browser_session, goto_stockbit, has_app_error, launch_page, BrowserLockClass,
};

async fn eval(page: &chromiumoxide::Page, js: &str) -> String {
    match tokio::time::timeout(Duration::from_secs(8), page.evaluate(js)).await {
        Ok(Ok(v)) => v.into_value::<String>().unwrap_or_default(),
        _ => String::new(),
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let code = std::env::var("STOCKBIT_SHOT_CODE").unwrap_or_else(|_| "ESSA".to_string());
    let url = format!(
        "https://terminal.stockbit.com/main?layout=watchlist&tabName={code}&symbol={code}"
    );

    let _guard = acquire_browser_session(BrowserLockClass::Interactive).await?;
    let (browser, page) = launch_page().await.map_err(|e| e.to_string())?;

    println!("Buka URL: {url}");
    goto_stockbit(&page, &url).await.map_err(|e| e.to_string())?;
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

    // Inject interceptor fetch + XHR (capture URL + method + prefix body).
    let patch = r#"(() => {
        window.__cap = window.__cap || [];
        if (window.__patched) return 'already';
        window.__patched = true;
        const of = window.fetch;
        window.fetch = function(...args) {
            let url = '';
            try { url = (args[0] && args[0].url) ? args[0].url : String(args[0]); } catch (e) {}
            const p = of.apply(this, args);
            p.then((res) => {
                try {
                    const clone = res.clone();
                    clone.text().then((t) => {
                        if (/fundamental|band|valuation|findata|chart/i.test(url) || /Fundamental chart/i.test(t)) {
                            window.__cap.push({ via: 'fetch', url: url, body: (t || '').slice(0, 120) });
                        }
                    });
                } catch (e) {}
            }).catch(() => {});
            return p;
        };
        const oopen = XMLHttpRequest.prototype.open;
        const osend = XMLHttpRequest.prototype.send;
        XMLHttpRequest.prototype.open = function(m, u) { this.__url = u; this.__m = m; return oopen.apply(this, arguments); };
        XMLHttpRequest.prototype.send = function() {
            const self = this;
            this.addEventListener('load', function() {
                try {
                    const t = self.responseText || '';
                    if (/fundamental|band|valuation|findata|chart/i.test(String(self.__url)) || /Fundamental chart/i.test(t)) {
                        window.__cap.push({ via: 'xhr', url: String(self.__url), body: t.slice(0, 120) });
                    }
                } catch (e) {}
            });
            return osend.apply(this, arguments);
        };
        return 'patched';
    })()"#;
    println!("Inject interceptor: {}", eval(&page, patch).await);

    // Klik Analysis.
    let click_js = |want: &str, all: bool| {
        format!(
            r#"(() => {{
                const want = {want:?};
                const clickAll = {all};
                const vis = (el) => {{ const r = el.getBoundingClientRect(); const s = getComputedStyle(el); return r.width>0&&r.height>0&&s.display!=='none'&&s.visibility!=='hidden'; }};
                const txt = (el) => (el.innerText || el.textContent || '').trim();
                let c = Array.from(document.querySelectorAll('.css-1ll1mc4,[class*="e2ln8a0"],[class*="e101i19e"],[role="tab"],button')).filter((el)=>vis(el)&&txt(el)===want);
                if (c.length===0) c = Array.from(document.querySelectorAll('div,span,a,button,li')).filter((el)=>vis(el)&&txt(el)===want);
                if (c.length===0) return 'not-found';
                const targets = clickAll ? c : [c[0]];
                targets.forEach((t)=>{{ t.scrollIntoView({{block:'center'}}); const r=t.getBoundingClientRect(); const o={{bubbles:true,cancelable:true,view:window,clientX:r.left+r.width/2,clientY:r.top+r.height/2}}; ['pointerdown','mousedown','pointerup','mouseup','click'].forEach((ty)=>{{try{{t.dispatchEvent(new MouseEvent(ty,o));}}catch(e){{}}}}); }});
                return 'clicked=' + targets.length;
            }})()"#
        )
    };
    println!("Klik Analysis: {}", eval(&page, &click_js("Analysis", false)).await);
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("Klik 5 Years: {}", eval(&page, &click_js("5 Years", true)).await);
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Baca hasil capture.
    let dump = eval(
        &page,
        r#"(() => JSON.stringify((window.__cap || []).map((x) => ({ via: x.via, url: x.url, body: x.body }))))()"#,
    )
    .await;
    println!("=== CAPTURED ===");
    println!("{dump}");

    browser.close().await;
    Ok(())
}
