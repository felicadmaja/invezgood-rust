//! Buat order limit buy via DOM scrape Stockbit (mode trading + form buy).

use std::time::{Duration, Instant};

use chromiumoxide::page::Page;
use stockbit_browser::{goto_stockbit, save_error_screenshot};
use tokio::time::sleep;

use crate::portofolio_worker::ensure_trading_session;

const STOCKBIT_ORDER_URL: &str = "https://stockbit.com/securities/order";
const STEP_TIMEOUT: Duration = Duration::from_secs(45);

/// `expiry_dom_value`: GFD → `"0"`, GTC → `"1"`.
pub async fn create_buy_limit_order(
    page: &Page,
    emiten_name: &str,
    price: i32,
    lot: i32,
    expiry_dom_value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let result = create_buy_limit_order_inner(page, emiten_name, price, lot, expiry_dom_value).await;
    if result.is_err() {
        let _ = save_error_screenshot(page, "buylimit_failed").await;
    }
    result
}

async fn create_buy_limit_order_inner(
    page: &Page,
    emiten_name: &str,
    price: i32,
    lot: i32,
    expiry_dom_value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "BuyLimit: ensure trading session → order form ({emiten_name} price={price} lot={lot} expiry={expiry_dom_value})..."
    );
    ensure_trading_session(page)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;

    goto_stockbit(page, STOCKBIT_ORDER_URL)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    sleep(Duration::from_millis(800)).await;

    wait_for_selector(page, "#rc_select_0", STEP_TIMEOUT).await?;
    fill_and_enter(page, "#rc_select_0", emiten_name).await?;
    println!("BuyLimit: emiten {emiten_name} diisi + Enter.");

    wait_for_selector(page, r#"[data-cy="company-buy-button"]"#, STEP_TIMEOUT).await?;
    click_selector(page, r#"[data-cy="company-buy-button"]"#).await?;
    println!("BuyLimit: company-buy-button diklik.");

    wait_for_selector(page, r#"[data-cy="input-buy-price"]"#, STEP_TIMEOUT).await?;
    fill_and_enter(page, r#"[data-cy="input-buy-price"]"#, &price.to_string()).await?;
    println!("BuyLimit: price={price} diisi + Enter.");

    wait_for_selector(page, r#"[data-cy="input-lot"]"#, STEP_TIMEOUT).await?;
    fill_input(page, r#"[data-cy="input-lot"]"#, &lot.to_string()).await?;
    println!("BuyLimit: lot={lot} diisi.");

    set_expiry_select(page, expiry_dom_value).await?;
    println!("BuyLimit: expiry select value={expiry_dom_value}.");

    wait_for_selector(page, r#"button[data-cy="button-buy"][type="submit"]"#, STEP_TIMEOUT)
        .await?;
    click_selector(page, r#"button[data-cy="button-buy"][type="submit"]"#).await?;
    println!("BuyLimit: button-buy diklik.");

    wait_for_paragraph_text(page, "Confirm", STEP_TIMEOUT).await?;
    click_outer_button_of_paragraph(page, "Confirm").await?;
    println!("BuyLimit: Confirm diklik.");

    wait_for_paragraph_text(page, "Done", STEP_TIMEOUT).await?;
    println!("BuyLimit: Done muncul — order berhasil.");
    Ok(())
}

async fn wait_for_selector(
    page: &Page,
    selector: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let sel_js = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".into());
    loop {
        let found = page
            .evaluate(format!(
                r#"(() => {{
                    const el = document.querySelector({sel_js});
                    if (!el) return false;
                    const style = window.getComputedStyle(el);
                    if (style.display === 'none' || style.visibility === 'hidden') return false;
                    return true;
                }})()"#
            ))
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if found {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(format!("Timeout menunggu elemen {selector}").into());
        }
        sleep(Duration::from_millis(300)).await;
    }
}

async fn fill_input(
    page: &Page,
    selector: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sel_js = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".into());
    let val_js = serde_json::to_string(value).unwrap_or_else(|_| "\"\"".into());
    let ok = page
        .evaluate(format!(
            r#"(() => {{
                const el = document.querySelector({sel_js});
                if (!el) return false;
                el.focus();
                if (typeof el.select === 'function') el.select();
                const proto = Object.getOwnPropertyDescriptor(
                    window.HTMLInputElement.prototype, 'value'
                );
                if (proto && proto.set) proto.set.call(el, '');
                else el.value = '';
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                if (proto && proto.set) proto.set.call(el, {val_js});
                else el.value = {val_js};
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                return true;
            }})()"#
        ))
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    if !ok {
        return Err(format!("Gagal mengisi {selector}").into());
    }
    Ok(())
}

async fn press_enter(
    page: &Page,
    selector: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sel_js = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".into());
    let ok = page
        .evaluate(format!(
            r#"(() => {{
                const el = document.querySelector({sel_js});
                if (!el) return false;
                el.focus();
                for (const type of ['keydown', 'keypress', 'keyup']) {{
                    el.dispatchEvent(new KeyboardEvent(type, {{
                        key: 'Enter', code: 'Enter', keyCode: 13, which: 13, bubbles: true
                    }}));
                }}
                return true;
            }})()"#
        ))
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    if !ok {
        return Err(format!("Gagal tekan Enter pada {selector}").into());
    }
    Ok(())
}

async fn fill_and_enter(
    page: &Page,
    selector: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    fill_input(page, selector, value).await?;
    sleep(Duration::from_millis(200)).await;
    press_enter(page, selector).await?;
    sleep(Duration::from_millis(400)).await;
    Ok(())
}

async fn click_selector(
    page: &Page,
    selector: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let sel_js = serde_json::to_string(selector).unwrap_or_else(|_| "\"\"".into());
    let ok = page
        .evaluate(format!(
            r#"(() => {{
                const el = document.querySelector({sel_js});
                if (!el) return false;
                el.click();
                return true;
            }})()"#
        ))
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    if !ok {
        return Err(format!("Gagal klik {selector}").into());
    }
    sleep(Duration::from_millis(300)).await;
    Ok(())
}

async fn set_expiry_select(
    page: &Page,
    value: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let val_js = serde_json::to_string(value).unwrap_or_else(|_| "\"\"".into());
    let ok = page
        .evaluate(format!(
            r#"(() => {{
                const el = document.querySelector('select[name="expiry"]');
                if (!el) return false;
                el.value = {val_js};
                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                return el.value === {val_js};
            }})()"#
        ))
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    if !ok {
        return Err("Gagal set select[name=expiry]".into());
    }
    Ok(())
}

async fn wait_for_paragraph_text(
    page: &Page,
    text: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let text_js = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".into());
    loop {
        let found = page
            .evaluate(format!(
                r#"(() => {{
                    const nodes = Array.from(document.querySelectorAll('p'));
                    return nodes.some((p) => (p.innerText || p.textContent || '').trim() === {text_js});
                }})()"#
            ))
            .await?
            .into_value::<bool>()
            .unwrap_or(false);
        if found {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            return Err(format!("Timeout menunggu <p>{text}</p>").into());
        }
        sleep(Duration::from_millis(300)).await;
    }
}

async fn click_outer_button_of_paragraph(
    page: &Page,
    text: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let text_js = serde_json::to_string(text).unwrap_or_else(|_| "\"\"".into());
    let ok = page
        .evaluate(format!(
            r#"(() => {{
                const nodes = Array.from(document.querySelectorAll('p'));
                const p = nodes.find((el) => (el.innerText || el.textContent || '').trim() === {text_js});
                if (!p) return false;
                const btn = p.closest('button') || p.parentElement?.closest('button');
                if (!btn) return false;
                btn.click();
                return true;
            }})()"#
        ))
        .await?
        .into_value::<bool>()
        .unwrap_or(false);
    if !ok {
        return Err(format!("Tombol outer <p>{text}</p> tidak ditemukan").into());
    }
    sleep(Duration::from_millis(400)).await;
    Ok(())
}
