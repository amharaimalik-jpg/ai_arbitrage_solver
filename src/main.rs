// src/main.rs - All-in-One Sovereign Node (Min Payment + /info + Instant Client Portal on / + Strict RPC + Disk Persistence)
use std::collections::HashSet;
use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const KEYS_FILE: &str = "sovereign_active_keys.json";
const USED_TX_FILE: &str = "sovereign_used_txs.json";

#[derive(Serialize, Deserialize, Clone)]
struct IntelligencePayload {
    node_id: String,
    request_id: u64,
    source_feed: String,
    live_metric: String,
    status: String,
    timestamp: String,
    cryptographic_hash: String,
}

#[derive(Deserialize)]
struct BuyRequest {
    tx_hash: String,
    client_wallet: String,
}

#[derive(Serialize)]
struct BuyResponse {
    status: String,
    api_key: String,
    message: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

#[derive(Deserialize, Debug)]
struct RpcResponse {
    result: Option<TransactionDetails>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Debug)]
struct TransactionDetails {
    to: Option<String>,
    value: Option<String>,
    blockNumber: Option<String>,
}

#[derive(Clone)]
struct AppState {
    node_id: String,
    merchant_rabby_wallet: String,
    rpc_endpoint: String,
    min_payment_wei: u128,
    processed_requests: Arc<AtomicU64>,
    active_keys: Arc<Mutex<HashSet<String>>>,
    used_txs: Arc<Mutex<HashSet<String>>>,
    latest_payload: Arc<RwLock<IntelligencePayload>>,
    http_client: reqwest::Client,
}

fn load_set_from_disk(path: &str) -> HashSet<String> {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
        .map(|items| items.into_iter().collect())
        .unwrap_or_default()
}

fn save_set_to_disk(path: &str, set: &HashSet<String>) {
    let vec: Vec<String> = set.iter().cloned().collect();
    if let Ok(json_str) = serde_json::to_string_pretty(&vec) {
        let _ = fs::write(path, json_str);
    }
}

fn parse_hex_u128(hex_str: &str) -> u128 {
    let clean = hex_str
        .trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X");
    if clean.is_empty() {
        return 0;
    }
    u128::from_str_radix(clean, 16).unwrap_or(0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("🚀 Booting All-in-One Sovereign Node...");

    let merchant_rabby_wallet = std::env::var("MERCHANT_RABBY_WALLET").unwrap_or_else(|_| {
        "0x78f4acb57f5aa14768018caf1fc9106ac56e52f4f92744bee5dc7c98f428dcbf".to_string()
    });

    let node_id = std::env::var("NODE_ID")
        .unwrap_or_else(|_| "MALIK_SOVEREIGN_ORACLE_01".to_string());

    let rpc_endpoint = std::env::var("RPC_ENDPOINT")
        .unwrap_or_else(|_| "https://arb1.arbitrum.io/rpc".to_string());

    let min_payment_wei: u128 = std::env::var("MIN_PAYMENT_WEI")
        .ok()
        .and_then(|v| v.parse::<u128>().ok())
        .unwrap_or(100_000_000_000_000); // 0.0001 ETH default

    let initial_key = std::env::var("MASTER_API_KEY")
        .unwrap_or_else(|_| "MALIK_MASTER_PROD_KEY_2026".to_string());

    info!("💎 Treasury Locked to Wallet: {}", merchant_rabby_wallet);
    info!("🔗 RPC Endpoint Active: {}", rpc_endpoint);
    info!("💰 Minimum Payment (WEI): {}", min_payment_wei);

    let processed_requests = Arc::new(AtomicU64::new(0));

    let mut keys_set = load_set_from_disk(KEYS_FILE);
    keys_set.insert(initial_key);
    save_set_to_disk(KEYS_FILE, &keys_set);
    let active_keys = Arc::new(Mutex::new(keys_set));

    let txs_set = load_set_from_disk(USED_TX_FILE);
    let used_txs = Arc::new(Mutex::new(txs_set));

    let initial_payload = IntelligencePayload {
        node_id: node_id.clone(),
        request_id: 0,
        source_feed: "multi-feed-btc-oracle".to_string(),
        live_metric: "INITIALIZING_FEED".to_string(),
        status: "INITIALIZING".to_string(),
        timestamp: Utc::now().to_rfc3339(),
        cryptographic_hash: "PENDING".to_string(),
    };

    let latest_payload = Arc::new(RwLock::new(initial_payload));
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .build()?;

    let state = AppState {
        node_id,
        merchant_rabby_wallet,
        rpc_endpoint,
        min_payment_wei,
        processed_requests,
        active_keys,
        used_txs,
        latest_payload,
        http_client,
    };

    spawn_oracle_worker(state.clone());

    let app = Router::new()
        .route("/", get(portal_handler))
        .route("/info", get(info_handler))
        .route("/health", get(health_handler))
        .route("/intel", get(intel_handler))
        .route("/buy", post(buy_handler))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse()?;

    info!("🌐 Sovereign Production Node Live on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn fetch_btc_metric(client: &reqwest::Client) -> (String, String) {
    // 1. Coinbase Spot Price
    if let Ok(res) = client
        .get("https://api.coinbase.com/v2/prices/BTC-USD/spot")
        .header("User-Agent", "Mozilla/5.0 SovereignOracle/4.0")
        .send()
        .await
    {
        if let Ok(json_val) = res.json::<serde_json::Value>().await {
            if let Some(price_str) = json_val["data"]["amount"].as_str() {
                return (
                    "https://api.coinbase.com/v2/prices/BTC-USD/spot".to_string(),
                    format!("BTC_USD:{}", price_str),
                );
            }
        }
    }

    // 2. Kraken Ticker
    if let Ok(res) = client
        .get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD")
        .header("User-Agent", "Mozilla/5.0 SovereignOracle/4.0")
        .send()
        .await
    {
        if let Ok(json_val) = res.json::<serde_json::Value>().await {
            if let Some(price_str) = json_val["result"]["XXBTZUSD"]["c"][0].as_str() {
                return (
                    "https://api.kraken.com/0/public/Ticker?pair=XBTUSD".to_string(),
                    format!("BTC_USD:{}", price_str),
                );
            }
        }
    }

    // 3. CryptoCompare Price
    if let Ok(res) = client
        .get("https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD")
        .header("User-Agent", "Mozilla/5.0 SovereignOracle/4.0")
        .send()
        .await
    {
        if let Ok(json_val) = res.json::<serde_json::Value>().await {
            if let Some(price) = json_val["USD"].as_f64() {
                return (
                    "https://min-api.cryptocompare.com/data/price".to_string(),
                    format!("BTC_USD:{:.2}", price),
                );
            }
        }
    }

    // 4. Binance Ticker
    if let Ok(res) = client
        .get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
        .header("User-Agent", "Mozilla/5.0 SovereignOracle/4.0")
        .send()
        .await
    {
        if let Ok(json_val) = res.json::<serde_json::Value>().await {
            if let Some(price_str) = json_val["price"].as_str() {
                return (
                    "https://api.binance.com/api/v3/ticker/price".to_string(),
                    format!("BTC_USDT:{}", price_str),
                );
            }
        }
    }

    (
        "multi-feed-btc-oracle".to_string(),
        "FEED_UNAVAILABLE_RETRYING".to_string(),
    )
}

fn spawn_oracle_worker(state: AppState) {
    tokio::spawn(async move {
        loop {
            let timestamp = Utc::now().to_rfc3339();
            let current_total = state.processed_requests.fetch_add(1, Ordering::SeqCst) + 1;

            let (source_feed, metric_value) = fetch_btc_metric(&state.http_client).await;

            let raw_data = format!(
                "{}-{}-{}-{}",
                state.node_id, current_total, metric_value, timestamp
            );
            let mut hasher = Sha256::new();
            hasher.update(raw_data.as_bytes());
            let result_hash = format!("{:x}", hasher.finalize());

            let ledger_entry = format!(
                "TIMESTAMP: {} | ID: {} | SOURCE: {} | METRIC: {} | HASH: {}\n",
                timestamp, current_total, source_feed, metric_value, result_hash
            );
            if let Ok(mut file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open("sovereign_production_ledger.log")
            {
                let _ = file.write_all(ledger_entry.as_bytes());
            }

            {
                let mut payload_guard = state.latest_payload.write().await;
                *payload_guard = IntelligencePayload {
                    node_id: state.node_id.clone(),
                    request_id: current_total,
                    source_feed,
                    live_metric: metric_value,
                    status: "PRODUCTION_ACTIVE".to_string(),
                    timestamp,
                    cryptographic_hash: result_hash,
                };
            }

            sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn info_handler(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let min_eth = (state.min_payment_wei as f64) / 1e18;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "node_id": state.node_id,
            "payment_wallet": state.merchant_rabby_wallet,
            "min_payment_wei": state.min_payment_wei.to_string(),
            "min_payment_eth_approx": format!("{:.6}", min_eth),
            "endpoints": {
                "portal": "GET /",
                "info": "GET /info",
                "health": "GET /health",
                "buy": "POST /buy (body: {\"tx_hash\":\"0x...\",\"client_wallet\":\"0x...\"})",
                "intel": "GET /intel (header: x-api-key: MALIK_PROD_...)"
            }
        })),
    )
}

async fn portal_handler(State(state): State<AppState>) -> Html<String> {
    let min_eth = (state.min_payment_wei as f64) / 1e18;
    let html = format!(
        r##"<!doctype html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Malik Sovereign Oracle — بوابة الدفع والبيانات الحية</title>
  <style>
    body {{ font-family: system-ui, -apple-system, sans-serif; background:#0b0f17; color:#e6edf3; margin:0; padding:24px; }}
    .wrap {{ max-width:760px; margin:0 auto; background:#131a26; border:1px solid #263248; border-radius:14px; padding:24px; }}
    h1 {{ margin:0 0 8px; font-size:22px; color:#58a6ff; }}
    .sub {{ color:#9fb0c7; margin:0 0 20px; font-size:14px; }}
    .box {{ background:#0d131d; border:1px solid #222e42; border-radius:10px; padding:14px; margin-bottom:16px; }}
    .label {{ font-size:12px; color:#8b9bb4; margin-bottom:6px; }}
    .mono {{ font-family: monospace; word-break: break-all; font-size:13px; color:#7ee787; }}
    input {{ width:100%; box-sizing:border-box; padding:10px 12px; border-radius:8px; border:1px solid #2b3a55; background:#090d14; color:#fff; margin-bottom:10px; }}
    button {{ background:#238636; color:#fff; border:0; padding:10px 16px; border-radius:8px; cursor:pointer; font-weight:600; }}
    button.sec {{ background:#1f6feb; margin-right:8px; }}
    pre {{ background:#090d14; border:1px solid #222e42; padding:12px; border-radius:8px; overflow:auto; font-size:12px; direction:ltr; text-align:left; }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>⚡ {node_id}</h1>
    <p class="sub">ادفع على الشبكة المعتمدة ثم فعّل مفتاح API واحصل على البيانات الموقّعة فوراً.</p>

    <div class="box">
      <div class="label">عنوان المحفظة المستقبلة للدفع</div>
      <div class="mono" id="walletAddr">{wallet}</div>
      <div class="label" style="margin-top:10px;">الحد الأدنى للدفع</div>
      <div class="mono">{min_wei} wei (~{min_eth:.6} ETH)</div>
    </div>

    <div class="box">
      <div class="label">1) شراء وتفعيل مفتاح API عبر POST /buy</div>
      <input id="txHash" placeholder="tx_hash (0x... 66 chars)" />
      <input id="clientWallet" placeholder="client_wallet (0x...)" />
      <button onclick="buyKey()">تأكيد الدفع وإصدار المفتاح</button>
    </div>

    <div class="box">
      <div class="label">2) قراءة البيانات الموقّعة عبر GET /intel</div>
      <input id="apiKey" placeholder="x-api-key (MALIK_PROD_...)" />
      <button class="sec" onclick="fetchIntel()">جلب /intel</button>
      <button onclick="fetchHealth()">فحص /health</button>
    </div>

    <pre id="out">جاهز...</pre>
  </div>

  <script>
    const out = document.getElementById('out');
    async function buyKey() {{
      out.textContent = 'جاري التحقق من المعاملة...';
      const body = {{
        tx_hash: document.getElementById('txHash').value.trim(),
        client_wallet: document.getElementById('clientWallet').value.trim()
      }};
      const res = await fetch('/buy', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body)
      }});
      const data = await res.json();
      if (data.api_key) document.getElementById('apiKey').value = data.api_key;
      out.textContent = JSON.stringify(data, null, 2);
    }}
    async function fetchIntel() {{
      out.textContent = 'جاري جلب /intel...';
      const key = document.getElementById('apiKey').value.trim();
      const res = await fetch('/intel', {{ headers: {{ 'x-api-key': key }} }});
      out.textContent = JSON.stringify(await res.json(), null, 2);
    }}
    async function fetchHealth() {{
      const res = await fetch('/health');
      out.textContent = JSON.stringify(await res.json(), null, 2);
    }}
  </script>
</body>
</html>"##,
        node_id = state.node_id,
        wallet = state.merchant_rabby_wallet,
        min_wei = state.min_payment_wei,
        min_eth = min_eth
    );
    Html(html)
}

async fn health_handler(State(state): State<AppState>) -> (StatusCode, Json<serde_json::Value>) {
    let payload = state.latest_payload.read().await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "ONLINE",
            "node_id": state.node_id,
            "requests_processed": state.processed_requests.load(Ordering::Relaxed),
            "live_metric": payload.live_metric,
            "last_update": payload.timestamp
        })),
    )
}

async fn intel_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> (StatusCode, Json<serde_json::Value>) {
    let client_key = headers
        .get("x-api-key")
        .and_then(|val| val.to_str().ok())
        .unwrap_or("");

    let is_authorized = {
        let keys = state.active_keys.lock().await;
        keys.contains(client_key)
    };

    if !is_authorized {
        let err_res = ErrorResponse {
            error: "UNAUTHORIZED_ACCESS".to_string(),
            message: "Valid API Key required. Send payment to Malik's Wallet and post to /buy."
                .to_string(),
        };
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::to_value(err_res).unwrap()),
        );
    }

    let data = state.latest_payload.read().await.clone();
    (StatusCode::OK, Json(serde_json::to_value(data).unwrap()))
}

async fn buy_handler(
    State(state): State<AppState>,
    Json(payload): Json<BuyRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let timestamp = Utc::now().to_rfc3339();
    let tx_normalized = payload.tx_hash.trim().to_lowercase();

    if !tx_normalized.starts_with("0x") || tx_normalized.len() != 66 {
        let err_res = ErrorResponse {
            error: "INVALID_TX_HASH_FORMAT".to_string(),
            message: "Transaction hash must be a valid 66-character 0x-prefixed hash.".to_string(),
        };
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::to_value(err_res).unwrap()),
        );
    }

    {
        let used = state.used_txs.lock().await;
        if used.contains(&tx_normalized) {
            let err_res = ErrorResponse {
                error: "TX_HASH_ALREADY_USED".to_string(),
                message: "This transaction hash has already been redeemed for an API key."
                    .to_string(),
            };
            return (
                StatusCode::CONFLICT,
                Json(serde_json::to_value(err_res).unwrap()),
            );
        }
    }

    info!(
        "🔍 STRICT ON-CHAIN VERIFICATION | Tx: {} | Client: {}",
        tx_normalized, payload.client_wallet
    );

    let rpc_payload = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getTransactionByHash",
        "params": [tx_normalized],
        "id": 1
    });

    let rpc_res = match state
        .http_client
        .post(&state.rpc_endpoint)
        .json(&rpc_payload)
        .send()
        .await
    {
        Ok(res) => res,
        Err(e) => {
            warn!("⚠️ RPC Network Error: {}", e);
            let err_res = ErrorResponse {
                error: "RPC_UNAVAILABLE".to_string(),
                message: "Could not reach blockchain RPC endpoint to verify transaction."
                    .to_string(),
            };
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::to_value(err_res).unwrap()),
            );
        }
    };

    let rpc_json = match rpc_res.json::<RpcResponse>().await {
        Ok(parsed) => parsed,
        Err(e) => {
            warn!("⚠️ RPC Parse Error: {}", e);
            let err_res = ErrorResponse {
                error: "RPC_INVALID_RESPONSE".to_string(),
                message: "Invalid response returned by blockchain RPC node.".to_string(),
            };
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::to_value(err_res).unwrap()),
            );
        }
    };

    let tx = match rpc_json.result {
        Some(t) => t,
        None => {
            let err_res = ErrorResponse {
                error: "PAYMENT_NOT_FOUND_ONCHAIN".to_string(),
                message: "Transaction hash not found on the configured blockchain network."
                    .to_string(),
            };
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::to_value(err_res).unwrap()),
            );
        }
    };

    let to_matches = tx
        .to
        .as_deref()
        .map(|addr| addr.eq_ignore_ascii_case(&state.merchant_rabby_wallet))
        .unwrap_or(false);

    let value_wei = tx.value.as_deref().map(parse_hex_u128).unwrap_or(0);

    let is_mined = tx
        .blockNumber
        .as_deref()
        .map(|bn| !bn.trim().is_empty() && bn.trim() != "0x0")
        .unwrap_or(false);

    if !to_matches || value_wei < state.min_payment_wei || value_wei == 0 || !is_mined {
        let err_res = ErrorResponse {
            error: "PAYMENT_VERIFICATION_FAILED".to_string(),
            message: format!(
                "Transaction must be mined on-chain, sent to merchant wallet, and have value >= {} wei.",
                state.min_payment_wei
            ),
        };
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::to_value(err_res).unwrap()),
        );
    }

    let mut hasher = Sha256::new();
    hasher.update(tx_normalized.as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    let production_key = format!("MALIK_PROD_{}", &digest[..24.min(digest.len())]);

    {
        let mut keys = state.active_keys.lock().await;
        keys.insert(production_key.clone());
        save_set_to_disk(KEYS_FILE, &keys);
    }

    {
        let mut used = state.used_txs.lock().await;
        used.insert(tx_normalized.clone());
        save_set_to_disk(USED_TX_FILE, &used);
    }

    let sale_record = format!(
        "ONCHAIN_SALE | Time: {} | Tx: {} | ValueWei: {} | ClientWallet: {} | IssuedKey: {}\n",
        timestamp, tx_normalized, value_wei, payload.client_wallet, production_key
    );
    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("sovereign_sales_ledger.log")
    {
        let _ = file.write_all(sale_record.as_bytes());
    }

    info!(
        "💰 VERIFIED ON-CHAIN PAYMENT | Wei: {} | Key Issued",
        value_wei
    );

    let res = BuyResponse {
        status: "PAYMENT_VERIFIED_ONCHAIN_AND_SAVED".to_string(),
        api_key: production_key,
        message: "Verified on-chain payment! Welcome to Malik's Sovereign Intelligence Network."
            .to_string(),
    };

    (StatusCode::OK, Json(serde_json::to_value(res).unwrap()))
}