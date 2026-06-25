use std::env;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
struct ActionDef {
    id: &'static str,
    label: &'static str,
    description: &'static str,
    risk: &'static str,
}

const ACTIONS: &[ActionDef] = &[
    ActionDef { id: "refresh_market_data", label: "Refresh market data", description: "Request the data collector/daemon to refresh all cached market snapshots.", risk: "safe" },
    ActionDef { id: "generate_trade_plans", label: "Generate trade plans", description: "Request a dry-run optimizer pass to refresh trade_plans.json.", risk: "safe" },
    ActionDef { id: "rebalance_dry_run", label: "Rebalance dry-run", description: "Queue a portfolio rebalance proposal without live execution.", risk: "guarded" },
    ActionDef { id: "paper_smoke", label: "Paper smoke test", description: "Run the paper-trading smoke path and store the result in logs.", risk: "safe" },
    ActionDef { id: "pause_live_trading", label: "Pause live trading", description: "Set operator intent to pause live execution until manually resumed.", risk: "guarded" },
];

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn json_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 8);
    for ch in input.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn extract_string_field(body: &str, key: &str) -> Option<String> {
    let needle = format!("\"{}\"", key);
    let start = body.find(&needle)? + needle.len();
    let after_key = &body[start..];
    let colon = after_key.find(':')? + 1;
    let rest = after_key[colon..].trim_start();
    if !rest.starts_with('"') { return None; }
    let mut value = String::new();
    let mut escaped = false;
    for ch in rest[1..].chars() {
        if escaped {
            value.push(match ch { 'n' => '\n', 'r' => '\r', 't' => '\t', '"' => '"', '\\' => '\\', other => other });
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Some(value);
        } else {
            value.push(ch);
        }
    }
    None
}

fn action_exists(action_id: &str) -> bool {
    ACTIONS.iter().any(|a| a.id == action_id)
}

fn actions_json() -> String {
    let items = ACTIONS.iter().map(|a| format!(
        "{{\"id\":\"{}\",\"label\":\"{}\",\"description\":\"{}\",\"risk\":\"{}\"}}",
        json_escape(a.id), json_escape(a.label), json_escape(a.description), json_escape(a.risk)
    )).collect::<Vec<_>>().join(",");
    format!("[{}]", items)
}

fn read_queue(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| "[]".to_string())
}

fn append_action(path: &Path, action_id: &str, note: &str) -> std::io::Result<String> {
    if let Some(parent) = path.parent() { fs::create_dir_all(parent)?; }
    let id = format!("act-{}", now_unix());
    let entry = format!(
        "{{\"id\":\"{}\",\"action\":\"{}\",\"status\":\"queued\",\"source\":\"rust-operator-actions\",\"note\":\"{}\",\"created_at_unix\":{}}}",
        json_escape(&id), json_escape(action_id), json_escape(note), now_unix()
    );
    let mut existing = read_queue(path).trim().to_string();
    if existing.is_empty() || existing == "[]" {
        existing = format!("[\n  {}\n]\n", entry);
    } else if existing.ends_with(']') {
        existing.pop();
        existing = format!("{},\n  {}\n]\n", existing.trim_end(), entry);
    } else {
        existing = format!("[\n  {}\n]\n", entry);
    }
    let tmp = path.with_extension("json.tmp");
    {
        let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&tmp)?;
        file.write_all(existing.as_bytes())?;
        file.sync_all()?;
    }
    fs::rename(tmp, path)?;
    Ok(format!("{{\"ok\":true,\"id\":\"{}\",\"action\":\"{}\",\"status\":\"queued\"}}", json_escape(&id), json_escape(action_id)))
}

fn response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET,POST,OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type\r\nCache-Control: no-cache\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status, content_type, body.as_bytes().len(), body
    )
}

fn handle_request(req: &str, queue_path: &Path) -> String {
    let mut lines = req.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or("/").split('?').next().unwrap_or("/").trim_end_matches('/');

    if method == "OPTIONS" { return response("204 No Content", "application/json", ""); }
    if method == "GET" && (path.is_empty() || path == "/health") {
        return response("200 OK", "application/json", "{\"status\":\"ok\",\"service\":\"operator-actions\"}");
    }
    if method == "GET" && path == "/actions" {
        let queue = read_queue(queue_path);
        let body = format!("{{\"actions\":{},\"queue\":{},\"queue_path\":\"{}\"}}", actions_json(), queue.trim(), json_escape(&queue_path.display().to_string()));
        return response("200 OK", "application/json", &body);
    }
    if method == "POST" && path == "/actions/run" {
        let body = req.split("\r\n\r\n").nth(1).unwrap_or("{}");
        let action = extract_string_field(body, "action").unwrap_or_default();
        let note = extract_string_field(body, "note").unwrap_or_default();
        if !action_exists(&action) {
            return response("400 Bad Request", "application/json", "{\"ok\":false,\"error\":\"unknown action\"}");
        }
        return match append_action(queue_path, &action, &note) {
            Ok(payload) => response("200 OK", "application/json", &payload),
            Err(err) => response("500 Internal Server Error", "application/json", &format!("{{\"ok\":false,\"error\":\"{}\"}}", json_escape(&err.to_string()))),
        };
    }
    response("404 Not Found", "application/json", "{\"error\":\"not found\"}")
}

fn handle_stream(mut stream: TcpStream, queue_path: PathBuf) {
    let mut buf = vec![0; MAX_BODY_BYTES];
    let read = stream.read(&mut buf).unwrap_or(0);
    let req = String::from_utf8_lossy(&buf[..read]);
    let resp = handle_request(&req, &queue_path);
    let _ = stream.write_all(resp.as_bytes());
}

fn main() -> std::io::Result<()> {
    let bind = env::var("OPERATOR_ACTIONS_BIND").unwrap_or_else(|_| "0.0.0.0:8098".to_string());
    let queue_path = PathBuf::from(env::var("OPERATOR_ACTIONS_PATH").unwrap_or_else(|_| "data/operator-actions.json".to_string()));
    let listener = TcpListener::bind(&bind)?;
    eprintln!("operator-actions listening on {} queue={}", bind, queue_path.display());
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => handle_stream(stream, queue_path.clone()),
            Err(err) => eprintln!("accept error: {}", err),
        }
    }
    Ok(())
}
