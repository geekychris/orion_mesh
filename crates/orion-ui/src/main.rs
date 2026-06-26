//! OrionMesh admin UI server.
//!
//! Phase 1 scope: serve a single HTML page that calls the controller's /v1/nodes
//! and renders the current node table. Designed so Dev Portal can iframe-embed it
//! by passing a `?asset=` query param (handled in a later phase).

use anyhow::Result;
use axum::{Router, response::Html, routing::get};
use clap::Parser;
use std::net::SocketAddr;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "orion-ui", version, about = "OrionMesh admin UI")]
struct Args {
    /// HTTP bind address.
    #[arg(long, env = "ORION_UI_BIND", default_value = "127.0.0.1:7879")]
    bind: SocketAddr,

    /// Controller URL the page polls in the browser.
    #[arg(long, env = "ORION_CONTROLLER_URL", default_value = "http://127.0.0.1:7878")]
    controller: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!(bind = %args.bind, controller = %args.controller, "orion-ui starting");

    let controller_url = args.controller.clone();
    let router = Router::new().route(
        "/",
        get(move || {
            let url = controller_url.clone();
            async move { Html(index_html(&url)) }
        }),
    );

    let listener = tokio::net::TcpListener::bind(args.bind).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

fn index_html(controller_url: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>OrionMesh</title>
  <style>
    :root {{ --fg: #111; --muted: #666; --line: #ddd; --bg: #fafafa; --accent: #0b6e3d; --err: #b00020; }}
    body {{ font-family: -apple-system, system-ui, sans-serif; margin: 2rem; color: var(--fg); }}
    h1 {{ margin: 0 0 .25rem; }}
    .subtitle {{ color: var(--muted); margin-bottom: 1.5rem; font-size: .9rem; }}
    .subtitle code {{ background: var(--bg); padding: .1rem .35rem; border-radius: 3px; }}
    .grid {{ display: grid; grid-template-columns: 2fr 1fr; gap: 2rem; }}
    h2 {{ margin: 0 0 .75rem; font-size: 1.1rem; }}
    .meta {{ color: var(--muted); font-size: .85rem; margin-left: .5rem; }}
    table {{ border-collapse: collapse; width: 100%; font-size: .9rem; }}
    th, td {{ border: 1px solid var(--line); padding: .4rem .7rem; text-align: left; vertical-align: top; }}
    th {{ background: var(--bg); font-weight: 600; }}
    .runtimes {{ display: inline-block; background: #eef; padding: .05rem .4rem; border-radius: 3px; margin-right: .3rem; font-size: .75rem; }}
    .err {{ color: var(--err); padding: .5rem; border-left: 3px solid var(--err); background: #fff5f5; margin-bottom: 1rem; display: none; }}
    .err.show {{ display: block; }}
    .dim {{ color: var(--muted); }}
    .pill {{ display: inline-block; padding: .05rem .4rem; border-radius: 3px; background: var(--bg); font-size: .75rem; }}
    .pill.live {{ background: #e8f6ee; color: var(--accent); }}
    footer {{ margin-top: 2rem; color: var(--muted); font-size: .8rem; }}
  </style>
</head>
<body>
  <h1>OrionMesh <span class="pill live" id="status">connecting…</span></h1>
  <div class="subtitle">Controller <code>{controller_url}</code> · refreshes every 3 s</div>

  <div id="err" class="err"></div>

  <div class="grid">
    <section>
      <h2>Nodes <span class="meta" id="nodes-meta"></span></h2>
      <table id="nodes">
        <thead><tr>
          <th>Node</th><th>Arch / OS</th><th>CPU / Mem</th><th>Runtimes</th><th>Last seen</th>
        </tr></thead>
        <tbody><tr><td colspan="5" class="dim">connecting to controller…</td></tr></tbody>
      </table>
    </section>

    <section>
      <h2>Resources</h2>
      <table id="resources">
        <thead><tr><th>Kind</th><th class="dim">Count</th></tr></thead>
        <tbody><tr><td colspan="2" class="dim">loading…</td></tr></tbody>
      </table>
    </section>
  </div>

  <footer>
    OrionMesh Phase 1 substrate · <code>orion-ui</code> · talks to <code>{controller_url}</code>
  </footer>

  <script>
    const KINDS = ['Service','Task','Job','Schedule','Dataset','Model','Project','Secret','Volume','Network','Runtime','Capability','Policy','Integration'];

    function fmtBytes(b) {{
      if (!b) return '–';
      const u = ['B','KB','MB','GB','TB']; let i = 0; let n = b;
      while (n >= 1024 && i < u.length-1) {{ n /= 1024; i++; }}
      return `${{n.toFixed(n < 10 ? 1 : 0)}} ${{u[i]}}`;
    }}

    function showErr(msg) {{
      const e = document.querySelector('#err');
      e.textContent = msg;
      e.classList.add('show');
      document.querySelector('#status').textContent = 'offline';
      document.querySelector('#status').style.background = '#fde2e2';
      document.querySelector('#status').style.color = 'var(--err)';
    }}
    function clearErr() {{
      const e = document.querySelector('#err');
      e.classList.remove('show');
      const s = document.querySelector('#status');
      s.textContent = 'live';
    }}

    async function refreshNodes() {{
      const r = await fetch('{controller_url}/v1/nodes');
      if (!r.ok) throw new Error(`/v1/nodes: ${{r.status}} ${{r.statusText}}`);
      const rows = await r.json();
      document.querySelector('#nodes-meta').textContent = rows.length ? `${{rows.length}} node${{rows.length===1?'':'s'}}` : '';
      const tbody = document.querySelector('#nodes tbody');
      if (!rows.length) {{ tbody.innerHTML = '<tr><td colspan="5" class="dim">no agents reporting yet</td></tr>'; return; }}
      tbody.innerHTML = rows.map(n => {{
        const inv = n.inventory || {{}};
        const arch_os = inv.arch ? `${{inv.arch}} · ${{inv.os}}` : '<span class="dim">–</span>';
        const cpu_mem = inv.cpu_cores
          ? `${{inv.cpu_cores}} cores · ${{fmtBytes(inv.mem_total_bytes)}}`
          : '<span class="dim">–</span>';
        const runtimes = (inv.runtimes || []).map(rn => `<span class="runtimes">${{rn}}</span>`).join('') || '<span class="dim">–</span>';
        const seen = new Date(n.last_seen_at).toLocaleTimeString();
        return `<tr>
          <td><strong>${{n.node_id}}</strong><br><span class="dim">v${{n.agent_version}}</span></td>
          <td>${{arch_os}}</td>
          <td>${{cpu_mem}}</td>
          <td>${{runtimes}}</td>
          <td>${{seen}}</td>
        </tr>`;
      }}).join('');
    }}

    async function refreshResources() {{
      const counts = await Promise.all(KINDS.map(async k => {{
        try {{
          const r = await fetch(`{controller_url}/v1/resources/${{k}}`);
          if (!r.ok) return [k, '?'];
          const rows = await r.json();
          return [k, rows.length];
        }} catch (_) {{ return [k, '?']; }}
      }}));
      const filtered = counts.filter(([_, n]) => n !== 0 && n !== '?');
      const tbody = document.querySelector('#resources tbody');
      if (!filtered.length) {{
        tbody.innerHTML = '<tr><td colspan="2" class="dim">no resources applied yet</td></tr>';
      }} else {{
        tbody.innerHTML = filtered.map(([k, n]) => `<tr><td>${{k}}</td><td>${{n}}</td></tr>`).join('');
      }}
    }}

    async function refresh() {{
      try {{
        await Promise.all([refreshNodes(), refreshResources()]);
        clearErr();
      }} catch (e) {{
        showErr(String(e));
      }}
    }}

    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>"#
    )
}
