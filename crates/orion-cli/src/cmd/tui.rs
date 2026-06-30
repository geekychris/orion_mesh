//! `orion tui` — full-screen terminal dashboard.
//!
//! Polls the controller every few seconds, renders nodes / services /
//! queues / recent log line counts. Keys: `q` to quit, `r` to refresh now,
//! arrow keys / `1-4` to switch tabs.

use crate::{Ctx, http};
use anyhow::Result;
use clap::Args as ClapArgs;
use crossterm::event::{self, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, Tabs};
use ratatui::Terminal;
use serde_json::Value;
use std::io::stdout;
use std::time::{Duration, Instant};

#[derive(ClapArgs, Debug)]
pub struct Args {
    #[arg(long, default_value_t = 3)]
    pub refresh_seconds: u64,
}

#[derive(Default, Clone)]
struct Snapshot {
    nodes: Value,
    services: Value,
    queues: Value,
    instances: Value,
    diag_system: Value,
    last_refresh: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Clone, Copy)]
enum Tab {
    Overview,
    Nodes,
    Services,
    Queues,
}

impl Tab {
    fn next(self) -> Self {
        match self {
            Tab::Overview => Tab::Nodes,
            Tab::Nodes => Tab::Services,
            Tab::Services => Tab::Queues,
            Tab::Queues => Tab::Overview,
        }
    }
    fn prev(self) -> Self {
        match self {
            Tab::Overview => Tab::Queues,
            Tab::Nodes => Tab::Overview,
            Tab::Services => Tab::Nodes,
            Tab::Queues => Tab::Services,
        }
    }
    fn label(&self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::Nodes => "Nodes",
            Tab::Services => "Services",
            Tab::Queues => "Queues",
        }
    }
}

pub async fn run(ctx: &Ctx, args: Args) -> Result<()> {
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend)?;
    let result = run_loop(ctx, args, &mut terminal).await;
    disable_raw_mode().ok();
    execute!(stdout(), LeaveAlternateScreen).ok();
    result
}

async fn run_loop(
    ctx: &Ctx,
    args: Args,
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
) -> Result<()> {
    let mut snap = Snapshot::default();
    let mut tab = Tab::Overview;
    let mut last_fetch = Instant::now() - Duration::from_secs(args.refresh_seconds + 1);

    loop {
        if last_fetch.elapsed() >= Duration::from_secs(args.refresh_seconds) {
            snap = fetch(ctx).await;
            last_fetch = Instant::now();
        }
        terminal.draw(|f| draw(f, &snap, tab))?;
        if event::poll(Duration::from_millis(200))? {
            if let Event::Key(k) = event::read()? {
                match k.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char('r') => {
                        last_fetch = Instant::now() - Duration::from_secs(args.refresh_seconds + 1)
                    }
                    KeyCode::Right | KeyCode::Tab => tab = tab.next(),
                    KeyCode::Left | KeyCode::BackTab => tab = tab.prev(),
                    KeyCode::Char('1') => tab = Tab::Overview,
                    KeyCode::Char('2') => tab = Tab::Nodes,
                    KeyCode::Char('3') => tab = Tab::Services,
                    KeyCode::Char('4') => tab = Tab::Queues,
                    _ => {}
                }
            }
        }
    }
}

async fn fetch(ctx: &Ctx) -> Snapshot {
    let nodes = http::get_json::<Value>(ctx, "/v1/nodes").await.unwrap_or(Value::Null);
    let services = http::get_json::<Value>(ctx, "/v1/resources/Service").await.unwrap_or(Value::Null);
    let queues = http::get_json::<Value>(ctx, "/v1/resources/Queue").await.unwrap_or(Value::Null);
    let instances = http::get_json::<Value>(ctx, "/v1/instances").await.unwrap_or(Value::Null);
    let diag_system = http::get_json::<Value>(ctx, "/v1/diag/system").await.unwrap_or(Value::Null);
    Snapshot {
        nodes,
        services,
        queues,
        instances,
        diag_system,
        last_refresh: Some(chrono::Utc::now()),
    }
}

fn draw(f: &mut ratatui::Frame, snap: &Snapshot, tab: Tab) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // tabs
            Constraint::Min(0),    // body
            Constraint::Length(1), // status
        ])
        .split(f.area());

    let tab_titles: Vec<Line> = [Tab::Overview, Tab::Nodes, Tab::Services, Tab::Queues]
        .iter()
        .map(|t| Line::from(t.label()))
        .collect();
    let active = match tab {
        Tab::Overview => 0,
        Tab::Nodes => 1,
        Tab::Services => 2,
        Tab::Queues => 3,
    };
    let tabs_widget = Tabs::new(tab_titles)
        .block(Block::default().borders(Borders::BOTTOM).title("OrionMesh"))
        .select(active)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan));
    f.render_widget(tabs_widget, chunks[0]);

    match tab {
        Tab::Overview => draw_overview(f, chunks[1], snap),
        Tab::Nodes => draw_nodes(f, chunks[1], snap),
        Tab::Services => draw_services(f, chunks[1], snap),
        Tab::Queues => draw_queues(f, chunks[1], snap),
    }

    let status = format!(
        " refresh: {}    keys: q quit · r refresh · ←→ tabs · 1-4 jump ",
        snap.last_refresh
            .map(|t| t.format("%H:%M:%S").to_string())
            .unwrap_or_else(|| "—".into()),
    );
    f.render_widget(
        Paragraph::new(status).style(Style::default().fg(Color::DarkGray)),
        chunks[2],
    );
}

fn draw_overview(f: &mut ratatui::Frame, area: Rect, snap: &Snapshot) {
    let summary = overview_summary(snap);
    let text = vec![
        Line::from(vec![
            Span::styled("agents:    ", Style::default().fg(Color::DarkGray)),
            Span::styled(summary.agents.to_string(), Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::styled("services:  ", Style::default().fg(Color::DarkGray)),
            Span::raw(summary.services.to_string()),
        ]),
        Line::from(vec![
            Span::styled("queues:    ", Style::default().fg(Color::DarkGray)),
            Span::raw(summary.queues.to_string()),
        ]),
        Line::from(vec![
            Span::styled("instances: ", Style::default().fg(Color::DarkGray)),
            Span::raw(summary.instances_total.to_string()),
        ]),
        Line::from(vec![
            Span::styled("log buffer:", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(" {} lines", summary.log_lines)),
        ]),
        Line::from(vec![
            Span::styled("uptime:    ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(" {}s", summary.uptime_seconds)),
        ]),
    ];
    f.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL).title("Overview")),
        area,
    );
}

/// Pure transform: Nodes JSON → table rows. Each row is `[node_id, arch, os, last_seen]`.
pub(crate) fn node_rows(nodes: &Value) -> Vec<[String; 4]> {
    nodes
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|n| {
                    [
                        n.get("node_id").and_then(|s| s.as_str()).unwrap_or("?").to_owned(),
                        n.pointer("/inventory/arch").and_then(|s| s.as_str()).unwrap_or("-").to_owned(),
                        n.pointer("/inventory/os").and_then(|s| s.as_str()).unwrap_or("-").to_owned(),
                        n.get("last_seen_at").and_then(|s| s.as_str()).unwrap_or("-").to_owned(),
                    ]
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Pure transform: Services JSON → `[name, replicas, runtime_kind, restart_policy]`.
pub(crate) fn service_rows(services: &Value) -> Vec<[String; 4]> {
    services
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|s| {
                    [
                        s.pointer("/metadata/name").and_then(|s| s.as_str()).unwrap_or("?").to_owned(),
                        s.pointer("/spec/replicas").map(|v| v.to_string()).unwrap_or_else(|| "1".into()),
                        s.pointer("/spec/runtime/kind").and_then(|s| s.as_str()).unwrap_or("?").to_owned(),
                        s.pointer("/spec/restart_policy").and_then(|s| s.as_str()).unwrap_or("?").to_owned(),
                    ]
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Pure transform: Queues JSON → `[name, type, max_age_seconds]`.
pub(crate) fn queue_rows(queues: &Value) -> Vec<[String; 3]> {
    queues
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|q| {
                    [
                        q.pointer("/metadata/name").and_then(|s| s.as_str()).unwrap_or("?").to_owned(),
                        q.pointer("/spec/type").and_then(|s| s.as_str()).unwrap_or("work").to_owned(),
                        q.pointer("/spec/max_age_seconds").map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
                    ]
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Pure transform: extract the small overview fields used by `draw_overview`.
pub(crate) fn overview_summary(snap: &Snapshot) -> OverviewSummary {
    OverviewSummary {
        agents: snap.nodes.as_array().map(|a| a.len()).unwrap_or(0),
        services: snap.services.as_array().map(|a| a.len()).unwrap_or(0),
        queues: snap.queues.as_array().map(|a| a.len()).unwrap_or(0),
        instances_total: snap
            .diag_system
            .pointer("/instances/total")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        log_lines: snap
            .diag_system
            .pointer("/logs/buffered_lines")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        uptime_seconds: snap
            .diag_system
            .pointer("/controller/uptime_seconds")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OverviewSummary {
    pub agents: usize,
    pub services: usize,
    pub queues: usize,
    pub instances_total: u64,
    pub log_lines: u64,
    pub uptime_seconds: u64,
}

fn draw_nodes(f: &mut ratatui::Frame, area: Rect, snap: &Snapshot) {
    let rows: Vec<Row> = node_rows(&snap.nodes)
        .into_iter()
        .map(|r| Row::new(r.iter().map(|c| Cell::from(c.clone())).collect::<Vec<_>>()))
        .collect();
    let widths = [
        Constraint::Length(20),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Min(20),
    ];
    let table = Table::new(rows, widths)
        .header(Row::new(vec!["NODE", "ARCH", "OS", "LAST SEEN"]).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().borders(Borders::ALL).title("Nodes"));
    f.render_widget(table, area);
}

fn draw_services(f: &mut ratatui::Frame, area: Rect, snap: &Snapshot) {
    let rows: Vec<Row> = service_rows(&snap.services)
        .into_iter()
        .map(|r| Row::new(r.iter().map(|c| Cell::from(c.clone())).collect::<Vec<_>>()))
        .collect();
    let widths = [
        Constraint::Length(30),
        Constraint::Length(10),
        Constraint::Length(15),
        Constraint::Length(15),
    ];
    let table = Table::new(rows, widths)
        .header(Row::new(vec!["NAME", "REPLICAS", "RUNTIME", "RESTART"]).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().borders(Borders::ALL).title("Services"));
    f.render_widget(table, area);
}

fn draw_queues(f: &mut ratatui::Frame, area: Rect, snap: &Snapshot) {
    let rows: Vec<Row> = queue_rows(&snap.queues)
        .into_iter()
        .map(|r| Row::new(r.iter().map(|c| Cell::from(c.clone())).collect::<Vec<_>>()))
        .collect();
    let widths = [
        Constraint::Length(30),
        Constraint::Length(10),
        Constraint::Length(15),
    ];
    let table = Table::new(rows, widths)
        .header(Row::new(vec!["NAME", "TYPE", "MAX_AGE"]).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().borders(Borders::ALL).title("Queues"));
    f.render_widget(table, area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snap(nodes: Value, services: Value, queues: Value, diag: Value) -> Snapshot {
        Snapshot {
            nodes,
            services,
            queues,
            instances: Value::Null,
            diag_system: diag,
            last_refresh: None,
        }
    }

    #[test]
    fn node_rows_extracts_inventory_arch_os_and_last_seen() {
        let nodes = json!([
            { "node_id": "pi", "inventory": { "arch": "arm64", "os": "linux" }, "last_seen_at": "2026-06-30T01:23:45Z" },
            { "node_id": "mac", "inventory": { "arch": "arm64", "os": "macos" }, "last_seen_at": "2026-06-30T01:24:00Z" },
        ]);
        let rows = node_rows(&nodes);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], ["pi".to_string(), "arm64".into(), "linux".into(), "2026-06-30T01:23:45Z".into()]);
        assert_eq!(rows[1][0], "mac");
        assert_eq!(rows[1][2], "macos");
    }

    #[test]
    fn node_rows_handles_missing_fields_with_placeholders() {
        let nodes = json!([{ "node_id": "x" }]);
        let rows = node_rows(&nodes);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], ["x".to_string(), "-".into(), "-".into(), "-".into()]);
    }

    #[test]
    fn node_rows_handles_non_array_input() {
        assert!(node_rows(&Value::Null).is_empty());
        assert!(node_rows(&json!({"oops": 1})).is_empty());
    }

    #[test]
    fn service_rows_extracts_metadata_and_spec_fields() {
        let services = json!([
            {
                "metadata": { "name": "row-cruncher" },
                "spec": {
                    "replicas": 3,
                    "runtime": { "kind": "native" },
                    "restart_policy": "on_failure"
                }
            }
        ]);
        let rows = service_rows(&services);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], "row-cruncher");
        assert_eq!(rows[0][1], "3");
        assert_eq!(rows[0][2], "native");
        assert_eq!(rows[0][3], "on_failure");
    }

    #[test]
    fn service_rows_defaults_replicas_to_one_when_absent() {
        let services = json!([
            { "metadata": { "name": "minimal" }, "spec": { "runtime": { "kind": "native" }, "restart_policy": "always" } }
        ]);
        let rows = service_rows(&services);
        assert_eq!(rows[0][1], "1");
    }

    #[test]
    fn queue_rows_show_default_work_type_when_absent() {
        let queues = json!([
            { "metadata": { "name": "ps-rows" }, "spec": { "type": "work", "max_age_seconds": 3600 } },
            { "metadata": { "name": "no-type-set" }, "spec": {} },
        ]);
        let rows = queue_rows(&queues);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], ["ps-rows".to_string(), "work".into(), "3600".into()]);
        // Default to "work" when type is missing.
        assert_eq!(rows[1][1], "work");
        // max_age_seconds missing → "-"
        assert_eq!(rows[1][2], "-");
    }

    #[test]
    fn overview_summary_extracts_counts_and_nested_diag_fields() {
        let s = snap(
            json!([{ "node_id": "a" }, { "node_id": "b" }]),
            json!([{ "metadata": { "name": "svc" }, "spec": {} }]),
            json!([{ "metadata": { "name": "q" }, "spec": {} }]),
            json!({
                "instances": { "total": 7 },
                "logs": { "buffered_lines": 1234 },
                "controller": { "uptime_seconds": 9001 }
            }),
        );
        let o = overview_summary(&s);
        assert_eq!(o.agents, 2);
        assert_eq!(o.services, 1);
        assert_eq!(o.queues, 1);
        assert_eq!(o.instances_total, 7);
        assert_eq!(o.log_lines, 1234);
        assert_eq!(o.uptime_seconds, 9001);
    }

    #[test]
    fn overview_summary_zeros_when_diag_missing() {
        let s = snap(Value::Null, Value::Null, Value::Null, Value::Null);
        let o = overview_summary(&s);
        assert_eq!(o.agents, 0);
        assert_eq!(o.instances_total, 0);
        assert_eq!(o.uptime_seconds, 0);
    }
}
