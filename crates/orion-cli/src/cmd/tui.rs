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
    let agents = snap.nodes.as_array().map(|a| a.len()).unwrap_or(0);
    let svc_count = snap.services.as_array().map(|a| a.len()).unwrap_or(0);
    let q_count = snap.queues.as_array().map(|a| a.len()).unwrap_or(0);
    let inst_total = snap
        .diag_system
        .pointer("/instances/total")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let log_lines = snap
        .diag_system
        .pointer("/logs/buffered_lines")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let uptime = snap
        .diag_system
        .pointer("/controller/uptime_seconds")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let text = vec![
        Line::from(vec![
            Span::styled("agents:    ", Style::default().fg(Color::DarkGray)),
            Span::styled(agents.to_string(), Style::default().fg(Color::Green)),
        ]),
        Line::from(vec![
            Span::styled("services:  ", Style::default().fg(Color::DarkGray)),
            Span::raw(svc_count.to_string()),
        ]),
        Line::from(vec![
            Span::styled("queues:    ", Style::default().fg(Color::DarkGray)),
            Span::raw(q_count.to_string()),
        ]),
        Line::from(vec![
            Span::styled("instances: ", Style::default().fg(Color::DarkGray)),
            Span::raw(inst_total.to_string()),
        ]),
        Line::from(vec![
            Span::styled("log buffer:", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(" {log_lines} lines")),
        ]),
        Line::from(vec![
            Span::styled("uptime:    ", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(" {uptime}s")),
        ]),
    ];
    f.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL).title("Overview")),
        area,
    );
}

fn draw_nodes(f: &mut ratatui::Frame, area: Rect, snap: &Snapshot) {
    let rows: Vec<Row> = snap
        .nodes
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|n| {
                    Row::new(vec![
                        Cell::from(n.get("node_id").and_then(|s| s.as_str()).unwrap_or("?").to_owned()),
                        Cell::from(n.pointer("/inventory/arch").and_then(|s| s.as_str()).unwrap_or("-").to_owned()),
                        Cell::from(n.pointer("/inventory/os").and_then(|s| s.as_str()).unwrap_or("-").to_owned()),
                        Cell::from(n.get("last_seen_at").and_then(|s| s.as_str()).unwrap_or("-").to_owned()),
                    ])
                })
                .collect()
        })
        .unwrap_or_default();
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
    let rows: Vec<Row> = snap
        .services
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|s| {
                    Row::new(vec![
                        Cell::from(s.pointer("/metadata/name").and_then(|s| s.as_str()).unwrap_or("?").to_owned()),
                        Cell::from(s.pointer("/spec/replicas").map(|v| v.to_string()).unwrap_or_else(|| "1".into())),
                        Cell::from(s.pointer("/spec/runtime/kind").and_then(|s| s.as_str()).unwrap_or("?").to_owned()),
                        Cell::from(s.pointer("/spec/restart_policy").and_then(|s| s.as_str()).unwrap_or("?").to_owned()),
                    ])
                })
                .collect()
        })
        .unwrap_or_default();
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
    let rows: Vec<Row> = snap
        .queues
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|q| {
                    Row::new(vec![
                        Cell::from(q.pointer("/metadata/name").and_then(|s| s.as_str()).unwrap_or("?").to_owned()),
                        Cell::from(q.pointer("/spec/type").and_then(|s| s.as_str()).unwrap_or("work").to_owned()),
                        Cell::from(q.pointer("/spec/max_age_seconds").map(|v| v.to_string()).unwrap_or_else(|| "-".into())),
                    ])
                })
                .collect()
        })
        .unwrap_or_default();
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
