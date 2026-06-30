//! Pure decision logic for the reconciler and the workflow runner.
//!
//! These functions take immutable inputs (spec + observed state) and return
//! the action(s) to take. They don't touch NATS, the store, or any global
//! mutex — so they're easy to unit test.
//!
//! The control loops in `main.rs` call these to figure out *what* to do,
//! then handle the impure side (dispatch publish, store updates) themselves.

use orion_types::{RestartPolicy, ServiceSpec, WorkflowSpec};
use std::collections::HashMap;

// ============================================================ reconciler

#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileAction {
    /// Nothing to do — alive count matches desired (or every slot is in a
    /// terminal no-restart state).
    NoOp,
    /// No replicas alive and at least one slot is recoverable — re-dispatch
    /// the entire Service. The caller publishes a `ControlRun` with this many
    /// replicas. Returns the list of dead instance ids to purge from the
    /// registry (so we don't keep counting them as terminal forever).
    RedispatchAll {
        replicas: u32,
        purge: Vec<uuid::Uuid>,
    },
    /// Some replicas alive, some dead+restartable. Today we log this and
    /// wait for the others to die (Phase 5 follow-up).
    PartialUnderprovisioned {
        alive: u32,
        terminal: u32,
        want_restart: u32,
    },
}

/// One observed instance — the bits the reconciler needs to make a decision.
/// Pulled from `InstanceRecord` at the call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceObservation {
    pub instance_id: uuid::Uuid,
    pub exited: bool,
    pub exit_code: Option<i32>,
}

pub fn decide_reconcile(spec: &ServiceSpec, instances: &[InstanceObservation]) -> ReconcileAction {
    let desired = spec.replicas.unwrap_or(1).max(1);
    let alive = instances.iter().filter(|i| !i.exited).count() as u32;

    let mut want_restart_ids = Vec::new();
    let mut terminal: u32 = 0;
    for i in instances.iter().filter(|i| i.exited) {
        let restart = match spec.restart_policy {
            RestartPolicy::Always => true,
            RestartPolicy::OnFailure => i.exit_code.unwrap_or(0) != 0,
            RestartPolicy::Never => false,
        };
        if restart {
            want_restart_ids.push(i.instance_id);
        } else {
            terminal += 1;
        }
    }
    let want_restart = want_restart_ids.len() as u32;

    let alive_or_terminal = alive.saturating_add(terminal);
    let missing = desired.saturating_sub(alive_or_terminal);
    let to_launch = missing + want_restart;
    if to_launch == 0 {
        return ReconcileAction::NoOp;
    }
    if alive == 0 && terminal < desired {
        return ReconcileAction::RedispatchAll {
            replicas: desired,
            purge: want_restart_ids,
        };
    }
    ReconcileAction::PartialUnderprovisioned {
        alive,
        terminal,
        want_restart,
    }
}

// ============================================================ workflow

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StepStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

/// One step's lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepState {
    pub status: StepStatus,
}

/// Reported exit kind for a Task instance — comes from `InstanceRecord.exit_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskExitKind {
    Succeeded,
    Failed,
}

/// Inputs to `advance_workflow`.
pub struct WorkflowInputs<'a> {
    pub spec: &'a WorkflowSpec,
    pub progress: &'a HashMap<String, StepStatus>,
    /// Task name → most-recent exit kind (None = still running / never run).
    pub task_states: &'a HashMap<String, TaskExitKind>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowAdvance {
    /// Step state after applying observed task exits.
    pub next_progress: HashMap<String, StepStatus>,
    /// Step names to dispatch (their Task should be POSTed to /v1/dispatch).
    pub dispatch: Vec<String>,
    /// True when every step is Succeeded or Failed.
    pub finished: bool,
}

pub fn advance_workflow(inputs: WorkflowInputs<'_>) -> WorkflowAdvance {
    let WorkflowInputs { spec, progress, task_states } = inputs;
    let mut next = progress.clone();
    // Initialize unseen steps as Pending.
    for s in &spec.steps {
        next.entry(s.name.clone()).or_insert(StepStatus::Pending);
    }
    // Promote Running steps whose task has reported an exit.
    for s in &spec.steps {
        if matches!(next.get(&s.name), Some(StepStatus::Running)) {
            if let Some(kind) = task_states.get(&s.task.0) {
                let new = match kind {
                    TaskExitKind::Succeeded => StepStatus::Succeeded,
                    TaskExitKind::Failed => StepStatus::Failed,
                };
                next.insert(s.name.clone(), new);
            }
        }
    }
    // For each pending step: dispatch if deps are ready, or mark Failed if a
    // dep failed and continue_on_error=false.
    let mut dispatch = Vec::new();
    for s in &spec.steps {
        if !matches!(next.get(&s.name), Some(StepStatus::Pending)) {
            continue;
        }
        let blocked_by_failure = s.depends_on.iter().any(|d| {
            let dep = next.get(d).copied().unwrap_or(StepStatus::Pending);
            !spec.continue_on_error && matches!(dep, StepStatus::Failed)
        });
        if blocked_by_failure {
            next.insert(s.name.clone(), StepStatus::Failed);
            continue;
        }
        let deps_ready = s.depends_on.iter().all(|d| {
            let dep = next.get(d).copied().unwrap_or(StepStatus::Pending);
            matches!(dep, StepStatus::Succeeded)
                || (spec.continue_on_error && matches!(dep, StepStatus::Failed))
        });
        if deps_ready {
            dispatch.push(s.name.clone());
            next.insert(s.name.clone(), StepStatus::Running);
        }
    }
    let finished = next
        .values()
        .all(|s| matches!(s, StepStatus::Succeeded | StepStatus::Failed))
        && !next.is_empty();
    WorkflowAdvance { next_progress: next, dispatch, finished }
}

// ============================================================ Find API matcher

pub fn capabilities_match(
    advertised: &[orion_types::Capability],
    selector: &orion_types::CapabilitySelector,
) -> bool {
    use orion_types::AttrMatch;
    for (cap_name, checks) in &selector.requirements {
        let cap = match advertised.iter().find(|c| &c.name == cap_name) {
            Some(c) => c,
            None => return false,
        };
        for (attr_key, attr_match) in &checks.0 {
            let actual = match cap.attributes.get(attr_key) {
                Some(v) => v,
                None => return false,
            };
            match attr_match {
                AttrMatch::Equals(v) => {
                    if actual != v {
                        return false;
                    }
                }
                AttrMatch::OneOf(values) => {
                    if !values.iter().any(|v| v == actual) {
                        return false;
                    }
                }
                AttrMatch::Op(ops) => {
                    if !attr_op_matches(ops, actual) {
                        return false;
                    }
                }
            }
        }
    }
    true
}

pub fn attr_op_matches(op: &orion_types::AttrOp, actual: &serde_json::Value) -> bool {
    if let Some(v) = &op.eq {
        if actual != v {
            return false;
        }
    }
    if let Some(v) = &op.ne {
        if actual == v {
            return false;
        }
    }
    let actual_num = actual.as_f64();
    let cmp = |lhs: &Option<serde_json::Number>, op: fn(f64, f64) -> bool| -> bool {
        match (lhs, actual_num) {
            (Some(n), Some(a)) => n.as_f64().map_or(true, |x| op(a, x)),
            (Some(_), None) => false,
            (None, _) => true,
        }
    };
    cmp(&op.gt, |a, x| a > x)
        && cmp(&op.gte, |a, x| a >= x)
        && cmp(&op.lt, |a, x| a < x)
        && cmp(&op.lte, |a, x| a <= x)
}

// ============================================================ Prometheus format

/// Pure Prometheus text-format renderer. The handler in main.rs computes the
/// counters from runtime state and hands them here; this function just
/// formats. Keeping it pure makes the format testable without a controller.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub uptime_seconds: i64,
    pub agents_total: usize,
    pub agents_live: usize,
    pub instances_alive: usize,
    pub instances_exited: usize,
    pub instances_failed: usize,
    pub health_healthy: usize,
    pub health_unhealthy: usize,
    pub schedule_fires_total: u32,
}

pub fn format_prometheus(m: &MetricsSnapshot) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(1024);
    let _ = writeln!(out, "# HELP orion_controller_uptime_seconds Seconds since controller start");
    let _ = writeln!(out, "# TYPE orion_controller_uptime_seconds gauge");
    let _ = writeln!(out, "orion_controller_uptime_seconds {}", m.uptime_seconds);
    let _ = writeln!(out, "# HELP orion_agents_total Agents the controller has seen ever");
    let _ = writeln!(out, "# TYPE orion_agents_total gauge");
    let _ = writeln!(out, "orion_agents_total {}", m.agents_total);
    let _ = writeln!(out, "# HELP orion_agents_live Agents whose last heartbeat was within 30s");
    let _ = writeln!(out, "# TYPE orion_agents_live gauge");
    let _ = writeln!(out, "orion_agents_live {}", m.agents_live);
    let _ = writeln!(out, "# HELP orion_instances_alive Workload instances believed alive");
    let _ = writeln!(out, "# TYPE orion_instances_alive gauge");
    let _ = writeln!(out, "orion_instances_alive {}", m.instances_alive);
    let _ = writeln!(out, "# HELP orion_instances_exited Workload instances that have exited");
    let _ = writeln!(out, "# TYPE orion_instances_exited counter");
    let _ = writeln!(out, "orion_instances_exited {}", m.instances_exited);
    let _ = writeln!(out, "# HELP orion_instances_failed Workload instances that exited non-zero");
    let _ = writeln!(out, "# TYPE orion_instances_failed counter");
    let _ = writeln!(out, "orion_instances_failed {}", m.instances_failed);
    let _ = writeln!(out, "# HELP orion_health_status Instances reporting a health status");
    let _ = writeln!(out, "# TYPE orion_health_status gauge");
    let _ = writeln!(out, "orion_health_status{{status=\"healthy\"}} {}", m.health_healthy);
    let _ = writeln!(out, "orion_health_status{{status=\"unhealthy\"}} {}", m.health_unhealthy);
    let _ = writeln!(out, "# HELP orion_schedule_fires_total Total Schedule fires since controller start");
    let _ = writeln!(out, "# TYPE orion_schedule_fires_total counter");
    let _ = writeln!(out, "orion_schedule_fires_total {}", m.schedule_fires_total);
    out
}

/// Pure planner that combines the store-fetched workflow + task-instance
/// records into a per-workflow advance plan. Mirrors the impure path used by
/// `workflow_tick_once` but takes plain inputs so it can be exercised against
/// an in-memory store without NATS or the controller's loops.
///
/// Returns one entry per Workflow whose progress changed (or that has work to
/// dispatch). Each entry carries the new step-status map, the steps to
/// dispatch, and whether the workflow has finished.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowPlanEntry {
    pub workflow: String,
    pub next_progress: std::collections::HashMap<String, StepStatus>,
    pub dispatch: Vec<DispatchSpec>,
    pub finished: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DispatchSpec {
    pub step_name: String,
    pub task_name: String,
}

pub fn plan_all_workflows(
    workflows: &[orion_types::Resource],
    progress: &std::collections::HashMap<String, std::collections::HashMap<String, StepStatus>>,
    task_states: &std::collections::HashMap<String, TaskExitKind>,
) -> Vec<WorkflowPlanEntry> {
    let mut out = Vec::new();
    for wf in workflows {
        let name = wf.metadata.name.0.clone();
        let spec: orion_types::WorkflowSpec = match &wf.body {
            orion_types::ResourceBody::Workflow { spec, .. } => spec.clone(),
            _ => continue,
        };
        let empty_progress = std::collections::HashMap::new();
        let current = progress.get(&name).unwrap_or(&empty_progress);
        let plan = advance_workflow(WorkflowInputs {
            spec: &spec,
            progress: current,
            task_states,
        });
        let dispatch = plan
            .dispatch
            .iter()
            .filter_map(|step_name| {
                spec.steps.iter().find(|s| &s.name == step_name).map(|s| DispatchSpec {
                    step_name: step_name.clone(),
                    task_name: s.task.0.clone(),
                })
            })
            .collect();
        out.push(WorkflowPlanEntry {
            workflow: name,
            next_progress: plan.next_progress,
            dispatch,
            finished: plan.finished,
        });
    }
    out
}

// ============================================================ tests

#[cfg(test)]
mod tests {
    use super::*;
    use orion_types::{
        AttrChecks, AttrMatch, AttrOp, Capability, CapabilitySelector, RestartPolicy,
        ResourceName, ServiceSpec, WorkflowSpec, WorkflowStep,
    };
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use uuid::Uuid;

    // ---------------------------------------------------------------- reconciler

    fn obs(exited: bool, code: Option<i32>) -> InstanceObservation {
        InstanceObservation {
            instance_id: Uuid::new_v4(),
            exited,
            exit_code: code,
        }
    }

    fn svc(replicas: u32, policy: RestartPolicy) -> ServiceSpec {
        ServiceSpec {
            replicas: Some(replicas),
            restart_policy: policy,
            ..Default::default()
        }
    }

    #[test]
    fn reconciler_noop_when_alive_count_matches_desired() {
        let action = decide_reconcile(
            &svc(2, RestartPolicy::Always),
            &[obs(false, None), obs(false, None)],
        );
        assert_eq!(action, ReconcileAction::NoOp);
    }

    #[test]
    fn reconciler_redispatches_when_all_dead_under_always() {
        let i = obs(true, Some(1));
        let action = decide_reconcile(&svc(1, RestartPolicy::Always), &[i.clone()]);
        match action {
            ReconcileAction::RedispatchAll { replicas, purge } => {
                assert_eq!(replicas, 1);
                assert_eq!(purge, vec![i.instance_id]);
            }
            other => panic!("expected RedispatchAll, got {other:?}"),
        }
    }

    #[test]
    fn reconciler_on_failure_restarts_only_on_nonzero_exit() {
        // exit 0 → terminal, no restart
        let action_ok = decide_reconcile(
            &svc(1, RestartPolicy::OnFailure),
            &[obs(true, Some(0))],
        );
        assert_eq!(action_ok, ReconcileAction::NoOp);

        // exit 7 → redispatch
        let action_fail = decide_reconcile(
            &svc(1, RestartPolicy::OnFailure),
            &[obs(true, Some(7))],
        );
        assert!(matches!(
            action_fail,
            ReconcileAction::RedispatchAll { replicas: 1, .. }
        ));
    }

    #[test]
    fn reconciler_never_does_not_restart_even_on_failure() {
        let action = decide_reconcile(
            &svc(1, RestartPolicy::Never),
            &[obs(true, Some(99))],
        );
        assert_eq!(action, ReconcileAction::NoOp);
    }

    #[test]
    fn reconciler_partial_when_some_alive_some_dead() {
        let action = decide_reconcile(
            &svc(2, RestartPolicy::Always),
            &[obs(false, None), obs(true, Some(1))],
        );
        match action {
            ReconcileAction::PartialUnderprovisioned { alive, terminal, want_restart } => {
                assert_eq!(alive, 1);
                assert_eq!(terminal, 0);
                assert_eq!(want_restart, 1);
            }
            other => panic!("expected PartialUnderprovisioned, got {other:?}"),
        }
    }

    #[test]
    fn reconciler_noop_when_all_terminal_with_never() {
        let action = decide_reconcile(
            &svc(2, RestartPolicy::Never),
            &[obs(true, Some(0)), obs(true, Some(0))],
        );
        assert_eq!(action, ReconcileAction::NoOp);
    }

    // ---------------------------------------------------------------- workflow

    fn step(name: &str, task: &str, depends_on: &[&str]) -> WorkflowStep {
        WorkflowStep {
            name: name.into(),
            task: ResourceName::from(task),
            depends_on: depends_on.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    fn wf(steps: Vec<WorkflowStep>, continue_on_error: bool) -> WorkflowSpec {
        WorkflowSpec { steps, continue_on_error, description: None }
    }

    #[test]
    fn workflow_first_tick_dispatches_root_only() {
        // a → b → c
        let spec = wf(
            vec![
                step("a", "task-a", &[]),
                step("b", "task-b", &["a"]),
                step("c", "task-c", &["b"]),
            ],
            false,
        );
        let progress = HashMap::new();
        let task_states = HashMap::new();
        let out = advance_workflow(WorkflowInputs { spec: &spec, progress: &progress, task_states: &task_states });
        assert_eq!(out.dispatch, vec!["a"]);
        assert_eq!(out.next_progress.get("a"), Some(&StepStatus::Running));
        assert_eq!(out.next_progress.get("b"), Some(&StepStatus::Pending));
        assert!(!out.finished);
    }

    #[test]
    fn workflow_promotes_running_to_succeeded_and_fans_out_diamond() {
        // a → {b, c} → d
        let spec = wf(
            vec![
                step("a", "task-a", &[]),
                step("b", "task-b", &["a"]),
                step("c", "task-c", &["a"]),
                step("d", "task-d", &["b", "c"]),
            ],
            false,
        );
        let mut progress = HashMap::new();
        progress.insert("a".into(), StepStatus::Running);
        progress.insert("b".into(), StepStatus::Pending);
        progress.insert("c".into(), StepStatus::Pending);
        progress.insert("d".into(), StepStatus::Pending);
        let mut task_states = HashMap::new();
        task_states.insert("task-a".to_owned(), TaskExitKind::Succeeded);
        let out = advance_workflow(WorkflowInputs { spec: &spec, progress: &progress, task_states: &task_states });
        assert_eq!(out.next_progress["a"], StepStatus::Succeeded);
        assert!(out.dispatch.contains(&"b".to_owned()));
        assert!(out.dispatch.contains(&"c".to_owned()));
        assert_eq!(out.next_progress["b"], StepStatus::Running);
        assert_eq!(out.next_progress["c"], StepStatus::Running);
        // d not dispatched yet — its deps b, c still Running.
        assert!(!out.dispatch.contains(&"d".to_owned()));
        assert_eq!(out.next_progress["d"], StepStatus::Pending);
    }

    #[test]
    fn workflow_fail_fast_marks_dependents_failed_without_running_them() {
        // a → b
        let spec = wf(vec![step("a", "task-a", &[]), step("b", "task-b", &["a"])], false);
        let mut progress = HashMap::new();
        progress.insert("a".into(), StepStatus::Running);
        progress.insert("b".into(), StepStatus::Pending);
        let mut task_states = HashMap::new();
        task_states.insert("task-a".to_owned(), TaskExitKind::Failed);
        let out = advance_workflow(WorkflowInputs { spec: &spec, progress: &progress, task_states: &task_states });
        assert_eq!(out.next_progress["a"], StepStatus::Failed);
        assert_eq!(out.next_progress["b"], StepStatus::Failed);
        assert!(out.dispatch.is_empty());
        assert!(out.finished);
    }

    #[test]
    fn workflow_continue_on_error_runs_downstream_anyway() {
        let spec = wf(vec![step("a", "task-a", &[]), step("b", "task-b", &["a"])], true);
        let mut progress = HashMap::new();
        progress.insert("a".into(), StepStatus::Running);
        progress.insert("b".into(), StepStatus::Pending);
        let mut task_states = HashMap::new();
        task_states.insert("task-a".to_owned(), TaskExitKind::Failed);
        let out = advance_workflow(WorkflowInputs { spec: &spec, progress: &progress, task_states: &task_states });
        assert_eq!(out.next_progress["a"], StepStatus::Failed);
        assert_eq!(out.dispatch, vec!["b".to_owned()]);
        assert_eq!(out.next_progress["b"], StepStatus::Running);
        assert!(!out.finished);
    }

    #[test]
    fn workflow_finishes_when_every_step_terminal() {
        let spec = wf(vec![step("a", "task-a", &[]), step("b", "task-b", &["a"])], false);
        let mut progress = HashMap::new();
        progress.insert("a".into(), StepStatus::Running);
        progress.insert("b".into(), StepStatus::Running);
        let mut task_states = HashMap::new();
        task_states.insert("task-a".to_owned(), TaskExitKind::Succeeded);
        task_states.insert("task-b".to_owned(), TaskExitKind::Succeeded);
        let out = advance_workflow(WorkflowInputs { spec: &spec, progress: &progress, task_states: &task_states });
        assert_eq!(out.next_progress["a"], StepStatus::Succeeded);
        assert_eq!(out.next_progress["b"], StepStatus::Succeeded);
        assert!(out.finished);
    }

    // ---------------------------------------------------------------- find API

    fn cap(name: &str, attrs: Value) -> Capability {
        Capability::with_attributes(name, attrs)
    }

    fn selector(items: &[(&str, &[(&str, AttrMatch)])]) -> CapabilitySelector {
        let mut requirements = BTreeMap::new();
        for (cap_name, checks) in items {
            let mut m = BTreeMap::new();
            for (k, v) in *checks {
                m.insert((*k).to_owned(), v.clone());
            }
            requirements.insert((*cap_name).to_owned(), AttrChecks(m));
        }
        CapabilitySelector { requirements }
    }

    #[test]
    fn find_equals_match_and_miss() {
        let advertised = vec![cap("search", json!({ "dataset": "amiga" }))];
        // match
        let sel_ok = selector(&[("search", &[("dataset", AttrMatch::Equals(json!("amiga")))])]);
        assert!(capabilities_match(&advertised, &sel_ok));
        // miss on attr value
        let sel_no = selector(&[("search", &[("dataset", AttrMatch::Equals(json!("c64")))])]);
        assert!(!capabilities_match(&advertised, &sel_no));
        // miss because the capability isn't advertised
        let sel_missing_cap = selector(&[("llm", &[("dataset", AttrMatch::Equals(json!("amiga")))])]);
        assert!(!capabilities_match(&advertised, &sel_missing_cap));
        // miss because the attribute isn't on the cap
        let sel_missing_attr = selector(&[("search", &[("model", AttrMatch::Equals(json!("amiga")))])]);
        assert!(!capabilities_match(&advertised, &sel_missing_attr));
    }

    #[test]
    fn find_oneof_match_and_miss() {
        let advertised = vec![cap("model", json!({ "format": "gguf" }))];
        let ok = selector(&[(
            "model",
            &[(
                "format",
                AttrMatch::OneOf(vec![json!("gguf"), json!("safetensors")]),
            )],
        )]);
        assert!(capabilities_match(&advertised, &ok));
        let no = selector(&[(
            "model",
            &[(
                "format",
                AttrMatch::OneOf(vec![json!("safetensors"), json!("onnx")]),
            )],
        )]);
        assert!(!capabilities_match(&advertised, &no));
    }

    #[test]
    fn find_op_numeric_comparisons() {
        let advertised = vec![cap("llm", json!({ "min_vram_gb": 24 }))];
        let mk = |op: AttrOp| {
            selector(&[("llm", &[("min_vram_gb", AttrMatch::Op(op))])])
        };
        // gte: exact boundary passes
        assert!(capabilities_match(
            &advertised,
            &mk(AttrOp {
                gte: Some(serde_json::Number::from(24)),
                ..Default::default()
            })
        ));
        // gt: exact boundary fails
        assert!(!capabilities_match(
            &advertised,
            &mk(AttrOp {
                gt: Some(serde_json::Number::from(24)),
                ..Default::default()
            })
        ));
        // lte: passes
        assert!(capabilities_match(
            &advertised,
            &mk(AttrOp {
                lte: Some(serde_json::Number::from(24)),
                ..Default::default()
            })
        ));
        // lt: fails (24 < 24 is false)
        assert!(!capabilities_match(
            &advertised,
            &mk(AttrOp {
                lt: Some(serde_json::Number::from(24)),
                ..Default::default()
            })
        ));
        // eq: passes
        assert!(capabilities_match(
            &advertised,
            &mk(AttrOp {
                eq: Some(json!(24)),
                ..Default::default()
            })
        ));
        // ne: passes when actual != requested ne value
        assert!(capabilities_match(
            &advertised,
            &mk(AttrOp {
                ne: Some(json!(8)),
                ..Default::default()
            })
        ));
        // ne: fails when actual == ne value
        assert!(!capabilities_match(
            &advertised,
            &mk(AttrOp {
                ne: Some(json!(24)),
                ..Default::default()
            })
        ));
    }

    #[test]
    fn find_op_with_non_numeric_actual_rejects_when_numeric_check_present() {
        let advertised = vec![cap("model", json!({ "format": "gguf" }))];
        let sel = selector(&[(
            "model",
            &[(
                "format",
                AttrMatch::Op(AttrOp {
                    gt: Some(serde_json::Number::from(1)),
                    ..Default::default()
                }),
            )],
        )]);
        // "gguf" can't be compared numerically, the cmp returns false.
        assert!(!capabilities_match(&advertised, &sel));
    }

    // ---------------------------------------------------------------- prometheus

    #[test]
    fn prometheus_format_includes_every_metric_name() {
        let snap = MetricsSnapshot {
            uptime_seconds: 42,
            agents_total: 3,
            agents_live: 2,
            instances_alive: 5,
            instances_exited: 11,
            instances_failed: 2,
            health_healthy: 4,
            health_unhealthy: 1,
            schedule_fires_total: 7,
        };
        let text = format_prometheus(&snap);
        // Every metric name shows up at least once (HELP line) and as a
        // value (the data line). Failure of either is a regression.
        for name in [
            "orion_controller_uptime_seconds",
            "orion_agents_total",
            "orion_agents_live",
            "orion_instances_alive",
            "orion_instances_exited",
            "orion_instances_failed",
            "orion_health_status",
            "orion_schedule_fires_total",
        ] {
            assert!(text.contains(name), "expected metric {name} in:\n{text}");
        }
        assert!(text.contains("orion_controller_uptime_seconds 42"));
        assert!(text.contains("orion_agents_live 2"));
        assert!(text.contains("orion_instances_alive 5"));
        assert!(text.contains("orion_instances_failed 2"));
        assert!(text.contains("orion_health_status{status=\"healthy\"} 4"));
        assert!(text.contains("orion_schedule_fires_total 7"));
    }

    #[test]
    fn prometheus_format_well_formed_help_type_pairs() {
        let snap = MetricsSnapshot::default();
        let text = format_prometheus(&snap);
        // Every # HELP line should be followed by a # TYPE line.
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            if line.starts_with("# HELP ") {
                let next = lines.get(i + 1).copied().unwrap_or("");
                assert!(
                    next.starts_with("# TYPE "),
                    "HELP line not followed by TYPE: {line}\n  next: {next}"
                );
            }
        }
        // Every metric line (no leading #) must be a single token + a numeric
        // value, allowing labels in braces.
        for line in &lines {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            // Split on the LAST space — the value is always the last token.
            let (head, val) = line.rsplit_once(' ').expect("metric line has a space");
            val.parse::<f64>().unwrap_or_else(|_| panic!("non-numeric value: {line}"));
            assert!(!head.contains(' '), "metric name shouldn't contain raw space: {head}");
        }
    }

    // ---------------------------------------------------------------- workflow integration

    use orion_store::Store;

    async fn populate_workflow_fixture() -> Store {
        let store = Store::in_memory().await.unwrap();
        // Three Tasks: task-a, task-b, task-c.
        for tname in ["task-a", "task-b", "task-c"] {
            let task = orion_types::Resource::from_yaml(&format!(
                "apiVersion: orionmesh.dev/v1\nkind: Task\nmetadata: {{ name: {tname} }}\nspec:\n  runtime: {{ kind: native, exec: /usr/bin/true }}\n"
            ))
            .unwrap();
            store.upsert_resource(&task).await.unwrap();
        }
        // A workflow: a → b, then a → c (both depend on a).
        let wf_yaml = r#"
apiVersion: orionmesh.dev/v1
kind: Workflow
metadata: { name: integ-flow }
spec:
  steps:
    - name: step-a
      task: task-a
    - name: step-b
      task: task-b
      depends_on: [step-a]
    - name: step-c
      task: task-c
      depends_on: [step-a]
"#;
        let wf = orion_types::Resource::from_yaml(wf_yaml).unwrap();
        store.upsert_resource(&wf).await.unwrap();
        store
    }

    #[tokio::test]
    async fn workflow_plumbing_first_tick_loads_and_plans_root_only() {
        let store = populate_workflow_fixture().await;
        let workflows = store.list_by_kind("Workflow").await.unwrap();
        assert_eq!(workflows.len(), 1);
        let progress = std::collections::HashMap::new();
        let task_states = std::collections::HashMap::new();
        let plans = plan_all_workflows(&workflows, &progress, &task_states);
        assert_eq!(plans.len(), 1);
        let p = &plans[0];
        assert_eq!(p.workflow, "integ-flow");
        assert_eq!(p.dispatch.len(), 1);
        assert_eq!(p.dispatch[0].step_name, "step-a");
        assert_eq!(p.dispatch[0].task_name, "task-a");
        assert!(!p.finished);
    }

    #[tokio::test]
    async fn workflow_plumbing_advances_when_task_succeeds() {
        let store = populate_workflow_fixture().await;
        let workflows = store.list_by_kind("Workflow").await.unwrap();
        // Round 1: dispatch step-a (root). Save it as Running.
        let mut progress = std::collections::HashMap::new();
        let task_states_r1 = std::collections::HashMap::new();
        let plans_r1 = plan_all_workflows(&workflows, &progress, &task_states_r1);
        progress.insert("integ-flow".to_string(), plans_r1[0].next_progress.clone());

        // Round 2: simulate task-a succeeded. Should dispatch step-b and step-c.
        let mut task_states_r2 = std::collections::HashMap::new();
        task_states_r2.insert("task-a".to_string(), TaskExitKind::Succeeded);
        let plans_r2 = plan_all_workflows(&workflows, &progress, &task_states_r2);
        let p = &plans_r2[0];
        let dispatched: std::collections::HashSet<_> =
            p.dispatch.iter().map(|d| d.step_name.clone()).collect();
        assert!(dispatched.contains("step-b"));
        assert!(dispatched.contains("step-c"));
        assert_eq!(p.next_progress["step-a"], StepStatus::Succeeded);
        assert_eq!(p.next_progress["step-b"], StepStatus::Running);
        assert_eq!(p.next_progress["step-c"], StepStatus::Running);
        assert!(!p.finished);
    }

    #[tokio::test]
    async fn workflow_plumbing_finishes_when_all_tasks_complete() {
        let store = populate_workflow_fixture().await;
        let workflows = store.list_by_kind("Workflow").await.unwrap();
        // Pre-populate progress as if step-b and step-c are running.
        let mut progress = std::collections::HashMap::new();
        let mut steps = std::collections::HashMap::new();
        steps.insert("step-a".into(), StepStatus::Succeeded);
        steps.insert("step-b".into(), StepStatus::Running);
        steps.insert("step-c".into(), StepStatus::Running);
        progress.insert("integ-flow".to_string(), steps);

        let mut task_states = std::collections::HashMap::new();
        task_states.insert("task-a".to_string(), TaskExitKind::Succeeded);
        task_states.insert("task-b".to_string(), TaskExitKind::Succeeded);
        task_states.insert("task-c".to_string(), TaskExitKind::Succeeded);

        let plans = plan_all_workflows(&workflows, &progress, &task_states);
        let p = &plans[0];
        assert!(p.finished);
        assert_eq!(p.dispatch.len(), 0);
        assert_eq!(p.next_progress["step-b"], StepStatus::Succeeded);
        assert_eq!(p.next_progress["step-c"], StepStatus::Succeeded);
    }

    #[tokio::test]
    async fn workflow_plumbing_handles_no_workflows_in_store() {
        let store = Store::in_memory().await.unwrap();
        let workflows = store.list_by_kind("Workflow").await.unwrap();
        assert!(workflows.is_empty());
        let plans = plan_all_workflows(
            &workflows,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        );
        assert!(plans.is_empty());
    }

    #[test]
    fn find_multiple_caps_all_must_pass() {
        let advertised = vec![
            cap("search", json!({ "dataset": "amiga" })),
            cap("llm", json!({ "min_vram_gb": 24 })),
        ];
        // Both required, both present → match
        let sel = selector(&[
            ("search", &[("dataset", AttrMatch::Equals(json!("amiga")))]),
            (
                "llm",
                &[(
                    "min_vram_gb",
                    AttrMatch::Op(AttrOp { gte: Some(serde_json::Number::from(16)), ..Default::default() }),
                )],
            ),
        ]);
        assert!(capabilities_match(&advertised, &sel));
        // One required cap absent → miss
        let sel_extra = selector(&[
            ("search", &[("dataset", AttrMatch::Equals(json!("amiga")))]),
            (
                "wasm",
                &[("min_mem_mb", AttrMatch::Equals(json!(256)))],
            ),
        ]);
        assert!(!capabilities_match(&advertised, &sel_extra));
    }
}
