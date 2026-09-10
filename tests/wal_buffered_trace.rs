//! 串行子进程专项：默认零配置、真实 WAL、读取/确认闭环、计数和快照逐条核对。
//! 子进程隔离全局时钟、采集会话和后台任务；测试失败保留临时目录，不运行旧测试入口。
#![cfg(feature = "wal-trace")]

use std::{collections::{BTreeMap, HashMap, HashSet}, env, fs, io, path::{Path, PathBuf},
    process::Command, sync::{Arc, Mutex}, thread, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use futures::executor::block_on;
use pi_async_rt::rt::{multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder}, startup_global_time_loop, AsyncRuntime};
use pi_async_transaction::AsyncCommitLog;
use pi_guid::Guid;
use pi_store::{commit_logger::{CommitLoggerBuilder, CommitLoggerExt}, wal_trace};
use pi_store::log_store::log_file::LogMethod;
use serde_json::Value;

const NAME: &str = "test_wal_buffered_trace_real_serial_matrix";
const CHILD: &str = "PI_STORE_TRACE_TEST_CHILD";
const ROOT: &str = "PI_STORE_TRACE_TEST_ROOT";

#[test]
fn test_wal_buffered_trace_real_serial_matrix() {
    if let Ok(phase) = env::var(CHILD) { child(&phase); return; }
    for workers in [1usize, 4] {
        let root = temp(&format!("{workers}w"));
        fs::create_dir_all(&root).unwrap();
        for phase in ["write", "replay", "empty", "concurrent", "shutdown_pending", "overflow", "byte_limit"] {
            let mut child = Command::new(env::current_exe().unwrap())
                .args(["--exact", NAME, "--nocapture", "--test-threads=1"])
                .env(CHILD, phase).env(ROOT, &root).env("PI_STORE_TRACE_TEST_WORKERS", workers.to_string())
                .env_remove("PI_STORE_WAL_TRACE_DIR").env_remove("PI_STORE_WAL_TRACE_CAPACITY")
                .env_remove("PI_STORE_WAL_TRACE_MAX_BYTES").env_remove("PI_STORE_WAL_TRACE_WINDOW_MS")
                .spawn().unwrap();
            let start = Instant::now();
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    assert!(status.success(), "phase={phase}, workers={workers}, evidence={root:?}"); break;
                }
                if start.elapsed() > Duration::from_secs(45) { child.kill().unwrap(); let _ = child.wait(); panic!("phase timed out: {phase}, evidence={root:?}"); }
                thread::sleep(Duration::from_millis(10));
            }
        }
        fs::remove_dir_all(&root).unwrap();
    }
}

fn child(phase: &str) {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| { previous(info); std::process::abort(); }));
    let root = PathBuf::from(env::var_os(ROOT).unwrap());
    let workers: usize = env::var("PI_STORE_TRACE_TEST_WORKERS").unwrap().parse().unwrap();
    if matches!(phase, "overflow" | "byte_limit") {
        let directory = root.join(format!("trace-{phase}")); fs::create_dir_all(&directory).unwrap();
        let mut config = wal_trace::TraceConfig::new(directory, phase);
        config.window_ms = if phase == "overflow" { 60_000 } else { 10 };
        config.capacity = if phase == "overflow" { 16 } else { 65_536 };
        config.max_bytes = 65_536;
        wal_trace::start(config).unwrap();
    }
    let _clock = startup_global_time_loop(1);
    let rt = MultiTaskRuntimeBuilder::default().init_worker_size(workers).build();
    let (tx, rx) = crossbeam_channel::bounded(1);
    let task_rt = rt.clone(); let task_root = root.clone(); let phase_owned = phase.to_owned();
    rt.spawn(async move { let result = scenario(&task_rt, &task_root, &phase_owned).await; let _ = tx.send(result); }).unwrap();
    let pending = rx.recv_timeout(Duration::from_secs(30)).expect("real WAL task stalled").unwrap();
    if phase == "shutdown_pending" {
        assert!(pending.is_some());
        assert_eq!(wal_trace::shutdown(Duration::from_millis(2)).unwrap_err().kind(), io::ErrorKind::TimedOut);
    }
    drop(pending);
    // shutdown 在控制线程执行，不占用推动定时器/文件操作的 worker。
    let report = wal_trace::shutdown(Duration::from_secs(5)).unwrap();
    let lines = read_json(&report.path);
    assert_eq!(lines[0]["event"], "session_start");
    assert_eq!(lines.last().unwrap()["event"], "session_end");
    assert_eq!(lines.last().unwrap()["complete"].as_bool(), Some(report.complete));
    for line in &lines { assert_eq!(line["keyword"], wal_trace::KEYWORD); }
    assert!(fs::metadata(&report.path).unwrap().len() <= if matches!(phase, "overflow" | "byte_limit") { 65_536 } else { 2 * 1024 * 1024 * 1024 });
    if phase == "overflow" {
        assert!(!report.complete); assert!(report.dropped > 0); assert_eq!(report.produced, report.written + report.dropped);
    } else if phase == "byte_limit" {
        assert!(!report.complete); assert_eq!(report.reason, "byte_limit");
    } else {
        assert!(report.complete, "{report:?}"); assert_eq!(report.produced, report.written); assert_eq!(report.omitted, 0);
        verify_events(&lines, phase);
        assert!(report.path.starts_with(root.join("pi_store_wal_trace")), "automatic output must be outside WAL");
    }
    eprintln!("TRACE_TEST phase={phase} workers={workers} produced={} written={} complete={}", report.produced, report.written, report.complete);
}

async fn scenario(rt: &MultiTaskRuntime<()>, root: &Path, phase: &str)
    -> io::Result<Option<futures::future::BoxFuture<'static, io::Result<()>>>> {
    let path = root.join(if phase == "concurrent" { "concurrent-wal" } else if matches!(phase, "overflow" | "byte_limit") { phase } else { "wal" });
    let logger = CommitLoggerBuilder::new(rt.clone(), &path).log_block_limit(8192).delay_timeout(1).collect_interval(300_000).build().await?;
    match phase {
        "write" => {
            assert_eq!(logger.append(Guid(0), Vec::<u8>::new()).await?, 0);
            logger.flush(0).await?;
            // 小块定时提交及正常确认清理，与之后用于重播的文件分开。
            let h = logger.append(Guid(1), vec![1; 64]).await?; logger.flush(h).await?; logger.confirm(Guid(1)).await?;
            logger.confirm(Guid(u128::MAX)).await?;
            for id in 2u128..=13 {
                let bytes = payload(id);
                let h = logger.append(Guid(id), bytes).await?;
                if id % 4 == 0 { logger.append_check_point().await?; } // 未 flush 的当前块走真实辅助提交。
                logger.flush(h).await?;
                logger.flush(h).await?; // 已覆盖 UID 不得额外同步。
            }
            assert_eq!(logger.waiting_confirm_count().await, 12);
        }
        "replay" => {
            let actual = Arc::new(Mutex::new(Vec::new())); let seen = actual.clone(); let callback_logger = logger.clone();
            let result = logger.start_replay::<Vec<u8>, _>(Arc::new(move |uid: Guid, bytes: Vec<u8>| {
                assert_eq!(bytes, payload(uid.0)); seen.lock().unwrap().push(uid.0);
                block_on(async { let h = callback_logger.append_replay(uid.clone(), bytes).await?; callback_logger.flush_replay(h).await?; callback_logger.confirm_replay(uid).await })
            })).await?;
            assert_eq!(*actual.lock().unwrap(), (2u128..=13).collect::<Vec<_>>());
            assert_eq!(result, (12, (2u128..=13).map(|id| payload(id).len() + 16).sum()));
            assert_eq!(logger.waiting_confirm_count().await, 12);
            logger.finish_replay().await?; assert_eq!(logger.waiting_confirm_count().await, 0);
            assert_eq!(logger.confirm_total_count(), 12); logger.finish_replay().await?;
        }
        "empty" => {
            let result = logger.start_replay_ext::<Vec<u8>, _>(Arc::new(|entry: Option<(Guid, LogMethod, u64, Vec<u8>)>| { assert!(entry.is_none()); Ok(()) })).await?;
            assert_eq!(result, (0, 0)); logger.finish_replay().await?;
        }
        "concurrent" => {
            let mut expected = BTreeMap::new();
            for concurrency in [1usize, 4, 64, 256] {
                let (tx, rx) = async_channel::bounded(concurrency);
                for index in 0..concurrency {
                    let logger = logger.clone(); let tx = tx.clone(); let uid = (concurrency * 1000 + index) as u128;
                    rt.spawn(async move {
                        let result = async { let h = logger.append(Guid(uid), payload(uid)).await?; let start = Instant::now(); logger.flush(h).await?; Ok::<_, io::Error>((h, uid, start.elapsed().as_nanos() as u64)) }.await;
                        let _ = tx.send(result).await;
                    })?;
                }
                drop(tx);
                let mut delays = Vec::new(); let started = Instant::now();
                for _ in 0..concurrency { let (h, uid, delay) = rx.recv().await.unwrap()?; assert!(expected.insert(h, uid).is_none()); delays.push(delay); }
                delays.sort_unstable();
                eprintln!("TRACE_BENCH concurrency={concurrency} p50_ns={} p90_ns={} p95_ns={} p99_ns={} receive_elapsed_ns={}", percentile(&delays, 50), percentile(&delays, 90), percentile(&delays, 95), percentile(&delays, 99), started.elapsed().as_nanos());
            }
            let actual = Arc::new(Mutex::new(Vec::new())); let seen = actual.clone();
            let count = logger.start_replay_ext::<Vec<u8>, _>(Arc::new(move |entry: Option<(Guid, LogMethod, u64, Vec<u8>)>| {
                if let Some((uid, _, _, bytes)) = entry { assert_eq!(bytes, payload(uid.0)); seen.lock().unwrap().push(uid.0); } Ok(())
            })).await?;
            assert_eq!(*actual.lock().unwrap(), expected.values().copied().collect::<Vec<_>>());
            assert_eq!(count.0, expected.len()); logger.finish_replay().await?;
        }
        "overflow" | "byte_limit" => {
            for id in 1..=600u128 { let h = logger.append(Guid(id), vec![id as u8; 32]).await?; assert!(h > 0); }
            logger.append_check_point().await?; // 采集耗尽不允许阻止真实持久化。
            assert_eq!(logger.waiting_confirm_count().await, 600);
        }
        "shutdown_pending" => return Ok(Some(logger.flush(0))),
        _ => unreachable!(),
    }
    Ok(None)
}

fn verify_events(lines: &[Value], phase: &str) {
    let events: Vec<_> = lines.iter().filter(|line| line.get("operation_id").is_some()).collect();
    let mut ids = HashSet::new(); let mut by_id = HashMap::new(); let mut sequences: HashMap<u64, Vec<u64>> = HashMap::new();
    let mut actual_counts = HashMap::<String, u64>::new();
    for event in &events {
        let id = event["operation_id"].as_u64().unwrap(); assert!(id > 0 && ids.insert(id)); by_id.insert(id, *event);
        assert_ne!(event["outcome"], "begin");
        *actual_counts.entry(event["event"].as_str().unwrap().into()).or_default() += 1;
        if let Some(seq) = event["lock_seq"].as_u64() { sequences.entry(event["logger_id"].as_u64().unwrap()).or_default().push(seq); assert!(event["lock_hold_ns"].is_u64()); assert!(event["lock_wait_ns"].is_u64()); }
        if event["event"] == "sync_call" { assert!(event["poll_count"].as_u64().unwrap() >= 1); assert_eq!(event["written_bytes"].as_u64().unwrap(), event["block_bytes"].as_u64().unwrap() + 16); }
        if event["event"] == "root_append" && event["log_uid"] != 0 { for field in ["call_start_ns", "first_poll_ns", "checkpoint_wait_start_ns", "checkpoint_acquired_ns", "append_start_ns", "append_end_ns", "registration_end_ns", "current_lock_wait_ns", "memory_append_ns"] { assert!(event[field].is_u64(), "missing {field}: {event}"); } }
    }
    for sequence in sequences.values_mut() { sequence.sort_unstable(); assert_eq!(*sequence, (1..=sequence.len() as u64).collect::<Vec<_>>()); }
    for event in &events {
        if let Some(parent) = event["parent_id"].as_u64().filter(|id| *id != 0) { assert!(by_id.contains_key(&parent), "missing parent {parent} for {event}"); }
        if event["event"] == "timer_spawn" { let timer = by_id[&event["timer_id"].as_u64().unwrap()]; assert_eq!(timer["event"], "timer_run"); assert_eq!(event["log_uid"], timer["log_uid"]); }
    }
    let mut summary_counts = HashMap::<String, u64>::new();
    for window in lines.iter().filter(|line| line["event"] == "window") { for count in window["counts"].as_array().unwrap() { *summary_counts.entry(count["event"].as_str().unwrap().into()).or_default() += count["completed_records"].as_u64().unwrap(); } }
    for (event, count) in actual_counts { assert_eq!(summary_counts[&event], count); }
    assert!(lines.iter().any(|line| line["event"] == "progress" && line["owner_id"] == 0 && line["commit_lock_waiting"] == 0));
    if phase == "write" {
        for kind in ["root_append", "root_flush", "commit_lock", "sync_call", "timer_spawn", "timer_run", "checkpoint_rotation", "file_split", "backup_rename", "root_confirm"] { assert!(events.iter().any(|line| line["event"] == kind), "missing kind {kind}"); }
        assert!(events.iter().any(|line| line["entry_uid_fast_path"] == 1));
        assert!(events.iter().any(|line| line["pending_commit_outcome"] == "commit_ok"));
        assert!(events.iter().any(|line| line["batch_wait_outcome"] == "received_ok"));
    }
    if phase == "shutdown_pending" {
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event"], "root_flush");
        assert_eq!(events[0]["outcome"], "cancelled");
        assert!(events[0].get("start_ns").is_none(), "unpolled operation has no execution start");
    }
}
fn payload(uid: u128) -> Vec<u8> { let len = [64, 256, 8192, 16_384][uid as usize % 4]; (0..len).map(|i| (uid as u8).wrapping_add(i as u8)).collect() }
fn read_json(path: &Path) -> Vec<Value> { let text = fs::read_to_string(path).unwrap(); assert!(text.ends_with('\n')); text.lines().map(|line| serde_json::from_str(line).unwrap()).collect() }
fn percentile(values: &[u64], p: usize) -> u64 { values[(values.len() * p).div_ceil(100).saturating_sub(1)] }
fn temp(label: &str) -> PathBuf { env::temp_dir().join(format!("pi_store_trace_{label}_{}_{}", std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())) }
