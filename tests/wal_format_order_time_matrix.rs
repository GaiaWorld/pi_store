//! WAL 二进制格式、物理顺序、数据内容和同步时间的真实验收矩阵。
//!
//! 父进程按固定顺序运行 1、4、64、256 个在途刷新配置；每个配置使用独立写入进程、验证
//! 进程、真实多线程运行时、真实 `CommitLogger`、真实 `LogFile`、同步文件写和本地文件系统。
//! 配置之间绝不并行。配置内部的异步屏障只用于制造被测提交锁的真实竞争，不代表多个独立
//! 测试并发执行。
//!
//! 验证进程不依赖被测加载器解释磁盘格式，而是从文件尾向前独立解析每个块：负载后固定跟随
//! 8 字节小端毫秒时间戳、4 字节小端 CRC32 和 4 字节小端负载长度。随后再用公开重播接口读取
//! 同一份 WAL，并把重播方法、事务编号、负载和时间戳逐项与原始磁盘事实交叉核对。

use std::{
    collections::HashSet,
    convert::TryInto,
    env, fs,
    fs::OpenOptions,
    future::Future,
    io::{Read, Seek, SeekFrom, Write},
    panic,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_lock::Barrier;
use crc32fast::Hasher;
use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder},
    startup_global_time_loop, AsyncRuntime,
};
use pi_async_transaction::AsyncCommitLog;
use pi_guid::Guid;
use pi_store::{
    commit_logger::{CommitLogger, CommitLoggerBuilder, CommitLoggerExt},
    log_store::log_file::{LogFile, LogMethod},
};

type TestResult<T = ()> = Result<T, String>;

const TEST_NAME: &str = "test_real_wal_format_order_content_time_matrix_is_exact";
const PHASE_ENV: &str = "PI_STORE_WAL_FORMAT_PHASE";
const CONFIG_ENV: &str = "PI_STORE_WAL_FORMAT_CONFIG";
const ROOT_ENV: &str = "PI_STORE_WAL_FORMAT_ROOT";
const EVIDENCE_FILE: &str = "writer-evidence.bin";
const EVIDENCE_MAGIC: &[u8; 8] = b"PIWFMT01";
const BLOCK_HEADER_LEN: usize = 16;
const COMMIT_ENTRY_OVERHEAD: usize = 1 + 2 + 16 + 4;
const DIRECTORY_LIMIT: u64 = 64 * 1024 * 1024;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(45);
const FULL_TIMEOUT: Duration = Duration::from_secs(180);
const RUNTIME_TIMEOUT: Duration = Duration::from_secs(35);
const MAX_FLUSH_LATENCY_MICROS: u64 = 20_000_000;

#[derive(Clone, Copy, Debug)]
enum Phase {
    WriteAndExit,
    Verify,
}

impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::WriteAndExit => "write-and-exit",
            Self::Verify => "verify",
        }
    }

    fn parse(value: &str) -> TestResult<Self> {
        match value {
            "write-and-exit" => Ok(Self::WriteAndExit),
            "verify" => Ok(Self::Verify),
            other => Err(format!("unknown WAL format phase: {other}")),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum PayloadProfile {
    Boundary,
    Tiny,
    Medium,
    Mixed,
}

#[derive(Clone, Copy, Debug)]
struct MatrixConfig {
    id: &'static str,
    index: usize,
    workers: usize,
    clock_tick_ms: usize,
    block_limit: usize,
    concurrency: usize,
    chunks: usize,
    checkpoint_mask: u64,
    profile: PayloadProfile,
    stale_timer_probe: bool,
}

const CONFIGS: [MatrixConfig; 4] = [
    MatrixConfig {
        id: "FORMAT-C1-BOUNDARY",
        index: 1,
        workers: 1,
        // 粗粒度时钟让“大小提交完成 -> 追加后继 -> 旧计时器到期”有稳定的可观察窗口。
        clock_tick_ms: 1_000,
        block_limit: 2 * 1024,
        concurrency: 1,
        chunks: 6,
        checkpoint_mask: 0,
        profile: PayloadProfile::Boundary,
        stale_timer_probe: true,
    },
    MatrixConfig {
        id: "FORMAT-C4-WAITER-CHECKPOINT",
        index: 2,
        workers: 4,
        clock_tick_ms: 10,
        block_limit: 32 * 1024,
        concurrency: 4,
        chunks: 5,
        checkpoint_mask: 1 << 2,
        profile: PayloadProfile::Tiny,
        stale_timer_probe: false,
    },
    MatrixConfig {
        id: "FORMAT-C64-SIZE",
        index: 3,
        workers: 4,
        clock_tick_ms: 10,
        block_limit: 2 * 1024,
        concurrency: 64,
        chunks: 4,
        checkpoint_mask: (1 << 1) | (1 << 2),
        profile: PayloadProfile::Medium,
        stale_timer_probe: false,
    },
    MatrixConfig {
        id: "FORMAT-C256-MIXED-CHECKPOINT",
        index: 4,
        workers: 4,
        clock_tick_ms: 10,
        block_limit: 8 * 1024,
        concurrency: 256,
        chunks: 3,
        checkpoint_mask: (1 << 0) | (1 << 1),
        profile: PayloadProfile::Mixed,
        stale_timer_probe: false,
    },
];

#[derive(Clone, Debug)]
struct ExpectedRecord {
    sequence: usize,
    uid: u128,
    payload: Vec<u8>,
    file_index: usize,
}

#[derive(Clone, Debug)]
struct DirectRecord {
    method_tag: u8,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[derive(Debug)]
struct FlushOutcome {
    sequence: usize,
    latency_micros: u64,
    result: std::io::Result<()>,
}

#[derive(Clone, Debug)]
struct ReplayRecord {
    method_tag: u8,
    timestamp_ms: u64,
    uid: u128,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct WriterEvidence {
    config_index: u64,
    start_ms: u64,
    end_ms: u64,
    elapsed_micros: u64,
    flush_p50_micros: u64,
    flush_p99_micros: u64,
    flush_max_micros: u64,
    direct_commit_not_before_ms: u64,
    stale_probe_flush_not_before_ms: u64,
    stale_probe_size_before: u64,
    stale_probe_size_after_wait: u64,
    chunk_release_ms: Vec<u64>,
}

#[derive(Clone, Debug)]
struct RawEntry {
    method_tag: u8,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct RawBlock {
    file_index: usize,
    timestamp_ms: u64,
    payload_len: usize,
    entries: Vec<RawEntry>,
}

#[derive(Debug)]
struct RawWal {
    files: Vec<usize>,
    blocks: Vec<RawBlock>,
    encoded_bytes: usize,
}

/// 串行验证真实 WAL 的编码、归属、顺序、内容、时间戳和有界批量效率。
#[test]
fn test_real_wal_format_order_content_time_matrix_is_exact() {
    if let Ok(phase) = env::var(PHASE_ENV) {
        install_abort_on_any_panic();
        let phase = Phase::parse(&phase).unwrap_or_else(|error| panic!("{error}"));
        let config_id = env::var(CONFIG_ENV).expect("format child must receive config id");
        let config = config_by_id(&config_id).unwrap_or_else(|error| panic!("{error}"));
        let root =
            PathBuf::from(env::var_os(ROOT_ENV).expect("format child must receive evidence root"));

        run_on_runtime(config, RUNTIME_TIMEOUT, move |rt| async move {
            match phase {
                Phase::WriteAndExit => write_matrix(&rt, &root, config).await,
                Phase::Verify => verify_matrix(&rt, &root, config).await,
            }
        })
        .unwrap_or_else(|error| panic!("{} {} failed: {error}", config.id, phase.name()));

        if matches!(phase, Phase::WriteAndExit) {
            // 所有刷新均已通过 Sync(true) 返回。直接退出刻意跳过后台整理任务和运行时析构，
            // 为验证进程保留“已持久化、未确认”的真实崩溃恢复边界。
            std::process::exit(0);
        }
        return;
    }

    let started = Instant::now();
    for config in CONFIGS {
        let root = unique_temp_root(config);
        fs::create_dir_all(&root).expect("creating WAL format evidence root must succeed");

        for phase in [Phase::WriteAndExit, Phase::Verify] {
            let remaining = FULL_TIMEOUT.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                panic!(
                    "WAL format matrix exceeded {FULL_TIMEOUT:?}; evidence is preserved at {root:?}",
                );
            }
            let timeout = std::cmp::min(PROCESS_TIMEOUT, remaining);
            if let Err(error) = run_phase_process(phase, config, &root, timeout) {
                panic!(
                    "{} {} failed; evidence is preserved at {:?}: {error}",
                    config.id,
                    phase.name(),
                    root,
                );
            }
        }

        fs::remove_dir_all(&root).expect("cleaning WAL format evidence root must succeed");
    }

    assert!(
        started.elapsed() <= FULL_TIMEOUT,
        "WAL format matrix exceeded {FULL_TIMEOUT:?}: {:?}",
        started.elapsed(),
    );
}

async fn write_matrix(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
    config: MatrixConfig,
) -> TestResult<()> {
    let wall_started = Instant::now();
    let start_ms = unix_ms()?;
    let direct_commit_not_before_ms = write_direct_log_file(rt, &root.join("direct-wal")).await?;
    let wal_path = root.join("commit-wal");
    let logger = build_logger(rt, &wal_path, config).await?;
    let expected = expected_records(config);
    let mut latencies = Vec::with_capacity(expected.len());
    let mut chunk_release_ms = Vec::with_capacity(config.chunks);
    let mut current_file_index = 1usize;
    let mut stale_probe_flush_not_before_ms = 0u64;
    let mut stale_probe_size_before = 0u64;
    let mut stale_probe_size_after_wait = 0u64;

    for chunk in 0..config.chunks {
        let start = chunk * config.concurrency;
        let end = start + config.concurrency;
        let mut handles = Vec::with_capacity(config.concurrency);
        for record in &expected[start..end] {
            let handle = logger
                .append(Guid(record.uid), record.payload.clone())
                .await
                .map_err(|error| {
                    format!(
                        "{} append sequence {} failed: {error}",
                        config.id, record.sequence,
                    )
                })?;
            let expected_handle = record.sequence + 1;
            if handle != expected_handle {
                return Err(format!(
                    "{} append handle mismatch for sequence {}: expected {}, observed {handle}",
                    config.id, record.sequence, expected_handle,
                ));
            }
            let observed_file = logger.check_point_of(Guid(record.uid)).await;
            if observed_file != Some(current_file_index) {
                return Err(format!(
                    "{} checkpoint mapping mismatch before flush for sequence {}: expected {}, observed {:?}",
                    config.id, record.sequence, current_file_index, observed_file,
                ));
            }
            handles.push((record.sequence, handle));
        }

        // 序号 0 的记录恰好使 2 KiB 块达到阈值，先由大小所有者同步并留下尚未到期的旧
        // 计时器；序号 1 只追加、不请求刷新。等待旧计时器完整到期期间，物理文件大小必须
        // 不变。这个判据直接识别“旧定时器同步（timer sync）抢走后继批次”，不依赖吞吐波动。
        if config.stale_timer_probe && chunk == 1 {
            stale_probe_size_before = numeric_directory_size(&wal_path)?;
            rt.timeout(config.clock_tick_ms * 2 + 100).await;
            stale_probe_size_after_wait = numeric_directory_size(&wal_path)?;
            if stale_probe_size_after_wait != stale_probe_size_before {
                return Err(format!(
                    "{} stale timer wrote the unflushed successor: bytes before={}, after={}",
                    config.id, stale_probe_size_before, stale_probe_size_after_wait,
                ));
            }
            stale_probe_flush_not_before_ms = unix_ms()?;
        }

        let checkpoint = checkpoint_after(config, chunk);
        let (release_ms, mut chunk_latencies, checkpoint_index) =
            run_flush_chunk(rt, &logger, config, chunk, handles, checkpoint).await?;
        chunk_release_ms.push(release_ms);
        latencies.append(&mut chunk_latencies);

        if checkpoint {
            let expected_new_index = current_file_index + 1;
            if checkpoint_index != Some(expected_new_index) {
                return Err(format!(
                    "{} checkpoint result after chunk {chunk}: expected {}, observed {:?}",
                    config.id, expected_new_index, checkpoint_index,
                ));
            }
            current_file_index = expected_new_index;
        } else if checkpoint_index.is_some() {
            return Err(format!(
                "{} unexpectedly returned a checkpoint for chunk {chunk}: {checkpoint_index:?}",
                config.id,
            ));
        }
    }

    // 让所有由大小提交创建、但已经失效的计时任务完整退出，再读取最终文件事实。它们只能
    // 做已提交水位检查，不能在测试结束后补写一个额外块。
    rt.timeout(config.clock_tick_ms * 2 + 100).await;

    if logger.append_total_count() != expected.len() {
        return Err(format!(
            "{} append count mismatch: expected {}, observed {}",
            config.id,
            expected.len(),
            logger.append_total_count(),
        ));
    }
    if logger.confirm_total_count() != 0 {
        return Err(format!(
            "{} confirm count must remain zero before recovery, observed {}",
            config.id,
            logger.confirm_total_count(),
        ));
    }
    let waiting = logger.waiting_confirm_count().await;
    if waiting != expected.len() {
        return Err(format!(
            "{} waiting-confirm count mismatch: expected {}, observed {waiting}",
            config.id,
            expected.len(),
        ));
    }
    for record in &expected {
        let observed = logger.check_point_of(Guid(record.uid)).await;
        if observed != Some(record.file_index) {
            return Err(format!(
                "{} final checkpoint mapping mismatch for sequence {}: expected {}, observed {:?}",
                config.id, record.sequence, record.file_index, observed,
            ));
        }
    }

    let expected_next_file = 2 + config.checkpoint_mask.count_ones() as usize;
    let observed_next_file = logger.current_check_point().await;
    if observed_next_file != expected_next_file {
        return Err(format!(
            "{} next physical file index mismatch: expected {expected_next_file}, observed {observed_next_file}",
            config.id,
        ));
    }

    latencies.sort_unstable();
    let flush_p50_micros = percentile(&latencies, 50);
    let flush_p99_micros = percentile(&latencies, 99);
    let flush_max_micros = latencies.last().copied().unwrap_or(0);
    if flush_max_micros > MAX_FLUSH_LATENCY_MICROS {
        return Err(format!(
            "{} flush latency exceeded hard bound: max={}us, bound={}us",
            config.id, flush_max_micros, MAX_FLUSH_LATENCY_MICROS,
        ));
    }

    let end_ms = unix_ms()?;
    let evidence = WriterEvidence {
        config_index: config.index as u64,
        start_ms,
        end_ms,
        elapsed_micros: duration_micros_u64(wall_started.elapsed()),
        flush_p50_micros,
        flush_p99_micros,
        flush_max_micros,
        direct_commit_not_before_ms,
        stale_probe_flush_not_before_ms,
        stale_probe_size_before,
        stale_probe_size_after_wait,
        chunk_release_ms,
    };
    write_evidence(&root.join(EVIDENCE_FILE), &evidence)?;

    let directory_bytes = directory_size(root)?;
    if directory_bytes > DIRECTORY_LIMIT {
        return Err(format!(
            "{} evidence directory exceeded {} bytes: {directory_bytes}",
            config.id, DIRECTORY_LIMIT,
        ));
    }
    Ok(())
}

async fn write_direct_log_file(rt: &MultiTaskRuntime<()>, path: &Path) -> TestResult<u64> {
    let log = LogFile::open(rt.clone(), path, 32, 16 * 1024 * 1024, None)
        .await
        .map_err(|error| format!("opening direct LogFile failed: {error}"))?;
    let records = direct_records();
    let mut last_handle = 0usize;
    for (index, record) in records.iter().enumerate() {
        let method = match record.method_tag {
            0 => LogMethod::Remove,
            1 => LogMethod::PlainAppend,
            other => return Err(format!("invalid direct method tag in fixture: {other}")),
        };
        let value = record.value.as_deref().unwrap_or(&[0xa5, 0x5a]);
        let handle = log.append(method, &record.key, value);
        if handle != index + 1 {
            return Err(format!(
                "direct LogFile handle mismatch: expected {}, observed {handle}",
                index + 1,
            ));
        }
        last_handle = handle;
    }

    let not_before_ms = unix_ms()?;
    log.commit(last_handle, true, false, None)
        .await
        .map_err(|error| format!("committing direct LogFile block failed: {error}"))?;
    if log.commited_uid() != last_handle {
        return Err(format!(
            "direct committed watermark mismatch: expected {last_handle}, observed {}",
            log.commited_uid(),
        ));
    }
    let committed_size = numeric_directory_size(path)?;

    // 已提交真实句柄与非法控制值 0 都必须是无 I/O 的幂等操作，不能获得“同步当前块”的
    // 特权。这里没有后继块，所以只验证磁盘字节和水位完全不变。
    log.commit(last_handle, true, false, None)
        .await
        .map_err(|error| format!("repeating direct commit failed: {error}"))?;
    log.commit(0, true, false, None)
        .await
        .map_err(|error| format!("committing invalid handle zero failed: {error}"))?;
    let repeated_size = numeric_directory_size(path)?;
    if repeated_size != committed_size || log.commited_uid() != last_handle {
        return Err(format!(
            "direct idempotent commits changed state: bytes {committed_size}->{repeated_size}, watermark {}",
            log.commited_uid(),
        ));
    }
    Ok(not_before_ms)
}

async fn run_flush_chunk(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    config: MatrixConfig,
    chunk: usize,
    handles: Vec<(usize, usize)>,
    checkpoint: bool,
) -> TestResult<(u64, Vec<u64>, Option<usize>)> {
    let participant_count = handles.len() + usize::from(checkpoint) + 1;
    let gate = Arc::new(Barrier::new(participant_count));
    let (outcome_tx, outcome_rx) = async_channel::bounded(handles.len());
    let checkpoint_channel =
        checkpoint.then(|| async_channel::bounded::<std::io::Result<usize>>(1));

    // 偶数批次先排入检查点任务，奇数批次后排入；屏障保证它们与 flush 同时获得运行资格，
    // 两种排队方向共同覆盖辅助提交所有者、大小所有者和等待者的现实竞争顺序。
    if checkpoint && chunk & 1 == 0 {
        spawn_checkpoint(
            rt,
            logger,
            gate.clone(),
            checkpoint_channel.as_ref().unwrap().0.clone(),
        )?;
    }

    for order_index in flush_order(handles.len(), chunk) {
        let (sequence, handle) = handles[order_index];
        let task_logger = logger.clone();
        let task_gate = gate.clone();
        let task_sender = outcome_tx.clone();
        rt.spawn(async move {
            task_gate.wait().await;
            let started = Instant::now();
            let result = task_logger.flush(handle).await;
            let outcome = FlushOutcome {
                sequence,
                latency_micros: duration_micros_u64(started.elapsed()),
                result,
            };
            let _ = task_sender.send(outcome).await;
        })
        .map_err(|error| {
            format!(
                "{} spawning flush for sequence {sequence} failed: {error:?}",
                config.id,
            )
        })?;
    }
    drop(outcome_tx);

    if checkpoint && chunk & 1 == 1 {
        spawn_checkpoint(
            rt,
            logger,
            gate.clone(),
            checkpoint_channel.as_ref().unwrap().0.clone(),
        )?;
    }

    let release_ms = unix_ms()?;
    gate.wait().await;

    let mut seen = HashSet::with_capacity(handles.len());
    let mut latencies = Vec::with_capacity(handles.len());
    for _ in 0..handles.len() {
        let outcome = outcome_rx.recv().await.map_err(|error| {
            format!(
                "{} flush result channel closed in chunk {chunk}: {error}",
                config.id
            )
        })?;
        if !seen.insert(outcome.sequence) {
            return Err(format!(
                "{} duplicate flush result for sequence {}",
                config.id, outcome.sequence,
            ));
        }
        outcome.result.map_err(|error| {
            format!(
                "{} flush sequence {} in chunk {chunk} failed: {error}",
                config.id, outcome.sequence,
            )
        })?;
        latencies.push(outcome.latency_micros);
    }
    if seen.len() != handles.len() {
        return Err(format!(
            "{} flush completion count mismatch in chunk {chunk}: expected {}, observed {}",
            config.id,
            handles.len(),
            seen.len(),
        ));
    }

    let checkpoint_index = if let Some((_sender, receiver)) = checkpoint_channel {
        Some(
            receiver
                .recv()
                .await
                .map_err(|error| {
                    format!(
                        "{} checkpoint channel closed in chunk {chunk}: {error}",
                        config.id
                    )
                })?
                .map_err(|error| {
                    format!("{} checkpoint in chunk {chunk} failed: {error}", config.id)
                })?,
        )
    } else {
        None
    };
    Ok((release_ms, latencies, checkpoint_index))
}

fn spawn_checkpoint(
    rt: &MultiTaskRuntime<()>,
    logger: &CommitLogger,
    gate: Arc<Barrier>,
    sender: async_channel::Sender<std::io::Result<usize>>,
) -> TestResult<()> {
    let task_logger = logger.clone();
    rt.spawn(async move {
        gate.wait().await;
        let result = task_logger.append_check_point().await;
        let _ = sender.send(result).await;
    })
    .map_err(|error| format!("spawning concurrent checkpoint failed: {error:?}"))?;
    Ok(())
}

async fn verify_matrix(
    rt: &MultiTaskRuntime<()>,
    root: &Path,
    config: MatrixConfig,
) -> TestResult<()> {
    let evidence = read_evidence(&root.join(EVIDENCE_FILE))?;
    if evidence.config_index != config.index as u64 {
        return Err(format!(
            "{} evidence config mismatch: expected {}, observed {}",
            config.id, config.index, evidence.config_index,
        ));
    }
    if evidence.start_ms > evidence.end_ms {
        return Err(format!(
            "{} writer time bounds are reversed: {} > {}",
            config.id, evidence.start_ms, evidence.end_ms,
        ));
    }
    if evidence.chunk_release_ms.len() != config.chunks {
        return Err(format!(
            "{} chunk timestamp count mismatch: expected {}, observed {}",
            config.id,
            config.chunks,
            evidence.chunk_release_ms.len(),
        ));
    }
    if evidence.flush_max_micros > MAX_FLUSH_LATENCY_MICROS {
        return Err(format!(
            "{} persisted maximum flush latency exceeds hard bound: {}us",
            config.id, evidence.flush_max_micros,
        ));
    }
    if config.stale_timer_probe {
        if evidence.stale_probe_flush_not_before_ms == 0 {
            return Err(format!(
                "{} stale timer probe timestamp is missing",
                config.id
            ));
        }
        if evidence.stale_probe_size_before != evidence.stale_probe_size_after_wait {
            return Err(format!(
                "{} stale timer probe changed bytes before flush: {} -> {}",
                config.id, evidence.stale_probe_size_before, evidence.stale_probe_size_after_wait,
            ));
        }
    }

    let direct = parse_raw_wal(&root.join("direct-wal"))?;
    verify_direct_raw(config, &evidence, &direct)?;

    let expected = expected_records(config);
    let wal_path = root.join("commit-wal");
    let raw = parse_raw_wal(&wal_path)?;
    verify_commit_raw(config, &evidence, &expected, &raw)?;

    // 先复制未修改的持久化事实，再让公开重播接口因 CRC 错误走真实失败路径。翻转的是第一条
    // PlainAppend 的值字节，字段边界仍合法，因此失败必须来自校验而不是测试制造的解析越界。
    let corrupt_path = root.join("corrupt-wal");
    copy_numeric_wal(&wal_path, &corrupt_path)?;
    corrupt_first_value_byte(&corrupt_path)?;
    verify_corrupt_replay(rt, &corrupt_path, config).await?;

    let logger = build_logger(rt, &wal_path, config).await?;
    let observed = Arc::new(Mutex::new(Vec::<ReplayRecord>::new()));
    let end_markers = Arc::new(AtomicUsize::new(0));
    let callback_observed = observed.clone();
    let callback_end_markers = end_markers.clone();
    let replay_result = logger
        .start_replay_ext::<Vec<u8>, _>(Arc::new(
            move |entry: Option<(Guid, LogMethod, u64, Vec<u8>)>| {
                match entry {
                    Some((uid, method, timestamp_ms, payload)) => {
                        callback_observed
                            .lock()
                            .map_err(|_| {
                                std::io::Error::other("replay observation mutex poisoned")
                            })?
                            .push(ReplayRecord {
                                method_tag: method_tag(method),
                                timestamp_ms,
                                uid: uid.0,
                                payload,
                            });
                    }
                    None => {
                        callback_end_markers.fetch_add(1, Ordering::SeqCst);
                    }
                }
                Ok(())
            },
        ))
        .await
        .map_err(|error| format!("{} replay with timestamps failed: {error}", config.id))?;

    let expected_replay_bytes = expected
        .iter()
        .map(|record| 16 + record.payload.len())
        .sum::<usize>();
    if replay_result != (expected.len(), expected_replay_bytes) {
        return Err(format!(
            "{} replay totals mismatch: expected ({}, {}), observed {:?}",
            config.id,
            expected.len(),
            expected_replay_bytes,
            replay_result,
        ));
    }
    if end_markers.load(Ordering::SeqCst) != 1 {
        return Err(format!(
            "{} replay end marker count mismatch: expected 1, observed {}",
            config.id,
            end_markers.load(Ordering::SeqCst),
        ));
    }
    let observed = observed
        .lock()
        .map_err(|_| "replay observation mutex poisoned".to_owned())?
        .clone();
    verify_replay_matches_raw(config, &expected, &raw, &observed)?;

    logger
        .finish_replay()
        .await
        .map_err(|error| format!("{} finish_replay failed: {error}", config.id))?;
    if logger.waiting_confirm_count().await != 0
        || logger.append_total_count() != 0
        || logger.confirm_total_count() != 0
    {
        return Err(format!(
            "{} observation-only replay changed transaction counters: waiting={}, append={}, confirm={}",
            config.id,
            logger.waiting_confirm_count().await,
            logger.append_total_count(),
            logger.confirm_total_count(),
        ));
    }

    let directory_bytes = directory_size(root)?;
    if directory_bytes > DIRECTORY_LIMIT * 2 {
        return Err(format!(
            "{} verification directory exceeded {} bytes after corruption copy: {directory_bytes}",
            config.id,
            DIRECTORY_LIMIT * 2,
        ));
    }

    let physical_delays = raw
        .blocks
        .iter()
        .zip(evidence.chunk_release_ms.iter())
        .map(|(block, released)| block.timestamp_ms.saturating_sub(*released))
        .collect::<Vec<_>>();
    let physical_p99_ms = percentile(&physical_delays, 99);
    let bytes_per_sync = raw.encoded_bytes as f64 / raw.blocks.len() as f64;
    println!(
        "WAL_FORMAT_RESULT config={} workers={} concurrency={} records={} files={} physical_syncs={} encoded_bytes={} bytes_per_sync={:.1} writer_elapsed_us={} flush_p50_us={} flush_p99_us={} flush_max_us={} physical_commit_p99_ms={} format_exact=true order_exact=true content_exact=true time_exact=true serial=true",
        config.id,
        config.workers,
        config.concurrency,
        expected.len(),
        raw.files.len(),
        raw.blocks.len(),
        raw.encoded_bytes,
        bytes_per_sync,
        evidence.elapsed_micros,
        evidence.flush_p50_micros,
        evidence.flush_p99_micros,
        evidence.flush_max_micros,
        physical_p99_ms,
    );
    Ok(())
}

fn verify_direct_raw(
    config: MatrixConfig,
    evidence: &WriterEvidence,
    raw: &RawWal,
) -> TestResult<()> {
    let expected = direct_records();
    if raw.files != vec![1] || raw.blocks.len() != 1 {
        return Err(format!(
            "{} direct LogFile topology mismatch: files={:?}, blocks={}",
            config.id,
            raw.files,
            raw.blocks.len(),
        ));
    }
    let block = &raw.blocks[0];
    verify_timestamp_bounds(
        config.id,
        "direct block",
        block.timestamp_ms,
        evidence.direct_commit_not_before_ms,
        evidence.end_ms,
    )?;
    if block.entries.len() != expected.len() {
        return Err(format!(
            "{} direct entry count mismatch: expected {}, observed {}",
            config.id,
            expected.len(),
            block.entries.len(),
        ));
    }
    for (index, (actual, expected)) in block.entries.iter().zip(expected.iter()).enumerate() {
        if actual.method_tag != expected.method_tag
            || actual.key != expected.key
            || actual.value != expected.value
        {
            return Err(format!(
                "{} direct entry {index} mismatch: method {} vs {}, key_len {} vs {}, value_len {:?} vs {:?}",
                config.id,
                actual.method_tag,
                expected.method_tag,
                actual.key.len(),
                expected.key.len(),
                actual.value.as_ref().map(Vec::len),
                expected.value.as_ref().map(Vec::len),
            ));
        }
    }
    let expected_payload = expected.iter().map(encoded_direct_entry_len).sum::<usize>();
    if block.payload_len != expected_payload
        || raw.encoded_bytes != expected_payload + BLOCK_HEADER_LEN
    {
        return Err(format!(
            "{} direct encoded length mismatch: expected payload/total {}/{}, observed {}/{}",
            config.id,
            expected_payload,
            expected_payload + BLOCK_HEADER_LEN,
            block.payload_len,
            raw.encoded_bytes,
        ));
    }
    Ok(())
}

fn verify_commit_raw(
    config: MatrixConfig,
    evidence: &WriterEvidence,
    expected: &[ExpectedRecord],
    raw: &RawWal,
) -> TestResult<()> {
    let checkpoint_count = config.checkpoint_mask.count_ones() as usize;
    let expected_files = (1..=checkpoint_count + 1).collect::<Vec<_>>();
    if raw.files != expected_files {
        return Err(format!(
            "{} WAL file sequence mismatch: expected {:?}, observed {:?}",
            config.id, expected_files, raw.files,
        ));
    }
    if raw.blocks.len() != config.chunks {
        return Err(format!(
            "{} physical sync count mismatch: expected one block per logical batch ({}), observed {}",
            config.id,
            config.chunks,
            raw.blocks.len(),
        ));
    }

    let mut previous_timestamp = 0u64;
    let mut flattened = Vec::with_capacity(expected.len());
    for (chunk, block) in raw.blocks.iter().enumerate() {
        if block.entries.len() != config.concurrency {
            return Err(format!(
                "{} block {chunk} entry count mismatch: expected {}, observed {}",
                config.id,
                config.concurrency,
                block.entries.len(),
            ));
        }
        verify_timestamp_bounds(
            config.id,
            &format!("block {chunk}"),
            block.timestamp_ms,
            evidence.chunk_release_ms[chunk],
            evidence.end_ms,
        )?;
        if chunk > 0 && block.timestamp_ms < previous_timestamp {
            return Err(format!(
                "{} physical block timestamps decreased at block {chunk}: {} -> {}",
                config.id, previous_timestamp, block.timestamp_ms,
            ));
        }
        previous_timestamp = block.timestamp_ms;
        for entry in &block.entries {
            flattened.push((block, entry));
        }
    }

    if flattened.len() != expected.len() {
        return Err(format!(
            "{} physical entry count mismatch: expected {}, observed {}",
            config.id,
            expected.len(),
            flattened.len(),
        ));
    }
    for (position, ((block, actual), expected)) in flattened.iter().zip(expected.iter()).enumerate()
    {
        if actual.method_tag != 1 {
            return Err(format!(
                "{} sequence {} method tag mismatch: expected PlainAppend(1), observed {}",
                config.id, expected.sequence, actual.method_tag,
            ));
        }
        let expected_key = expected.uid.to_le_bytes();
        if actual.key.as_slice() != expected_key {
            return Err(format!(
                "{} physical key mismatch at position {position}, sequence {}",
                config.id, expected.sequence,
            ));
        }
        if actual.value.as_deref() != Some(expected.payload.as_slice()) {
            return Err(format!(
                "{} physical value mismatch at position {position}, sequence {}: expected {} bytes, observed {:?}",
                config.id,
                expected.sequence,
                expected.payload.len(),
                actual.value.as_ref().map(Vec::len),
            ));
        }
        if block.file_index != expected.file_index {
            return Err(format!(
                "{} checkpoint ownership mismatch for sequence {}: expected file {}, observed file {}",
                config.id, expected.sequence, expected.file_index, block.file_index,
            ));
        }
        if config.stale_timer_probe && expected.sequence == 1 {
            verify_timestamp_bounds(
                config.id,
                "stale-timer successor block",
                block.timestamp_ms,
                evidence.stale_probe_flush_not_before_ms,
                evidence.end_ms,
            )?;
        }
    }

    let expected_payload_bytes = expected
        .iter()
        .map(|record| COMMIT_ENTRY_OVERHEAD + record.payload.len())
        .sum::<usize>();
    let expected_encoded_bytes = expected_payload_bytes + config.chunks * BLOCK_HEADER_LEN;
    if raw.encoded_bytes != expected_encoded_bytes {
        return Err(format!(
            "{} encoded byte total mismatch: expected {expected_encoded_bytes}, observed {}",
            config.id, raw.encoded_bytes,
        ));
    }
    Ok(())
}

fn verify_replay_matches_raw(
    config: MatrixConfig,
    expected: &[ExpectedRecord],
    raw: &RawWal,
    observed: &[ReplayRecord],
) -> TestResult<()> {
    if observed.len() != expected.len() {
        return Err(format!(
            "{} replay entry count mismatch: expected {}, observed {}",
            config.id,
            expected.len(),
            observed.len(),
        ));
    }
    let raw_timestamps = raw
        .blocks
        .iter()
        .flat_map(|block| std::iter::repeat_n(block.timestamp_ms, block.entries.len()))
        .collect::<Vec<_>>();
    for (position, ((actual, expected), raw_timestamp)) in observed
        .iter()
        .zip(expected.iter())
        .zip(raw_timestamps.iter())
        .enumerate()
    {
        if actual.method_tag != 1
            || actual.uid != expected.uid
            || actual.payload != expected.payload
            || actual.timestamp_ms != *raw_timestamp
        {
            return Err(format!(
                "{} replay mismatch at position {position}, sequence {}: method={}, uid={:#034x}, payload_len={}, time={} (raw time={})",
                config.id,
                expected.sequence,
                actual.method_tag,
                actual.uid,
                actual.payload.len(),
                actual.timestamp_ms,
                raw_timestamp,
            ));
        }
    }
    Ok(())
}

async fn verify_corrupt_replay(
    rt: &MultiTaskRuntime<()>,
    path: &Path,
    config: MatrixConfig,
) -> TestResult<()> {
    let logger = build_logger(rt, path, config).await?;
    let result = logger
        .start_replay_ext::<Vec<u8>, _>(Arc::new(|_entry| Ok(())))
        .await;
    let error = match result {
        Ok(result) => {
            return Err(format!(
                "{} corrupted WAL unexpectedly replayed successfully: {result:?}",
                config.id,
            ));
        }
        Err(error) => error,
    };
    let message = error.to_string().to_ascii_lowercase();
    if !message.contains("checksum") {
        return Err(format!(
            "{} corrupted WAL returned wrong error class: {error}",
            config.id,
        ));
    }
    logger.finish_replay().await.map_err(|finish_error| {
        format!(
            "{} finish_replay after checksum error failed: {finish_error}",
            config.id,
        )
    })?;
    Ok(())
}

fn parse_raw_wal(path: &Path) -> TestResult<RawWal> {
    let files = numeric_log_files(path)?;
    if files.is_empty() {
        return Err(format!(
            "WAL directory contains no numeric log files: {path:?}"
        ));
    }
    let mut blocks = Vec::new();
    let mut encoded_bytes = 0usize;
    let mut file_indexes = Vec::with_capacity(files.len());

    for (file_index, file_path) in files {
        let bytes = fs::read(&file_path)
            .map_err(|error| format!("reading raw WAL file {file_path:?} failed: {error}"))?;
        encoded_bytes = encoded_bytes
            .checked_add(bytes.len())
            .ok_or_else(|| "raw WAL byte count overflow".to_owned())?;
        file_indexes.push(file_index);
        let mut file_blocks = parse_raw_file(file_index, &file_path, &bytes)?;
        blocks.append(&mut file_blocks);
    }
    Ok(RawWal {
        files: file_indexes,
        blocks,
        encoded_bytes,
    })
}

fn parse_raw_file(file_index: usize, path: &Path, bytes: &[u8]) -> TestResult<Vec<RawBlock>> {
    let mut cursor = bytes.len();
    let mut reversed = Vec::new();
    while cursor > 0 {
        if cursor < BLOCK_HEADER_LEN {
            return Err(format!(
                "raw WAL file {path:?} has {} trailing bytes, shorter than a block header",
                cursor,
            ));
        }
        let header_offset = cursor - BLOCK_HEADER_LEN;
        let timestamp_ms = read_u64_le(&bytes[header_offset..header_offset + 8])?;
        let checksum = read_u32_le(&bytes[header_offset + 8..header_offset + 12])?;
        let payload_len = read_u32_le(&bytes[header_offset + 12..cursor])? as usize;
        if payload_len == 0 || payload_len > header_offset {
            return Err(format!(
                "raw WAL file {path:?} has invalid payload length {payload_len} at header {header_offset}",
            ));
        }
        let payload_offset = header_offset - payload_len;
        let payload = &bytes[payload_offset..header_offset];
        let mut hasher = Hasher::new();
        hasher.update(payload);
        hasher.update(&timestamp_ms.to_le_bytes());
        let calculated = hasher.finalize();
        if calculated != checksum {
            return Err(format!(
                "raw WAL CRC32 mismatch in {path:?} at block offset {payload_offset}: header={checksum}, calculated={calculated}",
            ));
        }
        let entries = parse_payload(path, payload_offset, payload)?;
        reversed.push(RawBlock {
            file_index,
            timestamp_ms,
            payload_len,
            entries,
        });
        cursor = payload_offset;
    }
    reversed.reverse();
    Ok(reversed)
}

fn parse_payload(path: &Path, block_offset: usize, payload: &[u8]) -> TestResult<Vec<RawEntry>> {
    let mut entries = Vec::new();
    let mut cursor = 0usize;
    while cursor < payload.len() {
        let entry_offset = cursor;
        let method_tag = *payload.get(cursor).ok_or_else(|| {
            format!(
                "missing method tag in {path:?} at {}",
                block_offset + cursor
            )
        })?;
        cursor += 1;
        if method_tag > 1 {
            return Err(format!(
                "invalid method tag {method_tag} in {path:?} at {}",
                block_offset + entry_offset,
            ));
        }
        let key_len = take_u16(payload, &mut cursor, path, block_offset)? as usize;
        let key = take_bytes(payload, &mut cursor, key_len, path, block_offset)?.to_vec();
        let value = if method_tag == 0 {
            None
        } else {
            let value_len = take_u32(payload, &mut cursor, path, block_offset)? as usize;
            Some(take_bytes(payload, &mut cursor, value_len, path, block_offset)?.to_vec())
        };
        entries.push(RawEntry {
            method_tag,
            key,
            value,
        });
    }
    if cursor != payload.len() {
        return Err(format!(
            "payload parser did not end at block boundary in {path:?}: {cursor} != {}",
            payload.len(),
        ));
    }
    Ok(entries)
}

fn expected_records(config: MatrixConfig) -> Vec<ExpectedRecord> {
    let mut records = Vec::with_capacity(config.chunks * config.concurrency);
    let mut file_index = 1usize;
    for chunk in 0..config.chunks {
        for slot in 0..config.concurrency {
            let sequence = chunk * config.concurrency + slot;
            let uid = 0xf17e_0000_0000_0000_0000_0000_0000_0000u128
                | ((config.index as u128) << 96)
                | sequence as u128;
            let len = payload_len(config.profile, sequence);
            records.push(ExpectedRecord {
                sequence,
                uid,
                payload: deterministic_payload(config.index as u64, sequence, len),
                file_index,
            });
        }
        if checkpoint_after(config, chunk) {
            file_index += 1;
        }
    }
    records
}

fn payload_len(profile: PayloadProfile, sequence: usize) -> usize {
    match profile {
        // CommitLogger 的固定键使每条记录有 23 字节编码开销。因此 2024/2025/2026 分别是
        // 2 KiB 块阈值的差 1、恰好达到、超过 1 字节边界。
        PayloadProfile::Boundary => [2_025, 1, 2_024, 2_026, 65_535, 1024 * 1024][sequence % 6],
        PayloadProfile::Tiny => [1, 7, 16, 31][sequence % 4],
        PayloadProfile::Medium => [64, 255, 1024, 4096][sequence % 4],
        PayloadProfile::Mixed => [1, 7, 31, 255, 2048, 8192, 32_768, 65_536][sequence % 8],
    }
}

fn deterministic_payload(config_index: u64, sequence: usize, len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64
        ^ config_index.rotate_left(17)
        ^ (sequence as u64).wrapping_mul(0xa076_1d64_78bd_642f);
    let mut payload = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(state as u8);
    }
    if len >= 8 {
        payload[..8].copy_from_slice(b"PIWALFMT");
    }
    if len >= 16 {
        payload[8..16].copy_from_slice(&(sequence as u64).to_le_bytes());
    }
    if len >= 20 {
        payload[16..20].copy_from_slice(&(len as u32).to_le_bytes());
    }
    if len >= 28 {
        payload[len - 8..].copy_from_slice((!(sequence as u64)).to_le_bytes().as_ref());
    }
    payload
}

fn direct_records() -> Vec<DirectRecord> {
    let mut maximum_key = Vec::with_capacity(u16::MAX as usize);
    for index in 0..u16::MAX as usize {
        maximum_key.push((index as u8).wrapping_mul(31).wrapping_add(7));
    }
    vec![
        DirectRecord {
            method_tag: 1,
            key: Vec::new(),
            value: Some(Vec::new()),
        },
        DirectRecord {
            method_tag: 0,
            key: vec![0, 0xff, 0],
            value: None,
        },
        DirectRecord {
            method_tag: 1,
            key: maximum_key,
            value: Some(vec![0]),
        },
        DirectRecord {
            method_tag: 1,
            key: b"binary\0key".to_vec(),
            value: Some(vec![0, 1, 0xff, 0, 0x7f]),
        },
    ]
}

fn encoded_direct_entry_len(record: &DirectRecord) -> usize {
    let value_len = record.value.as_ref().map_or(0, |value| 4 + value.len());
    1 + 2 + record.key.len() + value_len
}

fn checkpoint_after(config: MatrixConfig, chunk: usize) -> bool {
    config.checkpoint_mask & (1u64 << chunk) != 0
}

fn flush_order(count: usize, chunk: usize) -> Vec<usize> {
    match chunk % 4 {
        0 => (0..count).collect(),
        1 => (0..count).rev().collect(),
        2 => (0..count).step_by(2).chain((1..count).step_by(2)).collect(),
        _ => {
            let mut order = Vec::with_capacity(count);
            let mut low = 0usize;
            let mut high = count;
            while low < high {
                high -= 1;
                order.push(high);
                if low < high {
                    order.push(low);
                    low += 1;
                }
            }
            order
        }
    }
}

fn method_tag(method: LogMethod) -> u8 {
    match method {
        LogMethod::Remove => 0,
        LogMethod::PlainAppend => 1,
    }
}

fn verify_timestamp_bounds(
    config_id: &str,
    label: &str,
    timestamp_ms: u64,
    not_before_ms: u64,
    not_after_ms: u64,
) -> TestResult<()> {
    if timestamp_ms < not_before_ms || timestamp_ms > not_after_ms {
        Err(format!(
            "{config_id} {label} timestamp outside write interval: {timestamp_ms} not in [{not_before_ms}, {not_after_ms}]",
        ))
    } else {
        Ok(())
    }
}

fn write_evidence(path: &Path, evidence: &WriterEvidence) -> TestResult<()> {
    let mut bytes = Vec::with_capacity(8 + 11 * 8 + 4 + evidence.chunk_release_ms.len() * 8);
    bytes.extend_from_slice(EVIDENCE_MAGIC);
    for value in [
        evidence.config_index,
        evidence.start_ms,
        evidence.end_ms,
        evidence.elapsed_micros,
        evidence.flush_p50_micros,
        evidence.flush_p99_micros,
        evidence.flush_max_micros,
        evidence.direct_commit_not_before_ms,
        evidence.stale_probe_flush_not_before_ms,
        evidence.stale_probe_size_before,
        evidence.stale_probe_size_after_wait,
    ] {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes.extend_from_slice(&(evidence.chunk_release_ms.len() as u32).to_le_bytes());
    for timestamp in &evidence.chunk_release_ms {
        bytes.extend_from_slice(&timestamp.to_le_bytes());
    }
    fs::write(path, bytes).map_err(|error| format!("writing evidence {path:?} failed: {error}"))
}

fn read_evidence(path: &Path) -> TestResult<WriterEvidence> {
    let bytes = fs::read(path)
        .map_err(|error| format!("reading writer evidence {path:?} failed: {error}"))?;
    let mut cursor = 0usize;
    let magic = take_bytes(&bytes, &mut cursor, EVIDENCE_MAGIC.len(), path, 0)?;
    if magic != EVIDENCE_MAGIC {
        return Err(format!("invalid writer evidence magic in {path:?}"));
    }
    let config_index = take_u64(&bytes, &mut cursor, path, 0)?;
    let start_ms = take_u64(&bytes, &mut cursor, path, 0)?;
    let end_ms = take_u64(&bytes, &mut cursor, path, 0)?;
    let elapsed_micros = take_u64(&bytes, &mut cursor, path, 0)?;
    let flush_p50_micros = take_u64(&bytes, &mut cursor, path, 0)?;
    let flush_p99_micros = take_u64(&bytes, &mut cursor, path, 0)?;
    let flush_max_micros = take_u64(&bytes, &mut cursor, path, 0)?;
    let direct_commit_not_before_ms = take_u64(&bytes, &mut cursor, path, 0)?;
    let stale_probe_flush_not_before_ms = take_u64(&bytes, &mut cursor, path, 0)?;
    let stale_probe_size_before = take_u64(&bytes, &mut cursor, path, 0)?;
    let stale_probe_size_after_wait = take_u64(&bytes, &mut cursor, path, 0)?;
    let count = take_u32(&bytes, &mut cursor, path, 0)? as usize;
    let mut chunk_release_ms = Vec::with_capacity(count);
    for _ in 0..count {
        let value = take_bytes(&bytes, &mut cursor, 8, path, 0)?;
        chunk_release_ms.push(read_u64_le(value)?);
    }
    if cursor != bytes.len() {
        return Err(format!(
            "writer evidence {path:?} has {} trailing bytes",
            bytes.len() - cursor,
        ));
    }
    Ok(WriterEvidence {
        config_index,
        start_ms,
        end_ms,
        elapsed_micros,
        flush_p50_micros,
        flush_p99_micros,
        flush_max_micros,
        direct_commit_not_before_ms,
        stale_probe_flush_not_before_ms,
        stale_probe_size_before,
        stale_probe_size_after_wait,
        chunk_release_ms,
    })
}

fn copy_numeric_wal(source: &Path, destination: &Path) -> TestResult<()> {
    fs::create_dir_all(destination)
        .map_err(|error| format!("creating corrupted WAL directory failed: {error}"))?;
    for (_index, source_file) in numeric_log_files(source)? {
        let name = source_file
            .file_name()
            .ok_or_else(|| format!("numeric WAL file has no name: {source_file:?}"))?;
        let destination_file = destination.join(name);
        fs::copy(&source_file, &destination_file).map_err(|error| {
            format!("copying WAL file {source_file:?} to {destination_file:?} failed: {error}",)
        })?;
    }
    Ok(())
}

fn corrupt_first_value_byte(path: &Path) -> TestResult<()> {
    let first_file = numeric_log_files(path)?
        .into_iter()
        .find(|(_, file)| file.metadata().map(|meta| meta.len() > 0).unwrap_or(false))
        .ok_or_else(|| format!("corruption fixture has no nonempty WAL file: {path:?}"))?
        .1;
    let value_offset = COMMIT_ENTRY_OVERHEAD as u64;
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&first_file)
        .map_err(|error| format!("opening WAL corruption target {first_file:?} failed: {error}"))?;
    file.seek(SeekFrom::Start(value_offset))
        .map_err(|error| format!("seeking WAL corruption target failed: {error}"))?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte)
        .map_err(|error| format!("reading WAL corruption byte failed: {error}"))?;
    byte[0] ^= 0x01;
    file.seek(SeekFrom::Start(value_offset))
        .map_err(|error| format!("reseeking WAL corruption target failed: {error}"))?;
    file.write_all(&byte)
        .map_err(|error| format!("writing WAL corruption byte failed: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("syncing WAL corruption target failed: {error}"))
}

fn numeric_log_files(path: &Path) -> TestResult<Vec<(usize, PathBuf)>> {
    let mut files = Vec::new();
    let entries = fs::read_dir(path)
        .map_err(|error| format!("reading WAL directory {path:?} failed: {error}"))?;
    for entry in entries {
        let entry =
            entry.map_err(|error| format!("reading WAL directory entry failed: {error}"))?;
        let file_type = entry
            .file_type()
            .map_err(|error| format!("reading WAL entry type failed: {error}"))?;
        if !file_type.is_file() {
            return Err(format!(
                "unexpected non-file entry in WAL directory: {:?}",
                entry.path()
            ));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|name| format!("non-UTF-8 WAL filename: {name:?}"))?;
        if name.len() != 9 || !name.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(format!("unexpected WAL filename in {path:?}: {name}"));
        }
        let index = name
            .parse::<usize>()
            .map_err(|error| format!("parsing WAL filename {name} failed: {error}"))?;
        files.push((index, entry.path()));
    }
    files.sort_by_key(|(index, _)| *index);
    for pair in files.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(format!(
                "duplicate numeric WAL index {} in {path:?}",
                pair[0].0
            ));
        }
    }
    Ok(files)
}

fn numeric_directory_size(path: &Path) -> TestResult<u64> {
    numeric_log_files(path)?
        .into_iter()
        .try_fold(0u64, |total, (_, file)| {
            let len = file
                .metadata()
                .map_err(|error| format!("reading WAL metadata for {file:?} failed: {error}"))?
                .len();
            total
                .checked_add(len)
                .ok_or_else(|| format!("WAL directory byte count overflow at {file:?}"))
        })
}

fn directory_size(path: &Path) -> TestResult<u64> {
    let mut total = 0u64;
    let mut pending = vec![path.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("reading evidence directory {directory:?} failed: {error}"))?
        {
            let entry = entry.map_err(|error| format!("reading evidence entry failed: {error}"))?;
            let metadata = entry
                .metadata()
                .map_err(|error| format!("reading evidence metadata failed: {error}"))?;
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if metadata.is_file() {
                total = total
                    .checked_add(metadata.len())
                    .ok_or_else(|| "evidence directory byte count overflow".to_owned())?;
            }
        }
    }
    Ok(total)
}

fn take_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    path: &Path,
    base_offset: usize,
) -> TestResult<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| format!("binary cursor overflow in {path:?}"))?;
    if end > bytes.len() {
        return Err(format!(
            "truncated binary field in {path:?} at {}: need {len} bytes, remaining {}",
            base_offset + *cursor,
            bytes.len().saturating_sub(*cursor),
        ));
    }
    let result = &bytes[*cursor..end];
    *cursor = end;
    Ok(result)
}

fn take_u16(bytes: &[u8], cursor: &mut usize, path: &Path, base: usize) -> TestResult<u16> {
    let field = take_bytes(bytes, cursor, 2, path, base)?;
    Ok(u16::from_le_bytes(field.try_into().unwrap()))
}

fn take_u32(bytes: &[u8], cursor: &mut usize, path: &Path, base: usize) -> TestResult<u32> {
    let field = take_bytes(bytes, cursor, 4, path, base)?;
    read_u32_le(field)
}

fn take_u64(bytes: &[u8], cursor: &mut usize, path: &Path, base: usize) -> TestResult<u64> {
    let field = take_bytes(bytes, cursor, 8, path, base)?;
    read_u64_le(field)
}

fn read_u32_le(bytes: &[u8]) -> TestResult<u32> {
    bytes
        .try_into()
        .map(u32::from_le_bytes)
        .map_err(|_| format!("expected 4 bytes, observed {}", bytes.len()))
}

fn read_u64_le(bytes: &[u8]) -> TestResult<u64> {
    bytes
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| format!("expected 8 bytes, observed {}", bytes.len()))
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = (sorted.len() * percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn duration_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn unix_ms() -> TestResult<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .map_err(|error| format!("system time precedes UNIX_EPOCH: {error}"))
}

async fn build_logger(
    rt: &MultiTaskRuntime<()>,
    path: &Path,
    config: MatrixConfig,
) -> TestResult<CommitLogger> {
    CommitLoggerBuilder::new(rt.clone(), path)
        .log_block_limit(config.block_limit)
        .delay_timeout(10)
        .log_file_limit(2 * 1024 * 1024 * 1024)
        .collect_interval(5 * 60 * 1000)
        .build()
        .await
        .map_err(|error| format!("{} building real CommitLogger failed: {error}", config.id))
}

fn config_by_id(id: &str) -> TestResult<MatrixConfig> {
    CONFIGS
        .iter()
        .copied()
        .find(|config| config.id == id)
        .ok_or_else(|| format!("unknown WAL format configuration: {id}"))
}

fn install_abort_on_any_panic() {
    let default_hook = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}

fn run_on_runtime<T, F, Fut>(config: MatrixConfig, timeout: Duration, build: F) -> TestResult<T>
where
    T: Send + 'static,
    F: FnOnce(MultiTaskRuntime<()>) -> Fut,
    Fut: Future<Output = TestResult<T>> + Send + 'static,
{
    let _time_loop = startup_global_time_loop(config.clock_tick_ms as u64);
    let rt = MultiTaskRuntimeBuilder::default()
        .init_worker_size(config.workers)
        .build();
    let future = build(rt.clone());
    let (result_tx, result_rx) = bounded(1);
    rt.spawn(async move {
        let _ = result_tx.send(future.await);
    })
    .map_err(|error| format!("{} spawning matrix phase failed: {error:?}", config.id))?;
    result_rx
        .recv_timeout(timeout)
        .map_err(|error| format!("{} matrix phase exceeded {timeout:?}: {error}", config.id))?
}

fn run_phase_process(
    phase: Phase,
    config: MatrixConfig,
    root: &Path,
    timeout: Duration,
) -> TestResult<()> {
    let executable = env::current_exe()
        .map_err(|error| format!("locating WAL format test executable failed: {error}"))?;
    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(TEST_NAME)
        .arg("--nocapture")
        .arg("--test-threads=1")
        .env(PHASE_ENV, phase.name())
        .env(CONFIG_ENV, config.id)
        .env(ROOT_ENV, root)
        .spawn()
        .map_err(|error| {
            format!(
                "spawning {} {} child failed: {error}",
                config.id,
                phase.name(),
            )
        })?;
    let status = wait_for_child(&mut child, timeout)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "{} {} child exited with {status}",
            config.id,
            phase.name(),
        ))
    }
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> TestResult<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("checking WAL format child failed: {error}"))?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("WAL format child exceeded {timeout:?}"));
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn unique_temp_root(config: MatrixConfig) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow UNIX_EPOCH")
        .as_nanos();
    env::temp_dir().join(format!(
        "pi_store_wal_format_{}_{}_{}",
        config.index,
        std::process::id(),
        nanos,
    ))
}
