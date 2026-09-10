//! 有界 WAL 诊断；输出均带 `PI_STORE_WAL_TRACE`，以 `event` 分类。
//!
//! 业务锁内只填写局部记录，最外层暂存区在所有业务锁释放后统一提交。不输出逐次开始
//! 记录，不逐 poll 计时。诊断成功不等于事务成功；丢失、未收尾和输出错误均使完整性失败。

use std::{env, fmt::Write as _, fs::{File, OpenOptions}, future::Future,
    io::{self, BufWriter, Write}, ops::{Deref, DerefMut}, path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, atomic::{AtomicBool, AtomicU64, Ordering}},
    thread, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use crossbeam_channel::{bounded, Receiver, Sender};

pub const KEYWORD: &str = "PI_STORE_WAL_TRACE";
const FOOTER_RESERVE: u64 = 4096;
const CLOSED: u64 = 1 << 63;
const MAX_LOGGERS: usize = 1024;
const MAX_PATH: usize = 65_536;
const PATH_BUDGET: u64 = 8 * 1024 * 1024;
const RECORDS_PER_OPERATION: usize = 32;
const HELPERS_PER_OPERATION: usize = 8;
const OPERATION_SLOTS: usize = 1024;

/// 启动时配置。分配资源和创建文件可能阻塞，必须从不持有业务锁的控制线程启动。
#[derive(Clone, Debug)]
pub struct TraceConfig {
    pub directory: PathBuf,
    pub run_id: String,
    /// 日志窗口，不是 WAL 延时、全局时钟或 worker 休眠间隔。
    pub window_ms: u64,
    pub capacity: usize,
    pub max_bytes: u64,
}
impl TraceConfig {
    pub fn new(directory: impl Into<PathBuf>, run_id: impl Into<String>) -> Self {
        Self { directory: directory.into(), run_id: run_id.into(), window_ms: 1000,
            capacity: 65_536, max_bytes: 2 * 1024 * 1024 * 1024 }
    }
    fn validate(&self) -> io::Result<()> {
        if self.run_id.is_empty() || self.run_id.len() > 128 || !(10..=60_000).contains(&self.window_ms)
            || !(16..=262_144).contains(&self.capacity) || !(65_536..=16 * 1024 * 1024 * 1024).contains(&self.max_bytes) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid trace configuration"));
        }
        if !self.directory.is_dir() { return Err(io::Error::new(io::ErrorKind::NotFound, "trace directory must exist")); }
        Ok(())
    }
}
/// `complete` 只证明当前观测范围内的记录收口，不证明 WAL 数据正确。
#[derive(Clone, Debug)]
pub struct TraceReport {
    pub path: PathBuf, pub produced: u64, pub written: u64, pub dropped: u64,
    pub omitted: u64, pub outstanding: u64, pub complete: bool, pub reason: String,
}
static SESSION: OnceLock<Result<Option<Arc<Session>>, String>> = OnceLock::new();

/// 根日志实例构建前启动。重复调用返回 AlreadyExists，失败也占用初始化槽，不静默换配置。
pub fn start(config: TraceConfig) -> io::Result<PathBuf> {
    let mut initialized = false;
    let result = SESSION.get_or_init(|| { initialized = true; Session::create(config).map(Some).map_err(|e| e.to_string()) });
    if !initialized { return Err(io::Error::new(io::ErrorKind::AlreadyExists, "trace already initialized")); }
    match result { Ok(Some(s)) => Ok(s.path.clone()), Err(e) => Err(io::Error::other(e.clone())), Ok(None) => Err(io::Error::other("trace disabled")) }
}
/// 控制线程停止接纳新观测，等待已接纳操作及其子任务结束、队列排空。不得阻塞 WAL worker。
/// 超时不取消业务或写线程，可再次调用；强杀进程或缺少结束记录不能声称诊断完整。
pub fn shutdown(timeout: Duration) -> io::Result<TraceReport> {
    let session = match SESSION.get() { Some(Ok(Some(s))) => s, _ => return Err(io::Error::new(io::ErrorKind::NotFound, "trace not active")) };
    session.operations.fetch_or(CLOSED, Ordering::AcqRel);
    if let Some(worker) = session.worker.get() { worker.unpark(); }
    let start = Instant::now();
    loop {
        if let Some(report) = session.report.lock().unwrap_or_else(|p| p.into_inner()).clone() { return Ok(report); }
        if start.elapsed() >= timeout { return Err(io::Error::new(io::ErrorKind::TimedOut, "trace still draining")); }
        thread::sleep(Duration::from_millis(1));
    }
}
fn session_from_env(wal_path: &Path) -> Result<Option<Arc<Session>>, String> {
    // 默认与首个根 WAL 目录并列，不在 WAL 目录内创建诊断文件。
    let dir = match env::var_os("PI_STORE_WAL_TRACE_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => wal_path.canonicalize().map_err(|e| e.to_string())?.parent()
            .ok_or_else(|| "WAL root has no parent for trace directory".to_string())?.join("pi_store_wal_trace"),
    };
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let mut c = TraceConfig::new(PathBuf::from(dir), env::var("PI_STORE_WAL_TRACE_RUN_ID").unwrap_or_else(|_| "unspecified".into()));
    for (key, field) in [("PI_STORE_WAL_TRACE_WINDOW_MS", &mut c.window_ms), ("PI_STORE_WAL_TRACE_MAX_BYTES", &mut c.max_bytes)] {
        if let Ok(value) = env::var(key) { *field = value.parse().map_err(|_| format!("invalid {key}"))?; }
    }
    if let Ok(value) = env::var("PI_STORE_WAL_TRACE_CAPACITY") { c.capacity = value.parse().map_err(|_| "invalid capacity")?; }
    Session::create(c).map(Some).map_err(|e| e.to_string())
}
struct Session {
    path: PathBuf, directory: PathBuf, origin: Instant, session_id: String,
    sender: Sender<Record>, pool_tx: Sender<Box<Storage>>, pool_rx: Receiver<Box<Storage>>,
    operations: AtomicU64, emit_gate: AtomicU64, next_id: AtomicU64,
    produced: AtomicU64, dropped_full: AtomicU64, dropped_stopped: AtomicU64,
    omitted: AtomicU64, retained_paths: AtomicU64, failed: AtomicBool,
    loggers: Mutex<Vec<Arc<LoggerState>>>, worker: OnceLock<thread::Thread>, report: Mutex<Option<TraceReport>>,
}
impl Session {
    fn create(config: TraceConfig) -> io::Result<Arc<Self>> {
        config.validate()?;
        let epoch = SystemTime::now().duration_since(UNIX_EPOCH).map_err(io::Error::other)?.as_nanos();
        let pid = std::process::id();
        let directory = config.directory.canonicalize()?;
        let session_id = format!("{pid}-{epoch}");
        let path = directory.join(format!("wal-trace-{session_id}.jsonl"));
        let file = OpenOptions::new().write(true).create_new(true).open(&path)?;
        let (sender, receiver) = bounded(config.capacity);
        let (pool_tx, pool_rx) = bounded(OPERATION_SLOTS);
        for _ in 0..OPERATION_SLOTS { pool_tx.try_send(Box::new(Storage::new())).map_err(|_| io::Error::other("trace pool initialization"))?; }
        let s = Arc::new(Self { path, directory, origin: Instant::now(), session_id, sender, pool_tx, pool_rx,
            operations: AtomicU64::new(0), emit_gate: AtomicU64::new(0), next_id: AtomicU64::new(1),
            produced: AtomicU64::new(0), dropped_full: AtomicU64::new(0), dropped_stopped: AtomicU64::new(0),
            omitted: AtomicU64::new(0), retained_paths: AtomicU64::new(0), failed: AtomicBool::new(false),
            loggers: Mutex::new(Vec::new()), worker: OnceLock::new(), report: Mutex::new(None) });
        let mut writer = BufWriter::with_capacity(65_536, file);
        let header = format!("{{\"keyword\":\"{KEYWORD}\",\"event\":\"session_start\",\"schema_version\":2,\"session_id\":{},\"pid\":{pid},\"epoch_ns\":\"{epoch}\",\"run_id\":{},\"pi_store\":\"{}\",\"build_id_declared\":{},\"window_ms\":{},\"capacity\":{},\"record_bytes\":{},\"operation_slots\":{OPERATION_SLOTS},\"records_per_operation\":{RECORDS_PER_OPERATION},\"helpers_per_operation\":{HELPERS_PER_OPERATION},\"path_budget_bytes\":{PATH_BUDGET},\"max_bytes\":{},\"global_clock_ms_verified\":null,\"worker_sleep_ms_verified\":null}}\n",
            json(&s.session_id), json(&config.run_id), env!("CARGO_PKG_VERSION"), env::var("PI_STORE_WAL_TRACE_BUILD_ID").ok().as_deref().map(json).unwrap_or_else(|| "null".into()),
            config.window_ms, config.capacity, std::mem::size_of::<Record>(), config.max_bytes);
        writer.write_all(header.as_bytes())?; writer.flush()?;
        let bytes = header.len() as u64;
        let window_ms = config.window_ms;
        let max_bytes = config.max_bytes;
        let cloned = s.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        thread::Builder::new().name("pi-store-wal-trace".into()).spawn(move || {
            let _ = cloned.worker.set(thread::current()); let _ = ready_tx.send(());
            write_windows(cloned, receiver, writer, config, bytes);
        })?;
        ready_rx.recv().map_err(|_| io::Error::other("trace writer startup failed"))?;
        eprintln!("{KEYWORD} event=session_start path={:?} window_ms={window_ms} max_bytes={max_bytes}", s.path);
        Ok(s)
    }
    fn now(&self) -> u64 { self.origin.elapsed().as_nanos().min(u64::MAX as u128) as u64 }
    fn admit(&self, inherited: bool) -> bool {
        let mut old = self.operations.load(Ordering::Acquire);
        loop {
            if self.failed.load(Ordering::Relaxed) || (!inherited && old & CLOSED != 0) { return false; }
            if old & !CLOSED == CLOSED - 1 || (inherited && old & !CLOSED == 0) { return false; }
            match self.operations.compare_exchange_weak(old, old + 1, Ordering::AcqRel, Ordering::Acquire) { Ok(_) => return true, Err(actual) => old = actual }
        }
    }
    fn emit(&self, record: Record) {
        // 关闭位与在途数共用原子，避免收口后又有生产者进入。这里只在业务锁外调用。
        let mut old = self.emit_gate.load(Ordering::Acquire);
        loop {
            if old & CLOSED != 0 { return; }
            match self.emit_gate.compare_exchange_weak(old, old + 1, Ordering::AcqRel, Ordering::Acquire) { Ok(_) => break, Err(actual) => old = actual }
        }
        self.produced.fetch_add(1, Ordering::Relaxed);
        if self.failed.load(Ordering::Acquire) { self.dropped_stopped.fetch_add(1, Ordering::Relaxed); }
        else { match self.sender.try_send(record) {
            Ok(()) => (),
            Err(crossbeam_channel::TrySendError::Full(_)) => { self.dropped_full.fetch_add(1, Ordering::Relaxed); }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => { self.dropped_stopped.fetch_add(1, Ordering::Relaxed); }
        } }
        self.emit_gate.fetch_sub(1, Ordering::Release);
    }
}

struct LoggerState {
    id: u64, metadata: String, lock_sequence: AtomicU64,
    waiting: AtomicU64, peak: AtomicU64, owner: AtomicU64, owner_since: AtomicU64, owner_stage: AtomicU64,
}
/// 只登记根 CommitLogger，独立 LogFile 不扩入观测范围。
#[derive(Clone, Default)]
pub(crate) struct Logger(Option<(Arc<Session>, Arc<LoggerState>)>);
impl Logger {
    pub(crate) fn register(path: &Path, initial_file: &Path, runtime: usize, block: usize, delay: usize, file_limit: u64, collect: usize) -> Self {
        let result = SESSION.get_or_init(|| {
            let result = session_from_env(path);
            if let Err(error) = &result { eprintln!("{KEYWORD} event=start_error error={error}"); }
            result
        });
        let Ok(Some(s)) = result else { return Self::default() };
        let absolute = match path.canonicalize() { Ok(p) => p, Err(_) => { s.omitted.fetch_add(1, Ordering::Relaxed); return Self::default(); } };
        if absolute == s.directory || s.directory.starts_with(&absolute) || absolute.as_os_str().len() > MAX_PATH || initial_file.as_os_str().len() > MAX_PATH {
            s.omitted.fetch_add(1, Ordering::Relaxed); return Self::default();
        }
        if !s.admit(false) { return Self::default(); }
        let mut loggers = s.loggers.lock().unwrap_or_else(|p| p.into_inner());
        if loggers.len() == MAX_LOGGERS { s.omitted.fetch_add(1, Ordering::Relaxed); s.operations.fetch_sub(1, Ordering::Release); return Self::default(); }
        let id = loggers.len() as u64 + 1;
        let metadata = format!("{{\"keyword\":\"{KEYWORD}\",\"event\":\"root_logger_ready\",\"session_id\":{},\"logger_id\":{id},\"runtime_id\":{runtime},\"wal_path\":{},\"initial_file\":{},\"block_limit\":{block},\"delay_ms\":{delay},\"checkpoint_limit\":{file_limit},\"collect_interval_ms\":{collect},\"auto_split\":false}}\n", json(&s.session_id), path_json(&absolute), path_json(initial_file));
        let state = Arc::new(LoggerState { id, metadata, lock_sequence: AtomicU64::new(0),
            waiting: AtomicU64::new(0), peak: AtomicU64::new(0), owner: AtomicU64::new(0),
            owner_since: AtomicU64::new(0), owner_stage: AtomicU64::new(0) }); loggers.push(state.clone());
        s.operations.fetch_sub(1, Ordering::Release);
        Self(Some((s.clone(), state)))
    }
    pub(crate) fn now(&self) -> u64 { self.0.as_ref().map_or(0, |(s, _)| s.now()) }
}

pub(crate) mod f {
    pub const START: usize = 0; pub const TOTAL: usize = 1; pub const UID: usize = 2;
    pub const CID_LO: usize = 3; pub const CID_HI: usize = 4; pub const PARENT: usize = 5;
    pub const WAIT: usize = 6; pub const HOLD: usize = 7; pub const BEFORE: usize = 8;
    pub const AFTER: usize = 9; pub const SEQ: usize = 10; pub const BYTES: usize = 11;
    pub const MAX_UID: usize = 12; pub const WRITTEN: usize = 13; pub const POLLS: usize = 14;
    pub const NOTIFY_START: usize = 15; pub const NOTIFY_NS: usize = 16; pub const TARGETS: usize = 17;
    pub const NOTIFIED: usize = 18; pub const SEND_FAILED: usize = 19; pub const ENTRY_FAST: usize = 20;
    pub const RECV_START: usize = 21; pub const RECV_NS: usize = 22; pub const TIMEOUT: usize = 23;
    pub const CALL_START: usize = 24; pub const FIRST_POLL: usize = 25; pub const CHECK_WAIT: usize = 26;
    pub const CHECK_ACQUIRED: usize = 27; pub const APPEND_START: usize = 28; pub const APPEND_END: usize = 29;
    pub const REGISTER_END: usize = 30; pub const CURRENT_WAIT: usize = 31; pub const MEMORY_NS: usize = 32;
    pub const OLD_FILE: usize = 33; pub const NEW_FILE: usize = 34; pub const MARK_OLD: usize = 35;
    pub const PENDING_UID: usize = 36; pub const PENDING_NS: usize = 37; pub const FOUND: usize = 38;
    pub const PENDING_COUNT: usize = 39; pub const ROTATION1: usize = 40; pub const ROTATION2: usize = 41;
    pub const RETURN_COUNT: usize = 42; pub const RETURN_BYTES: usize = 43; pub const BUFFERED: usize = 44;
    pub const CONFIRMED: usize = 45; pub const OS_ERROR: usize = 46;
    pub const TIMER_ID: usize = 47;
}
const N: usize = 48;
const NAMES: [&str; N] = ["start_ns", "total_ns", "log_uid", "cid_lo", "cid_hi", "parent_id", "lock_wait_ns", "lock_hold_ns", "committed_before", "committed_at_acquire", "lock_seq", "block_bytes", "batch_max_log_uid", "written_bytes", "poll_count", "notify_start_ns", "notify_total_ns", "notify_target_count", "notify_completed_count", "notify_send_failed_count", "entry_uid_fast_path", "batch_wait_start_ns", "batch_wait_ns", "requested_delay_ms", "call_start_ns", "first_poll_ns", "checkpoint_wait_start_ns", "checkpoint_acquired_ns", "append_start_ns", "append_end_ns", "registration_end_ns", "current_lock_wait_ns", "memory_append_ns", "old_file_index", "new_file_index", "mark_old_confirmed", "pending_log_uid", "pending_commit_ns", "registration_found", "checkpoint_pending_before", "rotation_id_1", "rotation_id_2", "returned_records", "returned_bytes", "buffered_at_start", "confirm_ok_count", "os_error", "timer_id"];
const KINDS: [&str; 12] = ["root_append", "root_flush", "commit_lock", "sync_call", "timer_spawn", "timer_run", "checkpoint_rotation", "file_split", "backup_rename", "root_confirm", "replay", "maintenance"];
pub(crate) mod kind {
    pub const APPEND: usize = 0; pub const FLUSH: usize = 1; pub const COMMIT: usize = 2;
    pub const SYNC: usize = 3; pub const SPAWN: usize = 4; pub const TIMER: usize = 5;
    pub const ROTATE: usize = 6; pub const SPLIT: usize = 7; pub const BACKUP: usize = 8;
    pub const CONFIRM: usize = 9; pub const REPLAY: usize = 10; pub const MAINTENANCE: usize = 11;
}
enum PathData { Owned(PathBuf), Shared(Arc<PathBuf>) }
struct RetainedPath { data: PathData, session: Arc<Session>, bytes: u64 }
impl Drop for RetainedPath { fn drop(&mut self) { self.session.retained_paths.fetch_sub(self.bytes, Ordering::Relaxed); } }
impl RetainedPath { fn path(&self) -> &Path { match &self.data { PathData::Owned(p) => p, PathData::Shared(p) => p } } }
pub(crate) struct Record {
    logger: u64, id: u64, kind: usize, mask: u64, values: [u64; N],
    pub(crate) outcome: &'static str, pub(crate) trigger: &'static str, pub(crate) mode: &'static str,
    pub(crate) stage: &'static str, pub(crate) pending: &'static str, pub(crate) recv: &'static str,
    error: Option<io::ErrorKind>, paths: [Option<RetainedPath>; 2],
}
impl Record {
    fn new(kind: usize) -> Self { Self { logger: 0, id: 0, kind, mask: 0, values: [0; N], outcome: "cancelled", trigger: "", mode: "", stage: "", pending: "", recv: "", error: None, paths: [None, None] } }
    pub(crate) fn set(&mut self, index: usize, value: u64) { self.values[index] = value; self.mask |= 1 << index; }
    pub(crate) fn get(&self, index: usize) -> Option<u64> { (self.mask & (1 << index) != 0).then_some(self.values[index]) }
    pub(crate) fn error(&mut self, e: &io::Error) { self.outcome = "error"; self.error = Some(e.kind()); if let Some(code) = e.raw_os_error() { self.set(f::OS_ERROR, code as u64); } }
    pub(crate) fn cid(&mut self, cid: u128) { self.set(f::CID_LO, cid as u64); self.set(f::CID_HI, (cid >> 64) as u64); }
}
struct Storage { rows: Vec<Record>, receivers: Vec<async_channel::Receiver<HelperPacket>> }
impl Storage { fn new() -> Self { Self { rows: Vec::with_capacity(RECORDS_PER_OPERATION), receivers: Vec::with_capacity(HELPERS_PER_OPERATION) } } }
/// 原容量 1 私有通道携带的结果；两条记录为硬上限。
pub(crate) struct HelperPacket { pub(crate) result: io::Result<()>, rows: [Option<Record>; 2] }
/// 必须先于业务 guard 声明。普通模式持有预分配池的独占槽；helper 模式只能回传不能直出。
pub(crate) struct Trace {
    logger: Logger, storage: Option<Box<Storage>>, small: [Option<Record>; 2],
    primary: Option<Record>, admitted: bool, helper: bool, lost: u64,
    pub(crate) flush_fast: Option<bool>, pub(crate) recv_start: Option<u64>,
    pub(crate) recv_ns: Option<u64>, pub(crate) recv_outcome: &'static str,
}
impl Trace {
    pub(crate) fn disabled() -> Self { Self { logger: Logger::default(), storage: None, small: [None, None], primary: None, admitted: false, helper: false, lost: 0, flush_fast: None, recv_start: None, recv_ns: None, recv_outcome: "" } }
    pub(crate) fn new(logger: &Logger, kind: usize) -> Self { Self::make(logger, kind, false, false) }
    fn make(logger: &Logger, kind: usize, inherited: bool, helper: bool) -> Self {
        let mut t = Self::disabled(); t.helper = helper;
        let Some((s, _)) = &logger.0 else { return t };
        if !s.admit(inherited) { return t; }
        t.logger = logger.clone(); t.admitted = true;
        if !helper { match s.pool_rx.try_recv() { Ok(storage) => t.storage = Some(storage), Err(_) => { s.omitted.fetch_add(1, Ordering::Relaxed); t.finish_admission(); return t; } } }
        if kind != kind::MAINTENANCE { t.primary = Some(t.record(kind, 0, "")); }
        t
    }
    pub(crate) fn active(&self) -> bool { self.admitted }
    pub(crate) fn now(&self) -> u64 { if self.admitted { self.logger.now() } else { 0 } }
    pub(crate) fn primary_mut(&mut self) -> Option<&mut Record> { self.primary.as_mut() }
    pub(crate) fn primary_id(&self) -> u64 { self.primary.as_ref().map_or(0, |r| r.id) }
    fn record(&self, kind: usize, parent: u64, trigger: &'static str) -> Record {
        let mut r = Record::new(kind); r.trigger = trigger;
        if self.admitted { if let Some((s, l)) = &self.logger.0 { r.logger = l.id; r.id = s.next_id.fetch_add(1, Ordering::Relaxed); if parent != 0 { r.set(f::PARENT, parent); } } }
        r
    }
    pub(crate) fn span(&mut self, kind: usize, parent: u64, trigger: &'static str) -> Span<'_> { let r = self.record(kind, parent, trigger); Span { trace: self, record: Some(r), acquired: None, waiting: false, commit: false } }
    pub(crate) fn root(&mut self) -> Span<'_> { let r = self.primary.take().unwrap_or_else(|| Record::new(kind::MAINTENANCE)); Span { trace: self, record: Some(r), acquired: None, waiting: false, commit: false } }
    fn push(&mut self, mut r: Record) {
        if r.id == 0 { return; }
        if thread::panicking() && r.outcome == "cancelled" { r.outcome = "unwind"; }
        if let Some(s) = self.storage.as_mut() { if s.rows.len() < RECORDS_PER_OPERATION { s.rows.push(r); } else { self.lost += 1; } }
        else if self.helper { if let Some(slot) = self.small.iter_mut().find(|slot| slot.is_none()) { *slot = Some(r); } else { self.lost += 1; } }
    }
    pub(crate) fn child_timer(&self) -> Self { if self.admitted { Self::make(&self.logger, kind::TIMER, true, false) } else { Self::disabled() } }
    pub(crate) fn helper_trace(&self) -> Self { if self.admitted { Self::make(&self.logger, kind::MAINTENANCE, true, true) } else { Self::disabled() } }
    pub(crate) fn can_host_helper(&self) -> bool { self.storage.as_ref().is_some_and(|s| s.receivers.len() < HELPERS_PER_OPERATION) }
    pub(crate) fn host_helper(&mut self, receiver: async_channel::Receiver<HelperPacket>) -> usize { let s = self.storage.as_mut().expect("checked helper capacity"); let index = s.receivers.len(); s.receivers.push(receiver); index }
    pub(crate) async fn receive_helper(&mut self, index: usize) -> Result<HelperPacket, async_channel::RecvError> { self.storage.as_ref().expect("hosted receiver").receivers[index].recv().await }
    pub(crate) fn merge_helper(&mut self, packet: HelperPacket) -> io::Result<()> { for r in packet.rows.into_iter().flatten() { self.push(r); } packet.result }
    fn packet(&mut self, result: io::Result<()>) -> HelperPacket { HelperPacket { result, rows: std::mem::replace(&mut self.small, [None, None]) } }
    /// Closed 只有在父级释放全部业务锁后才可出现，退回的包才能由辅助任务直接导出。
    pub(crate) fn send_helper(&mut self, sender: async_channel::Sender<HelperPacket>, result: io::Result<()>) {
        match sender.try_send(self.packet(result)) {
            Ok(()) => (),
            Err(async_channel::TrySendError::Closed(p)) => { if let Some((s, _)) = &self.logger.0 { for r in p.rows.into_iter().flatten() { s.emit(r); } } }
            Err(async_channel::TrySendError::Full(p)) => { self.lost += p.rows.iter().filter(|r| r.is_some()).count() as u64; }
        }
    }
    pub(crate) fn omit(&mut self) { if self.admitted { self.lost += 1; } }
    fn finish_admission(&mut self) { if self.admitted { if let Some((s, _)) = &self.logger.0 { s.operations.fetch_sub(1, Ordering::Release); } self.admitted = false; } }
    fn retain(&mut self, data: PathData) -> Option<RetainedPath> {
        let bytes = match &data { PathData::Owned(p) => p.capacity(), PathData::Shared(p) => p.capacity() } as u64;
        let Some((s, _)) = &self.logger.0 else { return None };
        if s.retained_paths.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| old.checked_add(bytes).filter(|n| *n <= PATH_BUDGET)).is_err() { self.lost += 1; return None; }
        Some(RetainedPath { data, session: s.clone(), bytes })
    }
}
impl Drop for Trace {
    fn drop(&mut self) {
        if let Some(r) = self.primary.take() { self.push(r); }
        if let Some(mut storage) = self.storage.take() {
            // 必须 close 后 drain；不能先看空再关闭。此时祖先业务 guard 已全部释放。
            for rx in storage.receivers.drain(..) { rx.close(); if let Ok(p) = rx.try_recv() { for r in p.rows.into_iter().flatten() { if storage.rows.len() < RECORDS_PER_OPERATION { storage.rows.push(r); } else { self.lost += 1; } } } }
            if let Some((s, _)) = &self.logger.0 { for r in storage.rows.drain(..) { s.emit(r); } let _ = s.pool_tx.try_send(storage); }
        } else if self.helper { self.lost += self.small.iter().filter(|r| r.is_some()).count() as u64; }
        if let Some((s, _)) = &self.logger.0 { s.omitted.fetch_add(self.lost, Ordering::Relaxed); }
        self.finish_admission();
    }
}
pub(crate) struct Span<'a> { pub(crate) trace: &'a mut Trace, record: Option<Record>, acquired: Option<u64>, waiting: bool, commit: bool }
impl<'a> Span<'a> {
    pub(crate) fn id(&self) -> u64 { self.record.as_ref().map_or(0, |r| r.id) }
    pub(crate) fn now(&self) -> u64 { self.trace.now() }
    pub(crate) fn rec(&mut self) -> &mut Record { self.record.as_mut().expect("live span") }
    pub(crate) fn set(&mut self, index: usize, value: u64) { self.rec().set(index, value); }
    pub(crate) fn mark(&mut self, index: usize) { if self.id() != 0 { let now = self.now(); self.set(index, now); } }
    pub(crate) fn begin(&mut self) { self.mark(f::START); }
    pub(crate) fn wait_commit(&mut self) {
        if self.id() != 0 { if let Some((_, l)) = &self.trace.logger.0 { let n = l.waiting.fetch_add(1, Ordering::Relaxed) + 1; l.peak.fetch_max(n, Ordering::Relaxed); self.waiting = true; } }
        self.begin();
    }
    pub(crate) fn stage(&self, stage: u64) { if self.commit { if let Some((_, l)) = &self.trace.logger.0 { l.owner_stage.store(stage, Ordering::Relaxed); } } }
    pub(crate) fn finish(&mut self, outcome: &'static str) { if let Some(start) = self.rec().get(f::START) { let elapsed = self.now().saturating_sub(start); self.set(f::TOTAL, elapsed); } self.rec().outcome = outcome; }
    pub(crate) fn result<T>(&mut self, result: &io::Result<T>) { self.finish(if result.is_ok() { "success" } else { "error" }); if let Err(e) = result { self.rec().error(e); } }
    pub(crate) fn path(&mut self, slot: usize, path: PathBuf) { if self.id() != 0 { let value = self.trace.retain(PathData::Owned(path)); self.rec().paths[slot] = value; } }
    pub(crate) fn shared_path(&mut self, path: Arc<PathBuf>) { if self.id() != 0 { let value = self.trace.retain(PathData::Shared(path)); self.rec().paths[0] = value; } }
    pub(crate) fn hold<G>(mut self, guard: G, commit: bool, total: bool) -> Held<'a, G> {
        if self.id() != 0 {
            let now = self.now(); self.acquired = Some(now);
            if let Some(start) = self.rec().get(f::START) { self.set(f::WAIT, now.saturating_sub(start)); }
            if commit {
                if let Some((_, logger)) = &self.trace.logger.0 {
                    if self.waiting { logger.waiting.fetch_sub(1, Ordering::Relaxed); self.waiting = false; }
                    let seq = logger.lock_sequence.fetch_add(1, Ordering::Relaxed) + 1;
                    logger.owner.store(self.id(), Ordering::Relaxed); logger.owner_since.store(now, Ordering::Relaxed); logger.owner_stage.store(1, Ordering::Relaxed);
                    self.set(f::SEQ, seq); self.commit = true;
                }
            }
        }
        Held { guard: Some(guard), span: self, total }
    }
    pub(crate) fn child(&mut self, kind: usize, trigger: &'static str, link: Option<usize>) -> Span<'_> {
        let record = self.trace.record(kind, self.id(), trigger);
        if let Some(field) = link { self.set(field, record.id); }
        Span { trace: self.trace, record: Some(record), acquired: None, waiting: false, commit: false }
    }
}
impl Drop for Span<'_> {
    fn drop(&mut self) {
        if self.waiting { if let Some((_, l)) = &self.trace.logger.0 { l.waiting.fetch_sub(1, Ordering::Relaxed); } }
        if let Some(mut r) = self.record.take() {
            if r.kind == kind::FLUSH {
                if let Some(value) = self.trace.flush_fast { r.set(f::ENTRY_FAST, u64::from(value)); }
                if let Some(value) = self.trace.recv_start { r.set(f::RECV_START, value); }
                if let Some(value) = self.trace.recv_ns { r.set(f::RECV_NS, value); }
                r.recv = self.trace.recv_outcome;
            }
            self.trace.push(r);
        }
    }
}
/// 原 guard 的透明包装：释放前采样，真正解锁后才结束本层计时，记录仍仅暂存。
pub(crate) struct Held<'a, G> { guard: Option<G>, pub(crate) span: Span<'a>, total: bool }
impl<G: Deref> Deref for Held<'_, G> { type Target = G::Target; fn deref(&self) -> &Self::Target { self.guard.as_ref().expect("held guard").deref() } }
impl<G: DerefMut> DerefMut for Held<'_, G> { fn deref_mut(&mut self) -> &mut Self::Target { self.guard.as_mut().expect("held guard").deref_mut() } }
impl<G> Drop for Held<'_, G> {
    fn drop(&mut self) {
        if let Some(acquired) = self.span.acquired { let hold = self.span.now().saturating_sub(acquired); self.span.set(f::HOLD, hold); }
        // 必须在真正解锁前清除，避免误清除下一个所有者；后台跨字段快照只作近似观察。
        if self.span.commit { if let Some((_, l)) = &self.span.trace.logger.0 { l.owner.store(0, Ordering::Relaxed); l.owner_stage.store(0, Ordering::Relaxed); } }
        drop(self.guard.take());
        if self.total { let outcome = self.span.rec().outcome; self.span.finish(outcome); }
    }
}
/// 只在栈上累加轮询次数；不读时钟、不新建唤醒器、不主动唤醒或增加 poll。
pub(crate) async fn observe<F: Future>(polls: &mut u64, future: F) -> F::Output {
    futures::pin_mut!(future);
    futures::future::poll_fn(|cx| { *polls += 1; future.as_mut().poll(cx) }).await
}

fn json(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2); out.push('"');
    for c in value.chars() { match c { '"' => out.push_str("\\\""), '\\' => out.push_str("\\\\"), '\n' => out.push_str("\\n"), '\r' => out.push_str("\\r"), '\t' => out.push_str("\\t"), c if c <= '\u{1f}' => { let _ = write!(out, "\\u{:04x}", c as u32); }, c => out.push(c) } }
    out.push('"'); out
}
fn path_json(path: &Path) -> String {
    let mut out = String::from("{\"encoding\":\"");
    #[cfg(unix)] { use std::os::unix::ffi::OsStrExt; out.push_str("unix_hex\",\"value\":\""); for byte in path.as_os_str().as_bytes() { let _ = write!(out, "{byte:02x}"); } }
    #[cfg(windows)] { use std::os::windows::ffi::OsStrExt; out.push_str("windows_utf16_hex\",\"value\":\""); for unit in path.as_os_str().encode_wide() { let _ = write!(out, "{unit:04x}"); } }
    out.push_str("\"}"); out
}
fn encode(record: &Record, session: &str, line: &mut String) {
    line.clear();
    let _ = write!(line, "{{\"keyword\":\"{KEYWORD}\",\"event\":\"{}\",\"session_id\":{},\"logger_id\":{},\"operation_id\":{},\"outcome\":{}", KINDS[record.kind], json(session), record.logger, record.id, json(record.outcome));
    for (name, value) in [("trigger", record.trigger), ("mode", record.mode), ("last_stage", record.stage), ("pending_commit_outcome", record.pending), ("batch_wait_outcome", record.recv)] { if !value.is_empty() { let _ = write!(line, ",\"{name}\":{}", json(value)); } }
    for (index, name) in NAMES.iter().enumerate() {
        if index == f::CID_LO || index == f::CID_HI { continue; }
        if let Some(value) = record.get(index) { let name = if index == f::BYTES && record.kind == kind::APPEND { "payload_bytes" } else { name }; let _ = write!(line, ",\"{name}\":{value}"); }
    }
    if let (Some(lo), Some(hi)) = (record.get(f::CID_LO), record.get(f::CID_HI)) { let _ = write!(line, ",\"commit_uid\":\"{hi:016x}{lo:016x}\""); }
    if let Some(error) = record.error { let _ = write!(line, ",\"error_kind\":\"{error:?}\""); }
    for (index, name) in ["source_path", "target_path"].iter().enumerate() { if let Some(path) = &record.paths[index] { let _ = write!(line, ",\"{name}\":{}", path_json(path.path())); } }
    line.push_str("}\n");
}
fn write_limited(writer: &mut impl Write, line: &str, bytes: &mut u64, limit: u64) -> io::Result<()> {
    if bytes.saturating_add(line.len() as u64).saturating_add(FOOTER_RESERVE) > limit { return Err(io::Error::new(io::ErrorKind::FileTooLarge, "trace byte limit")); }
    writer.write_all(line.as_bytes())?; *bytes += line.len() as u64; Ok(())
}
fn write_windows(s: Arc<Session>, receiver: Receiver<Record>, mut writer: BufWriter<File>, config: TraceConfig, mut bytes: u64) {
    let mut written = 0u64; let mut registered = 0; let mut window = 0u64; let mut last = s.now();
    let mut line = String::with_capacity(2048); let mut reason = "shutdown";
    loop {
        thread::park_timeout(Duration::from_millis(config.window_ms));
        window += 1; let begin = s.now();
        let mut counts = [0u64; 12]; let mut errors = [0u64; 12]; let mut syncs = [0u64; 3]; let mut sync_bytes = [0u64; 3];
        let mut notified = 0u64; let mut registered_waiters = 0u64;
        let result: io::Result<()> = (|| {
            { let loggers = s.loggers.lock().unwrap_or_else(|p| p.into_inner());
                for logger in loggers.iter().skip(registered) { write_limited(&mut writer, &logger.metadata, &mut bytes, config.max_bytes)?; }
                registered = loggers.len();
            }
            // 固定本窗口最大处理量；持续生产不能造成无限排空。
            let available = receiver.len();
            for _ in 0..available {
                let Ok(record) = receiver.try_recv() else { break };
                counts[record.kind] += 1; if record.error.is_some() { errors[record.kind] += 1; }
                if record.kind == kind::SYNC && record.outcome == "success" { let index = match record.trigger { "timer" => 1, "checkpoint" => 2, _ => 0 }; syncs[index] += 1; sync_bytes[index] += record.get(f::BYTES).unwrap_or(0); }
                notified += record.get(f::NOTIFIED).unwrap_or(0);
                registered_waiters += u64::from(record.kind == kind::COMMIT && record.outcome == "waiter_registered");
                encode(&record, &s.session_id, &mut line);
                write_limited(&mut writer, &line, &mut bytes, config.max_bytes)?; written += 1;
            }
            let end = s.now();
            line = format!("{{\"keyword\":\"{KEYWORD}\",\"event\":\"window\",\"session_id\":{},\"window\":{window},\"start_ns\":{last},\"end_ns\":{begin},\"write_elapsed_ns\":{},\"event_produced\":{},\"event_written\":{written},\"dropped_full\":{},\"dropped_stopped\":{},\"omitted\":{},\"operation_outstanding\":{},\"recorder_inflight\":{},\"queue_depth\":{},\"queue_capacity\":{},\"retained_path_bytes\":{},\"file_bytes_before_summary\":{bytes},\"waiter_registered\":{registered_waiters},\"notify_completed\":{notified},\"counts\":[", json(&s.session_id), end - begin, s.produced.load(Ordering::Relaxed), s.dropped_full.load(Ordering::Relaxed), s.dropped_stopped.load(Ordering::Relaxed), s.omitted.load(Ordering::Relaxed), s.operations.load(Ordering::Acquire) & !CLOSED, s.emit_gate.load(Ordering::Acquire) & !CLOSED, receiver.len(), config.capacity, s.retained_paths.load(Ordering::Relaxed));
            for index in 0..KINDS.len() { if index != 0 { line.push(','); } let _ = write!(line, "{{\"event\":\"{}\",\"completed_records\":{},\"errors\":{}}}", KINDS[index], counts[index], errors[index]); }
            line.push_str("],\"sync\":[");
            for (index, trigger) in ["request", "timer", "checkpoint"].iter().enumerate() { if index != 0 { line.push(','); } let _ = write!(line, "{{\"trigger\":\"{trigger}\",\"calls\":{},\"block_bytes\":{}}}", syncs[index], sync_bytes[index]); }
            line.push_str("]}\n"); write_limited(&mut writer, &line, &mut bytes, config.max_bytes)?;
            { let loggers = s.loggers.lock().unwrap_or_else(|p| p.into_inner());
              for l in loggers.iter() {
                line = format!("{{\"keyword\":\"{KEYWORD}\",\"event\":\"progress\",\"session_id\":{},\"logger_id\":{},\"window\":{window},\"at_ns\":{end},\"approximate\":true,\"commit_lock_waiting\":{},\"waiting_peak_lifetime\":{},\"owner_id\":{},\"owner_since_ns\":{},\"owner_stage\":{}}}\n", json(&s.session_id), l.id, l.waiting.load(Ordering::Relaxed), l.peak.load(Ordering::Relaxed), l.owner.load(Ordering::Relaxed), l.owner_since.load(Ordering::Relaxed), l.owner_stage.load(Ordering::Relaxed));
                write_limited(&mut writer, &line, &mut bytes, config.max_bytes)?;
              }
            }
            writer.flush()
        })();
        last = begin;
        if let Err(error) = result { reason = if error.kind() == io::ErrorKind::FileTooLarge { "byte_limit" } else { "io_error" }; s.failed.store(true, Ordering::Release); s.operations.fetch_or(CLOSED, Ordering::AcqRel); break; }
        if s.operations.load(Ordering::Acquire) == CLOSED && receiver.is_empty() { break; }
    }
    s.emit_gate.fetch_or(CLOSED, Ordering::AcqRel);
    while s.emit_gate.load(Ordering::Acquire) & !CLOSED != 0 { thread::yield_now(); }
    let produced = s.produced.load(Ordering::Acquire);
    let dropped = s.dropped_full.load(Ordering::Acquire) + s.dropped_stopped.load(Ordering::Acquire);
    let omitted = s.omitted.load(Ordering::Acquire); let outstanding = s.operations.load(Ordering::Acquire) & !CLOSED;
    let mut report = TraceReport { path: s.path.clone(), produced, written, dropped, omitted, outstanding,
        complete: reason == "shutdown" && produced == written && dropped == 0 && omitted == 0 && outstanding == 0, reason: reason.into() };
    let footer = format!("{{\"keyword\":\"{KEYWORD}\",\"event\":\"session_end\",\"session_id\":{},\"reason\":\"{reason}\",\"event_produced\":{produced},\"event_written\":{written},\"event_dropped\":{dropped},\"omitted\":{omitted},\"outstanding\":{outstanding},\"complete\":{}}}\n", json(&s.session_id), report.complete);
    if writer.write_all(footer.as_bytes()).and_then(|_| writer.flush()).is_err() { report.complete = false; report.reason = "io_error".into(); }
    eprintln!("{KEYWORD} event=session_stop reason={} complete={} path={:?}", report.reason, report.complete, report.path);
    *s.report.lock().unwrap_or_else(|p| p.into_inner()) = Some(report);
}
