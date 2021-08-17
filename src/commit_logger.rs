use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::collections::hash_map::Entry;
use std::io::{Error, Result, ErrorKind};

use futures::future::{FutureExt, BoxFuture};
use bytes::BufMut;

use guid::Guid;
use hash::XHashMap;
use r#async::{lock::{mutex_lock::Mutex,
                     rw_lock::RwLock},
              rt::multi_thread::MultiTaskRuntime};
use async_transaction::AsyncCommitLog;

use crate::log_store::log_file::{LogMethod, LogFile};

///
/// 默认的提交日志的文件大小，为了防止自动生成新的可写文件，所以默认为最大
///
const DEFAULT_COMMIT_LOG_FILE_SIZE: usize = 16 * 1024 * 1024 * 1024;

///
/// 默认的提交日志的块大小，单位B
///
const DEFAULT_COMMIT_LOG_BLOCK_SIZE: usize = 8192;

///
/// 默认的延迟提交的超时时长，单位ms
///
const DEFAULT_DELAY_COMMIT_TIMEOUT: usize = 1;

///
/// 默认的整理检查点的间隔时长，单位ms
///
const DEFAULT_COLLECT_INTERVAL: usize = 5 * 60 * 1000;

///
/// 基于日志文件的提交日志记录器的构建器
///
pub struct CommitLoggerBuilder {
    rt:                 MultiTaskRuntime<()>,   //异步运行时
    path:               PathBuf,                //提交日志记录器的日志文件所在路径
    log_file_limit:     usize,                  //日志文件的可写文件大小限制，单位字节
    log_block_limit:    usize,                  //日志文件的块大小限制，单位字节
    delay_timeout:      usize,                  //延迟刷新提交日志的时间，单位毫秒
    collect_interval:   usize,                  //整理检查点的间隔时长，单位毫秒
}

unsafe impl Send for CommitLoggerBuilder {}
unsafe impl Sync for CommitLoggerBuilder {}

impl CommitLoggerBuilder {
    /// 构建一个基于日志文件的提交日志记录器的构建器
    pub fn new<P: AsRef<Path>>(rt: MultiTaskRuntime<()>,
                               dir: P) -> Self {
        CommitLoggerBuilder {
            rt,
            path: dir.as_ref().to_path_buf(),
            log_file_limit: DEFAULT_COMMIT_LOG_FILE_SIZE,
            log_block_limit: DEFAULT_COMMIT_LOG_BLOCK_SIZE,
            delay_timeout: DEFAULT_DELAY_COMMIT_TIMEOUT,
            collect_interval: DEFAULT_COLLECT_INTERVAL,
        }
    }

    /// 设置日志文件的块大小限制，超过限制以后，会强制刷新可写文件
    pub fn log_block_limit(mut self, mut limit: usize) -> Self {
        if limit < 2048 || limit > 32 * 1024 * 1024 {
            limit = DEFAULT_COMMIT_LOG_BLOCK_SIZE
        }

        self.log_block_limit = limit;
        self
    }

    /// 设置日志文件的超时时长，日志文件在超时后，会强制刷新可写文件
    pub fn delay_timeout(mut self, mut timeout: usize) -> Self {
        if timeout < 1 || timeout > 10 {
            timeout = DEFAULT_DELAY_COMMIT_TIMEOUT
        }

        self.delay_timeout = timeout;
        self
    }

    /// 设置整理检查点的间隔时长
    pub fn collect_interval(mut self, mut interval: usize) -> Self {
        if interval < 1000 || interval > 60 * 60 * 1000 {
            interval = DEFAULT_COLLECT_INTERVAL
        }

        self.collect_interval = interval;
        self
    }

    /// 异步构建一个基于日志文件的提交日志记录器
    pub async fn build(mut self) -> Result<CommitLogger> {
        let file = LogFile::open(self.rt.clone(),
                                 self.path.clone(),
                                 self.log_block_limit,
                                 self.log_file_limit,
                                 None).await?;

        let rt = self.rt;
        let delay_timeout = self.delay_timeout;
        let check_points = Mutex::new(XHashMap::default());

        let inner = InnerCommitLogger {
            rt: rt.clone(),
            file,
            delay_timeout,
            check_points,
        };

        Ok(CommitLogger(Arc::new(inner)))
    }
}

///
/// 基于日志文件的提交日志记录器
///
#[derive(Clone)]
pub struct CommitLogger(Arc<InnerCommitLogger>);

unsafe impl Send for CommitLogger {}
unsafe impl Sync for CommitLogger {}

impl AsyncCommitLog for CommitLogger {
    type C = usize;
    type Cid = Guid;

    fn append<B>(&self, commit_uid: Self::Cid, log: B) -> BoxFuture<Result<Self::C>>
        where B: BufMut + AsRef<[u8]> + Send + Sized + 'static {
        let logger = self.clone();

        async move {
            let mut check_pointes_locked = logger.0.check_points.lock().await;

            //追加指定的提交日志
            let log_handle = logger.0.file.append(LogMethod::PlainAppend,
                                                  commit_uid.0.to_le_bytes().as_ref(),
                                                  log.as_ref());

            //注册本次事务到检查点表
            check_pointes_locked.insert(commit_uid, log_handle);

            Ok(log_handle)
        }.boxed()
    }

    fn flush(&self, log_handle: Self::C) -> BoxFuture<Result<()>> {
        let mut logger = self.clone();

        async move {
            logger.0.file.delay_commit(log_handle,
                                       false,
                                       logger.0.delay_timeout).await
        }.boxed()
    }

    fn confirm(&self, commit_uid: Self::Cid) -> BoxFuture<Result<()>> {
        let logger = self.clone();

        async move {
            let mut check_pointes_locked = logger.0.check_points.lock().await;

            let _ = check_pointes_locked.remove(&commit_uid);
            if check_pointes_locked.len() > 0 {
                //提交日志记录的检查点表未清空，则立即返回确认提交成功
                return Ok(())
            }

            //提交日志记录的检查点已清空，则立即强制生成新的可写文件，并忽略强制生成新的可写文件是否成功
            if let Ok(_) = self.0.file.split().await {
                //生成新的可写文件成功，则将上一个可写文件设置为备份的只读文件，并立即返回
                return self.0.file.last_readable_to_back().await;
            }

            Ok(())
        }.boxed()
    }
}

// 基于日志文件的内部提交日志记录器
struct InnerCommitLogger {
    rt:                 MultiTaskRuntime<()>,           //异步运行时
    file:               LogFile,                        //日志文件
    delay_timeout:      usize,                          //延迟刷新提交日志的时间，单位毫秒
    check_points:       Mutex<XHashMap<Guid, usize>>,   //提交日志记录器的检查点表
}


