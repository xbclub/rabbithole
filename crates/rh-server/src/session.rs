//! 会话:客户端在线会话与下发队列。
//!
//! 语义(设计 §3.3/§3.4):
//! - 每客户端同时至多一个会话;新会话注册即替换旧会话(取消域级联)。
//! - 下发队列归属会话:会话替换即废弃;全量 SYNC 为队首。
//! - pending_requests:request_id 一次性消费,10s 过期。

use rh_protocol::message::ServerMessage;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_util::sync::CancellationToken;

/// 服务端数据流聚合源(round-9 多载体池):首条物理连接由
/// [`crate::control::run_session_loop`] 的数据接受循环直读;同会话的
/// 附加载体(HELLO 携带 session_id 报到的后续连接)聚合进池。None =
/// 单载体(旧行为);Some = 已有附加载体(自附加连接合流接收)。
/// 随 Session 移除/替换整体释放,池内附加载体连接随之关闭(不泄漏)。
pub type SessionMuxPool = Mutex<Option<Arc<rh_transport::Multiplexer>>>;

pub struct PendingRequest {
    pub expires: Instant,
    /// TCP 池场景:通道到达后进入哪个池;UDP/visitor 由对应管理器领取。
    pub purpose: RequestPurpose,
}

#[derive(Debug, Clone)]
pub enum RequestPurpose {
    /// TCP 数据通道(入池或直接配对等待中的用户连接)。
    TcpPool { proxy_id: u64 },
    /// UDP 通道(该规则当前唯一通道)。
    UdpChannel { proxy_id: u64 },
    /// visitor 侧中继通道。compress 为签发时两端一致的中继压缩决策。
    VisitorRelay {
        visitor_id: u64,
        user_conn_id: u64,
        compress: bool,
    },
    /// stcp/sudp 中继的代理侧通道(到达即指派/直通并交付配对)。
    RelayProxy { proxy_id: u64, compress: bool },
}

pub struct Session {
    pub client_id: String,
    pub session_id: String,
    pub epoch: u64,
    pub cancel: CancellationToken,
    /// 下发队列(控制消息,容量受限)。
    pub queue_tx: mpsc::Sender<ServerMessage>,
    /// 已入队未确认的 revision → 时间(确认超时管理)。
    pub unconfirmed: Mutex<BTreeMap<u64, Instant>>,
    /// 客户端已应用到的 revision(APPLY_RESULT 上报)。
    pub applied_revision: AtomicU64,
    /// 会话内单调 request_id。
    next_request_id: AtomicU64,
    /// 在途数据通道请求(request_id → 记录)。
    pub pending_requests: Mutex<BTreeMap<u64, PendingRequest>>,
    pub started_at: Instant,
    /// 本会话数据连接计数(限额)。
    pub data_conn_count: AtomicU64,
    /// 多载体聚合池(round-9):见 [`SessionMuxPool`]。建会话时 None;
    /// 首个附加载体报到时升级为 Some(池);Session 被替换(epoch 变化)
    /// 或移除时随之 drop,附加载体连接关闭,不得泄漏到新会话。
    pub mux_pool: SessionMuxPool,
    /// mux_pool None→Some 升级通知(round-9):数据接受循环可能仍 park
    /// 在单源分支(首载体 accept)——池自身的 changed 只唤醒其订阅者,
    /// 唤不醒尚未观察到池的任务,须由升级方在此补一发通知。仅在升级
    /// 时触发;单载体会话永不通知。
    pub mux_pool_changed: Notify,
}

impl Session {
    pub fn new(
        client_id: String,
        session_id: String,
        epoch: u64,
        cancel: CancellationToken,
        queue_tx: mpsc::Sender<ServerMessage>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client_id,
            session_id,
            epoch,
            cancel: cancel.clone(),
            queue_tx,
            unconfirmed: Mutex::new(BTreeMap::new()),
            applied_revision: AtomicU64::new(0),
            next_request_id: AtomicU64::new(1),
            pending_requests: Mutex::new(BTreeMap::new()),
            started_at: Instant::now(),
            data_conn_count: AtomicU64::new(0),
            mux_pool: Mutex::new(None),
            mux_pool_changed: Notify::new(),
        })
    }

    pub fn alloc_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// 登记一个在途数据通道请求。
    pub async fn add_pending_request(&self, id: u64, req: PendingRequest) {
        let mut m = self.pending_requests.lock().await;
        // 顺带清理过期项
        let now = Instant::now();
        m.retain(|_, v| v.expires > now);
        m.insert(id, req);
    }

    /// 一次性消费 request_id;迟到/过期/未知一律 None。
    /// 失效某规则的全部在途数据通道请求(规则重建/删除的代次隔离)。
    pub async fn invalidate_proxy_requests(&self, proxy_id: u64) {
        let mut m = self.pending_requests.lock().await;
        m.retain(|_, p| {
            !matches!(
                p.purpose,
                crate::session::RequestPurpose::UdpChannel { proxy_id: id }
                    | crate::session::RequestPurpose::RelayProxy { proxy_id: id, .. }
                    | crate::session::RequestPurpose::TcpPool { proxy_id: id }
                if id == proxy_id
            )
        });
    }

    pub async fn take_pending_request(&self, id: u64) -> Option<PendingRequest> {
        let mut m = self.pending_requests.lock().await;
        let req = m.remove(&id)?;
        if req.expires < Instant::now() {
            return None;
        }
        Some(req)
    }

    pub async fn mark_unconfirmed(&self, revision: u64) {
        self.unconfirmed
            .lock()
            .await
            .insert(revision, Instant::now());
    }

    /// 确认处理:清除 ≤ revision 的等待;返回仍在等待的最小 revision。
    pub async fn confirm_applied(&self, revision: u64, ok: bool) -> Option<u64> {
        if ok {
            let prev = self.applied_revision.load(Ordering::Relaxed);
            if revision > prev {
                self.applied_revision.store(revision, Ordering::Relaxed);
            }
            let mut m = self.unconfirmed.lock().await;
            m.retain(|&r, _| r > revision);
            m.keys().next().copied()
        } else {
            self.unconfirmed.lock().await.keys().next().copied()
        }
    }

    pub fn applied(&self) -> u64 {
        self.applied_revision.load(Ordering::Relaxed)
    }

    pub async fn send(&self, msg: ServerMessage) -> bool {
        self.queue_tx.send(msg).await.is_ok()
    }
}

/// 每客户端会话注册表。
pub struct SessionRegistry {
    map: dashmap::DashMap<String, (Arc<Session>, u64)>,
    counter: AtomicU64,
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            map: dashmap::DashMap::new(),
            counter: AtomicU64::new(1),
        }
    }

    pub fn next_epoch(&self) -> u64 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }

    /// 注册(替换)会话;返回被替换的旧会话(若有)。
    pub fn register(&self, sess: Arc<Session>) -> Option<Arc<Session>> {
        let prev = self
            .map
            .insert(sess.client_id.clone(), (sess.clone(), sess.epoch));
        prev.map(|(s, _)| s)
    }

    pub fn get(&self, client_id: &str) -> Option<Arc<Session>> {
        self.map.get(client_id).map(|e| e.value().0.clone())
    }

    /// 移除会话,仅当当前注册的就是该会话(指针比对,防旧清理误删新会话)。
    /// remove_if 原子完成比对+删除,避免比对后、删除前新会话插入被误删。
    pub fn remove_if_current(&self, client_id: &str, sess: &Arc<Session>) {
        self.map
            .remove_if(client_id, |_k, (cur, _)| Arc::ptr_eq(cur, sess));
    }

    pub fn online_clients(&self) -> Vec<String> {
        self.map.iter().map(|e| e.key().clone()).collect()
    }

    pub fn count_online(&self) -> usize {
        self.map.len()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
