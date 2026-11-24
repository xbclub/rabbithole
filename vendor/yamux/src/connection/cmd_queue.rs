use crate::frame::header::StreamId;
use futures::Stream;
use nohash_hasher::IntMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// Error returned by [`CmdSender`] when the receiving end has been closed or dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SendError;

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("command queue closed")
    }
}

impl std::error::Error for SendError {}

/// Error returned by [`CmdSender::try_send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrySendError<T> {
    Full(T),
    Closed(T),
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TryRecvError {
    Empty,
    Disconnected,
}

type WakerPair = ([Option<Waker>; 2], Vec<Waker>);

struct Slot<T> {
    id: StreamId,
    state: Mutex<SlotState<T>>,
}

struct SlotState<T> {
    queue: VecDeque<T>,
    cap: usize,
    sender_count: usize,
    next_sender_id: usize,
    closed: bool,
    none_emitted: bool,
    enqueued_in_ready: bool,
    group: Option<Arc<GroupInner<T>>>,
    /// Per-sender wakers (`(sender_id, Waker)`) so split `StreamReadHalf` and
    /// `StreamWriteHalf` sharing the same slot never clobber each other's waker.
    /// Remains empty (zero heap allocation) unless backpressure occurs.
    sender_wakers: Vec<(usize, Waker)>,
}

struct GroupInner<T> {
    ready: Mutex<ReadyState<T>>,
}

struct ReadyState<T> {
    queue: VecDeque<Arc<Slot<T>>>,
    waker: Option<Waker>,
}

fn register_sender_waker(wakers: &mut Vec<(usize, Waker)>, sender_id: usize, waker: &Waker) {
    for (id, existing) in wakers.iter_mut() {
        if *id == sender_id {
            existing.clone_from(waker);
            return;
        }
    }
    wakers.push((sender_id, waker.clone()));
}

fn take_sender_wakers(wakers: &mut Vec<(usize, Waker)>) -> WakerPair {
    let mut inline: [Option<Waker>; 2] = [None, None];
    let mut extra = Vec::new();
    if !wakers.is_empty() {
        for (i, (_, w)) in wakers.drain(..).enumerate() {
            if i < 2 {
                inline[i] = Some(w);
            } else {
                extra.push(w);
            }
        }
    }
    (inline, extra)
}

fn wake_pair((inline, extra): WakerPair) {
    for w in inline.into_iter().flatten() {
        w.wake();
    }
    for w in extra {
        w.wake();
    }
}

/// Enqueues `slot` into `group.ready` if attached and not yet enqueued.
///
/// Strict lock hierarchy: caller MUST hold `slot.state` lock and MUST NOT hold `group.ready`.
fn enqueue_slot_locked<T>(slot: &Arc<Slot<T>>, st: &mut SlotState<T>) -> Option<Waker> {
    if st.enqueued_in_ready {
        return None;
    }
    let group = st.group.as_ref()?;
    st.enqueued_in_ready = true;
    let mut ready = group.ready.lock();
    ready.queue.push_back(Arc::clone(slot));
    ready.waker.take()
}

/// Creates a bounded per-stream command channel (`cap == 0` is clamped to `1`).
pub(crate) fn channel<T>(id: StreamId, cap: usize) -> (CmdSender<T>, CmdReceiver<T>) {
    let cap = cap.max(1);
    let slot = Arc::new(Slot {
        id,
        state: Mutex::new(SlotState {
            queue: VecDeque::with_capacity(cap.min(64)),
            cap,
            sender_count: 1,
            next_sender_id: 1,
            closed: false,
            none_emitted: false,
            enqueued_in_ready: false,
            group: None,
            sender_wakers: Vec::new(),
        }),
    });
    (
        CmdSender {
            slot: Arc::clone(&slot),
            sender_id: 0,
        },
        CmdReceiver { slot: Some(slot) },
    )
}

/// Sending half of a per-stream command channel.
pub(crate) struct CmdSender<T> {
    slot: Arc<Slot<T>>,
    sender_id: usize,
}

impl<T> CmdSender<T> {
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        let mut st = self.slot.state.lock();
        if st.closed {
            return Poll::Ready(Err(SendError));
        }
        if st.queue.len() < st.cap {
            return Poll::Ready(Ok(()));
        }
        register_sender_waker(&mut st.sender_wakers, self.sender_id, cx.waker());
        Poll::Pending
    }

    pub(crate) fn start_send(&mut self, val: T) -> Result<(), SendError> {
        let wake_group = {
            let mut st = self.slot.state.lock();
            if st.closed {
                return Err(SendError);
            }
            st.queue.push_back(val);
            enqueue_slot_locked(&self.slot, &mut st)
        };
        if let Some(w) = wake_group {
            w.wake();
        }
        Ok(())
    }

    pub(crate) fn try_send(&mut self, val: T) -> Result<(), TrySendError<T>> {
        let wake_group = {
            let mut st = self.slot.state.lock();
            if st.closed {
                return Err(TrySendError::Closed(val));
            }
            if st.queue.len() >= st.cap {
                return Err(TrySendError::Full(val));
            }
            st.queue.push_back(val);
            enqueue_slot_locked(&self.slot, &mut st)
        };
        if let Some(w) = wake_group {
            w.wake();
        }
        Ok(())
    }

    pub(crate) fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), SendError>> {
        match self.poll_ready(cx) {
            Poll::Ready(Err(_)) => Poll::Ready(Ok(())),
            other => other,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn close_channel(&mut self) {
        let (wakers, wake_group) = {
            let mut st = self.slot.state.lock();
            st.closed = true;
            let wakers = take_sender_wakers(&mut st.sender_wakers);
            let wake_group = if !st.none_emitted {
                enqueue_slot_locked(&self.slot, &mut st)
            } else {
                None
            };
            (wakers, wake_group)
        };
        wake_pair(wakers);
        if let Some(w) = wake_group {
            w.wake();
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.slot.state.lock().closed
    }
}

impl<T> Clone for CmdSender<T> {
    fn clone(&self) -> Self {
        let sender_id = {
            let mut st = self.slot.state.lock();
            st.sender_count += 1;
            let id = st.next_sender_id;
            st.next_sender_id += 1;
            id
        };
        Self {
            slot: Arc::clone(&self.slot),
            sender_id,
        }
    }
}

impl<T> Drop for CmdSender<T> {
    fn drop(&mut self) {
        let wake_group = {
            let mut st = self.slot.state.lock();
            st.sender_count -= 1;
            if !st.sender_wakers.is_empty() {
                st.sender_wakers.retain(|(id, _)| *id != self.sender_id);
            }
            if st.sender_count == 0 && !st.none_emitted {
                enqueue_slot_locked(&self.slot, &mut st)
            } else {
                None
            }
        };
        if let Some(w) = wake_group {
            w.wake();
        }
    }
}

/// Receiving half of a per-stream command channel, typically pushed into [`CmdGroup`].
pub(crate) struct CmdReceiver<T> {
    slot: Option<Arc<Slot<T>>>,
}

impl<T> CmdReceiver<T> {
    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let slot = self.slot.as_ref().expect("receiver not consumed");
        let (res, wakers) = {
            let mut st = slot.state.lock();
            if let Some(cmd) = st.queue.pop_front() {
                let wakers = if st.queue.len() < st.cap {
                    take_sender_wakers(&mut st.sender_wakers)
                } else {
                    ([None, None], Vec::new())
                };
                (Ok(cmd), wakers)
            } else if st.sender_count == 0 || st.closed {
                (Err(TryRecvError::Disconnected), ([None, None], Vec::new()))
            } else {
                (Err(TryRecvError::Empty), ([None, None], Vec::new()))
            }
        };
        wake_pair(wakers);
        res
    }
}

impl<T> Drop for CmdReceiver<T> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            let (wakers, dropped_queue) = {
                let mut st = slot.state.lock();
                st.closed = true;
                st.none_emitted = true;
                let wakers = take_sender_wakers(&mut st.sender_wakers);
                let dropped_queue = std::mem::take(&mut st.queue);
                (wakers, dropped_queue)
            };
            drop(dropped_queue);
            wake_pair(wakers);
        }
    }
}

/// Round-robin intrusive-style command queue group replacing `SelectAll<TaggedStream<StreamId, mpsc::Receiver<T>>>`.
///
/// Strict lock hierarchy: always lock `slot.state` FIRST -> `group.ready` SECOND; never the reverse.
pub(crate) struct CmdGroup<T> {
    slots: IntMap<StreamId, Arc<Slot<T>>>,
    inner: Arc<GroupInner<T>>,
}

impl<T> Default for CmdGroup<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> CmdGroup<T> {
    pub(crate) fn new() -> Self {
        Self {
            slots: IntMap::default(),
            inner: Arc::new(GroupInner {
                ready: Mutex::new(ReadyState {
                    queue: VecDeque::new(),
                    waker: None,
                }),
            }),
        }
    }

    pub(crate) fn push(&mut self, mut rx: CmdReceiver<T>) {
        let slot = rx.slot.take().expect("CmdReceiver already consumed");
        let id = slot.id;
        let wake_group = {
            let mut st = slot.state.lock();
            st.group = Some(Arc::clone(&self.inner));
            if (!st.queue.is_empty() || st.sender_count == 0 || st.closed) && !st.none_emitted {
                enqueue_slot_locked(&slot, &mut st)
            } else {
                None
            }
        };
        self.slots.insert(id, slot);
        if let Some(w) = wake_group {
            w.wake();
        }
    }

    /// Marks all active slots closed and wakes blocked senders while keeping buffered
    /// commands drainable via `poll_next`.
    pub(crate) fn close_receivers(&mut self) {
        for slot in self.slots.values() {
            let wakers = {
                let mut st = slot.state.lock();
                st.closed = true;
                take_sender_wakers(&mut st.sender_wakers)
            };
            wake_pair(wakers);
        }
    }

    /// Closes all active slots, wakes all blocked senders, discards queued commands,
    /// and clears internal state.
    pub(crate) fn clear(&mut self) {
        for (_, slot) in self.slots.drain() {
            let (wakers, dropped_queue) = {
                let mut st = slot.state.lock();
                st.closed = true;
                st.none_emitted = true;
                st.enqueued_in_ready = false;
                st.group = None;
                let wakers = take_sender_wakers(&mut st.sender_wakers);
                let dropped_queue = std::mem::take(&mut st.queue);
                (wakers, dropped_queue)
            };
            drop(dropped_queue);
            wake_pair(wakers);
        }
        let (ready_queue, ready_waker) = {
            let mut ready = self.inner.ready.lock();
            (std::mem::take(&mut ready.queue), ready.waker.take())
        };
        drop(ready_queue);
        drop(ready_waker);
    }

    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }
}

impl<T> Drop for CmdGroup<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T> Stream for CmdGroup<T> {
    type Item = (StreamId, Option<T>);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let slot = {
                let mut ready = self.inner.ready.lock();
                match ready.queue.pop_front() {
                    Some(slot) => slot,
                    None => {
                        if self.slots.is_empty() {
                            return Poll::Ready(None);
                        }
                        if let Some(w) = &mut ready.waker {
                            w.clone_from(cx.waker());
                        } else {
                            ready.waker = Some(cx.waker().clone());
                        }
                        return Poll::Pending;
                    }
                }
            };

            enum Step<T> {
                Item(T, WakerPair),
                Closed,
                Empty,
            }

            let step = {
                let mut st = slot.state.lock();
                if let Some(cmd) = st.queue.pop_front() {
                    let wakers = if st.queue.len() < st.cap {
                        take_sender_wakers(&mut st.sender_wakers)
                    } else {
                        ([None, None], Vec::new())
                    };
                    if (!st.queue.is_empty() || st.sender_count == 0 || st.closed)
                        && !st.none_emitted
                    {
                        // Keep `enqueued_in_ready = true` and re-queue to back for round-robin fairness.
                        let mut ready = self.inner.ready.lock();
                        ready.queue.push_back(Arc::clone(&slot));
                    } else {
                        st.enqueued_in_ready = false;
                    }
                    Step::Item(cmd, wakers)
                } else if (st.sender_count == 0 || st.closed) && !st.none_emitted {
                    st.none_emitted = true;
                    st.enqueued_in_ready = false;
                    st.group = None;
                    Step::Closed
                } else {
                    st.enqueued_in_ready = false;
                    Step::Empty
                }
            };

            match step {
                Step::Item(cmd, wakers) => {
                    wake_pair(wakers);
                    return Poll::Ready(Some((slot.id, Some(cmd))));
                }
                Step::Closed => {
                    self.slots.remove(&slot.id);
                    return Poll::Ready(Some((slot.id, None)));
                }
                Step::Empty => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::StreamExt;
    use futures::task::{noop_waker, waker, ArcWake};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountWaker(AtomicUsize);

    impl ArcWake for CountWaker {
        fn wake_by_ref(me: &Arc<Self>) {
            me.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[derive(Default)]
    struct FlagWaker(AtomicBool);

    impl ArcWake for FlagWaker {
        fn wake_by_ref(me: &Arc<Self>) {
            me.0.store(true, Ordering::SeqCst);
        }
    }

    struct ThreadWaker(std::thread::Thread);

    impl std::task::Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    #[test]
    fn cap_zero_gives_single_slot() {
        let (mut tx, mut rx) = channel::<u32>(StreamId::new(1), 0);
        let nw = noop_waker();
        let mut ncx = Context::from_waker(&nw);
        assert!(matches!(tx.poll_ready(&mut ncx), Poll::Ready(Ok(()))));
        tx.start_send(10).unwrap();

        let fw = Arc::new(FlagWaker::default());
        let w = waker(fw.clone());
        let mut cx = Context::from_waker(&w);
        assert!(tx.poll_ready(&mut cx).is_pending());
        assert!(!fw.0.load(Ordering::SeqCst));

        assert_eq!(rx.try_recv(), Ok(10));
        assert!(fw.0.load(Ordering::SeqCst));
        assert!(matches!(tx.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    }

    #[test]
    fn cloned_senders_preserve_independent_wakers() {
        let (mut tx1, mut rx) = channel::<u32>(StreamId::new(1), 1);
        let mut tx2 = tx1.clone();
        tx1.try_send(1).unwrap();

        let w1 = Arc::new(FlagWaker::default());
        let w2 = Arc::new(FlagWaker::default());
        let wk1 = waker(w1.clone());
        let wk2 = waker(w2.clone());
        let mut cx1 = Context::from_waker(&wk1);
        let mut cx2 = Context::from_waker(&wk2);

        assert!(tx1.poll_ready(&mut cx1).is_pending());
        assert!(tx2.poll_ready(&mut cx2).is_pending());

        assert_eq!(rx.try_recv(), Ok(1));
        assert!(
            w1.0.load(Ordering::SeqCst),
            "first parked sender waker must not be overwritten by clone"
        );
        assert!(w2.0.load(Ordering::SeqCst), "second parked sender woken");
    }

    #[test]
    fn per_stream_backpressure_isolation() {
        let sid_a = StreamId::new(1);
        let sid_b = StreamId::new(3);
        let (mut tx_a, rx_a) = channel(sid_a, 2);
        let (mut tx_b, rx_b) = channel(sid_b, 2);
        let mut group = CmdGroup::new();
        group.push(rx_a);
        group.push(rx_b);

        let nw = noop_waker();
        let mut ncx = Context::from_waker(&nw);

        // Fill stream A completely; stream A blocks.
        assert!(tx_a.poll_ready(&mut ncx).is_ready());
        tx_a.start_send(100u32).unwrap();
        assert!(tx_a.poll_ready(&mut ncx).is_ready());
        tx_a.start_send(101u32).unwrap();
        let wa = Arc::new(FlagWaker::default());
        let w_a = waker(wa.clone());
        let mut cx_a = Context::from_waker(&w_a);
        assert!(tx_a.poll_ready(&mut cx_a).is_pending());

        // Stream B remains completely unblocked and delivers immediately.
        assert!(tx_b.poll_ready(&mut ncx).is_ready());
        tx_b.start_send(200u32).unwrap();
        assert!(tx_b.poll_ready(&mut ncx).is_ready());
        tx_b.start_send(201u32).unwrap();

        assert_eq!(
            group.poll_next_unpin(&mut ncx),
            Poll::Ready(Some((sid_a, Some(100))))
        );
        assert!(
            wa.0.load(Ordering::SeqCst),
            "stream A woken once room freed"
        );
        assert_eq!(
            group.poll_next_unpin(&mut ncx),
            Poll::Ready(Some((sid_b, Some(200))))
        );
        assert_eq!(
            group.poll_next_unpin(&mut ncx),
            Poll::Ready(Some((sid_a, Some(101))))
        );
        assert_eq!(
            group.poll_next_unpin(&mut ncx),
            Poll::Ready(Some((sid_b, Some(201))))
        );
        assert!(group.poll_next_unpin(&mut ncx).is_pending());
    }

    #[test]
    fn round_robin_fairness_across_ready_streams() {
        let s1 = StreamId::new(1);
        let s2 = StreamId::new(3);
        let s3 = StreamId::new(5);
        let (mut tx1, rx1) = channel(s1, 4);
        let (mut tx2, rx2) = channel(s2, 4);
        let (mut tx3, rx3) = channel(s3, 4);
        for i in 0..3 {
            tx1.try_send(10 + i).unwrap();
            tx2.try_send(20 + i).unwrap();
            tx3.try_send(30 + i).unwrap();
        }
        let mut group = CmdGroup::new();
        group.push(rx1);
        group.push(rx2);
        group.push(rx3);

        let nw = noop_waker();
        let mut cx = Context::from_waker(&nw);
        let mut seen = Vec::new();
        while let Poll::Ready(Some((id, Some(v)))) = group.poll_next_unpin(&mut cx) {
            seen.push((id, v));
        }
        assert_eq!(
            seen,
            vec![
                (s1, 10),
                (s2, 20),
                (s3, 30),
                (s1, 11),
                (s2, 21),
                (s3, 31),
                (s1, 12),
                (s2, 22),
                (s3, 32),
            ]
        );
    }

    #[test]
    fn queued_commands_delivered_before_none_on_sender_drop() {
        let sid = StreamId::new(7);
        let (mut tx1, rx) = channel(sid, 8);
        let mut tx2 = tx1.clone();
        tx1.try_send(1).unwrap();
        tx2.try_send(2).unwrap();
        drop(tx1);

        let mut group = CmdGroup::new();
        group.push(rx);
        let nw = noop_waker();
        let mut cx = Context::from_waker(&nw);

        // Dropping only one clone must NOT emit None.
        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, Some(1))))
        );
        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, Some(2))))
        );
        assert!(group.poll_next_unpin(&mut cx).is_pending());
        assert_eq!(group.len(), 1);

        // Queue another command, then drop the last clone before draining.
        tx2.try_send(3).unwrap();
        drop(tx2);

        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, Some(3))))
        );
        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, None)))
        );
        assert!(group.is_empty());
        assert_eq!(group.poll_next_unpin(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn clear_and_drop_wake_blocked_senders_with_err() {
        // 1) clear() wakes blocked senders and causes subsequent sends to error.
        let sid = StreamId::new(9);
        let (mut tx_a, rx_a) = channel(sid, 1);
        let mut tx_b = tx_a.clone();
        tx_a.try_send(42).unwrap();

        let wa = Arc::new(FlagWaker::default());
        let wb = Arc::new(FlagWaker::default());
        let w_a = waker(wa.clone());
        let w_b = waker(wb.clone());
        let mut cx_a = Context::from_waker(&w_a);
        let mut cx_b = Context::from_waker(&w_b);
        assert!(tx_a.poll_ready(&mut cx_a).is_pending());
        assert!(tx_b.poll_ready(&mut cx_b).is_pending());

        let mut group = CmdGroup::new();
        group.push(rx_a);
        assert!(!group.is_empty());

        group.clear();
        assert!(group.is_empty());
        assert!(wa.0.load(Ordering::SeqCst));
        assert!(wb.0.load(Ordering::SeqCst));
        assert!(tx_a.is_closed());
        assert!(matches!(
            tx_a.poll_ready(&mut cx_a),
            Poll::Ready(Err(SendError))
        ));
        assert!(matches!(tx_a.start_send(99), Err(SendError)));
        assert!(matches!(tx_b.try_send(99), Err(TrySendError::Closed(99))));

        // 2) Drop of CmdGroup also clears and wakes blocked senders with Err.
        let (mut tx2, rx2) = channel(StreamId::new(11), 1);
        tx2.try_send(1).unwrap();
        let w2 = Arc::new(FlagWaker::default());
        let wk2 = waker(w2.clone());
        let mut cx2 = Context::from_waker(&wk2);
        assert!(tx2.poll_ready(&mut cx2).is_pending());

        let mut group2 = CmdGroup::new();
        group2.push(rx2);
        drop(group2);
        assert!(w2.0.load(Ordering::SeqCst));
        assert!(matches!(
            tx2.poll_ready(&mut cx2),
            Poll::Ready(Err(SendError))
        ));

        // 3) Drop of unadopted CmdReceiver wakes blocked senders with Err.
        let (mut tx3, rx3) = channel(StreamId::new(13), 1);
        tx3.try_send(1).unwrap();
        let w3 = Arc::new(FlagWaker::default());
        let wk3 = waker(w3.clone());
        let mut cx3 = Context::from_waker(&wk3);
        assert!(tx3.poll_ready(&mut cx3).is_pending());
        drop(rx3);
        assert!(w3.0.load(Ordering::SeqCst));
        assert!(matches!(
            tx3.poll_ready(&mut cx3),
            Poll::Ready(Err(SendError))
        ));
    }

    #[test]
    fn close_receivers_drains_queued_then_closes() {
        let sid = StreamId::new(15);
        let (mut tx, rx) = channel(sid, 4);
        tx.try_send(10).unwrap();
        tx.try_send(20).unwrap();

        let mut group = CmdGroup::new();
        group.push(rx);
        group.close_receivers();
        assert!(tx.is_closed());
        assert!(matches!(tx.try_send(30), Err(TrySendError::Closed(30))));

        let nw = noop_waker();
        let mut cx = Context::from_waker(&nw);
        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, Some(10))))
        );
        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, Some(20))))
        );
        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, None)))
        );
        assert_eq!(group.poll_next_unpin(&mut cx), Poll::Ready(None));
        // Dropping tx after close_receivers already emitted None must not panic or duplicate.
        drop(tx);
        assert_eq!(group.poll_next_unpin(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn close_channel_emits_none_once() {
        let sid = StreamId::new(17);
        let (mut tx, rx) = channel(sid, 4);
        let mut group = CmdGroup::new();
        group.push(rx);

        let cw = Arc::new(CountWaker::default());
        let w = waker(cw.clone());
        let mut cx = Context::from_waker(&w);
        assert!(group.poll_next_unpin(&mut cx).is_pending());

        tx.try_send(7).unwrap();
        tx.close_channel();
        assert!(cw.0.load(Ordering::SeqCst) >= 1);

        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, Some(7))))
        );
        assert_eq!(
            group.poll_next_unpin(&mut cx),
            Poll::Ready(Some((sid, None)))
        );
        assert_eq!(group.poll_next_unpin(&mut cx), Poll::Ready(None));
        drop(tx);
        assert_eq!(group.poll_next_unpin(&mut cx), Poll::Ready(None));
    }

    #[test]
    fn concurrent_multithread_send_poll_stress() {
        const STREAMS: usize = 8;
        const MSGS_PER_STREAM: usize = 2000;
        const CAP: usize = 16;

        let mut group = CmdGroup::new();
        let mut senders = Vec::with_capacity(STREAMS);
        for i in 0..STREAMS {
            let sid = StreamId::new((i as u32) * 2 + 1);
            let (tx, rx) = channel::<usize>(sid, CAP);
            group.push(rx);
            senders.push((sid, tx));
        }

        let consumer = std::thread::spawn(move || {
            let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
            let mut cx = Context::from_waker(&waker);
            let mut received: HashMap<StreamId, Vec<usize>> = HashMap::new();
            let mut closed_counts: HashMap<StreamId, usize> = HashMap::new();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);

            loop {
                assert!(std::time::Instant::now() < deadline, "consumer timed out");
                match group.poll_next_unpin(&mut cx) {
                    Poll::Ready(Some((id, Some(v)))) => {
                        received.entry(id).or_default().push(v);
                    }
                    Poll::Ready(Some((id, None))) => {
                        *closed_counts.entry(id).or_default() += 1;
                    }
                    Poll::Ready(None) => break,
                    Poll::Pending => {
                        std::thread::park_timeout(std::time::Duration::from_millis(5));
                    }
                }
            }
            (received, closed_counts)
        });

        let producers: Vec<_> = senders
            .into_iter()
            .map(|(sid, mut tx)| {
                std::thread::spawn(move || {
                    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
                    let mut cx = Context::from_waker(&waker);
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                    for seq in 0..MSGS_PER_STREAM {
                        loop {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "producer {sid} timed out"
                            );
                            match tx.poll_ready(&mut cx) {
                                Poll::Ready(Ok(())) => {
                                    tx.start_send(seq).expect("channel open");
                                    break;
                                }
                                Poll::Ready(Err(e)) => panic!("unexpected close: {e}"),
                                Poll::Pending => {
                                    std::thread::park_timeout(std::time::Duration::from_millis(5));
                                }
                            }
                        }
                    }
                    drop(tx);
                })
            })
            .collect();

        for p in producers {
            p.join().expect("producer thread");
        }
        let (received, closed_counts) = consumer.join().expect("consumer thread");

        assert_eq!(received.len(), STREAMS);
        assert_eq!(closed_counts.len(), STREAMS);
        let expected: Vec<usize> = (0..MSGS_PER_STREAM).collect();
        for i in 0..STREAMS {
            let sid = StreamId::new((i as u32) * 2 + 1);
            assert_eq!(received.get(&sid), Some(&expected));
            assert_eq!(closed_counts.get(&sid), Some(&1));
        }
    }
}
