//! Uring state lifecycle.
//! Partly borrow from tokio-uring.

use std::{
    collections::VecDeque,
    io,
    task::{Context, Poll, Waker},
};

use crate::{driver::op::CompletionMeta, utils::slab::Ref};

pub(crate) const IORING_CQE_F_BUFFER: u32 = 1 << 0;
pub(crate) const IORING_CQE_F_MORE: u32 = 1 << 1;

#[derive(Debug)]
pub(crate) struct MultishotCqe {
    pub result: io::Result<u32>,
    pub flags: u32,
    pub is_final: bool,
}

#[derive(Debug)]
pub(crate) enum MultishotPollResult {
    Ready(MultishotCqe),
    Terminated(MultishotCqe),
    Pending,
    Done,
}

pub(crate) enum Lifecycle {
    /// The operation has been submitted to uring and is currently in-flight
    Submitted,

    /// The submitter is waiting for the completion of the operation
    Waiting(Waker),

    /// The submitter no longer has interest in the operation result. The state
    /// must be passed to the driver and held until the operation completes.
    #[allow(dead_code)]
    Ignored(Box<dyn std::any::Any>),

    /// The operation has completed.
    Completed(io::Result<u32>, u32),

    /// Active multishot operation with queued completions
    Multishot {
        queue: VecDeque<MultishotCqe>,
        waker: Option<Waker>,
        terminated: bool,
    },
}

impl Lifecycle {
    #[inline]
    pub(crate) fn new_multishot(queue_capacity: usize) -> Self {
        Lifecycle::Multishot {
            queue: VecDeque::with_capacity(queue_capacity),
            waker: None,
            terminated: false,
        }
    }
}

impl<'a> Ref<'a, Lifecycle> {
    pub(crate) fn complete(mut self, result: io::Result<u32>, flags: u32) {
        let is_final = (flags & IORING_CQE_F_MORE) == 0;
        let ref_mut = &mut *self;

        match ref_mut {
            Lifecycle::Submitted => {
                *ref_mut = Lifecycle::Completed(result, flags);
            }
            Lifecycle::Waiting(_) => {
                let old = std::mem::replace(ref_mut, Lifecycle::Completed(result, flags));
                match old {
                    Lifecycle::Waiting(waker) => {
                        waker.wake();
                    }
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            }
            Lifecycle::Multishot {
                queue,
                waker,
                terminated,
            } => {
                queue.push_back(MultishotCqe {
                    result,
                    flags,
                    is_final,
                });
                if is_final {
                    *terminated = true;
                }
                if let Some(w) = waker.take() {
                    w.wake();
                }
            }
            Lifecycle::Ignored(..) => {
                if is_final {
                    self.remove();
                }
            }
            Lifecycle::Completed(..) => unsafe { std::hint::unreachable_unchecked() },
        }
    }

    #[allow(clippy::needless_pass_by_ref_mut)]
    pub(crate) fn poll_op(mut self, cx: &mut Context<'_>) -> Poll<CompletionMeta> {
        let ref_mut = &mut *self;
        match ref_mut {
            Lifecycle::Submitted => {
                *ref_mut = Lifecycle::Waiting(cx.waker().clone());
                return Poll::Pending;
            }
            Lifecycle::Waiting(waker) => {
                if !waker.will_wake(cx.waker()) {
                    *ref_mut = Lifecycle::Waiting(cx.waker().clone());
                }
                return Poll::Pending;
            }
            _ => {}
        }

        match self.remove() {
            Lifecycle::Completed(result, flags) => Poll::Ready(CompletionMeta { result, flags }),
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }

    pub(crate) fn drop_op<T: 'static>(mut self, data: &mut Option<T>) -> bool {
        let ref_mut = &mut *self;
        let terminated = match ref_mut {
            Lifecycle::Submitted | Lifecycle::Waiting(_) => false,
            Lifecycle::Completed(..) => true,
            Lifecycle::Multishot { terminated, .. } => *terminated,
            Lifecycle::Ignored(..) => unsafe { std::hint::unreachable_unchecked() },
        };

        if terminated {
            self.remove();
            true
        } else {
            let boxed: Box<dyn std::any::Any> = if let Some(d) = data.take() {
                Box::new(d)
            } else {
                Box::new(())
            };
            *ref_mut = Lifecycle::Ignored(boxed);
            false
        }
    }

    pub(crate) fn poll_multishot(mut self, cx: &mut Context<'_>) -> MultishotPollResult {
        let ref_mut = &mut *self;

        match ref_mut {
            Lifecycle::Multishot {
                queue,
                waker,
                terminated,
            } => {
                if let Some(cqe) = queue.pop_front() {
                    let is_final = cqe.is_final;

                    if *terminated && queue.is_empty() {
                        return MultishotPollResult::Terminated(cqe);
                    }

                    if is_final {
                        MultishotPollResult::Terminated(cqe)
                    } else {
                        MultishotPollResult::Ready(cqe)
                    }
                } else if *terminated {
                    MultishotPollResult::Done
                } else {
                    *waker = Some(cx.waker().clone());
                    MultishotPollResult::Pending
                }
            }

            _ => MultishotPollResult::Done,
        }
    }
}
