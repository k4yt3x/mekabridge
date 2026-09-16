//! The session feed: one connection per session, carrying every turn's events, whoever started the
//! turn, for as long as the bridge runs.
//!
//! The reader does one thing: keep the connection open and hand every frame to the session task as
//! fast as it arrives. It never touches the store and decides nothing, which is what keeps meka's
//! per-consumer buffer drained while the session task is busy with a request of its own. A reader
//! that fell behind would be told so with a notice and kept, but a model streaming a reply produces
//! frames faster than a request round trip, so the two have to be separate tasks.
//!
//! A dropped connection is reopened from the last id the session task handled, and meka replays
//! what was missed, across turns. The reader says when a connection has spoken, since that is when
//! the session task reconciles what it was waiting on, whether or not the replay had a hole in it.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::meka::{MekaClient, MekaError, StreamItem};

/// What the reader hands the session task.
///
/// Every variant names the session it came from. Event ids are numbered per session and this
/// channel is thousands deep, so a rebind leaves the feed of the session just replaced still
/// draining into a task that has already moved on; without the name there is nothing to tell those
/// events apart from the new session's, and crediting one of their ids to the feed position would
/// reopen the new feed from a number that means nothing in its numbering.
#[derive(Debug)]
pub enum FeedEvent {
    /// One event off the feed.
    Item { session: Uuid, item: StreamItem },
    /// A connection is open, at startup or after a drop.
    ///
    /// `at` is when it opened, and it is what the session task reconciles against: anything handed
    /// over before that moment may have had its outcome reported while nothing was listening, and
    /// anything after is on this connection. Sent on the open rather than on the first frame,
    /// because a session that is quiet for hours is the ordinary case and a hand-over whose fate
    /// was missed cannot wait for somebody to speak.
    Connected { session: Uuid, at: DateTime<Utc> },
    /// meka says the session the feed was opened on no longer exists. Named, because a post may
    /// have hit the same 404 first and bound a replacement already.
    SessionGone { session: Uuid },
}

/// Longest wait between attempts to reopen the feed.
///
/// Short, because nothing else tells the bridge what became of a hand-over: while the feed is down
/// an answered message reads as unanswered, so the cost of a wasted attempt is a request and the
/// cost of a long wait is a person waiting.
const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(30);

/// Hold the session's feed open, reconnecting until `shutdown`.
///
/// `session` is the session to follow, and changes when the bridge binds a replacement; a
/// connection open on the old one is dropped and the new one's feed opened. `position` is the last
/// event id the session task handled, read fresh on every connection so the replay starts strictly
/// after it; zero means nothing handled yet.
pub async fn feed_reader(
    meka: MekaClient,
    mut session: watch::Receiver<Option<Uuid>>,
    position: Arc<AtomicU64>,
    sender: mpsc::Sender<FeedEvent>,
    shutdown: CancellationToken,
) {
    let mut failures = 0_u32;
    loop {
        let current = *session.borrow_and_update();
        let session_id = match current {
            Some(session_id) => session_id,
            None => {
                // Nothing to follow yet: the session task is still binding one.
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    changed = session.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        continue;
                    }
                }
            }
        };
        let last = position.load(Ordering::SeqCst);
        let opened = tokio::select! {
            () = shutdown.cancelled() => return,
            opened = meka.open_feed(session_id, (last > 0).then_some(last)) => opened,
        };
        let stream = match opened {
            Ok(stream) => {
                if sender
                    .send(FeedEvent::Connected {
                        session: session_id,
                        at: Utc::now(),
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                stream
            }
            Err(error) if error.is_session_missing() => {
                tracing::warn!(
                    "meka no longer knows session {session_id}, so its feed cannot be opened"
                );
                if sender
                    .send(FeedEvent::SessionGone {
                        session: session_id,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
                // Waited for rather than retried at once: the door is gone, and the session task
                // is the one that opens another. Timed as well as woken, because it only opens one
                // when `[session].recreate_on_missing` is on, and without the timer this would be
                // the single non-shutdown path where the reader stops for good -- deaf even to a
                // 404 that was transient.
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(RECONNECT_DELAY_MAX) => {}
                    changed = session.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
                continue;
            }
            Err(error) => {
                failures += 1;
                let wait = reconnect_delay(failures);
                tracing::warn!(
                    "could not open the session feed ({error}); trying again in {wait:?}"
                );
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(wait) => {}
                }
                continue;
            }
        };

        let mut stream = Box::pin(stream);
        let mut spoke = false;
        loop {
            let next = tokio::select! {
                () = shutdown.cancelled() => return,
                next = stream.next() => next,
                // A rebind while this connection is open: it follows a session that is no longer
                // the bridge's, so it is dropped for the new one's feed.
                changed = session.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    break;
                }
            };
            match next {
                Some(Ok(item)) => {
                    // The backoff is reset by a connection that carried something rather than by
                    // one that merely opened: one dying the instant it speaks would otherwise be
                    // retried in a hot loop.
                    spoke = true;
                    failures = 0;
                    if sender
                        .send(FeedEvent::Item {
                            session: session_id,
                            item,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                // A frame this build cannot read decides nothing on its own, and reconnecting
                // would replay it out of meka's ring to fail identically.
                Some(Err(MekaError::Decode(reason))) => {
                    tracing::warn!("skipping a feed frame this build cannot read: {reason}");
                }
                Some(Err(error)) => {
                    tracing::warn!("lost the session feed ({error}); reconnecting");
                    break;
                }
                None => {
                    tracing::warn!("the session feed closed; reconnecting");
                    break;
                }
            }
        }
        // A connection that never spoke is a failure like a refused one; one that did resets the
        // backoff above, so a drop after hours of quiet is retried at once.
        if !spoke {
            failures += 1;
        }
        let wait = reconnect_delay(failures);
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(wait) => {}
        }
    }
}

/// How long to wait before the next attempt, after `failures` in a row.
///
/// Doubles from one second and is capped; jittered so a bridge and a meka restarting together
/// under systemd do not retry in lockstep.
fn reconnect_delay(failures: u32) -> Duration {
    use rand::RngExt as _;

    let base = Duration::from_secs(1) * 2_u32.saturating_pow(failures.saturating_sub(1).min(5));
    let capped = base.min(RECONNECT_DELAY_MAX);
    let jitter = rand::rng().random_range(0..=(capped.as_millis() / 4) as u64);
    capped + Duration::from_millis(jitter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reconnect_wait_grows_and_is_bounded() {
        // A connection that spoke and then dropped is retried after the base wait: `failures` is
        // zero after a good connection, and one after a single refusal.
        for failures in [0, 1] {
            let wait = reconnect_delay(failures);
            assert!(
                wait >= Duration::from_secs(1) && wait < Duration::from_secs(2),
                "{failures}: {wait:?}"
            );
        }
        let wait = reconnect_delay(3);
        assert!(
            wait >= Duration::from_secs(4) && wait < Duration::from_secs(6),
            "{wait:?}"
        );
        // Bounded, and bounded without panicking on whatever count a long outage reaches.
        for failures in [10, u32::MAX] {
            let wait = reconnect_delay(failures);
            assert!(
                wait >= RECONNECT_DELAY_MAX
                    && wait <= RECONNECT_DELAY_MAX + RECONNECT_DELAY_MAX / 4,
                "{failures}: {wait:?}"
            );
        }
    }
}
