//! The inbound path: channel events into the durable queue, and the queue into turns.
//!
//! Two tasks, deliberately separate. The writer persists every event before acknowledging it, so a
//! crash cannot swallow a message a user already sent. The drain loop claims batches and runs
//! turns, and is the only thing that talks to meka, which is what enforces "one turn at a time"
//! without any locking: there is exactly one drain loop.
//!
//! Batching is the reason messages that pile up during a turn become one turn rather than several.
//! That matches what happens to a person who puts their phone down: they come back to the whole
//! conversation, not to one message at a time, and it saves a provider round trip per message.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    bridge::{
        envelope::{self, Envelope},
        turn::TurnRunner,
    },
    channel::{ChannelRegistry, ConversationId, InboundEvent},
    config::Config,
    meka::{MekaClient, MekaError, TurnOutcome},
    store::{ConversationRecord, EnqueueOutcome, QueuedMessage, Store},
};

/// Safety-net poll interval. The writer notifies the drain loop directly, so this only covers rows
/// that became eligible without an enqueue, such as a failed batch returning to `pending`.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Length of the per-turn fence marker. Six hex characters is 24 bits, which is far more than
/// enough against a user guessing it inside a single turn.
const NONCE_BYTES: usize = 3;

/// Persist events from every channel into the queue.
///
/// Runs until the sender side closes, which happens when all channels have stopped.
pub async fn writer(
    store: Store,
    config: Arc<Config>,
    mut events: mpsc::Receiver<InboundEvent>,
    wake_drain: Arc<Notify>,
) {
    while let Some(mut event) = events.recv().await {
        let conversation = event.conversation().clone();
        if let Err(error) = record_conversation(&store, &event).await {
            tracing::error!(conversation = %conversation, "failed to record conversation: {}", error);
            continue;
        }
        if let Err(error) = register_attachments(&store, &mut event).await {
            // Registration is what mints the handles the agent fetches by, so without it the files
            // are unreachable. The message still goes through: its text is usually the point, and
            // the envelope says the attachment cannot be fetched rather than pretending otherwise.
            tracing::error!(conversation = %conversation, "failed to register attachments: {}", error);
        }

        let payload = match serde_json::to_string(&event) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::error!(conversation = %conversation, "failed to encode event: {}", error);
                continue;
            }
        };

        let outcome = store
            .enqueue(
                conversation.as_str(),
                event.external_id(),
                &payload,
                event.timestamp(),
                config.bridge.max_queue_depth,
            )
            .await;

        match outcome {
            Ok(EnqueueOutcome::Queued) => wake_drain.notify_one(),
            Ok(EnqueueOutcome::Duplicate) => {
                tracing::debug!(
                    conversation = %conversation,
                    external_id = event.external_id(),
                    "ignoring a redelivered message"
                );
            }
            Ok(EnqueueOutcome::Dropped) => {
                // Counted rather than silently discarded: the next envelope tells the agent its
                // view of the conversation is incomplete.
                tracing::warn!(
                    conversation = %conversation,
                    "inbound queue is full at {} messages; dropping",
                    config.bridge.max_queue_depth
                );
                if let Err(error) = store.note_dropped(1).await {
                    tracing::error!("failed to record a dropped message: {}", error);
                }
            }
            Err(error) => {
                tracing::error!(conversation = %conversation, "failed to enqueue: {}", error);
            }
        }
    }
    tracing::info!("inbound writer stopped: all channels have shut down");
}

/// Register the files an event brought with it and stamp each with the handle the agent fetches by.
///
/// Done here rather than in the channel: the channel has no store handle, and giving it one would
/// couple every platform to the database for no other reason. Runs before the payload is
/// serialized, so the handles travel with the queued event and survive a restart.
async fn register_attachments(
    store: &Store,
    event: &mut InboundEvent,
) -> Result<(), crate::store::StoreError> {
    let InboundEvent::Message(message) = event;
    for (index, attachment) in message.attachments.iter_mut().enumerate() {
        let handle = store
            .register_attachment(crate::store::AttachmentRecord {
                // Stable across a redelivery of the same message, so a replay reuses the handle
                // already issued rather than minting a second one for the same file.
                id: format!("{}:{}:{index}", message.conversation, message.external_id),
                conversation_id: message.conversation.as_str().to_string(),
                channel_id: message.channel.as_str().to_string(),
                kind: attachment.kind.as_str().to_string(),
                file_ref: attachment.file_ref.clone(),
                thumb_ref: attachment.thumb_ref.clone(),
                file_name: attachment.file_name.clone(),
                media_type: attachment.media_type.clone(),
                bytes: attachment.bytes,
                path: None,
                created_at: message.timestamp,
            })
            .await?;
        attachment.handle = Some(handle);
    }
    Ok(())
}

/// Store the conversation an event came from, so it stays in the address book the agent can list.
async fn record_conversation(
    store: &Store,
    event: &InboundEvent,
) -> Result<(), crate::store::StoreError> {
    let InboundEvent::Message(message) = event;
    store
        .upsert_conversation(ConversationRecord {
            id: message.conversation.as_str().to_string(),
            channel_id: message.channel.as_str().to_string(),
            platform: message.platform.as_str().to_string(),
            chat: message.conversation.chat().to_string(),
            thread: message.conversation.thread().map(str::to_string),
            title: message
                .chat_title
                .clone()
                .or_else(|| Some(message.sender.display_name.clone())),
            kind: message.chat_kind.as_str().to_string(),
            created_at: message.timestamp,
            last_inbound_at: Some(message.timestamp),
            last_outbound_at: None,
        })
        .await
}

/// Everything the drain loop needs.
pub struct DrainContext {
    pub store: Store,
    pub config: Arc<Config>,
    pub meka: MekaClient,
    pub channels: Arc<ChannelRegistry>,
    pub runner: TurnRunner,
    /// Guards the one-per-process reconciliation of the session's permission level.
    pub permission_checked: Arc<tokio::sync::OnceCell<()>>,
}

/// Claim batches and run turns until `shutdown` fires.
///
/// A turn already in flight is allowed to finish; only the wait between turns is interruptible.
/// Cutting a turn off mid-flight would leave its batch `in_flight` for the next start to recover,
/// having already spent the provider tokens.
pub async fn drain_loop(context: DrainContext, wake: Arc<Notify>, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("drain loop stopping");
                return;
            }
            () = wake.notified() => {}
            () = tokio::time::sleep(DRAIN_POLL_INTERVAL) => {}
        }

        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let batch = match context
                .store
                .claim_batch(context.config.bridge.batch_max_messages)
                .await
            {
                Ok(batch) => batch,
                Err(error) => {
                    tracing::error!("failed to claim a batch: {}", error);
                    break;
                }
            };
            if batch.is_empty() {
                break;
            }
            deliver(&context, batch).await;
        }
    }
}

/// Hand one batch to the agent and record what happened to it.
async fn deliver(context: &DrainContext, batch: Vec<QueuedMessage>) {
    let sequences: Vec<i64> = batch.iter().map(|message| message.seq).collect();

    let mut events: Vec<InboundEvent> = Vec::with_capacity(batch.len());
    let mut undecodable = Vec::new();
    for message in &batch {
        match serde_json::from_str::<InboundEvent>(&message.payload) {
            Ok(event) => events.push(event),
            Err(error) => {
                // A payload this build cannot read will never become readable, so retrying it would
                // wedge the queue behind it forever.
                tracing::error!(
                    seq = message.seq,
                    "dropping an undecodable queue payload: {}",
                    error
                );
                undecodable.push(message.seq);
            }
        }
    }
    if !undecodable.is_empty()
        && let Err(error) = context.store.complete_batch(&undecodable).await
    {
        tracing::error!("failed to discard undecodable payloads: {}", error);
    }
    if events.is_empty() {
        return;
    }

    let conversations: BTreeSet<ConversationId> = events
        .iter()
        .map(|event| event.conversation().clone())
        .collect();

    let dropped = context.store.take_dropped().await.unwrap_or_else(|error| {
        tracing::error!("failed to read the dropped-message counter: {}", error);
        0
    });

    let session_id = match ensure_session(context).await {
        Ok(session_id) => session_id,
        Err(error) => {
            record_failure(context, &sequences, &error.to_string()).await;
            return;
        }
    };

    // Tracks whether the message actually submitted carried a preamble, which is not the same as
    // whether one was needed at the start: the session-recreate path below binds a fresh session
    // and adds a preamble even when the first attempt did not have one.
    let mut preamble_included = context
        .store
        .preamble_sent()
        .await
        .map(|sent| !sent)
        .unwrap_or(true);
    let preamble_text = if preamble_included {
        Some(envelope::preamble(&channel_identities(context).await))
    } else {
        None
    };

    let nonce = nonce();
    let message = Envelope {
        events: &events,
        dropped,
        preamble: preamble_text.as_deref(),
        nonce: &nonce,
    }
    .render();

    tracing::info!(
        messages = events.len(),
        conversations = conversations.len(),
        session_id = %session_id,
        "submitting a turn"
    );

    let result = submit(context, session_id, &message, &conversations).await;

    // meka forgetting the session (its row was deleted, or the database was replaced) is
    // recoverable exactly once: bind a fresh session and replay the same batch into it.
    let result = match result {
        Err(error) if error.is_session_missing() && context.config.session.recreate_on_missing => {
            tracing::warn!(
                "meka no longer knows session {}; creating a replacement. The agent's memory of \
                 earlier conversations is gone. If this keeps happening, check meka's \
                 `[serve].delete_on_idle`: with it on, an idle session's row is deleted rather \
                 than merely evicted, which wipes the assistant every idle_timeout.",
                session_id
            );
            if let Err(error) = context.store.clear_session_id().await {
                tracing::error!("failed to clear the stale session binding: {}", error);
            }
            match ensure_session(context).await {
                Ok(replacement) => {
                    // A replacement session has an empty context, so it needs orienting even if the
                    // one it replaces had already been oriented.
                    preamble_included = true;
                    let preamble_text = envelope::preamble(&channel_identities(context).await);
                    let message = Envelope {
                        events: &events,
                        dropped,
                        preamble: Some(&preamble_text),
                        nonce: &nonce,
                    }
                    .render();
                    submit(context, replacement, &message, &conversations).await
                }
                Err(error) => Err(error),
            }
        }
        other => other,
    };

    match result {
        // A turn that produced meka's empty-response stand-in and called nothing did no work at
        // all: no message was sent, no tool ran. Handing the batch over again is therefore free of
        // side effects, and far better than leaving somebody who just messaged the bot in silence.
        Ok(report) if report.produced_nothing() => {
            tracing::warn!(
                text = %report.text_preview.trim(),
                "the model returned an empty response and called no tools; retrying the batch"
            );
            record_failure(context, &sequences, "the model returned an empty response").await;
        }
        Ok(report) => {
            match &report.outcome {
                TurnOutcome::Finished { stop_reason, .. } => tracing::info!(
                    stop_reason = %stop_reason,
                    sends = report.sends,
                    tool_calls = report.tool_calls,
                    text_chars = report.text_length,
                    "turn finished"
                ),
                TurnOutcome::Cancelled { reason } => {
                    tracing::warn!(reason = %reason, "turn cancelled");
                }
            }
            if report.is_silent() {
                // Not an error: the agent is allowed to read something and say nothing. Logged at
                // warn with what it produced instead, because from the other end this looks exactly
                // like a broken bridge, and the text is usually what explains which one it was.
                tracing::warn!(
                    conversations = conversations.len(),
                    tool_calls = report.tool_calls,
                    text = %report.text_preview.trim(),
                    "the agent sent no messages this turn"
                );
            }
            if let Err(error) = context.store.complete_batch(&sequences).await {
                tracing::error!("failed to mark a delivered batch: {}", error);
            }
            if preamble_included && let Err(error) = context.store.mark_preamble_sent().await {
                tracing::error!("failed to record that the preamble was sent: {}", error);
            }
            if let Err(error) = context.store.mark_turn_completed(chrono::Utc::now()).await {
                tracing::error!("failed to record the turn timestamp: {}", error);
            }
        }
        // The turn was accepted and then the stream died. meka keeps running it, so the batch did
        // reach the agent and resubmitting would duplicate a reply the user is about to receive.
        // The messages are marked delivered once the session goes idle: what the agent chose to do
        // with them is its business, and that is exactly the contract for a normal turn too.
        Err(error) if error.turn_may_still_be_running() => {
            tracing::warn!(
                "lost the turn stream ({}); the turn is still running, waiting for it to finish",
                error
            );
            match context
                .meka
                .wait_until_idle(session_id, context.config.meka.turn_timeout)
                .await
            {
                Ok(true) => {
                    tracing::info!("the interrupted turn finished; marking its batch delivered");
                    if let Err(error) = context.store.complete_batch(&sequences).await {
                        tracing::error!("failed to mark a delivered batch: {}", error);
                    }
                    if preamble_included
                        && let Err(error) = context.store.mark_preamble_sent().await
                    {
                        tracing::error!("failed to record that the preamble was sent: {}", error);
                    }
                    if let Err(error) = context.store.mark_turn_completed(chrono::Utc::now()).await
                    {
                        tracing::error!("failed to record the turn timestamp: {}", error);
                    }
                }
                Ok(false) | Err(_) => {
                    // Still busy after a full turn budget, or unreachable. Requeueing risks a
                    // duplicate, but leaving the batch in flight forever loses it outright, and
                    // the attempt counter bounds how often that can repeat.
                    tracing::error!(
                        "could not confirm the interrupted turn finished; requeueing its batch, \
                         which may deliver the same messages twice"
                    );
                    record_failure(context, &sequences, &error.to_string()).await;
                }
            }
        }
        Err(error) => {
            tracing::error!("turn failed: {}", error);
            record_failure(context, &sequences, &error.to_string()).await;
        }
    }
}

/// Submit a turn, waiting out a turn that is already running instead of failing on the 409.
///
/// A `turn-in-flight` rejection means one of this bridge's own earlier turns is still going, which
/// happens when a previous stream dropped. The batch was refused before it ran, so waiting and
/// resubmitting delivers it exactly once.
async fn submit(
    context: &DrainContext,
    session_id: Uuid,
    message: &str,
    conversations: &BTreeSet<ConversationId>,
) -> Result<crate::bridge::turn::TurnReport, MekaError> {
    let first = context.runner.run(session_id, message, conversations).await;
    if !first
        .as_ref()
        .err()
        .is_some_and(MekaError::is_turn_in_flight)
    {
        return first;
    }
    tracing::info!("a turn is already running on this session; waiting for it to finish");
    match context
        .meka
        .wait_until_idle(session_id, context.config.meka.turn_timeout)
        .await
    {
        Ok(true) => context.runner.run(session_id, message, conversations).await,
        Ok(false) => {
            tracing::warn!("the session is still busy after a full turn budget");
            first
        }
        Err(error) => {
            tracing::warn!("could not poll the session while waiting: {}", error);
            first
        }
    }
}

/// Mark a batch failed, and tell the operator about anything that will never be delivered.
async fn record_failure(context: &DrainContext, sequences: &[i64], reason: &str) {
    let outcome = context
        .store
        .fail_batch(sequences, reason, context.config.bridge.turn_retries)
        .await;
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            tracing::error!("failed to record a batch failure: {}", error);
            return;
        }
    };
    if !outcome.retrying.is_empty() {
        tracing::warn!(
            count = outcome.retrying.len(),
            "requeued messages after a failed turn"
        );
    }
    if outcome.exhausted.is_empty() {
        return;
    }
    tracing::error!(
        count = outcome.exhausted.len(),
        "giving up on messages after {} attempt(s); they will never reach the agent",
        context.config.bridge.turn_retries + 1
    );
    notify_owner(
        context,
        &format!(
            "mekabridge could not deliver {} message(s) to the agent after {} attempt(s). Last \
             error: {reason}",
            outcome.exhausted.len(),
            context.config.bridge.turn_retries + 1
        ),
    )
    .await;
}

/// Send an operator notice to the configured owner conversation.
///
/// This is the one case where the bridge writes chat content of its own. It is strictly about the
/// bridge being broken, never about the conversation, and it is skipped entirely when no owner is
/// configured.
async fn notify_owner(context: &DrainContext, text: &str) {
    let Some(owner) = &context.config.bridge.owner_conversation else {
        return;
    };
    let Some(conversation) = ConversationId::parse(owner) else {
        tracing::error!("[bridge].owner_conversation {:?} is not a valid id", owner);
        return;
    };
    let channel = match context.channels.resolve(&conversation) {
        Ok(channel) => channel,
        Err(error) => {
            tracing::error!("cannot reach the owner conversation: {}", error);
            return;
        }
    };
    if let Err(error) = channel
        .send_text(&conversation, text, &crate::channel::SendOptions::default())
        .await
    {
        tracing::error!("failed to notify the owner: {}", error);
    }
}

/// Bot identities for the preamble, so the agent knows which account people see it as.
async fn channel_identities(context: &DrainContext) -> Vec<(String, Option<String>)> {
    let mut identities = Vec::new();
    for channel in context.channels.iter() {
        let label = match channel.probe().await {
            Ok(identity) => identity
                .username
                .map(|username| format!("@{username}"))
                .or(Some(identity.display_name)),
            Err(error) => {
                tracing::debug!(channel = %channel.id(), "could not probe identity: {}", error);
                None
            }
        };
        identities.push((channel.id().as_str().to_string(), label));
    }
    identities
}

/// Bring a session's permission level in line with `[session].permission`.
///
/// A session's level is fixed when it is created, so without this an operator who edits the config
/// sees no effect and no explanation. `ask` is the case that makes it matter: meka prompts for
/// every tool call at that level and this bridge answers no prompts, so a session created at `ask`
/// cannot reply to anyone until its level changes.
///
/// Runs once per process, on the first turn rather than at startup, because the bridge comes up
/// before meka does.
async fn reconcile_permission(context: &DrainContext, session_id: Uuid) {
    if context.permission_checked.get().is_some() {
        return;
    }
    let desired = context.config.session.permission;
    match context.meka.session(session_id).await {
        Ok(info) if info.permission == desired.as_str() => {
            let _ = context.permission_checked.set(());
        }
        Ok(info) => {
            tracing::warn!(
                "session {} is at permission {:?} but the config says {:?}; updating it",
                session_id,
                info.permission,
                desired.as_str()
            );
            match context
                .meka
                .set_session_permission(session_id, desired)
                .await
            {
                Ok(()) => {
                    tracing::info!("session permission set to {:?}", desired.as_str());
                    let _ = context.permission_checked.set(());
                }
                Err(error) => {
                    tracing::error!("could not update the session permission: {}", error);
                }
            }
        }
        Err(error) => {
            tracing::debug!(
                "could not read the session to check its permission: {}",
                error
            );
        }
    }
}

/// Return the bound meka session, creating one if this is the first turn.
async fn ensure_session(context: &DrainContext) -> Result<Uuid, MekaError> {
    if let Some(session_id) = context
        .store
        .session_id()
        .await
        .map_err(|error| MekaError::Decode(error.to_string()))?
    {
        reconcile_permission(context, session_id).await;
        return Ok(session_id);
    }
    let session_id = context
        .meka
        .create_session(
            context.config.session.cwd.as_deref(),
            context.config.session.permission,
        )
        .await?;
    tracing::info!(session_id = %session_id, "created the meka session for this bridge");
    context
        .store
        .set_session_id(session_id)
        .await
        .map_err(|error| MekaError::Decode(error.to_string()))?;
    Ok(session_id)
}

/// Random fence marker for one turn's envelope.
fn nonce() -> String {
    use rand::RngExt as _;
    let bytes: [u8; NONCE_BYTES] = rand::rng().random();
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::channel::{
        Admission, Attachment, AttachmentKind, ChannelId, ChatKind, InboundMessage, Platform,
        Sender,
    };

    fn attachment(file_ref: &str) -> Attachment {
        Attachment {
            kind: AttachmentKind::Photo,
            file_name: None,
            media_type: Some("image/jpeg".to_string()),
            bytes: Some(2048),
            file_ref: file_ref.to_string(),
            thumb_ref: None,
            handle: None,
        }
    }

    fn event_with(attachments: Vec<Attachment>) -> InboundEvent {
        InboundEvent::Message(InboundMessage {
            channel: ChannelId::new("telegram"),
            platform: Platform::Telegram,
            conversation: ConversationId::parse("telegram:1").expect("valid"),
            external_id: "1".to_string(),
            message_id: "1".to_string(),
            chat_kind: ChatKind::Direct,
            chat_title: None,
            sender: Sender {
                id: "1".to_string(),
                display_name: "Alice".to_string(),
                username: None,
                is_bot: false,
                on_behalf_of_chat: false,
            },
            admission: Admission::User,
            text: "look".to_string(),
            reply_to: None,
            edited_at: None,
            forwarded_from: None,
            group_id: None,
            notes: Vec::new(),
            attachments,
            timestamp: Utc::now(),
        })
    }

    async fn store() -> Store {
        let store = Store::open_in_memory().await.expect("opens");
        store
            .upsert_conversation(ConversationRecord {
                id: "telegram:1".to_string(),
                channel_id: "telegram".to_string(),
                platform: "telegram".to_string(),
                chat: "1".to_string(),
                thread: None,
                title: Some("Alice".to_string()),
                kind: "direct".to_string(),
                created_at: Utc::now(),
                last_inbound_at: Some(Utc::now()),
                last_outbound_at: None,
            })
            .await
            .expect("conversation");
        store
    }

    #[tokio::test]
    async fn registration_stamps_a_handle_onto_every_attachment() {
        // The handle is the agent's only way to reach the file, and it has to be on the event
        // before the payload is serialized or it does not survive a restart.
        let store = store().await;
        let mut event = event_with(vec![attachment("AgACx1"), attachment("AgACx2")]);
        register_attachments(&store, &mut event)
            .await
            .expect("registers");

        let InboundEvent::Message(message) = &event;
        let handles: Vec<&str> = message
            .attachments
            .iter()
            .map(|attachment| {
                attachment
                    .handle
                    .as_deref()
                    .expect("every attachment gets a handle")
            })
            .collect();
        assert_eq!(handles.len(), 2);
        assert_ne!(handles[0], handles[1], "handles must be distinct");
    }

    #[tokio::test]
    async fn a_redelivered_message_reuses_its_original_handles() {
        // Telegram replays updates whose offset was never committed. Minting a second handle for
        // the same file would leave an orphan row the sweep later deletes out from under
        // the agent.
        let store = store().await;
        let mut first = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut first)
            .await
            .expect("registers");
        let mut second = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut second)
            .await
            .expect("registers again");

        let InboundEvent::Message(first) = &first;
        let InboundEvent::Message(second) = &second;
        assert_eq!(first.attachments[0].handle, second.attachments[0].handle);
    }

    #[tokio::test]
    async fn a_registered_attachment_can_be_looked_up_by_its_handle() {
        let store = store().await;
        let mut event = event_with(vec![attachment("AgACx1")]);
        register_attachments(&store, &mut event)
            .await
            .expect("registers");

        let InboundEvent::Message(message) = &event;
        let handle = message.attachments[0]
            .handle
            .as_deref()
            .expect("handle assigned");
        let record = store
            .attachment(handle)
            .await
            .expect("query")
            .expect("the handle resolves");
        assert_eq!(record.file_ref, "AgACx1");
        assert_eq!(record.channel_id, "telegram");
        assert!(record.path.is_none(), "nothing is downloaded on arrival");
    }

    #[tokio::test]
    async fn a_message_without_attachments_registers_nothing() {
        let store = store().await;
        let mut event = event_with(Vec::new());
        register_attachments(&store, &mut event)
            .await
            .expect("registers");
        assert!(store.attachment("1").await.expect("query").is_none());
    }

    #[test]
    fn nonces_are_hex_and_hard_to_guess() {
        let first = nonce();
        assert_eq!(first.len(), NONCE_BYTES * 2);
        assert!(first.chars().all(|character| character.is_ascii_hexdigit()));

        // Not a statistical test, just a guard against a constant sneaking in: a fixed fence marker
        // would let a user close the fence and forge a header.
        let samples: BTreeSet<String> = (0..32).map(|_| nonce()).collect();
        assert!(samples.len() > 1, "nonce must vary between turns");
    }
}
