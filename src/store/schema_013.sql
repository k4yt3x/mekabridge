-- When meka stops retrying the item a row was handed over as.
--
-- meka answers an inbox post with `retry_until`, the item's enqueue plus its own
-- `[serve] inbox_retry_ceiling`. The feed's `inbox.deferred` names the next attempt but not that
-- deadline, and the ceiling is meka's setting rather than this bridge's, so the owner's notice of a
-- failing turn can say when meka gives up only if the answer to the post was written down.
ALTER TABLE inbound_queue ADD COLUMN retry_until TEXT;
