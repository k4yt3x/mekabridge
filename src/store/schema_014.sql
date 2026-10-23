-- What the envelope said about a message beyond its sender and text, as JSON: where its words came
-- from, what it answered, how its sender was let in and whether they were allowlisted, and the
-- markers on the sender's account.
--
-- Kept so a message read back carries the same labels it arrived with. Before this a forward read
-- from history was the forwarder's own words, and a display name was the only sign of who spoke.
-- NULL on every row recorded before it, which the bridge reads as nothing being known rather than
-- as nothing being there.
ALTER TABLE messages ADD COLUMN labels TEXT;
