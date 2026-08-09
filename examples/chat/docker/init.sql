-- The Playwright suite runs its own relay against a separate database so test traffic never mixes
-- with the dev relay's inboxes.
CREATE DATABASE chat_test;
