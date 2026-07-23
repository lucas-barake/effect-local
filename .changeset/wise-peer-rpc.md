---
"@lucas-barake/effect-local-rpc": minor
"@lucas-barake/effect-local-sql": minor
"@lucas-barake/effect-local-browser": patch
---

Introduce platform neutral Effect RPC building blocks for authenticated peer synchronization, bounded server admission, and generated client transport integration.

Move peer session construction into the SQL package and add supervised session lifecycle APIs for server transports.

Keep the browser PeerSession subpath as a compatibility reexport of the transport neutral SQL API.
