---
"@lucas-barake/effect-local-browser": patch
"@lucas-barake/effect-local-sql": patch
---

Keep peer session creation behind active replica claims and bind browser transport setup to one replica permit.

Preserve same fiber shared gate reentrancy while an exclusive claim is waiting.

Cancel browser peer persistence and network work before resetting a closed session.
