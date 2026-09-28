# Cross-repository contracts

**State:** PROPOSED. EG owns the engine contract and this spec. Other owners keep their own implementation and native specs; links here define integration and validation, not a second implementation authority.

| Owner | Contract and T9 rows | Required cross-reference / acceptance |
|---|---|---|
| `agent-packages/agent-utilities` | Existing external graph schema proposal flow is an input to relational mapping; AU `fanout_backend` semantics and Debezium translator are sources for EG migration, not retained runtime authorities (EH-508/667/680/692). | Compare consumer behavior and delete old ingestion/mirror path only after EG parity and restore evidence. |
| `agent-packages/agent-connector-sdk` | Source/connector identity, capability declaration, app-API-first write-back and one-way import contract (EH-671/714). | Stable source IDs and idempotency; no duplicate secrets registry or bypass of audit reservation. |
| `agent-packages/graph-os` | `schema_context` exposed alongside `code_context`, source-position-aware query route and EH-658 audit reservation (EH-670/671/693). | Authenticated request, tenant and actor propagate to EG; live route and denied route traces. |
| `agent-packages/agents/immich-mcp` | Immich v3.2.2 OpenAPI-generated client, MCP tools, incremental media/people ingestion (EH-696). | I0–I6 in pilot (`plans/database/PILOT-IMMICH.md`); per-user scope and six-site fleet registration. |
| `agent-packages/agents/gramps-mcp` | Existing Gramps connector and ontology identity support pilot comparison and cross-app resolution (EH-687/694). | P0–P6 in pilot (`plans/database/PILOT-GRAMPS.md`); production tree remains unchanged through P5. |
| `agent-packages/emerald-exchange` and finance-v1 EG owner | Ghostfolio activities map into finance-v1 as one-way, provenance-bearing import (EH-714). | Cross-link T8 finance spec; no Ghostfolio retirement before EG accounting parity and an explicit decision. |
| `services/` | CloudNativePG groups by version/extension, MariaDB group, app roles, backups and restore drills (EH-682/683). | Per-app admission with rollback; Immich VectorChord group stays isolated. |
| `agent-packages/agents/repository-manager` | `workspace.yml` validation/mirror sync for any new connector package. | Validate all three manifest copies and release catalogs; never create a second fleet manifest. |

The current program authority is database README (`plans/database/README.md`), T9 lane map (`plans/refactor/TRAINS-8-9.md`) and ledger (`plans/refactor/LEDGER.md`). Each cross-repository PR should link its owner-native spec, this contract, exact ledger row and tested version of the counterparty. As owner specs land, replace these repository-level links with their stable spec IDs without changing ownership.
