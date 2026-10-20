# Memory Bridge: one canonical store shared across scenarios

> AstrBot plugin · Write once, recall everywhere, legacy stores stay read-only

## The problem

Memory usually ends up scattered: a vector store, a journal, raw group logs, platform-side watch history.

Each scenario only sees its own slice — posting on one platform can't recall what was said in a group chat, and vice versa.

## Features

- `remember` — write to the single canonical store (`memory.db`)
- `recall` — canonical store first, then a **federated query** across legacy stores, merged and ranked
- Legacy stores are read-only, so there are never two conflicting copies
- Chinese-friendly scoring: character bigrams + keyword hits × time decay
- Optional semantic search via local bge-small embeddings + numpy cosine
- `/mem_import` command for bulk imports

## Install

### Option 1: Plugin marketplace (recommended)

Search "唯一记忆桥" in the AstrBot WebUI plugin marketplace.

### Option 2: Download from Releases

Grab the zip from [Releases](../../releases), then WebUI → Plugins → Upload.

### Option 3: Clone manually

```bash
cd AstrBot/data/plugins
git clone https://github.com/RandyLuaW/astrbot_plugin_memory_bridge.git
```

No extra Python dependencies.

## Configuration

| Key | Type | Default | Description |
|---|---|---|---|
| `semantic_enable` | bool | `true` | See Chinese README |
| `embedding_model` | string | `` | See Chinese README |
| `note` | string | `` | See Chinese README |

## Notes

- `semantic_enable` costs a few hundred MB of RAM for the embedding model
- The canonical store is plain SQLite in the plugin data directory
- Legacy stores are never written to

## License

MIT
