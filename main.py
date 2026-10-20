# -*- coding: utf-8 -*-
"""唯一记忆桥接层。

问题
----
    她的记忆现在分散在四个地方，各自为政：

        iris_memory   L2 向量记忆（560 条，faiss/metadata.db）
        self_journal  日记（journal.jsonl）
        ctx_window    群聊原文（sessions/*.jsonl）
        bilibili_ai_bot  B站观看/动作记忆（memory.json 等）

    每个场景只能看见自己那一份：B 站发动态时看不到她在群里喷了自己一身水，
    群里聊天时想不起她昨天在 B 站刷到的那封信。

做法
----
    **一个入口，一份正本，多份旧账。**

    - `remember()` 写入唯一正本（`memory.db`），不管从哪个场景来的
    - `recall()` 先查正本，再**联邦查询**上面四个旧库，合并去重后按相关度排序
    - 旧库只读、不回写。等正本攒够，旧库自然退化成历史档案，不会出现两份互相打架

    中文检索没有分词器，用 bigram + 关键词命中数打分，再乘时间衰减；
    够用且不依赖任何向量库（L2 的语义检索仍归 iris 管，这里只做跨场景拉取）。
"""

from __future__ import annotations

import asyncio
import json
import threading
import math
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star

PLUGIN_NAME = "astrbot_plugin_memory_bridge"
DEFAULT_LIMIT = 5
MAX_LIMIT = 10
# 单个旧库最多回多少条进候选池。不加这个上限，群聊里搜一个常用词
# （"哈哈"）会把几个群的 2000 行原文全捞出来参与打分。
MAX_PER_SOURCE = 200
def _default_model_path() -> str:
    """嵌入模型：AstrBot/models 和项目根 models 两处都找一遍。"""
    base = Path(__file__).resolve().parents[3]  # -> AstrBot/
    home = Path.home()
    for cand in (base / "models" / "bge-small-zh-v1.5",
                 base.parent / "models" / "bge-small-zh-v1.5",
                 home / "WorkBuddy" / "QQBot" / "models" / "bge-small-zh-v1.5"):
        if cand.is_dir():
            return str(cand)
    return str(base.parent / "models" / "bge-small-zh-v1.5")


DEFAULT_EMBED_MODEL = _default_model_path()

SCENE_LABEL = {
    "qq_group": "群聊",
    "qq_private": "私聊",
    "bilibili": "B站",
    "heybox": "小黑盒",
    "journal": "日记",
    "bridge": "记忆",
}

# 旧库位置（相对 AstrBot/data/plugin_data）
SOURCE_DIRS = {
    "iris": "astrbot_plugin_iris_memory/faiss/memory_default/metadata.db",
    "journal": "astrbot_plugin_self_journal/journal.jsonl",
    "ctx": "astrbot_plugin_ctx_window/sessions",
    "bilibili": "astrbot_plugin_bilibili_ai_bot/memory.json",
}


def _data_root() -> Path:
    return Path(__file__).resolve().parents[2] / "plugin_data"


def _self_dir() -> Path:
    d = _data_root() / PLUGIN_NAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def _db_path() -> Path:
    return _self_dir() / "memory.db"


# ---------------------------------------------------------------- 检索辅助


def _terms(query: str) -> list[str]:
    """把查询切成检索词：空白分词 + 中文/英文 bigram。

    没有分词器，bigram 是中文最省事又不至于太糟的办法。
    """
    q = (query or "").strip().lower()
    if not q:
        return []
    out: list[str] = []
    for tok in re.split(r"[\s，,。.！!？?、；;：:]+", q):
        tok = tok.strip()
        if len(tok) >= 2 and tok not in out:
            out.append(tok)
    for seg in re.findall(r"[\u4e00-\u9fff]+", q):
        for i in range(len(seg) - 1):
            bg = seg[i : i + 2]
            if bg not in out:
                out.append(bg)
    for seg in re.findall(r"[a-z0-9]{2,}", q):
        if seg not in out:
            out.append(seg)
    # 单字查询（"榜""水"）走不到 bigram，直接用原字，否则会误报"没给关键词"
    if not out and q:
        out = [q]
    return out[:10]


def _like(term: str) -> str:
    """把检索词转成 LIKE 模式。`%`/`_` 是通配符，用户搜「100%」「a_b」时必须转义，
    否则会退化成"匹配一切"。"""
    raw = str(term or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{raw}%"


def _score(text: str, terms: list[str], ts: float) -> float:
    """命中词数 + 时间衰减（半衰期 30 天）。"""
    if not text or not terms:
        return 0.0
    low = text.lower()
    hit = sum(1 for t in terms if t in low)
    if hit == 0:
        return 0.0
    freshness = 0.5 ** (max(0.0, time.time() - (ts or 0)) / (30 * 86400))
    return hit * (0.6 + 0.4 * freshness)


def _fmt_time(ts: float) -> str:
    if not ts:
        return "时间未知"
    gap = time.time() - ts
    if gap < 3600:
        return f"{int(gap // 60)} 分钟前"
    if gap < 86400:
        return f"{int(gap // 3600)} 小时前"
    if gap < 86400 * 30:
        return f"{int(gap // 86400)} 天前"
    return time.strftime("%Y-%m-%d", time.localtime(ts))


# ---------------------------------------------------------------- 存储


class MemoryStore:
    """唯一正本。所有场景的写入都落这里。"""

    def __init__(self, path: Path):
        self._path = path
        self._init()

    def _conn(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._path, timeout=10)
        con.row_factory = sqlite3.Row
        return con

    def _init(self) -> None:
        # 不建 fts5 表：默认 unicode61 分词器切不开中文，MATCH 查不了「当妈的那封信」
        # 这种词，实际检索一直是 bigram + LIKE。留一张不写的表只会让后来的人以为
        # 索引在这儿。（2026-10-20 审计删除，老库里残留的空表无害）
        with self._conn() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS memories(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    scene TEXT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'fact',
                    text TEXT NOT NULL,
                    tags TEXT DEFAULT '',
                    source TEXT DEFAULT 'self',
                    ref TEXT DEFAULT '',
                    importance REAL DEFAULT 0.5
                );
                CREATE INDEX IF NOT EXISTS idx_mem_ts ON memories(ts);
                CREATE INDEX IF NOT EXISTS idx_mem_scene ON memories(scene);
                """
            )

    def add(self, text: str, *, scene: str, kind: str, tags: str, source: str, ref: str, importance: float) -> int:
        text = (text or "").strip()
        if not text:
            return 0
        with self._conn() as con:
            cur = con.execute(
                "INSERT INTO memories(ts, scene, kind, text, tags, source, ref, importance)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (time.time(), scene, kind, text, tags, source, ref, importance),
            )
            return int(cur.lastrowid)

    def exists(self, text: str) -> bool:
        with self._conn() as con:
            row = con.execute(
                "SELECT 1 FROM memories WHERE text=? LIMIT 1", ((text or "").strip(),)
            ).fetchone()
            return row is not None

    def search(self, terms: list[str], limit: int) -> list[dict]:
        if not terms:
            return []
        like_clause = " OR ".join(["text LIKE ? ESCAPE '\\'"] * len(terms))
        params = [_like(t) for t in terms]
        with self._conn() as con:
            rows = con.execute(
                f"SELECT * FROM memories WHERE {like_clause}"
                f" ORDER BY ts DESC LIMIT ?",
                [*params, max(1, int(limit or 50))],
            ).fetchall()
        return [dict(r) for r in rows]

    def count(self) -> int:
        with self._conn() as con:
            return int(con.execute("SELECT COUNT(*) FROM memories").fetchone()[0])

    def texts(self) -> list[str]:
        """正本全文，给语义索引用。"""
        with self._conn() as con:
            return [str(r[0]) for r in con.execute("SELECT text FROM memories")]


# ---------------------------------------------------------------- 联邦查询


def _parse_ts(value: Any) -> float:
    """iris 的 timestamp 有 epoch 也有 ISO 字符串，两种都要吃。"""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        ts = float(value)
        return ts / 1000.0 if ts > 1e11 else ts
    raw = str(value).strip()
    try:
        ts = float(raw)
        return ts / 1000.0 if ts > 1e11 else ts
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return time.mktime(time.strptime(raw[:26], fmt))
        except Exception:
            continue
    return 0.0


def _search_iris(terms: list[str], limit: int) -> list[dict]:
    """iris L2：metadata.db 的 memories 表（content/group_id/user_id/timestamp）。"""
    path = _data_root() / SOURCE_DIRS["iris"]
    if not path.is_file() or not terms:
        return []
    like = " OR ".join(["content LIKE ? ESCAPE '\\'"] * len(terms))
    params = [_like(t) for t in terms]
    con = None
    try:
        con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            f"SELECT content, group_id, user_id, timestamp FROM memories WHERE {like} LIMIT 120",
            params,
        ).fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[memory-bridge] iris 查询失败：{exc}")
        return []
    finally:
        # 原来 close() 写在 try 里：一旦 execute 抛错，连接就漏了。
        # 这个函数每次 recall 都跑，漏起来是每秒级的句柄。
        if con is not None:
            try:
                con.close()
            except Exception:  # noqa: BLE001
                pass
    out = []
    for r in rows:
        out.append({
            "text": str(r["content"] or ""),
            "ts": _parse_ts(r["timestamp"]),
            "scene": "bridge",
            "origin": "iris",
        })
    return out


def _search_journal(terms: list[str], limit: int) -> list[dict]:
    path = _data_root() / SOURCE_DIRS["journal"]
    if not path.is_file():
        return []
    out = []
    try:
        from collections import deque as _deque

        with path.open(encoding="utf-8") as fh:
            tail = _deque(fh, maxlen=3000)
        for raw in tail:
            raw = raw.strip()
            if not raw:
                continue
            try:
                item = json.loads(raw)
            except Exception:
                continue
            text = str(item.get("content") or "").strip()
            ts = float(item.get("ts") or 0)
            low = text.lower()
            if text and (not terms or any(t in low for t in terms)):
                out.append({"text": text, "ts": ts, "scene": "journal", "origin": "journal"})
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"[memory-bridge] 日记读取失败：{exc}")
        return []
    return out[-MAX_PER_SOURCE:]


def _search_ctx(terms: list[str], limit: int) -> list[dict]:
    """群聊原文（她 + 群友），跨群。"""
    base = _data_root() / SOURCE_DIRS["ctx"]
    if not base.is_dir():
        return []
    out = []
    for path in base.glob("*GroupMessage*.jsonl"):
        try:
            from collections import deque as _deque

            with path.open(encoding="utf-8") as fh:
                tail = _deque(fh, maxlen=2000)
            for raw in tail:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    item = json.loads(raw)
                except Exception:
                    continue
                text = str(item.get("text") or "").strip()
                if not text or len(text) < 4:
                    continue
                low = text.lower()
                if terms and not any(t in low for t in terms):
                    continue
                role = "我" if item.get("role") == "assistant" else str(item.get("name") or "群友")
                out.append({
                    "text": text,
                    "ts": float(item.get("ts") or 0),
                    "scene": "qq_group",
                    "origin": "ctx",
                    "who": role,
                })
        except Exception:  # noqa: BLE001
            continue
    return out[-MAX_PER_SOURCE:]


def _search_bilibili(terms: list[str], limit: int) -> list[dict]:
    path = _data_root() / SOURCE_DIRS["bilibili"]
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:  # noqa: BLE001
        return []
    items = data if isinstance(data, list) else data.get("items") or []
    out = []
    for it in items:
        if not isinstance(it, dict):
            continue
        text = str(it.get("text") or it.get("content") or "").strip()
        if not text:
            continue
        low = text.lower()
        if terms and not any(t in low for t in terms):
            continue
        ts = float(it.get("ts") or 0)
        if not ts:
            try:
                ts = time.mktime(time.strptime(str(it.get("time"))[:19], "%Y-%m-%d %H:%M:%S"))
            except Exception:
                ts = 0.0
        out.append({"text": text, "ts": ts, "scene": "bilibili", "origin": "bilibili"})
    return out



class SemanticIndex:
    """可选的语义检索：给正本和日记做向量，余弦相似度。

    为什么默认关：会和 iris_memory 各加载一份 bge-small（92MB 权重 + 几百 MB
    运行时内存）。机器扛得住再开，配置项 `semantic_enable`。

    量级小（正本几百条），不建 faiss，numpy 暴力点积就够，
    也省掉索引落盘和版本兼容的麻烦。
    """

    def __init__(self, model_path: str):
        self._model_path = model_path
        self._model = None
        self._lock = threading.Lock()
        self._texts: list[str] = []
        self._vecs = None

    def available(self) -> bool:
        return Path(self._model_path).is_dir()

    def _load(self):
        if self._model is not None:
            return self._model
        from sentence_transformers import SentenceTransformer

        self._model = SentenceTransformer(self._model_path)
        logger.info("[memory-bridge] 嵌入模型已加载")
        return self._model

    def build(self, texts: list[str]) -> None:
        """重建索引（文本变化才调）。并发时只让一个线程建。"""
        import numpy as np

        with self._lock:
            self._texts = [t for t in texts if t]
            if not self._texts:
                self._vecs = None
                return
            model = self._load()
            self._vecs = np.asarray(
                model.encode(self._texts, normalize_embeddings=True, batch_size=32),
                dtype="float32",
            )

    def search(self, query: str, top: int = 20) -> list[tuple[str, float]]:
        import numpy as np

        if self._vecs is None or not self._texts:
            return []
        model = self._load()
        q = np.asarray(
            model.encode([query], normalize_embeddings=True), dtype="float32"
        )[0]
        sims = self._vecs @ q
        idx = np.argsort(-sims)[:top]
        return [(self._texts[i], float(sims[i])) for i in idx]



class MemoryBridgePlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.cfg = config or {}
        self.store = MemoryStore(_db_path())
        self._semantic = SemanticIndex(self._cfg_model())

    def _cfg(self, key: str, default=None):
        return self.cfg.get(key, default)

    def _cfg_model(self) -> str:
        return str(self._cfg("embedding_model", "") or DEFAULT_EMBED_MODEL)

    def _semantic_on(self) -> bool:
        v = self._cfg("semantic_enable", False)
        if isinstance(v, str):
            on = v.strip().lower() in ("1", "true", "yes", "on", "是")
        else:
            on = bool(v)
        # 两路都要过"模型在不在"这一关。原来字符串 "true" 直接 return True，
        # 模型没下下来时每次 recall 都白跑一次必然失败的 build。
        return on and self._semantic.available()

    def _semantic_texts(self) -> list[str]:
        """做向量的文本集：正本 + 日记（量小，语义检索覆盖这两处）。"""
        try:
            texts = self.store.texts()
        except Exception:  # noqa: BLE001
            texts = []
        try:
            texts += [i["text"] for i in _search_journal([], 0)]
        except Exception:
            pass
        return [t for t in texts if t]

    def _semantic_scores(self, query: str) -> dict[str, float]:
        """同步算一次相似度（调用方放线程里跑）。"""
        texts = self._semantic_texts()
        if not texts:
            return {}
        # 用整表比对而不是比条数 + 5 分钟定时重建：条数不变但内容换了
        # （remember 一条、导入一条）时旧判据看不出变化，会拿旧向量答新问题。
        stale = texts != self._semantic._texts or self._semantic._vecs is None  # noqa: SLF001
        if stale:
            self._semantic.build(texts)
        return dict(self._semantic.search(query, top=min(60, len(texts))))

    async def initialize(self) -> None:
        mode = "语义+词面" if self._semantic_on() else "仅词面"
        logger.info(
            f"[memory-bridge] 就绪｜正本 {self.store.count()} 条｜联邦源 4 路｜检索 {mode}"
        )
        if self._semantic_on():
            # 后台预热：模型加载 + 索引构建要几秒，别让第一次 recall 卡住
            try:
                self._warm = asyncio.create_task(asyncio.to_thread(self._warmup))
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"[memory-bridge] 预热失败（不影响词面检索）：{exc}")

    def _warmup(self) -> None:
        try:
            self._semantic_scores("预热")
            logger.info("[memory-bridge] 语义索引预热完成")
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"[memory-bridge] 语义预热失败：{exc}")

    async def terminate(self) -> None:
        task = getattr(self, "_warm", None)
        if task and not task.done():
            task.cancel()

    # ---------------------------------------------------------------- 工具

    @filter.llm_tool(name="recall")
    async def recall(self, event: AstrMessageEvent, query: str = "", limit: int = 5):
        """从你自己的记忆里找东西，不分场景。

        群聊、私聊、B站、日记里发生过的事都在这一份记忆里，可以一次查完。
        想回忆"我跟谁说过什么""我之前看过什么""这事我记过没有"，就用它。

        Args:
            query (string): 想找什么，几个关键词就行
            limit (number): 最多返回几条，默认 5，上限 10
        Returns:
            str: 记忆片段列表，每条前面标着来自哪里、多久之前
        """
        terms = _terms(query)
        if not terms:
            return "要给个关键词，我才知道找什么。"

        try:
            n = max(1, min(int(limit or DEFAULT_LIMIT), MAX_LIMIT))
        except Exception:
            n = DEFAULT_LIMIT

        pool: list[dict] = []
        for row in self.store.search(terms, n):
            pool.append({
                "text": str(row.get("text") or ""),
                "ts": float(row.get("ts") or 0),
                "scene": str(row.get("scene") or "bridge"),
                "origin": "canonical",
                "who": "",
            })
        for fn in (_search_iris, _search_journal, _search_ctx, _search_bilibili):
            try:
                pool.extend(fn(terms, n) or [])
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"[memory-bridge] {fn.__name__} 失败：{exc}")

        sim_map: dict[str, float] = {}
        if self._semantic_on():
            try:
                sim_map = await asyncio.to_thread(self._semantic_scores, query)
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"[memory-bridge] 语义检索失败，退回词面：{exc}")

        scored = []
        seen = set()
        for item in pool:
            text = re.sub(r"\s+", " ", item.get("text") or "").strip()
            if not text or text in seen:
                continue
            s = _score(text, terms, item.get("ts") or 0)
            sim = sim_map.get(text, 0.0)
            if sim >= 0.45:  # 语义近了也算命中，哪怕一个词都没对上
                s = max(s, sim * 3.0)
            if s <= 0:
                continue
            seen.add(text)
            scored.append((s, item, text))

        if not scored:
            return f"没找到跟「{query}」有关的记忆。"

        scored.sort(key=lambda x: (-x[0], -(x[1].get("ts") or 0)))
        lines = []
        for _s, item, text in scored[:n]:
            label = SCENE_LABEL.get(item.get("scene") or "", "记忆")
            who = item.get("who") or ""
            prefix = f"[{label}{('·' + who) if who else ''}·{_fmt_time(item.get('ts') or 0)}]"
            lines.append(f"{prefix} {text[:180]}")
        return "\n".join(lines)

    @filter.llm_tool(name="remember")
    async def remember(self, event: AstrMessageEvent, text: str = "", kind: str = "fact", tags: str = "", importance: float = 0.5):
        """把一件事记进你自己的记忆（唯一的一份，哪个场景都查得到）。

        值得记的是：答应过的事、别人的近况、你自己想留着的感觉、
        某天发生的一件具体小事。不用记流水账，也不用记工具调用。

        Args:
            text (string): 记什么，一句话，写清楚是谁/什么事
            kind (string): fact / moment / feeling / person / promise 之一，默认 fact
            tags (string): 几个关键词，逗号分隔，方便以后找
            importance (number): 0~1，越重要越接近 1
        Returns:
            str: 记下了 / 已经记过
        """
        body = re.sub(r"\s+", " ", (text or "")).strip()
        if not body:
            return "没给内容，没记。"
        if len(body) > 300:
            body = body[:300]
        if self.store.exists(body):
            return "这条已经记过了。"

        try:
            scene = "qq_private" if event.is_private_chat() else "qq_group"
        except Exception:
            scene = "bridge"
        try:
            imp = max(0.0, min(float(importance), 1.0))
        except Exception:
            imp = 0.5

        rowid = self.store.add(
            body,
            scene=scene,
            kind=str(kind or "fact").strip()[:20],
            tags=str(tags or "").strip()[:120],
            source="self",
            ref=str(getattr(event, "unified_msg_origin", "") or "")[:120],
            importance=imp,
        )
        logger.info(f"[memory-bridge] 记下 #{rowid}（{scene}）：{body[:40]}")
        return f"记下了（#{rowid}，正本共 {self.store.count()} 条）。"

    # ---------------------------------------------------------------- 导入

    @filter.command("mem_import")
    async def mem_import(self, event: AstrMessageEvent):
        """把已有日记导进唯一记忆（管理员，幂等）。"""
        try:
            allowed = bool(event.is_admin())
        except Exception:  # noqa: BLE001
            allowed = False   # 判定不了就当没权限，别 fail-open
        if not allowed:
            yield event.plain_result("仅管理员可用。")
            return

        added = 0
        for item in _search_journal([], 0):
            text = re.sub(r"\s+", " ", item["text"]).strip()
            if not text or self.store.exists(text):
                continue
            self.store.add(
                text[:300],
                scene="journal",
                kind="moment",
                tags="日记,导入",
                source="import:journal",
                ref="journal.jsonl",
                importance=0.6,
            )
            added += 1
        yield event.plain_result(f"导入完成：新增 {added} 条，正本共 {self.store.count()} 条。")
