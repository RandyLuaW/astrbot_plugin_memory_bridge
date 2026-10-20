# 唯一记忆桥：一份正本，跨场景共享记忆

> AstrBot 插件 · 写入正本，联邦召回，旧库保持只读

## 它解决什么问题

记忆通常分散在几处，各自为政：向量库、日记、群聊原文、平台侧观看记录。

每个场景只能看见自己那一份——在 A 平台发动态时想不起群里聊过什么，在群里聊天时想不起昨天刷到的内容。

## 功能

- `remember` — 写入唯一正本（`memory.db`），不管来自哪个场景
- `recall` — 先查正本，再**联邦查询**各路旧库，合并去重后按相关度排序
- 旧库只读、不回写：正本攒够后旧库自然退化成历史档案，不会出现两份互相打架
- 中文无分词器场景用 bigram + 关键词命中打分，再乘时间衰减
- 可选语义检索（`semantic_enable`），本地 bge-small 嵌入 + numpy 余弦
- `/mem_import` 指令，批量导入旧数据

## 工作机制

remember() → memory.db（唯一正本）
recall()   → 正本优先
           → 联邦查询：向量库 / 日记 / 群聊原文 / 平台侧记忆（只读）
           → 合并去重 → bigram + 关键词 + 时间衰减排序

语义检索可选：bge-small 嵌入 + numpy 暴力余弦，不依赖 faiss。

## 安装

### 方式一：插件市场（推荐）

WebUI 插件市场搜索「唯一记忆桥」。

### 方式二：Releases 下载

从 [Releases](../../releases) 下载 zip，WebUI → 插件 → 上传安装。

### 方式三：手动克隆

```bash
cd AstrBot/data/plugins
git clone https://github.com/RandyLuaW/astrbot_plugin_memory_bridge.git
```

无额外 Python 依赖。

## 配置项

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `semantic_enable` | bool | `true` | 启用语义检索。会在本插件内加载一份 bge-small 嵌入模型（约几百 MB 内存），与 iris_memory 各一份。内存充裕就开着。 |
| `embedding_model` | string | `` | 嵌入模型目录，留空则自动找 models/bge-small-zh-v1.5 |
| `note` | string | `` | 占位项，无需填写。正本存在 plugin_data/astrbot_plugin_memory_bridge/memory.db。 |

## 注意事项

- 开启 `semantic_enable` 会多占几百 MB 内存（嵌入模型），内存紧张就关掉
- 正本是 SQLite，位置在插件数据目录，可直接备份
- 旧库永远只读，不会污染已有数据

## License

MIT
