# 更新日志

所有重要变更记录于此。格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)。

## [1.0.0] - 2026-10-20

### 新增

- 首个公开版本。
- `remember` — 写入唯一正本（`memory.db`），不管来自哪个场景
- `recall` — 先查正本，再**联邦查询**各路旧库，合并去重后按相关度排序
- 旧库只读、不回写：正本攒够后旧库自然退化成历史档案，不会出现两份互相打架
- 中文无分词器场景用 bigram + 关键词命中打分，再乘时间衰减
- 可选语义检索（`semantic_enable`），本地 bge-small 嵌入 + numpy 余弦
- `/mem_import` 指令，批量导入旧数据

