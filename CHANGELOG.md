# Changelog

## 0.1.0 — 2026-11-11

首次公开发布 **Magpie for AgentRouter**。

- 接入 Magpie 原生账号登录、重试和多账号轮换。
- 默认启用分阶段超时与 Responses 请求 gzip 压缩。
- 最终 JSON 达到 64 KiB 且压缩后更小时才发送 gzip。
- 保留 Responses 历史项整理、工具定义去重及 Anthropic Messages 路径。
- 提供独立安装入口、固定依赖、发布测试和安装说明。
- 公开包不包含本机账号、配置、日志、模型目录或个人工具快照。

压缩减少上传字节，不减少上下文 token；尚未给出端到端性能提升倍数。
