# HANDOFF — Resin Dest-aware Soft Ban

> 给下一个 AI / 新会话用。从这里接着做，不必回翻旧聊天。

## 工作目录（必须）

```text
/home/ubuntu/resin-upstream
```

- 分支：`destban-feature`（跟踪 `origin/master`，当前约 `v1.2.0-5-g9b8ef8e`）
- **禁止**改：`/home/ubuntu/.openclaw/workspace` 里的生产 Resin / 现网二进制
- **禁止**占用生产端口 **2260**；本实例配置端口 **2261**

## 现网 vs 开发

| | 生产 | 本仓库开发实例 |
|--|------|----------------|
| 路径 | systemd + `/usr/local/bin/resin` | 本目录源码 |
| 端口 | **2260** | **2261**（见 `.env`） |
| 数据 | `~/.openclaw/workspace/resin-data` | `./data/{state,cache,log}` |
| 配置 | 已从生产 `state.db` 热备份到 `./data/state/state.db` | 首次启动会跑 schema 迁移 |

相关脚本：

- `scripts/run-dev.sh` — 跑本地二进制（需先 build）
- `scripts/sync-config-from-prod.sh` — 再从生产同步 state/seed
- `.env` — 已 gitignore；Admin/Proxy token 与生产相同，**PORT=2261**

## 已确认的需求

**目标**：节点主动探测仍健康，但业务访问**某目标域名**失败（如 `UPSTREAM_CONNECT_FAILED` / `connect_dial` / 503）时，**按（节点 × 域名）屏蔽**，路由不再选该节点；其它域名仍可用。

**证据（生产日志）**：

- 节点例：`全部/澳大利亚-001 |…|GPT⁺-AU`
- `upstream_stage=connect_dial`，`network_error`，`unexpected status: 503`
- 目标例：`auth.x.ai`、`cli-chat-proxy.grok.com`
- 权威域名列表**已包含**这些域名，但**不能**解决该问题（主动延迟只打 `latency_test_url`=gstatic）

## 已定设计（不要改方向，除非用户推翻）

完整说明：`doc/dest-ban-design.md`

**方案**：被动自动 Dest Ban 为主 + 手动/API 为辅；主动按域名全量探测不做主路径。

- 粒度：`(NodeHash, domain_key)`，默认 eTLD+1
- 阈值 + TTL 软屏蔽；路由 **硬排除**
- gstatic 探测成功 **不得** 清空全部 DestBan
- 与全局 `CircuitOpenSince` 并存：单域失败 → DestBan；整节点死 → 全局熔断

## 建议实现顺序（P0）

1. `internal/node`：DestBan 表结构 + `IsDestBanned` / 记录失败成功
2. `internal/topology`：`RecordDestResult`（或扩展被动反馈带 domain）
3. `internal/proxy`：失败/成功路径传入 target domain
4. `internal/routing`：`randomRoute` / sticky 选路过滤 ban
5. 配置项 + 单测（路由排除、TTL、成功解禁）
6. （P0.5）API/日志字段；先可不做 WebUI

**先设计已完成；下一步是在本仓库改代码 + 测试，不要动生产。**

## 给新会话的启动提示词（复制即用）

```text
工作目录：/home/ubuntu/resin-upstream
分支：destban-feature
请阅读 HANDOFF.md 与 doc/dest-ban-design.md，在本仓库实现 Dest-aware Soft Ban 的 P0（被动失败按节点×域名软屏蔽 + 路由硬排除）。
不要修改 /home/ubuntu/.openclaw/workspace 生产实例，不要占用 2260。
先读相关源码再改，改完跑相关单测。
```

## 环境注意

- 本机 Go / Node 若要 build：`go build -tags "with_quic with_wireguard with_grpc with_utls" -o resin ./cmd/resin`，WebUI 需时再 `webui` 里 npm build
- 生产进程仍在 2260，开发验证用 2261
- `data/` 已加入 `.gitignore`

## 状态

- [x] 独立 clone 最新源码
- [x] 同步生产配置到 `./data`，端口 2261
- [x] 设计文档 `doc/dest-ban-design.md`
- [ ] P0 代码实现
- [ ] 单测
- [ ] 本地 2261 冒烟（可选）
- [ ] 再考虑替换生产二进制（需用户明确同意）
