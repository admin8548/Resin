# Resin 目的地感知软熔断（Dest-aware Soft Ban）设计

## 需求确认（原始问题）

**场景**：节点探测健康（gstatic / Cloudflare 成功），但业务访问特定地址（如 auth.x.ai、cli-chat-proxy.grok.com）出现 503 / connect_dial 失败时，**不再选该节点**。

**核心冲突**：现有机制是**节点级全局熔断** + **延迟表**，未实现“**该节点对这个域名失败**”的隔离。

## 方案概述

**主方案**：**被动自动屏蔽**（Dest-aware Soft Ban）

- **粒度**：`(NodeHash, domain_key)` → ban TTL
- **domain_key**：`eTLD+1`（推荐）或 `full host`（可配）
- **触发**：业务请求的被动失败（`connect_dial`、`timeout`、`network_error` 等）
- **行为**：路由层**硬排除**，粘性路由支持同出口轮换

## 详细设计

### 1. 数据结构

```go
// NodeEntry 扩展
DestBanTable map[uint64]DestBanEntry   // key = xxh3(domain_key)
type DestBanEntry struct {
    FailCount    uint32
    BannedUntil  int64  // unix nano
    LastError    string // 可选
    LastFailAt   int64
}
```

- 采用 LRU 限制（类似 LatencyTable），避免内存爆炸。
- 支持并发（xsync.Map 或 fine-grained）。

### 2. 触发机制

**被动路径**（推荐）：
- `RecordPassiveResult` → 扩展为 `RecordDestResult(hash, domain, success)`
- 失败时：FailCount++ → >= threshold → BannedUntil = now + TTL
- 成功时：清 ban / 清计数

**主动路径**（可选 P2）：
- 对已 ban 的 `(node, domain)` 抽样重探
- 成功则提前解禁

### 3. 配置项（推荐默认）

```yaml
dest_ban_enabled: true
dest_ban_threshold: 2
dest_ban_ttl: 168h  # 7 days
dest_ban_scope: etld1          # 或 host
dest_ban_max_entries: 500      # per node
dest_ban_error_stages: ["connect_dial", "timeout", "network_error"]
```

### 4. 路由行为（核心）

**P2C / Random**：
- 候选列表中剔除 `IsDestBanned(node, targetDomain)`
- 候选全 ban → 错误 `NO_NODE_FOR_DEST`

**Sticky**：
- 租约节点 ban → 同出口 IP 轮换
- 无同 IP 节点 → 重新随机分配

### 5. 持久化与观测

- **持久化**：可先内存，后 P1 弱一致进 `nodes_dynamic`
- **日志**：新增字段 `dest_ban_hit` / `dest_ban_reason`
- **Metrics**：`dest_ban_count`、`dest_ban_skips`
- **WebUI**：节点详情页显示「当前屏蔽域名」列表
- **API**：新增 `POST /api/v1/nodes/{id}/dest-bans`（手动 ban/unban）

### 6. 与现有特性交互

| 特性 | 影响 |
|------|------|
| 全局熔断 | 单域名失败 → DestBan（默认）<br>多域名或主动探测失败 → 仍走全局 |
| LatencyTable | 失败时仍 `RecordLatency(nil)`（不写入高延迟）<br>权威列表继续保留作用 |
| 粘性 IP | 支持同 IP 轮换排除 ban 节点 |
| 平台隔离 | DestBan 全局共享，不受 Platform 过滤 |
| 跨订阅去重 | 共享节点 → 共享 DestBan |

## 实现阶段

| 阶段 | 工作内容 | 完成度 |
|------|----------|--------|
| **P0** | 被动失败 → DestBan + 路由硬排除 + 日志字段 | 核心需求 |
| **P0.5** | WebUI 查看与手动 ban/unban | 运维可用 |
| **P1** | 粘性同 IP 轮换感知；指标；可选持久化 | 完整粘性场景 |
| **P2** | 对 ban 项主动重探；多域失败升级全局熔断 | 高级自愈 |

## 明确不推荐

- 仅把业务域加进 `latency_authorities`（已验证不够）
- 把 `latency_test_url` 改成 x.ai（单 URL，失败变全局熔断）
- 只手动屏蔽（规模下不可持续）

---

## 参考

- 现有 `RecordPassiveResult` 接口
- `randomRoute` / P2C 选择逻辑
- `NodeEntry` 中的 `LatencyTable` 模式

（可进一步补充接口签名、SQL schema 变更、测试用例。）

