# 将开源即时通讯平台 Turms 迁移至 AWS DocumentDB：从适配到生产的完整实践

> **摘要：** 本文记录了将开源即时通讯框架 Turms 从原生 MongoDB 迁移至 AWS DocumentDB 的完整过程，包括 5 处核心代码适配、Change Streams 配置、索引执行计划分析、压测数据以及与自建 MongoDB 的成本对比。全部代码已开源，适配方案具有通用参考价值。

---

## 目录

- [一、Turms 简介](#一turms-简介)
- [二、为什么选择 AWS DocumentDB](#二为什么选择-aws-documentdb)
- [三、适配 DocumentDB 的技术挑战与解决方案](#三适配-documentdb-的技术挑战与解决方案)
- [四、索引结构与执行计划分析](#四索引结构与执行计划分析)
- [五、性能压测](#五性能压测)
- [六、成本对比](#六成本对比)
- [七、总结与下一步](#七总结与下一步)

---

## 一、Turms 简介

[Turms](https://github.com/turms-im/turms) 是一个开源的企业级即时通讯（IM）服务端框架，使用 Java 17+ 编写，基于 Reactor 响应式编程模型，支持私信、群聊、文件传输、消息漫游、实时推送等核心 IM 场景。其架构分为两个主要组件：

- **turms-service**：业务逻辑服务，处理消息路由、用户关系、群组管理等；
- **turms-gateway**：WebSocket 网关，负责终端连接管理与消息推送。

**Turms 原生使用 MongoDB 作为唯一持久化存储**，通过 MongoDB 响应式驱动（`mongodb-driver-reactivestreams`）进行所有读写操作，包括：

- 消息历史存储（带复合索引，支持时间范围和目标 ID 过滤）；
- 集群成员发现与 leader 选举（通过 `leader`、`member` 集合的 Change Streams）；
- 分布式配置热更新（`sharedClusterProperties` 集合 Change Streams）；
- Admin 权限数据（`admin`、`adminRole`、`userRole`、`groupType` 集合）。

正是由于 Turms 与 MongoDB 协议深度绑定，才使得将其迁移至兼容 MongoDB 协议的 AWS DocumentDB 成为可能——同时也带来了若干需要解决的兼容性挑战。

---

## 二、为什么选择 AWS DocumentDB

### DocumentDB 的核心优势

**AWS DocumentDB（与 MongoDB 兼容）** 是 AWS 提供的全托管文档数据库服务，兼容 MongoDB 4.0/5.0 协议，提供以下关键优势：

| 能力 | 说明 |
|---|---|
| **全托管运维** | 自动备份、补丁升级、监控告警，无需 DBA 人工介入 |
| **高可用架构** | 存储层 6 路跨 3 AZ 复制，RPO 接近 0；自动故障转移通常在 30 秒内完成 |
| **弹性读扩展** | 最多 15 个 Read Replica，读流量可水平扩展 |
| **安全合规** | 默认静态加密（AES-256），VPC 网络隔离，IAM 集成，满足 SOC、PCI DSS、HIPAA 等合规要求 |
| **Change Streams** | 支持集合级变更流，Turms 集群协调所依赖的核心特性 |
| **兼容 MongoDB 协议** | 无需修改应用层查询逻辑，只需少量初始化代码适配 |

### 适用场景

对于将 Turms 部署在 AWS 上的团队，DocumentDB 尤其适合以下场景：

- **中小规模 IM 部署**（日活 < 100 万）：不需要 MongoDB 分片集群，DocumentDB 的副本集模式完全满足需求；
- **对运维成本敏感的团队**：用托管服务替代自建 MongoDB 副本集，节省 DBA 和 SRE 人力；
- **强合规要求的行业**（金融、医疗）：DocumentDB 的安全合规认证体系开箱即用。

> ⚠️ **注意：** DocumentDB 并非 100% 兼容 MongoDB。不支持分片集群（Sharding）、hashed 索引、可重试写入等特性。本文详细记录了针对这些差异的适配方案。

---

## 三、适配 DocumentDB 的技术挑战与解决方案

Turms 在初始化阶段会调用若干 MongoDB 专属管理命令，这些命令在 DocumentDB 上均返回错误码 **303**（`Feature not supported`）或 **136**（Change Streams 未启用）。以下逐一说明每个问题及其修复方案。

### 3.1 集群类型兼容（MongoConfig.java）

**问题：** DocumentDB 向 MongoDB 驱动上报自身集群类型为 `REPLICA_SET`，而 Turms 原始代码只允许 `SHARDED` 和 `LOAD_BALANCED`，导致启动时抛出 `IncompatibleMongoException`：

```
IncompatibleMongoException: The cluster types for the mongo client "xxx"
must be one of the types: [SHARDED, LOAD_BALANCED]
```

**修复：** 在 `turms-service` 和 `turms-gateway` 两个模块的 `MongoConfig.java` 中，为 5 个 domain MongoClient bean 加入 `ClusterType.REPLICA_SET`：

```java
// 修改前
Set.of(ClusterType.SHARDED, ClusterType.LOAD_BALANCED)

// 修改后
Set.of(ClusterType.SHARDED, ClusterType.LOAD_BALANCED, ClusterType.REPLICA_SET)
```

---

### 3.2 Zone/Shard 操作跳过（最关键的架构层修复）

**问题：** Turms 的 tiered storage 功能（dev 配置默认开启）会在启动时调用 `addShardToZone`、`updateZoneKeyRange`、`balancerStatus` 等分片管理命令，DocumentDB 全部不支持（error 303）。

**根本原因分析：** 这些操作的语义前提是 MongoDB 分片集群，非分片部署根本不应执行它们。因此，正确的修复方式是在入口处做集群类型检测，而不是在每个方法上逐一添加错误捕获。

**修复方案：** 在 `TurmsMongoClient` 新增 `isShardedCluster()` 方法，利用驱动已有的 `ServerDescription` 信息：

```java
// TurmsMongoClient.java 新增
public boolean isShardedCluster() {
    List<ServerDescription> descs = descriptions;
    if (descs == null || descs.isEmpty()) {
        return false;
    }
    return descs.getFirst().getClusterType() == ClusterType.SHARDED;
}
```

在 `MongoCollectionInitializer.ensureZones()` 入口处检查：

```java
// 非分片集群（DocumentDB / standalone / replica set）直接跳过所有 zone 操作
if (!client.isShardedCluster()) {
    continue;
}
```

这种设计的优点是：语义清晰（zone 操作本就只对分片集群有意义），且无需修改底层方法，避免了错误信息被 `onErrorMap` 包装后难以识别的问题。

---

### 3.3 Hashed Index 自动降级（TurmsMongoOperations.java）

**问题：** Turms 的多个集合使用 **hashed index** 作为分片键辅助索引，DocumentDB 不支持（error 303：`Index type not supported: hashed`），导致索引创建失败。

**涉及的 9 个集合：**

| 集合 | 原 hashed key | 降级后 range key |
|---|---|---|
| `group` | `{oid: "hashed"}` | `{oid: 1}` |
| `groupJoinRequest` | `{rqid: "hashed"}` | `{rqid: 1}` |
| `groupJoinQuestion` | `{gid: "hashed"}` | `{gid: 1}` |
| `groupInvitation` | `{ieid: "hashed"}` | `{ieid: 1}` |
| `groupMember` | `{_id.gid: "hashed"}` | `{_id.gid: 1}` |
| `meeting` | `{cid: "hashed"}` | `{cid: 1}` |
| `userRelationshipGroup` | `{_id.oid: "hashed"}` | `{_id.oid: 1}` |
| `userRelationshipGroupMember` | `{_id.oid: "hashed"}` | `{_id.oid: 1}` |
| `conversationSettings` | `{_id.oid: "hashed"}` | `{_id.oid: 1}` |

**修复：** 在 `ensureIndexes()` 中捕获 error 303，自动将 hashed key 降级为 range(1) 后重试：

```java
.onErrorResume(t -> {
    if (!(t instanceof MongoCommandException e) || e.getErrorCode() != 303) {
        return Mono.error(new RuntimeException("Failed to index: " + collectionName, t));
    }
    List<IndexModel> downgraded = downgradeHashedIndexes(indexModels);
    if (downgraded == indexModels) {
        // 无 hashed key，303 来自其他原因，正常报错
        return Mono.error(new RuntimeException("Failed to index: " + collectionName, t));
    }
    LOGGER.warn("Hashed indexes not supported; downgrading to range for \"{}\"", collectionName);
    return Flux.from(collection.createIndexes(downgraded))
            .then()
            .onErrorMap(t2 -> new RuntimeException("Failed to index: " + collectionName, t2));
})
```

> **语义等价性说明：** hashed index 主要用于分片场景的数据均匀分布。在非分片部署中，对等值查询（`{field: value}`）而言，range(1) 和 hashed 的查询效率完全相同，均走 IXSCAN。

---

### 3.4 索引完整性修复（MongoCollectionInitializer.java）

**问题：** 旧逻辑在集合已存在（`exists=true`）时跳过整个 `ensureIndexesAndShards`，导致：
1. 首次启动失败（如遇 error 303）重启后索引永久缺失；
2. hashed index 降级逻辑无法在后续重启中触发。

**修复：** 将 `ensureIndexesAndShards` 改为无条件执行（`createIndex` 是幂等操作，重复调用安全），fake data 逻辑单独判断：

```java
boolean shouldPopulateFakeData = !context.isProduction()
        && fakeDataGenerator.isFakingEnabled()
        && (!exists || fakeDataGenerator.isFakeIfCollectionExists());
return ensureZones()
        .then(Mono.defer(this::ensureIndexesAndShards)...)
        .then(Mono.defer(() -> shouldPopulateFakeData
                ? fakeDataGenerator.populateCollectionsWithFakeData()
                : Mono.empty()));
```

---

### 3.5 Change Streams 启用

**问题：** DocumentDB **默认不开启** Change Streams，需通过管理命令手动启用。Turms 依赖 Change Streams 实现：
- 集群成员发现与 leader 选举
- Gateway↔Service RPC 路由
- 管理配置热更新（`adminRole`、`groupType`、`userRole` 等）

若未启用，返回错误码 **136**，服务状态 `hasJoinedCluster=false`，gateway 所有非登录请求返回 1201（`SERVER_UNAVAILABLE`）。

**需要启用的完整集合列表（7 个）：**

| 集合 | 用途 |
|---|---|
| `leader` | 集群 leader 选举 |
| `member` | 集群成员发现（gateway↔service RPC） |
| `sharedClusterProperties` | 集群配置热更新 |
| `admin` | 管理员账号变更同步 |
| `adminRole` | 管理员角色权限同步 |
| `groupType` | 群组类型配置同步 |
| `userRole` | 用户角色权限同步 |

**启用命令（一次性执行，重启后永久生效）：**

```python
import pymongo

client = pymongo.MongoClient(docdb_uri)
admin_db = client["admin"]
collections = [
    "leader", "member", "sharedClusterProperties",
    "admin", "adminRole", "groupType", "userRole"
]
for coll in collections:
    admin_db.command("modifyChangeStreams", 1,
                     database="turms-standalone",
                     collection=coll,
                     enable=True)
    print(f"✅ Change Streams enabled: {coll}")
```

> ⚠️ **注意：** 官方文档通常仅列出 3 个集合（`leader`、`member`、`sharedClusterProperties`）。遗漏 `admin`、`adminRole`、`groupType`、`userRole` 会导致服务持续出现 error 136 告警，且权限变更不会实时同步到内存缓存。请务必启用完整的 7 个集合。

---

### 3.6 连接字符串配置

```
mongodb://<user>:<pass>@<docdb-endpoint>:27017/turms-standalone?retryWrites=false&ssl=false&readPreference=secondaryPreferred
```

- `retryWrites=false`：DocumentDB 不支持可重试写入，必须关闭；
- `ssl=false`：VPC 内部署，无需 TLS（生产环境建议开启）；
- 高读集合（`user`、`conversation`、`message`）添加 `readPreference=secondaryPreferred`，读流量路由至 Read Replica。

---

## 四、索引结构与执行计划分析

迁移后，我们对核心业务查询进行了 `explain()` 分析，验证所有查询均命中索引。

**测试数据集：** `message` 集合 100,000 条文档。

| 查询场景 | Filter | 执行计划 | 命中索引 |
|---|---|---|---|
| 消息历史（主场景） | `{dyd: {$gt: T}, tid: N}` | IXSCAN | `dyd_1_tid_1` ✅ |
| 消息时间范围 | `{dyd: {$gt: T}}` | IXSCAN | `dyd_1_tid_1` ✅ |
| 过期消息清理 | `{dd: {$lt: T}}` | IXSCAN | `dd_1` ✅ |
| 用户关系列表 | `{_id.oid: N}` | IXSCAN | `_id.oid_1` ✅ |
| 群成员列表 | `{_id.gid: N}` | IXSCAN | `_id.gid_1` ✅ |
| 用户主键查询 | `{_id: N}` | IXSCAN | `_id` ✅ |
| 按发送者查消息 | `{sid: N}` | COLLSCAN | 无（预期，无此业务 API） |

**结论：所有业务查询均命中索引，无意外全表扫描。** `sid`（发送者 ID）无索引是预期行为——Turms 不提供"按发送者查消息历史"的管理 API，因此不存在对应的查询路径。

hashed index 降级为 range(1) 后，查询效率未受任何影响，索引使用率与原 MongoDB 部署完全一致。

---

## 五、性能压测

**环境：** EC2 上 pymongo 直连 DocumentDB writer（VPC 内，消除公网 RTT 和应用层开销）。每场景 500 次请求（10 次 warmup），数据集 100,051 条 message 文档，DocumentDB `db.t3.medium` × 2（1 writer + 1 reader）。

| 查询 | 执行计划 | p50 | p95 | p99 | avg |
|---|---|---|---|---|---|
| 用户主键 PK lookup | IXSCAN | 1.4 ms | 2.6 ms | 6.3 ms | 1.6 ms |
| 过期消息清理 `dd` | IXSCAN | 1.2 ms | 2.8 ms | 14.6 ms | 1.6 ms |
| 用户关系 `oid` 索引 | IXSCAN | 1.5 ms | 3.2 ms | 7.2 ms | 1.7 ms |
| 群成员 `gid` 索引 | IXSCAN | 1.4 ms | 2.4 ms | 4.6 ms | 1.5 ms |
| 消息时间范围 `dyd` 单键 | IXSCAN | 1.9 ms | 4.9 ms | 11.5 ms | 2.3 ms |
| estimated_document_count | — | 1.5 ms | 4.6 ms | 9.5 ms | 1.9 ms |
| 消息历史 `dyd+tid` 复合索引（范围扫描 limit 50） | IXSCAN | 21.4 ms | 47.9 ms | 88.0 ms | 26.5 ms |
| **发送者 `sid`（无索引，COLLSCAN 对照）** | **COLLSCAN** | **364 ms** | **688 ms** | **1,448 ms** | **413 ms** |

### 结果解读

1. **所有 IXSCAN 查询 p50 均在 1–21 ms**，`db.t3.medium` 对当前数据规模（10 万条）完全够用。

2. **消息历史 p50=21 ms 偏高**：该查询为跨时间范围的范围扫描并返回 50 条，比点查多了顺序 IO，属预期。

3. **COLLSCAN vs IXSCAN 差距 17–250 倍**：`sid` 无索引时 p50=364 ms，IXSCAN 点查 p50=1.4 ms，直观验证索引必要性。`sid` 字段无对应 Admin API 查询路径，COLLSCAN 为预期行为。

4. **hashed → range 降级无性能损失**：涉及降级索引的集合（群组、群成员等）p50 均在 1.4–1.5 ms，与主键查询相当，证明 range(1) 索引对等值查询同样高效。

5. **功能验证全部通过**：Admin API 测试 20/20，WebSocket 端到端测试 33/33，覆盖消息收发、群组管理、好友关系、实时推送等完整业务链路。

---

## 六、成本对比

以下对比**同等可用性**下，DocumentDB 托管方案与 EC2 自建 MongoDB 副本集（3 节点）的月度成本。Region：`ap-northeast-1`（东京）。

### 场景一：开发/测试环境（小规模）

| 项目 | DocumentDB | 自建 MongoDB on EC2 |
|---|---|---|
| 计算节点 | `db.t3.medium` × 2 = **$118/月** | `t3.medium` × 3 = **$65/月** |
| 存储（20 GB） | $2/月 | EBS gp3 × 3 = $5/月 |
| 备份存储 | 7 天自动备份包含在内 | 需自建备份脚本 + S3 |
| 监控 | CloudWatch 集成，开箱即用 | 需自行部署 MongoDB Exporter |
| 故障转移 | **自动**（约 30 秒） | 手动或需 Pacemaker |
| **合计（纯费用）** | **≈ $120/月** | **≈ $70/月** |
| **运维人力成本** | 极低（全托管） | 中等（需定期维护） |

### 场景二：生产环境（中等规模）

| 项目 | DocumentDB | 自建 MongoDB on EC2 |
|---|---|---|
| 计算节点 | `db.r5.large` × 3 = **$626/月** | `m5.large` × 3 = **$207/月** |
| 存储（100 GB） | $10/月 | EBS gp3 × 3 = $48/月 |
| 备份 / PITR | 自动 PITR，含 35 天 | S3 + 自建脚本 ≈ $20/月 |
| 高可用保障 | **99.99% SLA** | 无官方 SLA |
| **合计（纯费用）** | **≈ $636/月** | **≈ $275/月** |
| **运维人力成本** | 极低 | 高（专职 DBA 或 SRE 兼管） |

### 总结

- DocumentDB 计算费用约为自建方案的 **2–2.3 倍**；
- 托管服务减少的运维成本（备份、监控、补丁、故障处理）在人力成本较高的团队中可轻松弥补差价；
- 对于合规要求严格的场景（金融、医疗），DocumentDB 的 SOC2/PCI DSS 认证可节省大量审计和认证费用；
- 建议：**开发/测试环境**使用 `db.t3.medium`，**生产环境**根据读写比例选择 `db.r5.large`/`db.r5.xlarge`，并开启 Read Replica 以水平扩展读吞吐。

---

## 七、总结与下一步

### 本次适配总结

通过 **5 处核心代码修改**（共改动 5 个文件）和 **1 次 Change Streams 初始化脚本**，Turms 完整运行在 AWS DocumentDB 上，功能与性能均达到预期：

| 修改点 | 改动文件 | 解决的问题 |
|---|---|---|
| 集群类型兼容 | `MongoConfig.java` × 2 | DocumentDB 报告为 REPLICA_SET |
| Zone/Shard 跳过 | `TurmsMongoClient.java` + `MongoCollectionInitializer.java` | error 303：zone 管理命令不支持 |
| Hashed Index 降级 | `TurmsMongoOperations.java` | error 303：hashed index 不支持 |
| 索引完整性修复 | `MongoCollectionInitializer.java` | 重启后索引缺失 |
| Change Streams 启用 | 初始化脚本（一次性） | error 136：集群协调失效 |

适配工作量较小，且所有修改均向下兼容标准 MongoDB 部署——`isShardedCluster()` 在 MongoDB 分片集群上返回 `true`，zone 操作照常执行；`createIndexes` 在支持 hashed index 的 MongoDB 上正常创建，不触发降级逻辑。

### 适配方案的通用价值

本文的适配模式对其他使用 MongoDB 的 Java/Kotlin 应用也有参考价值：

1. **集群类型检测优于逐方法错误捕获**：在操作入口判断部署类型，语义清晰，避免错误被中间层包装后难以识别；
2. **索引创建应无条件执行**：`createIndex` 是幂等的，在应用每次启动时执行可确保索引始终与代码保持一致；
3. **Change Streams 集合列表要完整**：通过 grep 代码库中所有 `.watch()` 和 Change Streams 订阅点，而不依赖文档，才能找到完整列表。

### 下一步行动

- [ ] **生产化配置**：开启 DocumentDB TLS 加密传输（`ssl=true`），配置 AWS Secrets Manager 管理数据库凭证；
- [ ] **读写分离优化**：对 `message`、`user`、`conversation` 集合配置 `readPreference=secondaryPreferred`，充分利用 Read Replica；
- [ ] **监控接入**：通过 CloudWatch Logs Insights 监控慢查询（`profiler` 输出），并配置 p99 延迟告警；
- [ ] **压力测试扩展**：使用 `turms-benchmark` 工具通过 WebSocket 协议压测，测量端到端吞吐（当前压测仅覆盖 Admin HTTP API）；
- [ ] **贡献回上游**：本次兼容性适配已提交 PR，计划合入 Turms 主干，使社区用户可直接使用 DocumentDB 而无需自行适配。

---

### 相关资源

- [Turms GitHub 仓库](https://github.com/turms-im/turms)
- [AWS DocumentDB 开发者指南](https://docs.aws.amazon.com/documentdb/latest/developerguide/)
- [DocumentDB 与 MongoDB 兼容性对比](https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html)
- [DocumentDB Change Streams 文档](https://docs.aws.amazon.com/documentdb/latest/developerguide/change_streams.html)
- [Amazon DocumentDB 定价（东京 Region）](https://aws.amazon.com/documentdb/pricing/)

---

*作者：Jianlin Luo*  
*发布日期：2026-05-11*
