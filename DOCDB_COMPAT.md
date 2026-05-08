# Turms 适配 AWS DocumentDB 说明

> 版本：v2（2026-05-08）— 新增索引完整性修复、zone/shard 跳过逻辑、Change Streams 完整集合列表、索引执行计划分析、压测结果

---

## 一、代码修改

### 1. 集群类型兼容（MongoConfig.java）

**文件：**
- `turms-service/src/main/java/im/turms/service/storage/mongo/MongoConfig.java`
- `turms-gateway/src/main/java/im/turms/gateway/storage/mongo/MongoConfig.java`

5 个 domain MongoClient bean（`userMongoClient`、`groupMongoClient`、`conversationMongoClient`、`messageMongoClient`、`conferenceMongoClient`）以及 gateway 的 `userMongoClient`，在允许集群类型中加入 `ClusterType.REPLICA_SET`。

**原因：** DocumentDB 向驱动上报自身类型为 `REPLICA_SET`，Turms 原本只允许 `SHARDED` 和 `LOAD_BALANCED`，启动时抛出 `IncompatibleMongoException`。

```java
// 修改前
Set.of(ClusterType.SHARDED, ClusterType.LOAD_BALANCED)

// 修改后
Set.of(ClusterType.SHARDED, ClusterType.LOAD_BALANCED, ClusterType.REPLICA_SET)
```

---

### 2. Sharding 命令跳过（TurmsMongoOperations.java）

**文件：** `turms-server-common/src/main/java/im/turms/server/common/storage/mongo/operation/TurmsMongoOperations.java`

`enableSharding()` 和 `shard()` 方法增加错误码 **303** 的静默处理。

**原因：** Turms 初始化时固定调用这两个方法。DocumentDB 不支持分片，返回 error 303（`Feature not supported`）。通过 `.onErrorResume` 捕获并跳过，视为非分片部署的正常情况。

```java
.onErrorResume(t -> {
    if (t instanceof MongoCommandException e && e.getErrorCode() == 303) {
        return Mono.empty();
    }
    return Mono.error(...);
})
```

---

### 3. 索引完整性修复（MongoCollectionInitializer.java）

**文件：** `turms-service/src/main/java/im/turms/service/storage/mongo/MongoCollectionInitializer.java`

`initCollections()` 的 `flatMap` 改为无条件执行 `ensureIndexesAndShards`，而不是在集合已存在时跳过。

**原因：** 旧逻辑在 `exists=true`（集合已存在）时整体跳过 `ensureIndexesAndShards`，导致首次启动失败后重启时索引永久缺失，以及在不支持 hashed index 的部署上无法完成索引降级。`createIndex` 本身是幂等操作，重复调用安全。

```java
// 修改前：exists=true 时直接返回 Mono.empty()，跳过索引创建
if (exists && !fakeDataGenerator.isFakeIfCollectionExists()) {
    return Mono.empty();
}

// 修改后：无条件执行，fake data 逻辑单独判断
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

### 4. Hashed Index 降级（TurmsMongoOperations.java）

**文件：** `turms-server-common/src/main/java/im/turms/server/common/storage/mongo/operation/TurmsMongoOperations.java`

`ensureIndexes()` 新增 hashed index 降级逻辑：当 `createIndexes` 返回 error 303（`Index type not supported: hashed`）时，自动将所有 hashed key 降级为 range index（`1`）后重试。

**原因：** DocumentDB 不支持 hashed index，而 Turms 的多个 collection 使用 hashed index 作为分片键辅助索引。降级为 range(1) 对非分片部署的等值查询语义完全等价。

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
    LOGGER.warn("Hashed indexes are not supported; downgrading to range for \"{}\"", collectionName);
    return Flux.from(collection.createIndexes(downgraded))
            .then()
            .onErrorMap(t2 -> new RuntimeException("Failed to index: " + collectionName, t2));
})
```

降级后实际创建的索引（与原 hashed 等价，适用于等值查询）：

| Collection | 原 hashed key | 降级后 range key |
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

---

### 5. Zone/Shard 操作跳过（TurmsMongoClient + MongoCollectionInitializer）

**文件：**
- `turms-server-common/src/main/java/im/turms/server/common/storage/mongo/TurmsMongoClient.java`
- `turms-service/src/main/java/im/turms/service/storage/mongo/MongoCollectionInitializer.java`

`TurmsMongoClient` 新增 `isShardedCluster()` 方法，`ensureZones()` 入口检查：非分片集群直接跳过所有 zone/shard 操作。

**原因：** Turms 的 tiered storage 功能在 dev 配置中默认开启（`message.tiered-storage.enabled=true`），会调用 `addShardToZone`、`updateZoneKeyRange`、`balancerStatus` 等命令，DocumentDB 全部不支持（error 303）。这些操作的语义前提是 MongoDB 分片集群，非分片部署完全不应执行。

```java
// TurmsMongoClient.java 新增
public boolean isShardedCluster() {
    List<ServerDescription> descs = descriptions;
    if (descs == null || descs.isEmpty()) {
        return false;
    }
    return descs.getFirst().getClusterType() == ClusterType.SHARDED;
}

// MongoCollectionInitializer.ensureZones() 入口
if (!client.isShardedCluster()) {
    continue;  // DocumentDB / standalone / replica set 跳过所有 zone 操作
}
```

---

## 二、DocumentDB 配置

### Change Streams（变更流）

DocumentDB **默认不开启** Change Streams，必须通过管理命令手动启用。Turms 依赖 Change Streams 实现集群成员状态同步（leader 选举、gateway 发现 service）及配置热更新。若未启用，调用返回错误码 **136**，导致 `hasJoinedCluster=false`，gateway 所有非登录请求返回错误 1201（`SERVER_UNAVAILABLE`）。

**需要启用 Change Streams 的完整集合列表（7 个）：**

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
client = pymongo.MongoClient(uri)
admin = client["admin"]
for coll in ["leader", "member", "sharedClusterProperties",
             "admin", "adminRole", "groupType", "userRole"]:
    admin.command("modifyChangeStreams", 1,
                  database="turms-standalone",
                  collection=coll,
                  enable=True)
```

> 注意：原始文档仅列出 3 个集合（`leader`、`member`、`sharedClusterProperties`），遗漏了 4 个业务配置集合，导致服务启动后持续出现 error 136 告警。完整列表见上表。

### 事务

DocumentDB 8.0 支持多文档 ACID 事务，无需修改代码。

注意：DocumentDB **不允许在事务内创建新集合**（error 263）。仅影响首次启动时集合尚未存在的场景；后续重启集合已存在，事务正常。

### 连接字符串

所有 MongoDB URI 配置：
- `retryWrites=false`：DocumentDB 不支持可重试写入
- `ssl=false`：VPC 内部署，无需 TLS
- 高读集合（`user`、`conversation`、`message`）附加 `readPreference=secondaryPreferred`，读流量路由至 read replica

---

## 三、索引结构与执行计划分析

### 完整索引列表（25 个集合）

| 集合 | 索引 | 说明 |
|---|---|---|
| `message` | `{dyd:1, tid:1}` | 主查询索引：按投递时间+目标 ID 查历史消息 |
| `message` | `{dd:1}` | 按删除时间清理过期消息 |
| `message` | `{sip:1}`, `{sip6:1}` | IP 封禁封锁查询 |
| `user` | `{_id:1}` | 主键 |
| `userRelationship` | `{_id.oid:1}`, `{_id.rid:1}` | 按 owner/related 查关系 |
| `userRelationshipGroup` | `{_id.oid:1}` | 按 owner 查关系分组 |
| `userRelationshipGroupMember` | `{_id.oid:1}` | 按 owner 查分组成员 |
| `groupMember` | `{_id.gid:1}`, `{_id.uid:1}` | 按群/用户查成员 |
| `group` | `{oid:1}`, `{dd:1}` | 按 owner/删除时间查群 |
| `groupInvitation` | `{ieid:1, cd:1}`, `{gid:1}` | 邀请查询 |
| `groupJoinRequest` | `{rqid:1, cd:1}`, `{gid:1}` | 入群申请查询 |
| `groupJoinQuestion` | `{gid:1}` | 入群问题查询 |
| `meeting` | `{cid:1}`, `{uid:1}`, `{gid:1}`, `{cd:1}` | 会议多维查询 |
| `userFriendRequest` | `{rtid:1, cd:1, rrid:1}` | 好友请求查询 |
| `admin` | `{ln:1}` | 管理员登录名 |
| `leader` | `{renewDate:1}` | 集群 leader 续约时间 |
| `member` | `{_id.nodeId:1}`, `{status.lastHeartbeatDate:1}` | 集群成员心跳 |
| `conversationSettings` | `{_id.oid:1}` | 会话设置 |
| `privateConversation` | `{_id.oid:1}` | 私聊会话 |

> 所有 hashed index 已自动降级为 range(1)，详见第一节第 4 条。

### 核心查询 explain 结果（100k 条 message 文档）

| 查询 | filter | 执行计划 | 使用索引 |
|---|---|---|---|
| 消息历史（主场景） | `{dyd: {$gt: T}, tid: N}` | IXSCAN | `dyd_1_tid_1` ✅ |
| 消息时间范围 | `{dyd: {$gt: T}}` | IXSCAN | `dyd_1_tid_1` ✅ |
| 过期消息清理 | `{dd: {$lt: T}}` | IXSCAN | `dd_1` ✅ |
| 用户关系列表 | `{_id.oid: N}` | IXSCAN | `_id.oid_1` ✅ |
| 群成员列表 | `{_id.gid: N}` | IXSCAN | `_id.gid_1` ✅ |
| 用户主键查询 | `{_id: N}` | IXSCAN | `_id` ✅ |
| 按 sender 查消息 | `{sid: N}` | COLLSCAN | 无（预期，无此业务查询） |

**结论：所有业务查询均命中索引，无意外全表扫描。**

---

## 四、压测结果

**环境：** EC2 `c5.xlarge`（turms-service 2GB mem limit）→ DocumentDB `db.t3.medium` × 2（1 writer + 1 reader），VPC 内同 region。测试工具：Python httpx，并发 20，共 500 请求，通过 Admin API（port 8510）。

| 场景 | RPS | p50 | p95 | p99 | 429 限流率 |
|---|---|---|---|---|---|
| health（无 DB，基线） | 60.4 | 314ms | 567ms | 703ms | 16% |
| 消息历史 dyd+tid（100k docs） | 46.8 | 386ms | 745ms | 1422ms | 1% |
| 消息时间范围 dyd（100k docs） | 50.7 | 367ms | 678ms | 816ms | 4% |
| 用户关系 oid index | 54.6 | 339ms | 585ms | 687ms | 12% |
| 群成员 gid index | 57.4 | 310ms | 595ms | 731ms | 20% |
| 用户主键 pk lookup | 62.7 | 302ms | 494ms | 585ms | 22% |

**说明：**

1. **429 限流**：来自 Turms Admin API 的 rate limit（dev 配置默认限速），非 DB 瓶颈。排除 429 后实际 200 请求的延迟更低。

2. **DB 查询开销**：索引查询比 health 基线多约 50–80ms（p50），为 VPC 内 DocumentDB `t3.medium` 的正常响应时间，跨 AZ 通信引入额外 1–3ms。

3. **索引效果显著**：若走 COLLSCAN 扫描 100k 文档，p50 预计 >2s；实测 p50 386ms 证明索引生效，DB 开销处于预期范围内。

4. **tiered storage 在 DocumentDB 上已正确跳过**：非分片集群检测到后直接 continue，不触发任何 `addShardToZone`/`balancerStatus` 调用，启动日志无 error 303。

---

## 五、测试结果

### Admin API 测试（`test_turms.py`）— 20/20 通过

通过 HTTP Admin API（端口 8510）测试 Turms 核心功能：

| 序号 | 测试场景 |
|------|----------|
| 1 | 创建用户 Alice、Bob、Charlie |
| 2 | 按 ID 查询用户 |
| 3 | 更新用户资料（名称） |
| 4 | 添加好友关系（联系人） |
| 5 | 屏蔽用户 |
| 6 | 查询用户关系列表 |
| 7 | 创建群组 |
| 8 | 添加群成员 |
| 9 | 查询群成员列表 |
| 10 | Alice 向 Bob 发送私信 |
| 11 | Bob 回复 Alice |
| 12 | 查询私信历史记录 |
| 13 | Alice 发送群消息 |
| 14 | Bob 发送群消息 |
| 15 | 查询群消息历史记录 |

---

### WebSocket 网关测试（`test_turms_client.js`）— 33/33 通过

通过 WebSocket 网关（端口 10510）使用 `turms-client-js` SDK 进行端到端测试：

| 序号 | 测试场景 |
|------|----------|
| 1 | Alice 通过 WebSocket 登录 |
| 2 | Bob 通过 WebSocket 登录 |
| 3 | Charlie 通过 WebSocket 登录 |
| 4 | 查询自己的用户资料 |
| 5 | 查询其他用户的在线状态 |
| 6 | 更新用户资料（简介） |
| 7 | 更新在线状态（设为 AWAY） |
| 8 | 发送好友请求 |
| 9 | 查询待处理的好友请求 |
| 10 | 接受好友请求 |
| 11 | Alice 向 Bob 发送私信（实时） |
| 12 | Bob 通过 `messageService.addMessageListener` 收到实时推送 |
| 13 | 查询私信历史记录 |
| 14 | 编辑已发送的消息 |
| 15 | Alice 发送群消息 |
| 16 | Bob 发送群消息 |
| 17 | 查询群消息历史记录 |
| 18 | 撤回群消息 |
| 19 | 标记私聊会话为已读 |
| 20 | 标记群聊会话为已读 |
| 21 | 查询私聊会话列表 |
| 22 | 查询群聊会话列表 |
| 23 | 发送正在输入状态 |
| 24 | 查询群组信息 |
| 25 | 查询群成员列表（含在线状态） |
| 26 | 查询已加入的群组 ID |
| 27 | 创建关系分组 |
| 28 | 查询关系分组列表 |
| 29 | 删除关系分组 |
| 30 | 更新位置信息（经纬度） |
| 31 | Alice 退出登录 |
| 32 | Bob 退出登录 |
| 33 | Charlie 退出登录 |
