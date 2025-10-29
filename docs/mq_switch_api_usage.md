# MQ Switch API 使用文档

## 概述

MQ Switch API 是一个 RESTful 管理接口，用于向所有 StreamingNode 广播 MQ 切换控制消息。该接口通过 StreamingCoord 的 Broadcaster 机制，将 MQ 切换指令安全地分发到集群中的所有活跃 vchannel。

## API 端点

**路径**: `/api/v1/mq/switch`

**方法**: `POST`

**Content-Type**: `application/json`

## 请求参数

| 参数名 | 类型 | 必填 | 描述 |
|--------|------|------|------|
| `target_mq_type` | string | 是 | 目标 MQ 类型，例如: "kafka", "pulsar", "rocksmq" |
| `config` | object | 否 | 额外的配置参数，key-value 格式 |

## 请求示例

### 基本请求

```bash
curl -X POST http://<milvus-host>:9091/api/v1/mq/switch \
  -H "Content-Type: application/json" \
  -d '{
    "target_mq_type": "kafka"
  }'
```

### 带配置参数的请求

```bash
curl -X POST http://<milvus-host>:9091/api/v1/mq/switch \
  -H "Content-Type: application/json" \
  -d '{
    "target_mq_type": "kafka",
    "config": {
      "broker_address": "localhost:9092",
      "topic_prefix": "milvus-"
    }
  }'
```

## 响应格式

### 成功响应

**HTTP 状态码**: `200 OK`

```json
{
  "msg": "OK"
}
```

### 错误响应

#### 缺少必填参数

**HTTP 状态码**: `400 Bad Request`

```json
{
  "msg": "target_mq_type is required"
}
```

#### 请求体格式错误

**HTTP 状态码**: `400 Bad Request`

```json
{
  "msg": "Invalid request body"
}
```

#### 非主集群

**HTTP 状态码**: `500 Internal Server Error`

```json
{
  "msg": "failed to broadcast MQ switch message, current cluster is not primary, cannot perform MQ switch"
}
```

#### 广播失败

**HTTP 状态码**: `500 Internal Server Error`

```json
{
  "msg": "failed to broadcast MQ switch message, <error details>"
}
```

## 实现原理

### 1. 资源锁定

- 使用 `ExclusiveClusterResourceKey` 确保同一时刻只有一个 MQ Switch 操作执行
- 保证操作的原子性和一致性

### 2. 消息广播

- 通过 `broadcast.StartBroadcastWithResourceKeys` 启动广播
- 获取所有 StreamingNode 的 vchannel 列表
- 创建 ManualFlush 类型的广播消息作为控制消息载体
- 在消息的 properties 中携带 MQ Switch 信息：
  - `_mq_switch_operation`: "true"
  - `_mq_switch_target_type`: 目标 MQ 类型
  - `_mq_switch_config_<key>`: 自定义配置参数

### 3. 消息结构

```go
ManualFlushMessage {
    Header: {
        CollectionId: 0,  // 特殊标记，表示这是控制消息
        FlushTs: 0,       // 不是真正的 flush 操作
    },
    Properties: {
        "_mq_switch_operation": "true",
        "_mq_switch_target_type": "<target_mq_type>",
        "_mq_switch_config_<key>": "<value>",
        ...
    },
    BroadcastHeader: {
        VChannels: [vchannel1, vchannel2, ...],
    }
}
```

### 4. 执行流程

```
1. 接收 HTTP POST 请求
   ↓
2. 验证参数 (target_mq_type 必填)
   ↓
3. 启动 Broadcast (获取 ExclusiveClusterResourceKey)
   ↓
4. 检查当前集群是否为 Primary
   ↓
5. 获取所有 StreamingNode 和 vchannel 列表
   ↓
6. 构建广播消息 (ManualFlush + Properties)
   ↓
7. 调用 broadcaster.Broadcast() 发送消息
   ↓
8. 等待所有 vchannel 确认接收
   ↓
9. 返回成功响应
```

## 注意事项

1. **Primary 集群限制**: 只有 Primary 集群才能执行 MQ Switch 操作
2. **资源独占**: 使用集群级别的排他锁，同一时刻只能有一个 MQ Switch 操作
3. **消息确认**: Broadcast 操作会等待所有 vchannel 确认接收消息
4. **错误处理**: 如果某些 node 的 vchannel 获取失败，会记录警告但继续处理其他 node

## StreamingNode 端处理

StreamingNode 接收到 MQ Switch 消息后，应：

1. 在 Flusher 或 WAL Interceptor 中识别这是一个 MQ Switch 控制消息
2. 检查消息的 properties：
   ```go
   if msg.Properties().Get("_mq_switch_operation") == "true" {
       targetMQType := msg.Properties().Get("_mq_switch_target_type")
       // 执行 MQ 切换逻辑
   }
   ```
3. 执行相应的 MQ 切换操作
4. 调用 `streaming.WAL().Broadcast().Ack(ctx, msg)` 确认消息处理完成

## 相关代码文件

- **API Handler**: `internal/coordinator/restful_mgr_routes.go`
  - `HandleMQSwitch()`: HTTP 请求处理
  - `broadcastMQSwitchMessage()`: 广播消息逻辑

- **Broadcaster**: `internal/streamingcoord/server/broadcaster/broadcast/singleton.go`
  - `StartBroadcastWithResourceKeys()`: 启动广播

- **Message Builder**: `pkg/streaming/util/message/`
  - `NewManualFlushMessageBuilderV2()`: 构建消息

## 示例：完整的 MQ 切换流程

```bash
# 1. 向 Coordinator 发送 MQ Switch 请求
curl -X POST http://localhost:9091/api/v1/mq/switch \
  -H "Content-Type: application/json" \
  -d '{
    "target_mq_type": "kafka",
    "config": {
      "broker_address": "kafka-cluster:9092",
      "security_protocol": "SASL_SSL"
    }
  }'

# 2. Coordinator 日志示例
# INFO: broadcastMQSwitchMessage preparing to broadcast
#       targetMQType=kafka vchannelCount=10 vchannels=[by-dev-rootcoord-dml_0_v0, ...]
# INFO: broadcastMQSwitchMessage broadcast successful
#       targetMQType=kafka vchannelCount=10 broadcastID=12345

# 3. 成功响应
{
  "msg": "OK"
}
```

## 测试建议

1. 单节点测试
2. 多节点集群测试
3. Primary/Replica 集群测试
4. 并发请求测试（验证资源锁）
5. 异常情况测试（部分 node 失败）

