# Connector 本机公共服务协议 v1

`anywhere-cli serve` 在当前用户的私有 Unix socket 上提供 UTF-8 JSON-RPC 2.0，每行一帧，最大 1 MiB。默认路径为 `~/.agents-anywhere/connector.sock`，可用 `--socket` 指定绝对路径。目录权限 0700，socket 权限 0600；此版本服务面向支持 Unix socket 的系统，不监听 TCP。

服务是设备身份、Connector 凭据和 RuntimeLease 的唯一管理者。客户端连接或断开不会影响 Connector 生命周期；只有显式 `connector.stop` / `connector.restart` 会停止或重启所有 Runtime。管理操作串行执行。服务退出时关闭 Runtime、释放租约并删除自己的 socket。

## 请求

- `connector.getState`：官方 Controller 状态，加 `protocolVersion: 1`。running 表示 Connector 任务存活，不代表服务端已上线；上线仍以服务端 presence 为准。
- `connector.getIdentity`：`{serverUrl, connectorId}` 或 null，不包含 Connector token。
- `connector.attachAccount`：参数 `{serverUrl, userId, accessToken, recovery?}`。服务验证 OAuth 账号及设备归属，复用现有身份和凭据；首次连接才注册新设备。返回 `{connectorId, name}`。accessToken 只在本次请求内使用，不落盘。`recovery` 仅接受 `reconnect` 或 `recreate`，对应用户明确恢复凭据或重新创建设备的操作；普通连接绝不撤销凭据。已配置其他服务器或账号时拒绝替换。
- `connector.readLogs`：接受 `{before?, after?}`，返回 `{entries, hasMore, oldestId, newestId}`，每页最多 200 条。服务内存保留最近 10,000 条，重启后清空；长期日志交给 systemd journal。日志展示不回退到旧插件的私有 Connector 日志。
- `connector.start` / `connector.stop` / `connector.restart`：只操作已经保存的共享配置，不接受配置参数。

## 通知与错误

`connector/state` 广播公开状态，断开的客户端不阻塞其他客户端或服务。所有响应和通知不包含 Connector token 或 OAuth token。未知方法和无效参数使用标准 JSON-RPC 错误；凭据失效使用 -32011，客户端应展示显式恢复操作。socket 不可达时报告服务不可用，不启动内置 Connector，不注册备用设备。

DSH 外部模式通过此协议连接，保留自己的账号登录及 Bridge。它不写共享配置或私有 Connector 配置，不在插件退出、重新加载、退出账号或重置插件时停止公共服务。显式停止和重启按钮会影响共享服务中的所有 Agent。

内置托管模式在绑定账号、恢复或撤销设备凭据之前，必须通过原官方 stdio RPC 的 `connector.acquireOwnership` 取得全局租约，并在同一控制进程中完成配置和启动。公共服务已持有租约时，内置模式必须在任何设备凭据操作之前失败。
