# 共享 Connector 本机服务协议 v1

`anywhere-cli serve` 用 MeshCall 在 `<OS 用户 home>/.agents-anywhere/router/<实例>.sock` 上提供服务，
并把面向本机管理者的会合文件写到 `<OS 用户 home>/.agents-anywhere/connector-service`。目录权限 0700、
文件权限 0600；此版本只服务于支持 Unix socket（或 Windows 命名管道）的本机调用方，不监听 TCP。

服务是设备身份、Connector 凭据和 Runtime 租约的唯一管理者。管理者连接或断开不影响服务生命周期；
只有显式 `stop` / `restart` 会停止或重建所有 Runtime。服务退出时关闭 Runtime、释放租约并删除自己的会合文件。

## 会合文件

```json
{ "version": 1, "protocol": "meshcall", "instanceId": "<16 hex>", "pid": 1234,
  "endpoint": { "unixPath": "/home/<user>/.agents-anywhere/router/ab12.sock" },
  "key": "mk_service_<64 hex>" }
}
```

`key` 是 Router 的 `service` 角色密钥，只允许调用 `aa.local.v2.Shared/*`、`aa.local.v2.Logs/*` 和
`meshcall.router.v1.Registry/*`，不能注册服务，也不能访问 `aa.local.v2.Control` 或 `aa.dsh.v2.*`。
Router 每次启动都重新签发密钥，所以续接已有服务必须重新读取会合文件；文件被新实例替换时，旧实例不会删除它。

## 服务方法（`aa.local.v2.Shared`）

- `getState`：Connector 状态。`running` 表示 Runtime 任务存活，不代表服务端已上线；上线仍以服务端 presence 为准。
- `getIdentity`：`{serverUrl, connectorId, name?}`，不含 Connector token；尚未绑定时两个字段都是空字符串。
- `attachAccount(serverUrl, userId, accessToken, recovery?)`：服务验证 OAuth 账号与设备归属，复用现有身份和凭据；
  只有首次连接才注册新设备。`accessToken` 只在本次请求内使用，不落盘。`recovery` 只接受 `reconnect` 或 `recreate`，
  对应用户明确恢复凭据或重新创建设备；普通连接绝不撤销凭据。已配置其他服务器或账号时拒绝替换。
- `start` / `stop` / `restart`：只操作已经保存的共享配置，不接受配置参数。`start` 和 `restart` 会影响全部 Runtime。
- `events(since?)`：按 `seq` 递增的状态事件流；`getState` 提供当前值。

日志由 `aa.local.v2.Logs` 提供（`write` / `read` / `tail` / `clear`），内容按凭据形态脱敏；
长期日志交给 `~/.agents-anywhere/logs` 的分段文件与 systemd journal。

## 错误

`credential_expired` 与 `device_deleted` 带 `details.connectorId`，管理者应展示显式恢复操作；
`connector_error` 是服务端拒绝或网络失败的通用代码；未知方法返回 `method_not_found`。
所有响应和事件不包含 Connector token 或 OAuth token。

## 与管理者的边界

DSH 插件以管理者身份连接，保留自己的账号登录及 Bridge：它不写共享配置或私有 Connector 配置，
也不在插件退出、重新加载、退出账号或重置插件时停止公共服务。内置托管模式（`anywhere-cli rpc`）
仍然通过 stdio 会合行把 `host` 角色密钥交给启动它的进程；两种模式不能同时拥有同一台设备。
