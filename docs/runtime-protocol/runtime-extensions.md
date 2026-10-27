# 已安装的 Runtime 扩展

官方 Connector 保留内置 Codex、Claude、DSH Provider，并从 Python entry point 组 `agents_anywhere.runtime_providers.v1` 加载同一环境中已安装的第三方 Provider。

entry point 必须指向一个无参数工厂（可以是类），返回 `connector.runtime_protocol.RuntimeProvider` 实例。Runtime type 必须非空且唯一，不得覆盖内置 Provider。损坏、重复或不兼容扩展会记录错误并跳过，不阻止其余 Provider 启动。只安装可信扩展：加载工厂会执行扩展代码。

```toml
[project.entry-points."agents_anywhere.runtime_providers.v1"]
pi = "pi_aa.provider:PiProvider"
```

CLI start、stdio RPC 与公共服务 serve 使用同一 Provider 注册机制。扩展需要在 Connector 的 Python 环境中安装；DSH Bridge 所在的 Node 环境无需 Python 扩展。

所有 Runtime 可覆盖默认无操作的 `on_backend_reconnect()` 生命周期回调，在服务端连接恢复时重新发布状态。RuntimeInstance 透明转发此回调；扩展无需修改 Connector 私有对象。


## 代码更新与进程边界

Provider 工厂和 Runtime 对象在 Connector Python 进程内运行，所有内置和第三方 Runtime 共享该进程。entry point 注册不是热重载协议，也不提供扩展进程隔离。升级扩展包或 Connector 代码后，需重启宿主 Python 进程；重新创建 Runtime 仍可能使用缓存的模块与 Provider 类。

公共服务部署使用 `systemctl --user restart agents-anywhere-connector.service`。这会影响同设备全部 Runtime，正在进行的任务可能中断；原生会话文件保留。DSH 面板的 `connector.restart` 在同一进程中重建后端任务，不是代码升级入口。Pi 原生资源的 `/reload` 同样不重载 Python 适配层。

只更新 DSH Node 插件时，重启 DSH 即可重连 Bridge，不需要重启共享 Connector。生产安装来自 wheel 时，修改源码检出不会影响已安装环境。需要按扩展独立升级的部署，应另行设计 worker 隔离；当前接口不承诺该能力。

## 2.1.0 的本机通道

服务层（`anywhere-cli serve`）建立在官方 MeshCall 之上：它持有设备身份、Connector 配置与 RuntimeLease，并把本机管理接口发布为 `~/.agents-anywhere/connector-service`（0600 会合文件：Router 端点 + `service` 角色密钥）。管理者只能调用 `aa.local.v2.Shared` 与 `aa.local.v2.Logs`。

`anywhere-cli rpc` 保持官方 stdio 启动模式：父进程从 stdout 读一行 ready 消息，用 `host` 角色密钥连接 `aa.local.v2.Control`。两种模式不能同时拥有同一台设备；先取得 RuntimeLease 的一方生效，后者得到 `connector_already_running`。

公共服务的 contract 见 [connector-service](/contracts/connector-service/v1/README.md)。
