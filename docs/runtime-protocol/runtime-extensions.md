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

Provider工厂和Runtime对象在Connector Python进程内运行，所有内置和第三方Runtime共享该进程。entry point注册不是热重载协议，也不提供扩展进程隔离。升级扩展包或Connector代码后，需重启宿主Python进程；重新创建Runtime仍可能使用缓存的模块与Provider类。

公共服务部署使用 `systemctl --user restart agents-anywhere-connector.service`。这会影响同设备全部Runtime，正在进行的任务可能中断；原生会话文件保留。DSH面板的 `connector.restart` 在同一进程中重建后端任务，不是代码升级入口。Pi原生资源的 `/reload` 同样不重载Python适配层。

只更新DSH Node插件时，重启DSH即可重连Bridge，不需要重启共享Connector。生产安装来自wheel时，修改源码检出不会影响已安装环境。需要按扩展独立升级的部署，应另行设计worker隔离；当前接口不承诺该能力。
