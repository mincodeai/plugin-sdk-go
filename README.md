# plugin-sdk-go

MinCode 官方进程插件（`official-plugins/*/backend`）共用的 Go SDK。模块路径 `github.com/mincodeai/plugin-sdk-go`，`go 1.24.0`，**只依赖标准库**。

它把各插件里重复实现的 stdio NDJSON JSON-RPC 2.0 循环（运行时协议 v1：`plugin.initialize`、`ping`、`shutdown`、`invoke`、`plugin.task.*`，以及反向 `host.storage.*` / `host.secrets.*` 请求）、严格 JSON 校验、连接配置档（profiles）存储与凭据记忆收拢到一处。设计目标是**迁移不改变一个协议字节**：各插件之间所有可观察到的线上差异都是 `rpc.Options` 的开关，并由 `testdata/golden` 里从原插件二进制录制的对话逐字节回放验证。

## 在插件里引用

仓库外的插件直接 `go get github.com/mincodeai/plugin-sdk-go@latest`。

仓库内的插件 backend 仍然是独立模块，用 `GOWORK=off` 构建，通过仓库内相对路径替换引用 SDK：

```
require github.com/mincodeai/plugin-sdk-go v0.0.0-00010101000000-000000000000

replace github.com/mincodeai/plugin-sdk-go => ../../../packages/plugin-sdk-go
```

`vue-app/scripts/test-go-plugins.mjs` 会检查：凡是 require 了 SDK 的 `go.mod`，必须恰好有一条指向 `../../../packages/plugin-sdk-go` 的 replace（下述 vendored 使用方除外）。

### 仓库外构建：vendored 副本

脱离 monorepo 目录结构构建的 backend 无法解析上面的相对路径，改为携带一份 SDK 副本：

```
replace github.com/mincodeai/plugin-sdk-go => ./third_party/plugin-sdk-go
```

目前的使用方是 Vue + Go 插件模板（`tools/plugin-templates/visual-process-vue-go/backend`，会经 Node devkit 与 `mincode create` 复制到开发者机器）和 draw（gitlink 仓库，由自己的 CI 构建；协议配置与 excalidraw 相同）。副本由 `tools/plugin-sdk/sync-go-sdk.mjs` 生成：只含 `go.mod`、`README.md`、`LICENSE` 与非测试 `.go` 源码（不含 `*_test.go`），另写一个记录校验和的 `VERSION`。**不要手改副本**，改 SDK 后运行：

- `pnpm sync:go-sdk`：刷新全部副本（draw 未检出时跳过）；
- `pnpm verify:go-sdk-vendor`（CI 执行）：副本与本目录不一致即失败；`pnpm verify:plugin-template` 也会先检查模板副本；
- `node scripts/test-go-plugins.mjs draw`（在 `vue-app` 下）：要求 draw 的 replace 指向 `./third_party/plugin-sdk-go` 且副本无漂移。

devkit 导出时另附 `sdk/go`（同样的副本）和 `tools/sync-go-sdk.mjs`，仓库外的插件可用 `--source <kit>/sdk/go` 更新自己的副本。

不要提交 `go.work` / `go.work.sum`（已在根 `.gitignore`）；本地想联调可以自建 go.work，但 CI 与发布构建都是 `GOWORK=off`。

## 包

| 包 | 作用 |
| --- | --- |
| `rpc` | stdio JSON-RPC 服务循环 `Serve` / `Main`、所有线上行为开关 `Options`、两个家族预设 `StrictDefaults` / `NodeDefaults`、任务表 `TaskOptions`、`ReportProgress` |
| `host` | 反向请求客户端 `Client`（请求 id 关联、超时、关闭语义）、`Caller` 接口、`WithCaller` / `FromContext`、`Error` 与 `IsCode` |
| `jsonx` | 严格 JSON 判定 `Strict`（重复键、孤立代理项、非法 UTF-8）、带分类错误的 `DecodeObject`、不转义 HTML 的 `Marshal` |
| `jsvalue` | 从 Node 运行时移植来的插件用的 JS 语义值：保序对象 `Obj`、`Parse`（JSON.parse）、`Stringify`（JSON.stringify，数字格式与 U+2028 保持 JS 行为）、UTF-16 长度/切片、JS 空白与真值规则 |
| `profiles` | 版本化配置档文档 `Store`（`{"version":1,"profiles":[...]}`）、按字段记忆的凭据 `Secrets`（`profile.<id>.<field>`）、宿主错误分类 `Classify` |
| `rpctest` | golden 对话格式 `Parse` / `Format`、逐字节回放 `Replay`、内存宿主 `FakeHost`（插件单元测试用） |

录制工具与 golden 回放测试不在本模块内：它们放在仓库的 `packages/plugin-sdk-go-conformance/`（独立模块，不对外发布），以免把约 750 KB 的测试数据带进 `go get` 下载的包。

### rpc

```go
func main() {
	o := rpc.StrictDefaults("nats-client")
	o.FatalMessage = "NATS plugin transport failed"
	o.MaxInflight = 16
	o.Busy = rpc.NewError(rpc.CodeBusy, "请求过多，请稍后重试 / Too many concurrent requests")
	o.InvalidParams = rpc.NewError(rpc.CodeInvalidParams, "[NATS_INPUT] Invalid input or unsupported option.")
	o.KnownAction = known
	o.UnknownAction = func(a string) *rpc.Error { return rpc.NewError(rpc.CodeInvalidParams, "[NATS_INPUT] unknown action "+strconv.Quote(a)) }
	o.MapError = mapError // 插件自己的错误措辞
	o.DrainReplies = true
	o.OnClose = closeSessions
	o.Handler = handle
	rpc.Main(o)
}

func handle(ctx context.Context, action string, payload json.RawMessage) (any, error) {
	h := host.FromContext(ctx) // 反向请求
	...
}
```

- `Handler(ctx, action, payload)`：返回 `json.RawMessage` / `[]byte` 原样写出，其它值按回复风格序列化；返回 `*rpc.Error` 精确控制 code/message，其它错误经 `MapError`（默认 `-32000 err.Error()`）。panic 回复 `PanicError`。
- `Serve(ctx, stdin, stdout, stderr, o)` 可直接在测试里驱动；`Main(o)` 处理 SIGINT/SIGTERM，传输失败时写 `FatalMessage` 并以 1 退出。
- 任务：`o.Tasks = &rpc.TaskOptions{...}` 且宿主在 `plugin.initialize` 协商 `features.tasks=1` 后启用 `plugin.task.start/status/cancel`；handler 内用 `rpc.ReportProgress(ctx, v)` 报告进度。
- 主要开关（完整说明见 `rpc/options.go` 注释）：
  - 分帧：`MaxLineBytes`、`OverlongFatal`、`StrictJSON`、`RequireVersion`、`DecodeEnvelope`（自定义请求信封解码，如旧的 DisallowUnknownFields 结构体）、`Invalid` / `InvalidMessage` / `InvalidOmitID`（`-32700` 回复不带 id 成员）、`SkipEmptyLines` / `SkipBlankLines`、`NonStringMethod`、`IDs` / `MaxIDBytes`
  - 回复：`Style`（`StyleGo` / `StyleSorted` / `StyleJS`）、`EscapeHTML`
  - 方法：`MethodNotFound`、`RequireInitialize` / `NotInitialized`、`TasksNotNegotiated`、`RejectTaskMethods`
  - invoke：`MaxInflight` / `Busy`、`ParamsBeforeBusy`、`Payload`（`PayloadString` / `PayloadObject`）、`KeepEmptyPayload`（`PayloadString` 下空 payload 原样交给 handler，不补成 `{}`）、`InvalidParams`、`KnownAction` / `UnknownAction`、`PayloadNotObject` / `PayloadInvalidJSON`、`MaxResultBytes` / `ResultTooLarge`、`MapError`、`PanicError`
  - 生命周期：`DrainReplies` / `DrainTimeout`、`OnClose`
  - 反向请求：`Host`（`host.Options`）
- `PassHostErrors`：把 `*host.Error` 原样（宿主 code + message）作为回复，Node 家族默认使用。

两个预设：

| | `StrictDefaults(id)`（Go 原生家族） | `NodeDefaults()`（从 Node 运行时移植的家族） |
| --- | --- | --- |
| JSON | 严格（`jsonx.Strict`），必须 `"jsonrpc":"2.0"` | 宽松，不要求 jsonrpc |
| 非法行 | 回复 `-32700`（id null） | stderr 写 `Invalid plugin RPC request` |
| 超长行（1 MiB） | 致命，写 FatalMessage 退出 | 视为非法行 |
| id | 仅字符串/数字且 ≤256 字节 | 任何存在的 id（含 null） |
| 未知方法 | `-32601 Method not found` | `-32601 不支持的方法` |
| invoke | 需先 initialize；params `{action, payload string}` | 不需要；payload 规范化为对象；`MaxInflight 1`，忙时 `-32029 已有任务运行，请稍后重试`；未知 action `-32601 不支持的操作` |
| 宿主错误 | 由插件 `MapError` 决定 | `PassHostErrors` |
| 反向请求 id | `<id>-host-N` | `host-N` |

### host

`Client` 由 `rpc.Serve` 创建并通过 ctx 注入，插件只需 `host.FromContext(ctx).Call(ctx, method, params)`，得到原始 JSON 结果（无结果时为 `null`）或 `*host.Error`（`-32001` 未授权、`-32002` 存储/凭据失败、`-32003` 宿主不支持反向请求）。超时默认 10 s（`context.DeadlineExceeded`，可用 `TimeoutError` 替换）；传输关闭后返回 `Unavailable`（默认 `host.ErrUnavailable`）。`Options.SortKeys` / `EscapeHTML` / `MaxLineBytes` 复现不同插件的请求字节。

### jsonx

- `Strict(raw)`：整行恰好一个合法 JSON 值、合法 UTF-8、无孤立 `\u` 代理项、任意深度无重复键。
- `DecodeObject(raw, &v, DecodeOptions{Strict, DisallowUnknown, RejectNull, MaxBytes, UseNumber, EmptyAsObject})`：失败返回 `*DecodeError{Kind, Field}`，`Kind` 为 `TooLarge` / `NotStrict` / `NotObject` / `NullField` / `UnknownField` / `WrongType` / `TrailingData` / `Malformed`，插件据此给出自己的措辞。
- `Marshal(v)` 不转义 `<>&`；`MarshalStyle(v, escapeHTML)`。

### jsvalue

给从 Node 移植、需要逐字节复刻 `JSON.parse` / `JSON.stringify` 的插件（todo、knowledge-base、excalidraw 等）：`Obj` 保持 JS 自有属性顺序（数组下标键升序在前），`Stringify` 输出 `1e3 → 1000`、`1e21 → 1e+21`，U+2028/2029 不转义；另有 `Truthy`、`ToNumber`、`Trim`、`Len16` / `Slice16` / `Compare16` 等。

### profiles

```go
var store = profiles.Store{}                   // key "profiles", version 1
var creds = profiles.Secrets{IDPattern: idRe, Fields: []string{"password", "token"}}

store.Lock(); defer store.Unlock()
raw, err := store.Load(ctx)                      // []json.RawMessage；缺失/损坏/其它版本 → 空
switch profiles.Classify(err) {                  // Denied / Failed / TimedOut / Unavailable
case profiles.Denied: return nil, rpc.NewError(-32000, "[NATS_STORAGE] Host storage is not granted to this plugin.")
}
err = store.Save(ctx, list)                      // 宿主回 false → ErrNotConfirmed
index, err := creds.Index(ctx)                   // id → 已记忆字段（按 Fields 顺序）
v, ok, err := creds.Get(ctx, id, "password")
err = creds.Set(ctx, id, "password", "")         // 空值 = 删除
n, err := creds.Forget(ctx, liveIDs, true)       // 清理不在 liveIDs 中的凭据
```

配置档的结构、校验和面向用户的错误文案仍然留在插件里；本包只搬运字节并分类宿主错误。

为了让已有插件改用本包后存储字节和结果字节完全不变，`Store` 与 `Secrets` 提供了一组可选项，默认值即上面的行为：

- `Store`：`Field`（文档内列表字段名，如 openapi 的 `environments`）、`NoUnwrap`（不解开字符串包装的文档）、`EmptyDamaged`、`Strict`、`ProfilesFirst`、`EscapeHTML`、`MaxBytes` + `ErrTooLarge`（超限时不发请求）、`Confirm`（`ConfirmNotFalseOrNull` 默认 / `ConfirmNotFalse` / `ConfirmTrueOrObject`）。`Read` / `Decode` 返回 `Loaded{Entries, Raw, Missing, Damaged}`，按 encoding/json 规则解码；`Marshal` / `SaveRaw` 用于插件自己组装文档。
- `Secrets`：`Prefix`（如 `env.`）、`IDValid`（函数校验，替代 `IDPattern`）、`Dynamic []Pattern`（如 `header.*`、`meta.*`）、`Limits`、`Sorted`、`StrictList`、`KeepDuplicates`、`ConfirmSet` / `ConfirmDelete`；方法 `ValidID`、`Allowed`、`Limit`、`Order`、`List`、`IndexKeys`、`GetRaw`、`DeleteKey`、`Sweep(drop, bestEffort)`（`Forget` 是它的特例）。
- 错误分类：`ClassifyCodes(err, denied...)` 自定义哪些错误码算拒绝，`Unreadable(err)` 判断 -32002。

每个使用本包的官方插件都在 `backend/storage_compat_test.go` + `testdata/storage_compat.golden` 里固定了迁移前的存储字节，改动本包后跑一遍插件测试即可发现格式漂移。

### rpctest

对话文件（`.jsonl`）每行一个指令：

```
> {json}      写入插件 stdin（请求或宿主回复）
< {json}      下一行 stdout，逐字节比较（{{*}} 匹配任意字符串内容）
<~ {json}     相邻的 <~ 行作为多重集比较（并发回复顺序不定）
! text        一行 stderr（全部 stderr 作为多重集比较）
# eof         关闭 stdin
# wait 100ms  暂停（让异步任务状态稳定）
```

输入中的 `@@PAD<n>@@` 展开为 n 个 `x`，超长帧无需原样存储。`rpctest.Replay(t, transcript, serveFunc)` 回放；`rpctest.NewFakeHost()` 是实现了 `host.storage.*` / `host.secrets.*` 的内存宿主，`fh.Context(ctx)` 后可直接单测 handler，`fh.Fail` 注入错误，`fh.Calls` 记录调用。

## 官方插件预设表

每个插件迁移时应使用的选项值（由 `packages/plugin-sdk-go-conformance/golden/golden_test.go` 的 `preset` 逐字节验证；未列出的项取预设默认值）。

Go 原生家族（`StrictDefaults(id)`；`MapError`：`-32001` → 未授权文案，其它 `*host.Error` → 失败文案，均为 `-32000`）：

| 插件 | FatalMessage | MaxInflight | Busy（-32029） | DrainReplies | Tasks | RejectTaskMethods | 未知 action（-32602） |
| --- | --- | --- | --- | --- | --- | --- | --- |
| nats-client | `NATS plugin transport failed` | 16 | `请求过多，请稍后重试 / Too many concurrent requests` | 是 | 无 | 否 | `[NATS_INPUT] unknown action "x"` |
| kafka-inspector | `Kafka plugin transport failed` | 16 | `[KAFKA_BUSY] 请求过多，请稍后重试 / Too many concurrent requests` | 是 | 无 | 否 | `[KAFKA_INPUT] unknown action "x"` |
| zookeeper-manager | `ZooKeeper plugin transport failed` | 8 | `Too many concurrent requests; retry shortly` | 否 | 无 | 否 | `[ZK_INPUT] Unknown action.` |
| mock-server | `mock-server plugin transport failed` | 16 | `too many concurrent requests, retry later` | 否 | 无 | 否 | `[MOCK_INPUT] unknown action` |
| consul-manager | `Consul plugin transport failed` | 1 | `已有任务运行，请稍后重试` | 否 | 无 | 否 | `[CONSUL_INPUT] unknown action` |
| pprof-viewer | `pprof plugin transport failed` | 4 | `插件繁忙，请稍后重试 / Plugin busy, try again later` | 否 | `Cancelled: [PPROF_CANCELLED] Task cancelled.` | 是 | `[PPROF_INPUT] unsupported action` |
| grpc-debug | `grpc-debug plugin transport failed` | 8 | `插件繁忙，请稍后重试 / Plugin busy, try again later` | 否 | `Cancelled: [GRPC_CANCELLED] Task cancelled.` | 是 | `[GRPC_INPUT] unknown action` |
| docker-manager | `docker-manager: transport failed` | 6 | `Too many Docker requests in flight; retry shortly` | 否 | `InvalidID/LookupInvalidID: Invalid task ID`，`DuplicateID: Duplicate task ID`，`Prepare` 拒绝非任务 action | 是 | `[DOCKER_INPUT] unknown action` |
| http-load-tester | `http-load-tester plugin transport failed` | 8 | `too many concurrent requests` | 否 | `Prepare`：只允许 `run` | 是 | `[HTTP_LOAD_INPUT] unsupported action` |

InvalidParams（-32602，params 不是 `{action, payload string}`）：

| 插件 | InvalidParams | 存储文案前缀（未授权 / 失败） |
| --- | --- | --- |
| nats-client | `[NATS_INPUT] Invalid input or unsupported option.` | `[NATS_STORAGE] Host storage is not granted to this plugin.` / `... request failed.` |
| kafka-inspector | `[KAFKA_INPUT] Invalid input or unsupported option.` | `[KAFKA_STORAGE] Host storage ...`（同上，大写句式） |
| zookeeper-manager | `[ZK_INPUT] Invalid input or unsupported option.` | `[ZK_STORAGE] Plugin storage is not granted to this plugin.` / `[ZK_STORAGE] Plugin storage is unavailable; profiles need the MinCode desktop app.` |
| mock-server | `[MOCK_INPUT] invalid invoke params` | `[MOCK_STORAGE] host storage is not granted to this plugin` / `... request failed` |
| consul-manager | `[CONSUL_INPUT] invalid invoke envelope` | `[CONSUL_STORAGE] host storage ...`（小写句式） |
| pprof-viewer | `[PPROF_INPUT] Invalid invoke parameters.` | `[PPROF_STORAGE] host storage ...`（小写句式） |
| grpc-debug | `[GRPC_INPUT] Invalid invoke parameters.` | `[GRPC_STORAGE] Host storage ...`（大写句式） |
| docker-manager | `[DOCKER_INPUT] invalid invoke parameters` | `[DOCKER_STORAGE] host storage ...`（小写句式） |
| http-load-tester | `[HTTP_LOAD_INPUT] invoke params must be {action, payload}` | `[HTTP_LOAD_STORAGE] host storage ...`（小写句式） |

payload 的严格校验（`jsonx.DecodeObject(payload, &p, {Strict: true, DisallowUnknown: true})`）及其文案留在插件 handler 内。

Node 家族（`NodeDefaults()`）：

| 插件 | Style | Host.SortKeys | 空行 | NonStringMethod | 其它 | Tasks | RejectTaskMethods |
| --- | --- | --- | --- | --- | --- | --- | --- |
| devtools | `StyleGo` | 否 | 非法行 | `MethodTypeInvalid` | `RequireVersion`、`IDs: IDNonNull`、`EscapeHTML` | 无 | 否（`-32601 不支持的方法`） |
| certificate-manager | `StyleSorted` | 是 | `SkipEmptyLines` | `MethodTypeIgnore` | | `{SharedGate: true}` | — |
| hbuilder-simulator | `StyleSorted` | 是 | `SkipBlankLines` | 默认 | | 无 | 是 |
| todo | `StyleGo` | 否 | 非法行 | 默认 | | 无 | 是 |
| knowledge-base | `StyleJS` | 否 | 非法行 | 默认 | handler 忽略取消 | `{SharedGate: true}` | — |
| excalidraw | `StyleJS` | 否 | 非法行 | 默认 | | 无 | 是 |
| draw | `StyleJS` | 否 | 非法行 | 默认 | 与 excalidraw 相同，经 vendored 副本引用 | 无 | 是 |

“任务未协商”时 `plugin.task.*` 的回复：cert、docker、excalidraw、grpc、hbuilder、http、kb、pprof、todo 为 `-32601 Tasks were not negotiated`；nats、kafka、zk、mock、consul 为 `-32601 Method not found`；devtools 为 `-32601 不支持的方法`。

leetcode-cn（`StrictDefaults("leetcode-cn")`，复刻旧的最小同步循环）：`StrictJSON`、`RequireVersion`、`RequireInitialize` 均为 false；`DecodeEnvelope` 用 DisallowUnknownFields 结构体解码（要求 `"jsonrpc":"2.0"`、非空 id、非空 method，只读首个 JSON 值）；`InvalidMessage: invalid JSON-RPC request` + `InvalidOmitID`；`IDs: IDAny`；`MethodNotFound` 与 `TasksNotNegotiated` 均为 `method not found`；`FatalMessage: read JSON-RPC request: bufio.Scanner: token too long`；`MaxInflight 32`；`InvalidParams: invalid invoke parameters`；`KeepEmptyPayload`；`MapError` 把所有错误映射为 `-32602 err.Error()`（未知 action：`未知动作：x`）；`Tasks: {MaxRunning: 2, Deadline: 150s, Start}`，`Start` 只接受 `leetcode.run` / `leetcode.submit`，判题进度经 `ReportProgress` 上报。

全部 30 个官方 Go 插件都已使用本 SDK。etcd-manager、network-diagnostics、leetcode-cn、file-viewer、kubernetes-manager 以及 rabbitmq/pulsar/nacos/prometheus/object-storage/mqtt/openapi/web-navigation/prd-studio 的预设见 `packages/plugin-sdk-go-conformance/golden/golden_*_test.go` 中通过 `extraPresets` 注册的配置。

## golden 对话

golden 对话与回放测试位于 `packages/plugin-sdk-go-conformance/`（模块 `mincode/plugin-sdk-go-conformance`，`replace` 指向本目录，只在仓库内运行，由 `pnpm --dir vue-app test:plugin-go`（目标 `sdk-conformance`）覆盖）。`testdata/golden/<plugin>/*.jsonl` 由 `cmd/recorder` 驱动插件二进制录制（`basic`、`framing`、`host-errors`，以及适用时的 `busy`、`eof-drain`、`tasks`）。`golden/golden_test.go` 用每个插件的预设加一个做相同反向请求的桩 handler 回放全部对话，要求输出逐字节一致（`<~` 块按多重集合比较）。

格式：`> ` 写入 stdin 的行（请求或宿主回复），`< ` 下一行 stdout（逐字节），相邻的 `<~ ` 行按多重集合比较，`! ` stderr 行（整体按多重集合比较），`# eof` 关闭 stdin，`# wait 100ms` 暂停（tasks 场景让异步任务落定），`@@PAD<n>@@` 展开为 n 个 `x`。

录制是确定性的，不依赖静默窗口或固定的启动等待：

- 每一步写入一行后等待明确的完成条件：带普通 id（字符串或整数）的请求等到同 id 的回复（按解码后的 id 匹配，HTML 转义的 id 也能对上）；可能没有回复或回复 id 为 null 的行（坏帧、奇怪的 id、通知、空行，`framing` 全部步骤）之后发送一个录制器私有的 `plugin.ping`（id `rec-sync-N`）作为屏障，读循环按顺序处理，收到它的回复即说明该行已处理完；探针及其回复不写入 golden。启动也是如此：等首个 ping / initialize 的回复，而不是固定睡眠。
- 挂起的 invoke 等到预期数量的挂起宿主请求；`task.start` 返回 `running` 时用隐藏的 `plugin.task.status` 探针等任务结束（宿主调用被挂起时等到那条宿主请求）；`task.cancel` 有界等待任务离开 running。
- `plugin.shutdown` 等回复后再有界等待进程退出，之后的输入不会与退出赛跑；`# eof` 等 stdout 关闭。
- 规范顺序：每条输入后紧跟它引起的输出，宿主回复紧跟对应的宿主请求。`busy` 场景把占满并发闸门的 invoke 和被拒绝的那个作为一批连续写入，输出（被拒绝的回复 + `busyCalls` 条挂起宿主请求，默认 1）作为一个排序后的 `<~` 块；shutdown 排空的回复是排序后的 `<~` 块，shutdown 回复在最后；stderr 排序。
- 期望不满足（回复超时、挂起宿主请求数不对、批次后多出输出）时 recorder 报错并以非零退出。

重新录制（`-j` 并行录制多个插件）：

```
(cd official-plugins/<plugin>/backend && GOWORK=off go build -o /tmp/bins/<plugin> .)
(cd packages/plugin-sdk-go-conformance && GOWORK=off go run ./cmd/recorder -j 4 -bin /tmp/bins -out testdata/golden <plugin>)
```

同一组二进制重复录制（包括并发负载下）应得到逐字节相同的文件（`git diff --exit-code packages/plugin-sdk-go-conformance/testdata/golden`）。改动录制器或排序规则后，用 `cmd/goldencmp` 证明新旧录制语义等价——每个文件的输入序列（`>`、`# eof`、`# wait`）相同、stdout 行与 stderr 行作为多重集合相同：

```
(cd packages/plugin-sdk-go-conformance && GOWORK=off go run ./cmd/goldencmp [-missing-ok] OLD_DIR testdata/golden)
```

## 迁移步骤（每个插件）

1. `backend/go.mod` 加上文的 require + replace，`GOWORK=off go mod tidy`。
2. 用 `rpc.StrictDefaults(id)` / `rpc.NodeDefaults()` 加上表中的覆盖项构造 `Options`；把原来的读循环、分帧、id 过滤、回复编码、并发闸门、任务表、反向请求客户端删掉，只保留 action 分发与业务逻辑作为 `Handler`。
3. 错误文案：输入错误直接返回 `rpc.NewError(...)`；宿主错误在 `MapError` 或 handler 内用 `profiles.Classify` / `host.IsCode` 映射。
4. 配置档与凭据改用 `profiles.Store` / `profiles.Secrets`；handler 单测改用 `rpctest.FakeHost`。
5. 运行插件原有测试、`pnpm --dir vue-app test:plugin-go <plugin>`，并用 recorder 重新录制该插件 golden 确认零差异。

## 许可证

本 SDK 以 [Apache License 2.0](LICENSE) 发布。
