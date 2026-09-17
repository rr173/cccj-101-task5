# eventarch — 多来源设备遥测事件归档服务

面向多来源设备遥测的持久化事件归档服务。来源可能离线数小时后补传；接收端区分
**迟到（late）**、**重复（duplicate）**、**时钟回拨（clock_rollback）**，在不破坏
每台设备业务顺序的前提下持续形成可查询的不可变分段；运维人员可冻结某一时刻的视图
并从中发起回放，回放期间新数据照常写入但不会混入已冻结批次。写入确认（ACK）前完成
WAL fsync，节点重启不丢已确认数据；分段损坏时自动隔离并给出可继续处理的位置。

零第三方依赖（Python 3.11 标准库），单容器即可运行。

## 快速开始

### 容器（推荐）

```bash
docker build -t eventarch:0.1.0 .
docker run --rm -p 8080:8080 -v eventarch-data:/data eventarch:0.1.0
```

或：

```bash
docker compose up --build
```

### 本地（Python 3.11+）

```bash
EA_DATA_DIR=./data python3 -m eventarch.server
# 或 make run
```

### 验证

```bash
make test     # 单元测试（分类/持久化/冻结/损坏隔离）
make smoke    # 端到端：起服务→分类→冻结→重启→损坏→隔离→重建
```

## 事件模型与分类语义

写入事件（`POST /v1/ingest`）：

```json
{
  "events": [
    {
      "device_id": "dev-A",                 // 必填，设备标识
      "event_id": "01J…",                   // 必填，幂等键（来源侧唯一）
      "seq": 12345,                         // 必填，设备侧业务序号（单调）
      "device_ts": "2026-09-17T08:00:00Z",  // 必填，设备时钟
      "payload": { "temp": 21.5 }           // 任意 JSON
    }
  ]
}
```

每个事件返回 `{event_id, status, offset, flags}`：

| 标记 | 判定 | 处理 |
|---|---|---|
| `duplicate` | `(device_id, event_id)` 已存在（含批内重复） | **不重复写入**，返回原 offset，幂等 ACK |
| `late` | `now - device_ts > EA_LATE_THRESHOLD_SEC`（离线补传） | 正常入库，打标 |
| `clock_rollback` | `device_ts` 小于该设备已见最大设备时钟 | 正常入库，打标 |
| `seq_conflict` | `seq` 已出现但 `event_id` 不同（序号复用冲突） | 正常入库，打标 |

- **业务顺序**：分段按到达顺序物理追加（不可变）；每台设备另维护按
  `(seq, offset)` 排序的索引。迟到的补传数据进入当前开放段并打标，绝不回写
  已封存段，但设备查询仍按业务序号有序返回。
- **offset**：全局单调的接收序号，是冻结、回放、续传的统一游标。

## 核心 API

| 方法/路径 | 说明 |
|---|---|
| `POST /v1/ingest` | 批量写入（批末一次 fsync 后 ACK）；单项错误不拖垮整批 |
| `GET /v1/devices` | 设备清单（事件数、最大序号、最大设备时钟） |
| `GET /v1/devices/{id}/events?from_seq=&from_offset=&limit=` | 单设备**业务序**查询，返回 `events/gaps/next` 游标 |
| `GET /v1/segments` | 分段清单（状态、offset 区间、sha256）+ 开放段信息 |
| `GET /v1/segments/{id}/events?from_offset=&limit=` | 段内扫描（逐帧 CRC 校验） |
| `POST /v1/segments/{id}/rebuild` | 提交**后台修复作业**重建被隔离段（立即返回 `202 + job`，不阻塞前台） |
| `GET /v1/repairs?limit=` | 修复作业列表（含状态/尝试次数/阶段/错误） |
| `GET /v1/repairs/{job_id}` | 查询单个修复作业状态 |
| `POST /v1/repairs/{job_id}` | 等待作业到达终态（body 可带 `timeout` 秒） |
| `POST /v1/freeze` | 冻结当前视图：封存开放段，返回 `{id, end_offset, segments}` |
| `GET /v1/freezes` | 冻结列表 |
| `GET /v1/replay?freeze_id=&from_offset=&device_id=&limit=` | 回放冻结视图（不传 `freeze_id` 则回放到当前头） |
| `POST /v1/gc/plans` | 容量清退**预演**：body `{"cut": <offset>}`，只计算并返回固定的 `plan_id/stamp/items/size`，不落任何盘 |
| `POST /v1/gc/plans/{id}/apply` | 受理预演：首次 `202 {"gc_job"}`，重复 `200` 同一 id；预演后任一前提变化整单 `409` 且磁盘原样 |
| `GET /v1/gc/jobs/{id}` · `GET /v1/gc/jobs` | 清退作业进度（`status/stage/completed/total/size_freed/items`）/ 作业列表 |
| `GET /v1/gc/audit?limit=` | 成功清退项的持久审计（JSONL 追加，每条目一行 fsync） |
| `POST /v1/holds` | 创建/幂等续期读者保护区：`{hold_id, pos, ttl_seconds}`，保护 `pos` 所在项及更大位置 |
| `GET /v1/holds` · `DELETE /v1/holds/{id}` | 保护区列表 / 主动解除 |
| `POST /v2/groups` | 登记下游持久消费组 `{name, start, end?, lease_seconds}`（同名同声明幂等，同名异义 `409`，起点废弃 `410`） |
| `POST /v2/groups/{name}/claim` · `…/settle` · `…/renew` | 领取一批（不移动 checkpoint）/ 当前持有者按批交卷 / 延租（epoch 不变） |
| `POST /v2/groups/{name}/pause` · `…/resume` · `DELETE /v2/groups/{name}` | 暂停撤水闸 / 恢复续订 / 注销 |
| `GET /v2/groups` · `GET /v2/groups/{name}` | 消费组清单 / 单组 |
| `GET /v1/stats` · `GET /v1/healthz` | 运行指标（含 `gc` 段）/ 健康检查 |

### 冻结与回放

```bash
# 1. 冻结：返回 end_offset（排他视界），此后到达的数据进新段
curl -XPOST localhost:8080/v1/freeze -d '{"note":"nightly"}'

# 2. 回放冻结视图（分页：用 next_from_offset 续拉）
curl 'localhost:8080/v1/replay?freeze_id=frz-…&limit=1000'

# 3. 追平后从冻结视界继续消费新数据
curl 'localhost:8080/v1/replay?from_offset=<end_offset>'
```

回放只读取冻结时刻已封存的不可变段；回放期间新到数据写入新的开放段，
**不可能混入已冻结批次**。段被隔离时回放不中断：响应的 `gaps[]` 标注
`{segment, reason, resume_offset}`，流自动从下一健康段继续。

### 损坏隔离与续处理位置

- **启动校验**：逐段 sha256 比对，失败即隔离（`status=quarantined`）。
- **读时校验**：每次读取逐帧 CRC32；发现损坏立即隔离并持久化。
- **隔离影响范围**：被隔离段从查询/回放中剔除，响应携带
  `resume_offset = last_offset + 1`——即可继续处理的位置。
- **修复（后台作业，不独占服务）**：`POST /v1/segments/{id}/rebuild` 不做同步重活，
  只把一个修复作业入队并立即返回 `202 {"job": {...}}`。重型 I/O（扫描保留的
  WAL、写候选段）全部在有界后台工作池中、**在全局锁之外**完成；前台摄取、检索、
  冻结/回放始终响应。同一段已有活动作业时返回同一作业（幂等去重，绝不重复修复）。

```bash
curl -XPOST localhost:8080/v1/segments/seg-…/rebuild        # -> 202 + job id
curl localhost:8080/v1/repairs/job-…                         # 轮询状态
curl -XPOST localhost:8080/v1/repairs/job-… -d '{"timeout":30}'  # 阻塞等待终态
```

修复作业状态机：`queued → running(gathering_wal/staging/committing) →
succeeded | failed`，每次状态迁移落盘到 `state/repairs.json`。失败时
`error.type` 可能是 `wal_coverage_gone`（带 `resume_offset`，调用方可跳过该段）、
`conflict`（重试用尽）、`not_found`、`shutting_down` 等。

### 并发安全与不变量

修复与“日志淘汰 / 同一归档单元状态变化 / 服务重启”三类竞态都显式处理：

- **与日志淘汰竞态**：作业在计划阶段把目标段登记为活动修复，`_wal_keep_from`
  会把它钉在保留水位之内，收集器因此不可能删掉作业即将使用的 WAL；跨轮转
  读到撕裂尾时整体重试（作业幂等）。
- **与同一单元状态变化竞态（乐观版本 CAS）**：每段 meta 带单调 `version`。
  作业计划时快照版本，提交时仅短持锁做 CAS：版本被竞争者改变则回滚文件、
  按当前状态重新计划并重试（有界退避）；若已被同样字节修复则识别为无操作，
  被其它内容取代则安全放弃——**绝不覆盖无损文件**。
- **无损文件绝不原地改写**：候选段写入独立的 `stage-<job>-<n>/<seg>/` 目录并
  自校验 sha256；提交是两次同文件系统内 `rename`（live→`bak-…`、stage→live）
  + 目录 fsync，再原子提交 manifest，最后删除 bak。进程内提交失败会反向
  交换回滚；进程崩溃由启动对账按作业日志完成或回滚。
- **不丢确认数据 / 不产生重复记录**：重建范围严格按
  `[first_offset, last_offset]` 连续对齐且条数一致才提交；设备索引在锁内
  按段整体替换（先剔除该段旧条目再装入新索引），不会重复插入。
- **消费游标与快照视界不变**：修复只换段的字节与 sha256/版本，段的
  offset 区间、计数、全局 `next_offset`、`sealed_through` 以及所有冻结视界
  （`freezes.json`、`end_offset`、段 id 列表）均不变；回放/查询游标语义稳定。
- **陈旧读取防护**：读时损坏隔离带 `expected_sha`；持有旧文件句柄的读者在
  修复原子换入新字节后无法用旧 sha256 把新段再次隔离。
- **重启恢复**：未完成作业在启动时回滚半成品目录后重新入队运行；已记录
  `succeeded` 但崩溃在“换目录与 manifest 提交之间”的作业会在候选字节校验
  通过时补提交、否则回滚，保证已确认数据不丢、段不重。

## 容量清退（GC）预演与读者保护

容量回收分两步：**只读预演**与**后台执行**，二者严格隔离——预演绝不修改任何
字节（连状态文件都不写），真正的删除意图只在 apply 受理时落盘。

### 预演：`POST /v1/gc/plans`

```bash
curl -XPOST localhost:8080/v1/gc/plans -d '{"cut": 100000}'
# {
#   "plan_id": "gcp-…",          # 固定，仅存在于本进程内存
#   "stamp":  "…",               # 整单指纹（cut + 每项 stamp/size）
#   "items":  [{"id","first_offset","last_offset","count","stamp","size"}],
#   "size":   4966
# }
```

- `cut` 是 offset 水位：完全位于水位**之下**（`last_offset < cut`）的已封存、
  健康段才可能入选。
- 每个候选项的 `stamp` 覆盖其身份/版本/meta sha、**events.log 实际字节哈希**与
  字节大小；预演后任何一项的字节、引用关系、维修态、保护集合或 cut 选择发生变化，
  apply 都会让**整单**返回 `409`（冲突原因逐条给出），磁盘维持原样。
- 入选自动避开三类对象：
  - **快照引用集合**：被任一 freeze 的 `segments` 引用的段；
  - **正在维修集合**：登记在活动修复作业中的段（含隔离段）；
  - **尚未过期的保护区（hold）**：见下。
- 同一 plan 反复 apply 返回**相同结论**：首次 `202 {"gc_job"}`，之后恒为 `200`
  + 同一个 `gc_job` id；已冲突的 plan 再次 apply 仍冲突（结论粘性）。

### 读者保护：`POST /v1/holds` / `DELETE /v1/holds/{id}`

```bash
# pos 定位到所在段；该段以及所有 first_offset 更大的段都被保护
curl -XPOST localhost:8080/v1/holds -d '{"hold_id":"reader-a","pos":6,"ttl_seconds":300}'
# 同一 hold_id 再发即幂等续期（新 pos/ttl 生效），只影响“之后创建”的 plan
curl -XDELETE localhost:8080/v1/holds/reader-a
```

- hold 持久化（`state/holds.json`），按 wall-clock 过期；过期即视同不存在。
- `pos` 落在开放尾/已清退区间之外时，边界取 `pos` 自身（保护未来更大位置）。

### 后台执行与三阶段发布

`apply` 受理后只入队一个 `gc_job`（`GET /v1/gc/jobs/{id}` 轮询
`queued→running→succeeded` 与 `completed/total`），重 I/O 全部在有界后台工作池中、
**在全局锁之外**完成；前台写流量、键值读取、快照建立、历史读取都不被拖慢。
读到正在换位的极短窗口会立即得到可重试的 `503`（而非阻塞），换位完成后再读则是
永久的 `410`。

每个入选项按三个崩溃安全阶段发布，进程在任意两阶段之间退出，下次启动都依据持久意图
（`state/gc.json`）对账——**要么复原旧布局，要么接续同一个 gc_job**，不存在半套生效：

1. **目录换位**：live 段目录原子 rename 到 `segments/gcgrave-<job>/<seg>/`；
2. **元数据总表发布**：从 manifest 移除该段、写入 evicted 墓碑（tombstone），原子提交；
3. **audit 追加**：向 `state/gc_audit.log` 追加一行 fsync 的 JSONL，随后删除墓场。

### 清理后的读语义与 410 续读位置

- 清退段以墓碑（offset 区间 + 紧凑设备索引）留在 manifest 中：**offset、排序键
  （设备业务序）、快照边界全部保持原值**，`next_offset`、`sealed_through` 不变。
- 访问已清理位置 → **HTTP 410**，携带准确 `cursor`：越过“连续已清退游程”之后的
  下一个存活 offset。例如 `[0..4]`、`[5..9]` 两段都清退后，从 `from_offset=0`
  读得到 `cursor=10`；用该 cursor 续读即从存活尾部继续。
- 设备业务序查询把相邻清退游程合并成一个 `gaps[]`（`reason="evicted"` +
  `resume_offset`）；其余位置按原业务序返回。
- 清退后新写照常（落在更高 offset）；清退前建立、仅引用存活段的快照历史读取不受影响。

```
state/gc.json        # gc_job 持久意图日志（崩溃对账/幂等去重依据）
state/gc_audit.log   # 每条成功清退一行 JSONL（fsync 追加），GET /v1/gc/audit
state/holds.json     # 读者保护区（含过期时刻）
segments/gcgrave-<job>/<seg>/   # 目录换位后、audit 前的临时墓场
```

## 下游持久消费组（API v2）

为下游算子提供命名的、落盘的持久订阅。每个组维护自己的 checkpoint、epoch、
租约持有者与待交卷批次；**每组任一时刻只有一名生效持有者**。

| 方法/路径 | 说明 |
|---|---|
| `POST /v2/groups` | 登记 `{name, start, end?, lease_seconds}`；相同声明再次登记沿用原对象（`200`），同名异义声明 `409`；起点落入已废弃区 `410`（附精确可用起点 `usable_start`），绝不暗中跳跃 |
| `GET /v2/groups` · `GET /v2/groups/{name}` | 消费组清单 / 单组（checkpoint、epoch、状态、水闸、在途批次） |
| `POST /v2/groups/{name}/claim` | 领取一批：`{batch_key, lease_key, epoch, messages, gaps, next_at, …}`；领取本身**不移动 checkpoint** |
| `POST /v2/groups/{name}/settle` | 仅当前持有者可交卷 `{lease_key, batch_key, next_at}`；重复交卷视为办妥 |
| `POST /v2/groups/{name}/renew` | 当前持有者延租（epoch 不变，可带 `lease_seconds`） |
| `POST /v2/groups/{name}/pause` · `POST …/resume` | 暂停（撤水闸、吊销租约）/ 恢复（从原 checkpoint 继续，旧凭证作废） |
| `DELETE /v2/groups/{name}` | 注销：移除声明、在途批次与水闸 |

### 领取、交卷与持有者世代

```bash
# 1. 登记：start 为起始 offset，end 为可选的排他静态终点
curl -XPOST localhost:8080/v2/groups \
  -d '{"name":"downstream-a","start":0,"end":1000,"lease_seconds":30}'

# 2. 领取（可带 limit，默认 EA_GROUP_MAX_BATCH=500，上限 5000）
curl -XPOST localhost:8080/v2/groups/downstream-a/claim -d '{}'
# -> {batch_key, lease_key, epoch, checkpoint, messages[], gaps[], next_at, ...}

# 交卷前反复 claim 得到同一批（同一 batch_key/lease_key/epoch）；
# 3. 只有该批给出的 next_at 可被当前持有者提交
curl -XPOST localhost:8080/v2/groups/downstream-a/settle \
  -d '{"lease_key":"ltk-…","batch_key":"btk-…","next_at":500}'
```

- **在途批次**：交卷前反复 `claim` 返回原批次；交卷后才出现下一批。
- **交卷校验**（全部失败均 `409` 且 checkpoint 原封不动）：再次提交（同
  `batch_key` + 同 `next_at`）视为办妥、零副作用；`next_at` 倒退或越批、
  跨批 / 虚构 `batch_key`、失效或过期 `lease_key` 一律拒绝。
- **世代（epoch）**：当前持有者延租不改 epoch；租约过期后新的 `claim` 接管并
  取得**递增** epoch 与全新 `lease_key`，旧持有者此后的领取、延租、交卷全部
  `409`。交卷后未到期的同一持有者可直接领取下一批。
- **静态 / 动态**：声明了 `end` 的静态组在 checkpoint 抵达 `end` 后**永久结束**，
  此后到达的消息不可见（`claim` 返回 `finished:true`）；未声明 `end` 的动态组
  一直追随新增消息，暂时无数据时返回 `empty:true`。
- **缺口 gaps**：领取区间跨过被隔离段时显式给出 `gaps[]`（`reason/resume_offset`），
  消费者可交卷跳过；触及已废弃（evicted）区间返回 `410` 与精确 `cursor`。

### 容量回收水闸（watergate）

系统从每组 checkpoint 自动设置回收水闸：保护**包含 checkpoint 的段以及一切更靠后
的段**。已领取但尚未交卷的消息因此不可能被容量回收移除；交卷后水闸前移，旧区可被
清退而未读后缀仍被水闸圈住。暂停或注销时撤掉水闸。水闸与 GC 预演/执行联动：
水闸覆盖的段不进入 GC 计划；预演后水闸迁移会让整单 apply 返回 `409` 且磁盘原样。

### 落盘与三处断电缝隙

声明、checkpoint、epoch、持有者租约与待交卷批次全部 fsync 落盘。交卷按顺序发布：

1. **批次落盘**（claim 时 `v2_groups.json` + `v2_batches.json`）；
2. **checkpoint 落盘**（settle 写 `v2_groups.json`）；
3. **水闸迁移**（清批次账 + 写 `v2_gates.json`）。

在任一步之后强制结束进程，再次开机都满足：**已交卷批次不会再出现；待交卷批次仍使用
原 batch_key；水闸与 checkpoint 对齐；不遗留无人持有的水闸；checkpoint 不会被推进两遍**。
启动时以 `v2_groups.json` 内嵌的在途描述为准对账三份文件。

```
state/v2_groups.json   # 声明/checkpoint/epoch/持有者租约/在途批次描述
state/v2_batches.json  # 待交卷批次账（崩溃缝隙期间以组记录为准重建/裁剪）
state/v2_gates.json    # 每组的回收水闸边界（仅 active 组存在）
```

订阅的重 I/O（逐段扫描）在全局锁之外完成，与常规接入、查找、静态终点生成、整治任务、
容量回收并行时均及时答复，既有 cursor 含义与静态终点保持不变。

## 持久化与故障语义

```
$data_dir/
  wal/00000000000000000000.wal     # 追加写，[crc32|len|json] 帧，按封存边界轮转
  segments/seg-00000000000000000000/
    events.log                     # 不可变记录（同 WAL 帧格式）
    index.json                     # 设备 → (seq, offset, event_id, pos, len)
    meta.json                      # offset 区间、计数、sha256、时间窗
  state/manifest.json              # 段目录 + 封存水位 + evicted 墓碑（原子替换落盘）
  state/freezes.json               # 冻结视界
  state/repairs.json               # 后台修复作业日志（崩溃恢复/去重依据）
  state/gc.json                    # 清退作业持久意图日志（三阶段崩溃对账/幂等依据）
  state/gc_audit.log              # 成功清退项的 JSONL 追加审计（每行 fsync）
  state/holds.json                 # 读者保护区（hold_id/pos/边界/过期时刻）
  state/v2_groups.json             # 下游消费组（声明/checkpoint/epoch/租约/在途批次）
  state/v2_batches.json            # 待交卷批次账（batch_key → 描述）
  state/v2_gates.json              # 每组按 checkpoint 自动设置的回收水闸
  segments/stage-<job>-<n>/<seg>/  # 修复候选（提交前不触碰 live）
  segments/bak-<job>-<n>/<seg>/    # 原子交换期间的旧段（提交后删除）
  segments/gcgrave-<job>/<seg>/    # GC 目录换位后、audit 前的临时墓场
```

- **确认即持久**：每批写入先 WAL 追加 + `fsync`，再更新内存态并 ACK；
  段文件 fsync → manifest 原子提交 → WAL 轮转，顺序保证崩溃可恢复。
- **重启恢复**：校验所有封存段（sha256）→ 清理未提交的孤儿段目录 →
  截断 WAL 撕裂尾（torn tail）→ 重放未封存记录 → 重建内存索引。
  WAL 文件中部损坏时隔离该文件、记录 `wal_gaps` 并从下一有效位置继续，
  损失窗口显式可查（`GET /v1/stats`）。
- **冻结可重启**：freeze 元数据落盘，重启后仍可按原视界回放。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `EA_DATA_DIR` | `./data`（容器内 `/data`） | 数据目录 |
| `EA_ADDR` | `0.0.0.0:8080` | 监听地址 |
| `EA_SEGMENT_MAX_RECORDS` | `1000` | 段封存阈值（条数，批内超限自动切分） |
| `EA_SEGMENT_MAX_AGE_SEC` | `300` | 段封存阈值（开放时长，秒） |
| `EA_LATE_THRESHOLD_SEC` | `900` | 迟到判定阈值（秒） |
| `EA_WAL_RETAIN_SEGMENTS` | `8` | 保留多少个已封存段的 WAL 用于重建 |
| `EA_FSYNC` | `1` | 置 `0` 仅用于基准测试（丢失安全性） |
| `EA_MAX_BATCH` | `1000` | 单批最大事件数 |
| `EA_REPAIR_WORKERS` | `2` | 后台修复作业并发工作线程数 |
| `EA_REPAIR_MAX_ATTEMPTS` | `5` | 单个修复遇版本冲突/瞬时 I/O 的最大尝试次数 |
| `EA_REPAIR_RETRY_BACKOFF_SEC` | `0.1` | 重试退避基数（×尝试次数） |
| `EA_REPAIR_HISTORY` | `100` | 作业日志保留的终态作业条数（活动作业不裁剪） |
| `EA_GC_WORKERS` | `1` | 后台清退作业并发工作线程数 |
| `EA_GC_HISTORY` | `100` | 清退作业日志保留的终态作业条数（活动作业不裁剪） |
| `EA_GROUP_MAX_BATCH` | `500` | 单次 claim 最多下发的消息条数（上限 5000） |

## 设计取舍与限制

- 单节点；全局锁只保护内存态与 WAL 追加/提交点（均为短临界区），重型读/修复
  I/O 在锁外进行，后台修复作业因此不阻塞前台摄取、检索与快照；吞吐瓶颈在
  fsync 频率，按批聚合。
- 设备索引与去重表在内存中重建（启动重放段索引 + WAL 尾）；事件量级
  超出内存时需外置索引（当前架构可平滑替换 `DeviceState` 的存储）。
- 重复判定依赖 `event_id` 全量驻留内存；`seq` 冲突只打标不拒绝（保留现场）。
- 冻结会触发一次段封存，频繁冻结会产生较多小段。
- 时间戳一律规范化为 UTC ISO-8601；设备时钟仅用于分类，不参与排序。
