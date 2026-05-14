# Slurm 跨域 Broker (`slurmbrokerd`) MVP 详细设计文档 (实现版, **MVP-v2.0**)

> **版本**: **MVP-v2.0** (路由能力上提到 broker; 引入 `routes.conf` + test-only 探测 + 多远端候选)
> **上一版**: MVP_new (M01~M13 已落地, ctld 主导路由的过渡形态)
> **状态**: v2.0 设计阶段; v1.5 已落地的代码作为基线, 增量改造范围见本文档 §2.2.A 与 `跨域调度详设-差异变更说明.md` §2
> **基线**: `Broker详细设计文档MVP_new.md` (MVP_new) + **`跨域调度-概要设计.md`** (新顶层设计) + `跨域路由能力演进设计.md`
> **配套文档**:
>   - 顶层方案 / 端到端 → `跨域调度-概要设计.md`
>   - ctld 端配套设计 → `Slurmctld跨域详细设计文档MVP_v2.md`
>   - v1.5 → v2.0 差异蓝图 → `跨域调度详设-差异变更说明.md`
>   - 路由能力演进背景 → `跨域路由能力演进设计.md`
> **目标**: 本文档作为 broker v2.0 后续维护、培训、验收的**当前真相 (source of truth)**, 取代 v1.x 版本。

---

> **v1.x → v2.0 顶层差异 (一句话总结)**:
>
> | 条目 | v1.x (ctld 主导路由) | **v2.0 (broker 主导路由)** |
> |---|---|---|
> | 远端目的地决策 | ctld `partition.SendTo` 一对一映射, broker 只是转发器 | **broker `routes.conf` 多远端候选 + test-only 探测后选定 first OK** |
> | 应用软件白名单 | ctld `partition.AllowApp` | **broker `routes.conf::AllowApps=` (按路由维度)** |
> | 容量软限流 | ctld 复用 `partition.MaxJobs` 跨域在途计数 | **broker `cap_check.c` + `routes.conf::MaxInflight` / `RemoteMaxInflight`** |
> | `forward_job_msg_t` payload | 9 字段 + nested `job_desc_msg_t` (~150 字段) | **6 个原子字段, broker 自己 GET_JOB_INFO 反查 job_desc** |
> | 远端字段写回 ctld 时机 | ctld 转发前已写 `cd_remote_cluster_name` / `cd_remote_partition_name` | **broker 首次 `UPDATE_REMOTE_STATE` 包带过来, ctld 写入 (broker 才知道实际投递到哪)** |
> | broker 状态机 INIT 阶段 | 仅 stage_in | **stage_in 之前先 `route_decide()` + `cap_check()` + 逐远端 `test_only()` 探测** |
> | broker RPC | 8001~8016 (15 个) | **+ 8018/8019 (test-only 探测)**; 错误码 + 9010/9011/9012/9013/9020 |
> | 远端提交身份 | broker 在远端 `sudo -u <local_user>` 提交 | **broker.conf 新增 `SubmitMode=user|root_uid`, root_uid 用 `--uid=...`** |
>
> **总体取舍**: broker 从"转发桥"升级为"跨域调度大脑"; 复杂度大幅上提, 但换来 ctld 极简、灵活的多远端调度能力和清晰的"决策-执行"职责边界。

## 一、文档目的与定位

### 1.1 与旧版的关系

| 版本 | 状态 | 文件 |
|---|---|---|
| v0.1 (旧版裁剪规划) | 历史归档, 不再维护 | `Broker详细设计文档MVP.md` |
| MVP_new (v1.5, 落地实现版) | 已落地, M01~M13 完成; 路由仍由 ctld 主导 | `Broker详细设计文档MVP_new.md` |
| **v2.0 (本文)** | **设计阶段, 增量改造 v1.5 代码** | 本文档 (`Broker详细设计文档MVP_v2.md`) |

v2.0 在 v1.5 的代码骨架上做**增量**改造, 不重写。改造重点章节集中在 §2.4 数据结构 / §2.5 状态机 / §2.6 RPC / §2.7 流程 / §2.8 模块 / §2.10 配置。未标注 `★ v2.0 新增` / `★ v2.0 改造` 的章节, 与 v1.5 一致。

### 1.2 强约束（最重要——决定一切实现选择的指挥棒）

```
所有改动均在 src/slurmbrokerd/ 下；slurm 原生源码禁止修改；
所有逻辑都放到 src/slurmbrokerd/ 下；如必须扩展原生函数行为，
通过在 src/slurmbrokerd/ 内新增包装函数实现。
```

这条规则贯穿所有模块设计，并直接驱动了下列三个**与旧版不同**的关键技术决策（v1.5 已落地, v2.0 沿用）：

| 旧版假设 | 实际落地 | 触发原因 |
|---|---|---|
| `broker_job_t` 内嵌 `job_desc_msg_t *job_desc`，跨集群透传 | 改为 `app_name` + `script_path` 两个平铺字段 | `job_desc_msg_t` 是 Slurm 版本绑定结构；A=24.05 ↔ B=23.11 跨集群必须解耦 |
| 4 个 `ctld↔broker` RPC 直接复用 Slurm 的 `pack_msg`/`unpack_msg` 大 switch | broker 侧用一份私有 wire frame（`'BRKR'` 魔数 + `auth_g_pack` + 自定义 packing）实现，标 `LEGACY_M04_TRANSITIONAL`；待 ctld 同事在 `src/common/` 注册同名 msg_type 后机械删除 | 原生 `slurm_protocol_pack.c` 不准动 |
| `Include /etc/slurm/user_mapping.conf` 二级配置 | 用户映射直接在 `broker.conf` 内联 `LocalUser=...` 行（用 `S_P_LINE`） | `Include` 在 `parse_config.c` 的实现依赖原生函数；改为单文件解析后无须扩展原生解析器 |

### 1.3 broker 端 MVP 必做的对外契约 (v2.0)

| # | 契约 | v1.x | v2.0 |
|---|---|---|---|
| 1 | 接收 ctld 转发 | `BROKERD_REQUEST_FORWARD_JOB` (msg_type=8001) | 同; **payload 瘦身, 字段从 9+1 缩为 6 个原子字段** (§2.6) |
| 2 | 回写 ctld 远端状态 | `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` (8003); 写 `remote_*` 独立字段, 永不写 `comment` | 同; **首次状态包必须携带 `remote_cluster_name` / `remote_partition_name`** (broker 路由决策结果告知 ctld) |
| 3 | 通知 ctld 终态 | `BROKERD_REQUEST_BROKER_TERMINAL_STATE` (8004) | 同 |
| 4 | 改写 source 行 | receiver 端 `rewrite.c` + `software.c::lookup_software_path()` | 同; **新增 partition 名重写** (因为 broker 决策的远端 partition 可能与 ctld 提交时不同) |
| 5 | 处理 scancel 反向 | `BROKERD_REQUEST_BROKER_CANCEL` (8016) | 同 |
| **6** ★ v2.0 新 | **test-only 探测** | (无) | **`REQUEST_BROKER_TEST_ONLY` (8018) / `RESPONSE_BROKER_TEST_ONLY` (8019)**: broker→broker 探测远端 ctld 是否会真实接收, 不实际投递 |
| **7** ★ v2.0 新 | **路由决策反馈错误码** | 仅 `BROKERD_ERR_*` 9 个 | **新增 5010 NO_VIABLE_ROUTE / 5011 TEST_ONLY_REJECTED / 5012 TEST_ONLY_TIMEOUT / 5013 ALL_ROUTES_EXHAUSTED / 5020 CAP_FULL_SOFT_WAIT** (与 ctld 5010~5020 段对接) |

> 说明：8001/8003/8004 是 broker 私有占位定义。一旦 ctld 同事的 PR 在 `src/common/slurm_protocol_defs.h` 注册了**同号且同字段顺序**的 msg_type，broker 端这 3 个 `LEGACY_M04_TRANSITIONAL` 标记的代码块会被一次性删除（共 27 处，详见 §6.5）。**v2.0 新增的 8018/8019 同样走 `LEGACY_M04_TRANSITIONAL` 占位路径**, 后续 ctld 同事一并注册。

---

## 二、MVP 范围

### 2.1 用户故事 (v2.0)

| ID | 故事 | broker 端职责 (v2.0) |
|---|---|---|
| US-1 | sbatch `--allow-remote --app=...` 提交 | **接收 ctld 转发 → 加载 `routes.conf` 匹配候选路由 → `cap_check` 软限流 → 逐远端 test-only 探测 → 首次 OK 后正式 stage / 提交 / 同步状态 / 终态回写** ★ v2.0 改造 |
| US-2 | scancel 反向传播 | 接收 ctld 取消 → 跨集群传播 → 远端 `slurm_kill_job` |
| US-3 | `squeue --remote` / `sacct` 看跨域字段 | sync_ticker 周期推送 `remote_*` 独立字段; **首次推送时携带 broker 选定的 `remote_cluster_name` / `remote_partition_name`** ★ v2.0 改造 |
| US-4 | broker 重启恢复 | JSONL 持久化 + trace_id 幂等 + `state_machine_resume_inflight`; **resume 时若处于 INIT 路由阶段尚未 stage, 继续从 `route_attempted` 数组下一个候选探测** ★ v2.0 新增 |
| **US-5** ★ v2.0 新增 | `routes.conf` 热更新 | broker 监听 SIGHUP → `routes_loader_reload()` 重新解析 / atomic swap; 在途作业按内存中已选定的路由继续, 新作业用新表 |

### 2.2 范围分层

#### A. 对外接口 (★ v2.0 增量, 旧 E1~E5 不变)

| # | 项 | 落地位置 |
|---|---|---|
| E1 | ctld → broker `FORWARD_JOB` | `proto.h::brokerd_forward_job_msg_t` + `handler_ctld.c::handle_forward_job`; **v2.0 payload 瘦身**: `target_cluster` / `target_partition` / `account` / nested `job_desc` 全部删除, 仅保留 6 个原子字段 |
| E2 | broker → broker 5 RPC (FORWARD / STAGED_IN / QUERY_STATUS / CANCEL / CLEANUP) | 不变 |
| E3 | broker → ctld `UPDATE_REMOTE_STATE` 字段化 | `egress.c::ctld_update_remote_state`; **v2.0 首次包必须携带 `remote_cluster_name` / `remote_partition_name`** (broker 路由决策结果告知 ctld) |
| E4 | broker → ctld `TERMINAL_STATE` | `egress.c::ctld_inject_terminal_state` |
| E5 | broker → 本机 `lookup_software.sh` | `software.c::lookup_software_path` |
| **E6** ★ v2.0 新增 | broker → broker `TEST_ONLY` | `proto.h::brokerd_test_only_msg_t` (8018) + `handler_remote.c::handle_broker_test_only` + `egress.c::egress_test_only_sync` (8019 同步响应) |
| **E7** ★ v2.0 新增 | broker 端路由决策 | `route.c::route_decide()` / `cap_check.c::cap_check()` / `routes_loader.c::routes_load_file()`; 仅 broker 内部函数, 不对外暴露 |

#### B. broker 内部实现 (v2.0 增量)

| # | v1.5 落地 | v2.0 增量 | 状态 |
|---|---|---|---|
| I1 | 9 状态 + 两阶段 action queue + 9 个 `_on_xxx` | INIT 阶段内嵌"路由 + 容量 + test-only 探测"三步, 不增加状态数; 错误码增 5010~5020 段 | ★ 改造 |
| I2 | JSONL `broker_job_t` | 新增 `route_attempted[]` 数组字段 + `selected_route_id` 字段, 用于 resume 续探测 | ★ 增量 |
| I3 | broker→broker 私有 wire frame | 新增 8018/8019 (test-only) 沿用同样 wire frame | ★ 增量 |
| I4 | 单对端集群 (`remote_cluster_name`) | **多远端候选**: `routes.conf` 定义多 `RemoteCluster=` 段, broker 按 `Priority` 排序, 逐个探测; `remote_cluster_name` 字段语义改为"运行期已选定" | ★ 改造 |
| I5 | `rewrite.c` 单文件 | 新增 partition 名重写 (因为 broker 决策的 partition 可能与 ctld 提交时不同) | ★ 增量 |
| I6 | `sudo -u <remote_user> mkdir -p` | 同; 远端用户由 `LocalUser=` 行 + `routes.conf::RemoteCluster=` 维度复合查表 | ★ 改造 |
| I7 | `sudo -u <local_user> rsync` | 同; 但提交身份新增 `SubmitMode=root_uid` 模式 (broker.conf), 此时远端 broker 用 root + `sbatch --uid=<remote_uid>` | ★ 增量 |
| I8 | sync_ticker 10s | 同 | ✅ 不变 |
| **I14** ★ v2.0 新 | 路由表加载 | `routes_loader.c::routes_load_file()` 启动时 + SIGHUP 热加载; atomic swap; cache hash + last reload mtime | ★ 新增 |
| **I15** ★ v2.0 新 | 容量软限流 | `cap_check.c`: 按 `routes.conf::MaxInflight` (本地路由维度) + `RemoteMaxInflight` (远端集群维度) 二维计数; 命中返回 `BROKERD_ERR_CAP_FULL_SOFT_WAIT` (5020) | ★ 新增 |
| **I16** ★ v2.0 新 | test-only 探测 | `route.c::route_test_only_probe()`: 调 8018 远端 broker → 远端 broker 调远端 ctld 的 `submit_batch_job(test_only=true)`; 5s 超时退避 | ★ 新增 |
| I9-I13 | sbroker / metrics 等不做 | 同 | ✅ 不做 |

### 2.3 不做的能力

完全沿用旧版 §2.3。**新增一项废弃**：

| 能力 | 用户感知 | 状态 |
|---|---|---|
| ~~远端工作目录定时清理（M14 cleanup）~~ | ~~24h/7d 后 `rm -rf <dst_work_dir>`~~ | **本 MVP 暂不做，永不删除用户数据**；终态作业仅从内存表移除，远端目录留作运维取证 |

---

## 三、整体架构

### 3.1 系统视图

不变（与旧版 §3.1 同图）。两个 broker 监听端口：

| 端口 | 默认 | 协议 | 出/入方向 |
|---|---|---|---|
| `BrokerCtldPort` | 8442 | Slurm 原生 RPC frame | 本机 ctld → 本 broker（loopback ACL） |
| `BrokerPeerPort` | 8443 | broker 私有 wire frame | 远端对端 broker → 本 broker（DNS 白名单 ACL） |

### 3.2 端到端时序 (v2.0)

```
sbatch (US-1)                                                scancel (US-2)
─────────────                                                ──────────────
1. user sbatch --allow-remote --app=...                     1. user scancel <local_jobid>
2. ctld(A) 入队、判定 cross_region                         2. ctld(A) 标 cancel
3. ctld(A) → broker(A) :FORWARD_JOB(8001) ★ v2.0 瘦身 payload  3. ctld(A) → broker(A) :CANCEL(8016)
   (无 target_cluster / target_partition / job_desc)        4. handler_ctld::handle_cancel_from_ctld
4. handler_ctld::handle_forward_job                            - 设 cancel_requested = true
   - 溢出保护 + user_mapping_lookup                         5. state_machine 下一 tick (cancel 优先级)
   - broker_job_create + broker_job_table_add                   -> egress_cancel_async (8016)
   - state = INIT, persist_async_request                        -> transition CANCELLED
   - synchronously 回 RESPONSE_FORWARD_JOB
     ┌── ★ v2.0 INIT 阶段三步路由探测 ──┐
     │ 4.1 route_decide(): 查 routes.conf │
     │     按 (src_cluster, src_partition) │
     │     匹配多个 [Route] 段, 按        │
     │     Priority 排序成 candidates[]   │
     │ 4.2 cap_check(): 对 candidates[]    │
     │     逐个检查 MaxInflight /          │
     │     RemoteMaxInflight, 过滤        │
     │ 4.3 逐个 test_only_probe():         │
     │     • 调 8018 远端 broker          │
     │     • 远端 broker 调本地 ctld:     │
     │       submit_batch_job(test_only=1) │
     │     • 远端 ctld 真实做资源/ACL/QOS │
     │       校验, 但不实际投递          │
     │     • 5s 超时退避                  │
     │     首次 OK → selected_route       │
     │     全部失败 → 同步回 9013         │
     └────────────────────────────────────┘
5. state_machine tick (1Hz)
   _on_init: 已有 selected_route, stage_submit_in + transition STAGING_IN
   立即:
     egress.c::ctld_update_remote_state (8003) ★ 首次包带:
       remote_cluster_name = selected_route.remote_cluster
       remote_partition_name = selected_route.remote_partition
       remote_state = JOB_PENDING
6. stage worker (4 个之一):
   - sudo -u src_user rsync -e ssh ... --partial
   - 完成 → state_machine_transition STAGED_IN
   - egress_staged_in_async (8012)
7. broker(B) handler_broker_staged_in
   - rewrite_job_script (rewrite.c) ★ v2.0 含 partition 重写
   - sudo -u remote_user sbatch *.cd_modified.sh
     (或 v2.0 SubmitMode=root_uid 时: sudo sbatch --uid=remote_uid)
   - 抓 remote_job_id → RESPONSE_BROKER_SUBMITTED (8013)
8. broker(A) sync_ticker 每 10s 批量 8014:
   - 收 8015 → _apply_remote_status:
     PENDING -> ctld_update_remote_state (8003)
     RUNNING -> transition RUNNING + 8003
     DONE/FAIL -> stage_submit_out + transition STAGING_OUT
9. stage worker (反向 rsync) → COMPLETED
10. state_machine 终态 tick:
    - ctld_inject_terminal_state (8004)
    - broker_job_table_remove
```

> **v2.0 关键差异**: 第 3 步 ctld→broker 不再告诉 broker 要去哪 (target_cluster / target_partition 字段被删除); 第 4.1~4.3 步 broker 自己决定。这一变化让"路由决策中心"集中在 broker, ctld 只是触发者。
>
> **test-only 探测的设计权衡**: 引入 test-only 是为了在真实投递前用最便宜的方式验证资源 / ACL / QOS / partition 限制是否可承载, 失败时立刻 fail-fast 切下一个候选, 不需要走完 stage_in + 远端 sbatch 失败 + 反向 rsync 这个昂贵的弯路。test-only 利用 Slurm 原生 `--test-only` 语义, 远端 ctld 真实跑一遍 `_select_nodes_for_job()`, 但 `select_g_job_test()` 不分配资源。在 80% 的容量场景下能节省 ~50% 的 stage 带宽与 ~10s 的端到端延迟。

### 3.3 线程拓扑（实际 7 线程组）

```
slurmbrokerd 进程
├── Main Thread                  信号处理 + sleep(1) 主循环
├── Listener Thread              单线程 select 8442 / 8443 + self-pipe
├── State Machine Tick           1s 周期 + self-pipe wakeup
├── Sync Ticker                  10s 周期 + self-pipe wakeup
├── Persister Thread             30s 周期 + cond_timedwait（被 persist_async_request 提前唤醒）
└── Stage Worker × N             默认 N=4，cond_wait 阻塞，fork+exec rsync
```

**与旧版差异**：

- **Egress Worker 不再独立**：每个 egress 调用都在调用线程内同步完成（state_machine tick / sync_ticker / stage worker / handler）。详见 §3.4。
- **Persister 与 Cleanup 分离**：cleanup 子模块（M14）暂废弃，persister 单独成线程。
- **self-pipe 防 fd 重用竞争**：listener / state_machine / sync_ticker 三个长 sleep 循环都装了一个 wakeup pipe，`shutdown` 时主线程 `write(wakeup_pipe[1])` 让 select/cond 立即返回，避免被关闭的 fd 被新 fd 复用导致竞争。

### 3.4 Egress Worker 为什么不在线程图里

旧版 §3.3 列了 `Egress Worker × 2`，定位为"异步出站调用执行池"。落地时**刻意取消了独立线程**，改由调用线程同步阻塞执行。决策依据写在 `egress.c` 头部：

```26:32:src/slurmbrokerd/egress.c
 *  Threading model
 *  ---------------
 *  Each wrapper is fully synchronous on the calling thread:
 *  state_machine tick / sync_ticker call the wrapper, the wrapper
 *  blocks for at most timeout_s and returns. There is no background
 *  egress worker pool; M08-T1 risks table accepts the trade-off given
 *  MVP throughput targets (<= 50 RPC/s).
```

#### 决策理由（M08-T1 设计时的权衡）

1. **MVP 吞吐 ≤ 50 RPC/s**：2 个 worker 的并发能力远超需求。
2. **少一处生产者-消费者队列**：去掉一份 mutex/cond/线程 lifecycle，bug 面减一半。
3. **state_machine 两阶段 tick 已兜住阻塞风险**：Phase 1 持锁扫描入队，Phase 2 **释锁后**才执行 egress；即便 egress 阻塞 30s 也不会卡住其他读 `g_broker_jobs_lock` 的线程。
4. **`_retry_n_times` 强制 timeout + 退避**：每个 wrapper 自带超时上限，不会无限阻塞。

> 命名沿用：`egress_*_async` 函数名后缀的 `_async` 是从旧版规划继承的命名，**实际是同步阻塞**。`egress_init()` / `egress_fini()` 仅做 sanity check，没有起任何线程。

#### 7 个出站 RPC 现在由谁来跑

| 出站调用 | 实际执行线程 | 调用点 |
|---|---|---|
| `egress_forward_async` (8010 broker→broker) | **State Machine Tick** | `_on_init` Phase 2 |
| `egress_staged_in_async` (8012 broker→broker) | **Stage Worker** | rsync 完成回调 |
| `egress_query_status_sync` (8014 broker→broker) | **Sync Ticker** | 10s 周期 Phase 2 |
| `egress_cancel_async` (8016 broker→broker) | **State Machine Tick** | cancel 优先级 Phase 2 |
| `egress_cleanup_async` (8017 broker→broker) | （M14 废弃，未启用） | — |
| `ctld_update_remote_state` (8003 broker→ctld) | **Sync Ticker** | `_apply_remote_status` Phase 3 |
| `ctld_inject_terminal_state` (8004 broker→ctld) | **State Machine Tick** | 终态 tick Phase 2 |

Egress 的"职责"完整保留，只是它**不再有自己的线程**：每个出站 RPC 由"想发它的线程"自己跑同步阻塞。

#### 何时需要回头把它独立成线程

未来出现下列任意一条，就要把 egress 重新做成 worker pool：

- **吞吐升到 ≥ 200 RPC/s**：state_machine 一次 tick 串行发 200 个 8003 推送会拖慢 1Hz 节拍；
- **跨集群 RTT 长尾差**（经常 > 5s）：单个 8014 阻塞会让 sync_ticker 的 10s 周期崩盘；
- **多对端集群**：每个 peer 应有独立队列防止单 peer 故障传播。

改造路径很短：在 `egress.c` 内部加 `egress_request_t` 队列 + N 个 worker，对外 wrapper 函数签名一字不动，调用方零改动。可以与 §14.2 演进表中"broker→broker mTLS / HTTPS"那一栏并入同一个改造做。

---

## 四、数据结构

### 4.1 `broker_job_t` (v2.0)

```c
/* src/slurmbrokerd/broker_job.h */

#define BROKER_TRACE_ID_LEN 48
#define BROKER_MAX_ROUTE_CANDIDATES 8   /* ★ v2.0 新增上限 */

typedef enum {
    BROKER_STATE_INIT          = 0,
    BROKER_STATE_STAGING_IN    = 1,
    BROKER_STATE_STAGED_IN     = 2,
    BROKER_STATE_SUBMITTED     = 3,
    BROKER_STATE_RUNNING       = 4,
    BROKER_STATE_STAGING_OUT   = 5,
    BROKER_STATE_COMPLETED     = 6,
    BROKER_STATE_FAILED        = 7,
    BROKER_STATE_CANCELLED     = 8,
} broker_job_state_t;

typedef enum {
    BROKER_ROLE_ORIGINATOR = 0,
    BROKER_ROLE_RECEIVER   = 1,
} broker_role_t;

/* ★ v2.0 新增: INIT 阶段路由探测的子状态 */
typedef enum {
    BROKER_INIT_PHASE_DECIDING    = 0,   /* 还没调 route_decide */
    BROKER_INIT_PHASE_PROBING     = 1,   /* 正在逐个 test-only */
    BROKER_INIT_PHASE_SELECTED    = 2,   /* 已选定 selected_route_id, 可以进 stage_in */
    BROKER_INIT_PHASE_EXHAUSTED   = 3,   /* 全部候选均失败, 转 FAILED */
} broker_init_phase_t;

typedef struct broker_job {
    /* identity */
    char       trace_id[BROKER_TRACE_ID_LEN];   /* "<src_cluster>-<src_job_id>" */
    uint32_t   src_job_id;
    uint32_t   remote_job_id;                   /* 0 == not yet remote-submitted */
    broker_role_t role;
    uint8_t    hop_count;

    /* user identity */
    char      *src_user_name;
    uint32_t   src_uid;
    char      *remote_user_name;                /* ★ v2.0: 由 selected_route + LocalUser 行查表决定 */
    uint32_t   remote_uid;
    uint32_t   remote_gid;

    /* cluster routing (v2.0: dst_cluster / target_partition 现在是"运行期决策结果", 由 broker 填) */
    char      *src_cluster;                     /* 来自 forward_job_msg_t::src_cluster_name */
    char      *src_partition;                   /* ★ v2.0 新增: 来自 forward_job_msg_t::src_partition, 用于查 routes.conf */
    char      *dst_cluster;                     /* ★ v2.0: 由 route_decide() 选定后填 */
    char      *target_partition;                /* ★ v2.0: 同上 */
    char      *selected_route_id;               /* ★ v2.0 新增: routes.conf [Route route_xxx] 段 id */

    /* ★ v2.0 路由探测状态 (仅 INIT 阶段使用) */
    broker_init_phase_t  init_phase;
    char       *route_candidates[BROKER_MAX_ROUTE_CANDIDATES];   /* 按 Priority 排序的候选 route_id */
    uint8_t     route_candidates_count;
    uint8_t     route_attempted_mask;            /* bitmask: 第 i 位 = 已 probe 过 candidates[i] */
    uint8_t     route_current_idx;               /* 当前正在探测的候选下标 */

    /* working directories */
    char      *src_work_dir;
    char      *dst_work_dir;
    char      *script_path;                     /* originator 绝对路径；receiver 用 basename() */

    /* application identity (drives rewrite + lookup_software.sh + AllowApps 匹配) */
    char      *app_name;                        /* 取代旧版 job_desc_msg_t */

    /* state machine */
    broker_job_state_t state;
    char      *state_reason;
    uint32_t   retry_count;
    time_t     state_enter_time;
    time_t     submit_time;
    time_t     last_poll_time;

    /* terminal accounting */
    time_t     remote_start_time;
    time_t     remote_end_time;
    char      *remote_alloc_tres;
    int32_t    remote_exit_code;

    /* cancel propagation */
    bool       cancel_requested;
    bool       cancel_propagated;

    /* per-job lock */
    pthread_mutex_t lock;
} broker_job_t;
```

> **v1.5 → v2.0 关键变更**:
> 1. 新增 `src_partition` 字段: 由 `forward_job_msg_t::src_partition` 直接写入, 用于 `route.c::route_decide()` 在 `routes.conf` 中按 `(src_cluster, src_partition)` 二元组匹配候选路由。
> 2. `dst_cluster` / `target_partition` 字段语义改变: 从 v1.5 的"ctld 告知的目标"变为"broker 运行期决策结果"。
> 3. 新增 `selected_route_id` / `route_candidates[]` / `route_attempted_mask` / `route_current_idx` / `init_phase` 五个字段用于 v2.0 路由探测; 当 broker 重启 resume 时, 处于 INIT 阶段且 `init_phase==PROBING` 的作业能从 `route_attempted_mask` 续探测下一个候选。
> 4. 删除 `account` 字段 (v2.0 forward_job_msg_t 不再传 account; 远端 sbatch 不带 `--account`, 由远端 sacctmgr default account 接管)。
> 5. `remote_user_name` 不再由 ctld 端 user_mapping 决定, 而是按 `LocalUser=<src_user>@<dst_cluster>=<remote_user>` 多元查表 (因为 v2.0 可能选不同 dst_cluster)。

#### 4.1.1 `route_decide()` 输出结构

```c
/* src/slurmbrokerd/route.h, v2.0 新增 */
typedef struct {
    char    *route_id;            /* "[Route route_xahc_to_wz]" 段名 */
    char    *remote_cluster;
    char    *remote_partition;
    char    *remote_broker_host;
    uint16_t remote_broker_port;
    uint16_t priority;
} route_candidate_t;

extern int route_decide(const char *src_cluster,
                         const char *src_partition,
                         const char *app_name,
                         /* out */ route_candidate_t **candidates,
                         /* out */ size_t *count);
```

### 4.2 全局表

```c
/* src/slurmbrokerd/broker_job.c */

xhash_t        *g_broker_jobs;        /* key=trace_id (string), value=broker_job_t* */
list_t         *g_broker_jobs_list;   /* ordered iteration; does NOT free */
pthread_mutex_t g_broker_jobs_lock;   /* guards both above */
```

锁分层规则（已编码到 header 注释里）：

- `g_broker_jobs_lock` 保护**表结构**（增删元素），不保护元素内字段；
- 修改单条 `broker_job_t` 字段必须先释放表锁，再持 `job->lock`；
- 长操作（egress RPC、stage、rewrite）**不允许**持任何锁。

### 4.3 持久化（JSONL，单行一条）

`persist.c` 实现三文件原子 rename：

```
/var/spool/slurm/broker/broker_state.jsonl       # 当前
/var/spool/slurm/broker/broker_state.jsonl.tmp   # 写入中
/var/spool/slurm/broker/broker_state.jsonl.old   # 上一份
```

写入流程：

```
1. 全表 foreach -> broker_job_to_json (xstrdup 一行)
2. fwrite + fflush + fsync(state.tmp)
3. rename(state, state.old)
4. rename(state.tmp, state)
```

调度策略：

- **30 秒周期**：`persist_thread` 用 `pthread_cond_timedwait`；
- **事件触发**：任何 transition / handler / `state_machine_resume_inflight` 都可调 `persist_async_request()` 提前唤醒；
- **shutdown 兜底**：`main()` 在调用 `broker_fini()` 前显式跑一次 `broker_state_save()`，保证最终态落盘。

JSON schema (v2.0 实际样本):

```json
{"trace_id":"xian-12345","src_job_id":12345,"remote_job_id":8888,
 "role":0,"hop_count":0,
 "src_user_name":"test1","src_uid":20001,
 "remote_user_name":"wz_test1","remote_uid":20001,"remote_gid":20001,
 "src_cluster":"xian_cluster","src_partition":"xahcnormal",
 "dst_cluster":"wz_cluster","target_partition":"wznormal",
 "selected_route_id":"route_xahc_to_wz",
 "init_phase":2,
 "route_candidates":["route_xahc_to_wz","route_xahc_to_bj"],
 "route_attempted_mask":1,
 "route_current_idx":0,
 "src_work_dir":"/work/home/test1/case1",
 "dst_work_dir":"/work/home/wz_test1/.burst/xian_cluster/12345",
 "script_path":"/work/home/test1/case1/run.sh",
 "app_name":"lammps-2Aug2023-intelmpi2018",
 "state":4,"state_reason":"","retry_count":0,
 "state_enter_time":1715000000,"submit_time":1715000005,"last_poll_time":1715000060,
 "remote_start_time":1715000010,"remote_end_time":0,
 "remote_alloc_tres":"cpu=32,mem=128G,node=1","remote_exit_code":0,
 "cancel_requested":false,"cancel_propagated":false}
```

> **v1.5 → v2.0 JSON 兼容性**: v2.0 新增 5 个字段 (`src_partition` / `selected_route_id` / `init_phase` / `route_candidates` / `route_attempted_mask` / `route_current_idx`), 删除 1 个字段 (`account`)。 broker 启动 resume 时, 旧 v1.5 jsonl 缺少新字段, `broker_job_from_json()` 给默认值 (空数组 / 0 / NULL), 然后 `state_machine_resume_inflight()` 把 `init_phase` 设为 `SELECTED` (假设 v1.5 已选定路由, 因 v1.5 ctld 提前在 forward_job_msg 里填了 target_cluster/target_partition)。

### 4.4 重启重接力（`state_machine_resume_inflight`）

旧版没有这个机制。新增动机：

> 重启后所有恢复条目继承**旧的 `state_enter_time`**。如果某条目正卡在 `STAGING_IN`，state_machine 的超时是默认 12 分钟（`stage_timeout`），用户感知的 broker 重启延迟会非常糟糕。

实现：`broker_init()` 在 `state_machine_start()` 之后、`listener_start()` 之前调一次 `state_machine_resume_inflight()`，将所有 `STAGING_IN / STAGED_IN / STAGING_OUT` 任务的 `state_enter_time` 拨到"已过期到只剩 1s"，让下一次 tick 立刻走重试路径。`retry_count` 不重置（已超 3 次的就让它转 FAILED）。

---

## 五、状态机 (★ v2.0 INIT 阶段大改造)

### 5.1 状态跃迁表 (v2.0)

| 状态 | 入口动作 | 推进方式 | 失败处理 | 超时 (默认) |
|---|---|---|---|---|
| INIT | `broker_job_table_add` + persist | **★ v2.0**: `_on_init` 内嵌四个子阶段 (DECIDING → PROBING → SELECTED → STAGING_IN); 详见 §5.5 | 全部候选探测失败 → FAILED (state_reason="ALL_ROUTES_EXHAUSTED") | INIT 总超时 90s (DECIDING 1s + PROBING 80s + SELECTED → 立即进 STAGING_IN) |
| STAGING_IN | `stage_submit_in` 把任务塞 stage 队列 | stage worker 完成回调 `state_machine_transition(STAGED_IN)` | 重试 ≤ 3 → 否则 FAILED | `du -sb` × `StageTimeoutPerGB` + 600s |
| STAGED_IN | originator 已 8012；receiver 已收到准备 sbatch | originator: `egress_staged_in_async` 收到 8013 → SUBMITTED；receiver: `rewrite_job_script` + `sudo sbatch` → SUBMITTED | 重试 ≤ 3 → FAILED | 30s |
| SUBMITTED | 记录 `remote_job_id` | sync_ticker 拉到 RUNNING | 24h 仍 PENDING → FAILED | 24h |
| RUNNING | — | sync_ticker 拉到终态 | NODE_FAIL 一次重试 | `time_limit + 30 min`（暂未启用，依赖 ctld 字段） |
| STAGING_OUT | `stage_submit_out` 入队 | stage worker 完成回调 → COMPLETED | 重试 ≤ 3 → FAILED（远端目录保留） | 同 STAGING_IN |
| COMPLETED | `ctld_inject_terminal_state` + 出表 | — | — | — |
| FAILED | 同上 | — | — | — |
| CANCELLED | originator: `egress_cancel_async` + `ctld_inject_terminal_state` + 出表；receiver: `slurm_kill_job` + 出表 | — | — | — |

### 5.2 两阶段 tick 模型 (v2.0, INIT 阶段加 PROBING 子阶段)

```
state_machine_tick():
    -- Phase 1: 持 g_broker_jobs_lock --
    foreach job in table:
        if cancel_requested && !cancel_propagated && state ∉ terminal:
            enqueue SM_ACTION_CANCEL(trace_id)
        else switch (state):
            case INIT:
                switch (job->init_phase):
                    case DECIDING:    enqueue SM_ACTION_ROUTE_DECIDE     ★ v2.0 新
                    case PROBING:     enqueue SM_ACTION_TEST_ONLY_NEXT   ★ v2.0 新
                    case SELECTED:    enqueue SM_ACTION_INIT_TO_STAGE_IN
                    case EXHAUSTED:   内联 transition FAILED + state_reason="ALL_ROUTES_EXHAUSTED" ★ v2.0 新
            case STAGING_IN:  if timeout -> 内联 transition retry/FAILED
            case STAGED_IN:   if timeout -> 内联 transition retry/FAILED
            case SUBMITTED:   if 24h -> 内联 transition FAILED
            case STAGING_OUT: 同 STAGING_IN
            case COMPLETED|FAILED|CANCELLED:
                enqueue SM_ACTION_INJECT_TERMINAL(trace_id)
                enqueue SM_REMOVE(trace_id)
    -- Phase 2: 释锁，逐个执行 enqueued action --
    drain action queue:
        - ROUTE_DECIDE        ★ v2.0: route_decide() + cap_check() -> 填 route_candidates[] -> init_phase=PROBING
        - TEST_ONLY_NEXT       ★ v2.0: 取下一未试候选 -> egress::egress_test_only_sync (8018, 5s 超时) -> 成功 init_phase=SELECTED, 失败置 mask 下轮再试或 EXHAUSTED
        - INIT_TO_STAGE_IN     ★ v2.0: stage_submit_in + transition STAGING_IN + 立即调 ctld_update_remote_state 首次包
        - CANCEL              -> egress_cancel_async + transition CANCELLED
        - INJECT_TERMINAL     -> egress::ctld_inject_terminal_state
    drain remove queue:
        - broker_job_table_remove(trace_id)
```

### 5.3 状态推进点 (v2.0 INIT 内部拆 4 步)

| 推进点 | 触发方 | 周期/事件 |
|---|---|---|
| **INIT.DECIDING → INIT.PROBING** ★ v2.0 | state_machine Phase2 `SM_ACTION_ROUTE_DECIDE` | 1s tick |
| **INIT.PROBING → INIT.SELECTED** ★ v2.0 | state_machine Phase2 `SM_ACTION_TEST_ONLY_NEXT` 拿到 OK | 1s tick (每次一个候选, 串行) |
| **INIT.PROBING → INIT.EXHAUSTED** ★ v2.0 | 所有 `route_candidates[]` 探测均失败 | 1s tick |
| **INIT.SELECTED → STAGING_IN** ★ v2.0 | state_machine Phase2 `SM_ACTION_INIT_TO_STAGE_IN` + 同步首次 `ctld_update_remote_state` (带 selected_route) | 1s tick |
| STAGING_IN → STAGED_IN | stage worker 完成回调（exit=0） | 事件 |
| STAGED_IN → SUBMITTED | originator: 收 8013；receiver: `sudo sbatch` 成功 | 事件 |
| SUBMITTED → RUNNING | sync_ticker `_apply_remote_status` 拉到 JOB_RUNNING | 10s 周期 |
| RUNNING → STAGING_OUT | sync_ticker 拉到终态 → `stage_submit_out` | 10s 周期 |
| STAGING_OUT → COMPLETED | stage worker 完成回调 | 事件 |
| 任意 → CANCELLED | `cancel_requested` + 下一 tick | 1s 周期 |
| 任意 → FAILED | 各类超时 / 错误响应 (含 INIT.EXHAUSTED) | 混合 |

### 5.4 `state_machine_transition` API

任何线程都可以调；内部行为：

1. 持 `job->lock`；
2. `job->state == to` 时直接返回（幂等）；
3. 更新 `state_enter_time = time(NULL)`；
4. 替换 `state_reason`（NULL 清空）；
5. `persist_async_request()` 提示 checkpoint 线程；
6. `info()` 一行日志便于 grep。

> 历史 M07 直接使用的 `broker_job_set_state()` 是简化版（只改字段不持 persist hint）；M09 后约定**新代码统一用 `state_machine_transition`**。

### 5.5 INIT 阶段四子状态详细流程 (★ v2.0 新增章节)

```
INIT.DECIDING (作业刚入表)
   │
   │ Phase2 SM_ACTION_ROUTE_DECIDE:
   │   1. route_decide(src_cluster, src_partition, app_name, &candidates, &count)
   │      读 routes.conf, 匹配所有 LocalCluster=src_cluster && LocalPartition=src_partition
   │      && (AllowApps 不配置 OR app_name ∈ AllowApps) 的 [Route] 段
   │   2. cap_check(candidates[]): 按 MaxInflight / RemoteMaxInflight 过滤
   │   3. 按 Priority 排序后写入 job->route_candidates[] + count
   │   4. 若 count==0: ctld_forward_resp(rc=5010 NO_VIABLE_ROUTE) → 同步退表
   │      若全被 cap 过滤: ctld_forward_resp(rc=5020 CAP_FULL_SOFT_WAIT) → 同步退表
   │   5. 否则: job->init_phase = PROBING, job->route_current_idx = 0
   │
   ▼
INIT.PROBING (逐个 test-only 探测)
   │
   │ Phase2 SM_ACTION_TEST_ONLY_NEXT:
   │   1. idx = job->route_current_idx
   │   2. route = job->route_candidates[idx]
   │   3. test-only 探测:
   │      send REQUEST_BROKER_TEST_ONLY (8018) → 远端 broker, 携带:
   │         job_id, src_user_name, remote_user_name (查表),
   │         remote_partition=route.remote_partition,
   │         tres_req_str, time_limit, app_name, ...
   │      远端 broker 内部调本地 ctld 的 SUBMIT_BATCH_JOB(test_only=true)
   │      远端 ctld 真实做资源/ACL/QOS/partition 校验, 返回 OK/REJECTED
   │      5s 超时
   │   4. 结果:
   │      ─ OK → job->selected_route_id = route.route_id;
   │             job->dst_cluster = route.remote_cluster;
   │             job->target_partition = route.remote_partition;
   │             job->init_phase = SELECTED  ★ 进入下一子阶段
   │      ─ REJECTED / TIMEOUT → 置位 route_attempted_mask 第 idx 位:
   │             若 mask 未覆盖所有 count → job->route_current_idx++ (mod count)
   │             若 mask 已全覆盖 → job->init_phase = EXHAUSTED
   │
   ▼
INIT.SELECTED → STAGING_IN
   │
   │ Phase2 SM_ACTION_INIT_TO_STAGE_IN:
   │   1. egress::ctld_update_remote_state(8003) 首次包:
   │        remote_cluster_name = job->dst_cluster
   │        remote_partition_name = job->target_partition
   │        remote_state = JOB_PENDING
   │      (★ 让 ctld 写入 cd_remote_cluster_name / cd_remote_partition_name)
   │   2. stage_submit_in(job)
   │   3. state_machine_transition(STAGING_IN)
   │
   ▼
(后续与 v1.5 一致: STAGING_IN → STAGED_IN → SUBMITTED → RUNNING → ...)
```

#### 5.5.1 test-only 探测的特殊处理

| 场景 | 处理 |
|---|---|
| 单个候选 timeout (5s) | 视为失败 (mask 置位), 立即试下一个 — 远端 broker 可能掉线 |
| 单个候选 REJECTED (远端 ctld 资源不足/QOS 限制等) | 视为失败, 立即试下一个; state_reason 记录最近一次错误码 |
| 全部候选探测累计耗时 > 80s | INIT 阶段总超时, 转 FAILED, state_reason="INIT_TIMEOUT" (理论上 5s × 8 = 40s 足够, 但留 80s 余量) |
| broker 重启时作业卡在 PROBING | `state_machine_resume_inflight()` 检查 `route_attempted_mask`, 从 `(route_current_idx + 1) % count` 继续 (避免重试已失败的候选) |
| 探测时发现 `routes.conf` 已被 SIGHUP reload 且原 `route_candidates[]` 中某些 id 已不存在 | 跳过不存在的 id, 仅试仍存在的 |

#### 5.5.2 探测结果与 ctld 同步错误码映射

```c
/* src/slurmbrokerd/route.c::route_test_only_probe() 返回码 → ctld 错误码 */
switch (probe_rc) {
case PROBE_OK:                  return SLURM_SUCCESS;
case PROBE_TIMEOUT:             return BROKERD_ERR_TEST_ONLY_TIMEOUT;     /* 9012 */
case PROBE_REJECTED_RESOURCE:   /* 远端节点不足等 */
case PROBE_REJECTED_ACL:        /* 远端 ctld AllowAccounts 拒绝 */
case PROBE_REJECTED_QOS:        /* 远端 QOS 限制 */
    return BROKERD_ERR_TEST_ONLY_REJECTED;                                /* 9011 */
default:                        return BROKERD_ERR_TEST_ONLY_REJECTED;
}
/* 调用方 (state_machine Phase2) 看到 ALL_PROBES_FAILED 后:
 *   ctld_forward_resp(rc=BROKERD_ERR_ALL_ROUTES_EXHAUSTED=9013)
 *   ctld 收到 9013 → 置 cd_route_exhausted=1
 */
```

---

## 六、RPC 协议

### 6.1 双链路一览

| 链路 | 监听端口 | 帧格式 | 发送 API | 接收 API |
|---|---|---|---|---|
| ctld ↔ broker（loopback） | 8442 | **Slurm 原生 frame** | ctld 用 `slurm_send_recv_controller_rc_msg`；broker 用 `slurm_send_node_msg`/`slurm_send_rc_msg` 回 | broker `listener.c::_handle_ctld_conn` 调 `slurm_receive_msg` |
| broker ↔ broker（公网/专网） | 8443 | **broker 私有 wire frame**（`'BRKR'` 魔数 + auth_g_pack + payload） | `proto.c::proto_send_recv_to_peer` / `proto_send_to_peer` | `listener.c::_handle_peer_conn` 调 `brokerd_wire_parse` |

### 6.2 私有 wire frame 详解

```
+---------------+--------------+--------------+----------------+----------------+
|  uint32 magic | uint16 pv    | uint16 mtype | auth_g_pack    | payload pack   |
|  'BRKR'       | proto ver    | 8010..8017   | (Munge cred)   | (per-msg)      |
+---------------+--------------+--------------+----------------+----------------+
```

- `magic = BROKERD_WIRE_MAGIC = 0x524B5242`（`'BRKR'`），任何不以此打头的字节流被丢弃；
- `pv` 当前用 `SLURM_PROTOCOL_VERSION`，未来 broker 可独立演进版本号；
- `auth_g_*` 系列函数（`auth_g_create`/`pack`/`unpack`/`verify`）走 Slurm 自带的 Munge 插件，**不依赖**对端 ctld；
- payload 用 Slurm 的原子 pack 原语（`pack32` / `packstr` / `pack_time` / `packbool`）按字段顺序写。

为什么不用原生 `slurm_pack_msg`？因为它要扩展 `slurm_protocol_pack.c`，违反强约束。私有 frame 把广义"借 Slurm 协议骨架"压缩成"只借 socket / auth / pack 原语"。

### 6.3 消息类型表 (v2.0 共 13 个, ★ 新增 8018/8019)

| msg_type | 名称 | 方向 | 标记 | 落地位置 |
|---|---|---|---|---|
| 8001 | `BROKERD_REQUEST_FORWARD_JOB` | ctld → broker | LEGACY_M04_TRANSITIONAL | `proto_pack.c::_pack_forward_job_msg`; ★ v2.0 payload 瘦身 |
| 8002 | `BROKERD_RESPONSE_FORWARD_JOB` | broker → ctld | LEGACY_M04_TRANSITIONAL | 同上 |
| 8003 | `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` | broker → ctld | LEGACY_M04_TRANSITIONAL | `proto_pack.c::_pack_broker_remote_state_msg`; ★ v2.0 首次包必带 remote_cluster_name / remote_partition_name |
| 8004 | `BROKERD_REQUEST_BROKER_TERMINAL_STATE` | broker → ctld | LEGACY_M04_TRANSITIONAL | `proto_pack.c::_pack_broker_terminal_state_msg` |
| 8010 | `BROKERD_REQUEST_BROKER_FORWARD_JOB` | broker → broker | PERMANENT | `proto_pack.c::_pack_broker_forward_job_msg` |
| 8011 | `BROKERD_RESPONSE_BROKER_ACK` | broker → broker | PERMANENT | 同上 |
| 8012 | `BROKERD_REQUEST_BROKER_STAGED_IN` | broker → broker | PERMANENT | 同上 |
| 8013 | `BROKERD_RESPONSE_BROKER_SUBMITTED` | broker → broker | PERMANENT | 同上 |
| 8014 | `BROKERD_REQUEST_BROKER_QUERY_STATUS` | broker → broker | PERMANENT | 同上 |
| 8015 | `BROKERD_RESPONSE_BROKER_STATUS` | broker → broker | PERMANENT | 同上 |
| 8016 | `BROKERD_REQUEST_BROKER_CANCEL` | ctld → broker / broker → broker (dual-purpose) | PERMANENT | 同上 |
| 8017 | `BROKERD_REQUEST_BROKER_CLEANUP` | broker → broker | PERMANENT | 同上 |
| **8018** ★ v2.0 新 | `BROKERD_REQUEST_BROKER_TEST_ONLY` | broker → broker | LEGACY_M04_TRANSITIONAL | `proto_pack.c::_pack_test_only_msg` |
| **8019** ★ v2.0 新 | `BROKERD_RESPONSE_BROKER_TEST_ONLY` | broker → broker | LEGACY_M04_TRANSITIONAL | 同上 |

#### 6.3.1 `BROKERD_REQUEST_FORWARD_JOB` (8001) payload v2.0 瘦身

```c
/* src/slurmbrokerd/proto.h */
typedef struct {
    /* 身份信息 (v1.5 已有) */
    uint32_t  src_job_id;
    uint32_t  src_uid;
    uint32_t  src_gid;
    char     *src_user_name;

    /* ★ v2.0 新增: 用于 broker route_decide() 查 routes.conf */
    char     *src_cluster_name;       /* 取代 v1.5 target_cluster */
    char     *src_partition;          /* 取代 v1.5 target_partition */

    /* 输入文件位置 (v1.5 已有) */
    char     *src_work_dir;
    char     *script_path;

    /* 应用标识 (v1.5 已有, v2.0 字段名 cd_app_name) */
    char     *cd_app_name;            /* 取代 v1.5 app_name */

    /* v2.0 已删:
     *   char *target_cluster;
     *   char *target_partition;
     *   char *account;
     *   job_desc_msg_t *job_desc;
     */
} brokerd_forward_job_msg_t;
```

#### 6.3.2 `BROKERD_REQUEST_BROKER_TEST_ONLY` (8018) payload (★ v2.0 新增)

```c
/* src/slurmbrokerd/proto.h, v2.0 新增 */
typedef struct {
    char     trace_id[BROKER_TRACE_ID_LEN];
    uint32_t src_uid;
    char    *src_user_name;
    uint32_t remote_uid;
    char    *remote_user_name;        /* 由 originator 端 LocalUser= 查表 */
    char    *remote_partition;        /* 候选远端 partition 名 */
    char    *cd_app_name;              /* 远端 broker 据此查 routes.conf::AllowApps 验证 */

    /* job_desc 关键字段抽取 (避免传完整 job_desc_msg_t, 保持版本解耦) */
    uint32_t  num_tasks;
    uint32_t  cpus_per_task;
    uint64_t  pn_min_memory;
    uint32_t  time_limit_min;
    uint32_t  min_nodes;
    uint32_t  max_nodes;
    char     *gres_per_node;
    char     *qos;
    char     *tres_per_task;
} brokerd_test_only_msg_t;
```

#### 6.3.3 `BROKERD_RESPONSE_BROKER_TEST_ONLY` (8019) payload (★ v2.0 新增)

```c
typedef struct {
    char     trace_id[BROKER_TRACE_ID_LEN];
    uint16_t  result;                 /* 0=OK, 1=REJECTED, 2=TIMEOUT (远端 broker 自定) */
    uint32_t  reject_reason_code;     /* 远端 ctld 返回的 ESLURM_*; 0=N/A */
    char     *reject_reason_text;
} brokerd_test_only_resp_msg_t;
```

#### 6.3.4 `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` (8003) payload (★ v2.0 微调)

```c
/* v2.0 新增 first_pack 语义约束: 首次包必须填 remote_cluster_name + remote_partition_name */
typedef struct {
    uint32_t  src_job_id;
    char     *trace_id;
    char     *remote_cluster_name;     /* ★ v2.0: 首次包必填; 后续包可重复填 (幂等) */
    char     *remote_partition_name;   /* ★ v2.0: 同上 */
    uint32_t  remote_job_id;
    uint32_t  remote_state;
    char     *remote_alloc_tres;
    time_t    remote_start_time;
} brokerd_remote_state_msg_t;
```

### 6.4 错误码表 (broker 私有 9001-9099, v2.0 扩展)

| 代码 | 名称 | 含义 |
|---|---|---|
| 9001 | `BROKERD_ERR_OVERLOAD` | 在途数 ≥ `MaxInFlightJobs` (全局) |
| 9002 | `BROKERD_ERR_NO_USER_MAPPING` | `user_mapping_lookup` 返回 NULL |
| 9003 | `BROKERD_ERR_USER_MAPPING_MISMATCH` | receiver 反向校验 `remote_user` 不一致 |
| 9004 | `BROKERD_ERR_HOP_EXCEEDED` | `hop_count > 0` (MVP 严格 ≤ 1) |
| 9005 | `BROKERD_ERR_LOOKUP_FAILED` | `lookup_software.sh` 非 0 退出或路径非 `/` 开头 |
| 9006 | `BROKERD_ERR_LOOKUP_TIMEOUT` | `lookup_software.sh` 超时 |
| 9007 | `BROKERD_ERR_STAGE_FAILED` | rsync 子进程失败 |
| 9008 | `BROKERD_ERR_REMOTE_SUBMIT_FAILED` | receiver 端 sbatch 解析失败或退出非 0 |
| 9009 | `BROKERD_ERR_NOT_FOUND` | trace_id 在表中不存在 (cancel/cleanup 幂等返回成功) |
| **9010** ★ v2.0 新 | `BROKERD_ERR_NO_VIABLE_ROUTE` | `routes.conf` 内没有匹配 `(src_cluster, src_partition)` 的 [Route] 段, 或 AllowApps 拦截 |
| **9011** ★ v2.0 新 | `BROKERD_ERR_TEST_ONLY_REJECTED` | 远端 ctld 在 `submit_batch_job(test_only=1)` 阶段拒绝 (资源/ACL/QOS); broker 据此判断单个候选不可用 |
| **9012** ★ v2.0 新 | `BROKERD_ERR_TEST_ONLY_TIMEOUT` | test-only 探测 5s 超时; 视为单个候选不可用, 继续试下一个 |
| **9013** ★ v2.0 新 | `BROKERD_ERR_ALL_ROUTES_EXHAUSTED` | 所有 `route_candidates[]` 探测均失败; broker 同步回 ctld 此码, ctld 置 `cd_route_exhausted=1` |
| **9020** ★ v2.0 新 | `BROKERD_ERR_CAP_FULL_SOFT_WAIT` | 所有候选路由的 `MaxInflight` / `RemoteMaxInflight` 全部命中容量; **临时性**, ctld 收到后**不**置 `cd_route_exhausted`, 下轮扫描重试 |

`proto.c::brokerd_strerror()` 将上述映射到中英文双语字符串便于日志阅读。

#### 6.4.1 错误码 → ctld 错误码映射

```c
/* src/slurmbrokerd/handler_ctld.c::_brokerd_err_to_ctld_err() */
switch (brokerd_rc) {
case 0:                                    return SLURM_SUCCESS;
case BROKERD_ERR_OVERLOAD:                 return ESLURM_CR_BROKER_REJECTED;       /* 5002 */
case BROKERD_ERR_NO_USER_MAPPING:          return ESLURM_CR_USER_NO_MAPPING;        /* 5003 */
case BROKERD_ERR_LOOKUP_FAILED:
case BROKERD_ERR_LOOKUP_TIMEOUT:           return ESLURM_CR_LOOKUP_SOFTWARE_FAILED; /* 5007 */
case BROKERD_ERR_STAGE_FAILED:
case BROKERD_ERR_REMOTE_SUBMIT_FAILED:     return ESLURM_CR_REMOTE_SUBMIT_FAILED;   /* 5008 */
case BROKERD_ERR_NO_VIABLE_ROUTE:          return ESLURM_CR_NO_VIABLE_ROUTE;        /* 5010 */
case BROKERD_ERR_TEST_ONLY_REJECTED:       return ESLURM_CR_TEST_ONLY_REJECTED;     /* 5011 */
case BROKERD_ERR_TEST_ONLY_TIMEOUT:        return ESLURM_CR_TEST_ONLY_TIMEOUT;      /* 5012 */
case BROKERD_ERR_ALL_ROUTES_EXHAUSTED:     return ESLURM_CR_ALL_ROUTES_EXHAUSTED;   /* 5013 */
case BROKERD_ERR_CAP_FULL_SOFT_WAIT:       return ESLURM_CR_CAP_FULL_SOFT_WAIT;     /* 5020 */
default:                                    return ESLURM_CR_BROKER_REJECTED;
}
```

### 6.5 `LEGACY_M04_TRANSITIONAL` 与未来清理

由于强约束不允许动 `src/common/`，4 个 ctld↔broker 的 RPC（8001 / 8002 / 8003 / 8004）当前由 broker 这边**临时实现**一份 packing 代码，标 `LEGACY_M04_TRANSITIONAL`。当 ctld 工程师在 `src/common/slurm_protocol_defs.h` + `src/common/slurm_protocol_pack.c` 注册等同结构和 msg_type 之后，broker 这边将做一个**纯机械删除 PR**：

- 删 `proto.h` 中 4 个 `BROKERD_REQUEST_*` 宏 + 4 个 payload 结构 + 4 个 free 声明
- 删 `proto_pack.c` 中 4 对 `_pack/_unpack_*_msg` + dispatcher 4 个 case
- 删 `proto.c` 中 4 个 `brokerd_free_*` 实现
- `handler_ctld.c` 改用 `slurm_msg_t.data` 直接读原生结构

当前共 27 处 `LEGACY_M04_TRANSITIONAL` 标记，全文 grep 即可枚举。

### 6.6 `egress` 出站 API (`egress.h`, v2.0 扩展)

| 函数 | 方向 | 实现方式 | 调用方 |
|---|---|---|---|
| `egress_forward_async(job)` | broker→broker 8010 | `proto_send_recv_to_peer` + 内嵌 `_retry_n_times` 指数退避 | `state_machine._on_selected` (★ v2.0: 由 SELECTED 子态触发, 不再由 INIT 直接触发) |
| `egress_staged_in_async(job)` | broker→broker 8012 | 同上 | stage worker 完成回调 |
| `egress_query_status_sync(ids,n,&resp)` | broker→broker 8014 | 同步阻塞 | `sync_ticker` |
| `egress_cancel_async(job)` | broker→broker 8016 | `proto_send_to_peer` (fire-and-forget, SO_LINGER) | `state_machine` cancel 优先级 |
| `egress_cleanup_async(trace_id)` | broker→broker 8017 | 同上 | 当前未启用 (M14 废弃) |
| **★ v2.0 新** `egress_test_only_async(job, candidate)` | broker→broker 8018 | `proto_send_recv_to_peer` 异步 + 回调 (5s 超时) | `state_machine._on_probing` |
| `ctld_update_remote_state(job)` | broker→ctld 8003 | `slurm_send_recv_controller_rc_msg` | `sync_ticker._apply_remote_status` (★ v2.0: 首次包必带 remote_cluster_name/remote_partition_name) |
| `ctld_inject_terminal_state(job)` | broker→ctld 8004 | 同上 | `state_machine` 终态 |

#### 6.6.1 `egress_test_only_async` 关键实现要点 (★ v2.0 新增)

```c
/* src/slurmbrokerd/egress.c */
int egress_test_only_async(broker_job_t *job, route_candidate_t *cand,
                            test_only_callback_t cb)
{
    brokerd_test_only_msg_t req = {0};
    strncpy(req.trace_id, job->trace_id, BROKER_TRACE_ID_LEN);
    req.src_uid          = job->src_uid;
    req.src_user_name    = job->src_user_name;
    req.remote_uid       = cand->remote_uid;          /* 由 user_mapping 解析 */
    req.remote_user_name = cand->remote_user_name;
    req.remote_partition = cand->remote_partition;
    req.cd_app_name      = job->cd_app_name;
    req.num_tasks        = job->req_num_tasks;
    req.cpus_per_task    = job->req_cpus_per_task;
    req.pn_min_memory    = job->req_pn_min_memory;
    req.time_limit_min   = job->req_time_limit_min;
    req.min_nodes        = job->req_min_nodes;
    req.max_nodes        = job->req_max_nodes;
    req.gres_per_node    = job->req_gres_per_node;
    req.qos              = job->req_qos;
    req.tres_per_task    = job->req_tres_per_task;

    /* 投递至远端 broker, 5s 超时, 回调内 callback 触发状态机 retry/advance */
    return proto_send_async_with_cb(cand->target_broker_addr,
                                     BROKERD_REQUEST_BROKER_TEST_ONLY,
                                     &req,
                                     cb, job,
                                     TEST_ONLY_TIMEOUT_MS);
}
```

#### 6.6.2 远端 broker 收到 `BROKERD_REQUEST_BROKER_TEST_ONLY` 的处理流程 (★ v2.0 新增)

```c
/* src/slurmbrokerd/handler_peer.c */
static int _handle_test_only(slurm_msg_t *msg)
{
    brokerd_test_only_msg_t *req = msg->data;
    brokerd_test_only_resp_msg_t resp = { .trace_id = req->trace_id };

    /* Step 1: 反向 user_mapping 校验 */
    if (!user_mapping_match(req->src_uid, req->remote_uid)) {
        resp.result = 1;
        resp.reject_reason_code = ESLURM_INVALID_ACCOUNT;  /* 借用 ctld 错误码 */
        resp.reject_reason_text = "user_mapping_mismatch";
        goto reply;
    }

    /* Step 2: 构造一个最小 job_desc_msg_t 调本端 ctld submit_batch_job(test_only=1) */
    job_desc_msg_t desc = {0};
    desc.partition       = req->remote_partition;
    desc.user_id         = req->remote_uid;
    desc.num_tasks       = req->num_tasks;
    desc.cpus_per_task   = req->cpus_per_task;
    desc.pn_min_memory   = req->pn_min_memory;
    desc.time_limit      = req->time_limit_min;
    desc.min_nodes       = req->min_nodes;
    desc.max_nodes       = req->max_nodes;
    desc.gres_per_node   = req->gres_per_node;
    desc.qos             = req->qos;
    desc.script          = "#!/bin/sh\n# test-only probe\n";

    /* test_only 路径不真正落地 job_record, 仅触发 schedule plugin 预检 */
    submit_response_msg_t *sub_resp = NULL;
    int rc = slurm_submit_batch_job_test_only(&desc, &sub_resp);
    if (rc == SLURM_SUCCESS) {
        resp.result = 0;
    } else {
        resp.result = 1;
        resp.reject_reason_code = slurm_get_errno();
        resp.reject_reason_text = (char *) slurm_strerror(rc);
    }

reply:
    return proto_reply(msg, BROKERD_RESPONSE_BROKER_TEST_ONLY, &resp);
}
```

> 关键约束: `submit_batch_job_test_only` 是远端 ctld 已有的 `submit_batch_job(... test_only=1, hetjob=0 ...)` 路径, 不会持久化 job_record, 不会进度调度队列, **必须在 5s 内返回** 否则视为容量不足。

### 6.7 sync_ticker 三阶段批量查询（M13）

```
sync_ticker_tick():
    -- Phase 1: 持锁 + foreach 收集 trace_id --
    list = []
    foreach job: if role==ORIG && state ∈ {SUBMITTED,RUNNING}: list += trace_id

    -- Phase 2: 释锁 + 单次 RPC --
    rc = egress_query_status_sync(list, n, &resp)
    if rc != SUCCESS: warning(); 累计 consecutive_failures, 超 PollMaxRetries 升 error
    if rc == SUCCESS: consecutive_failures = 0

    -- Phase 3: 释锁 + 逐条 apply --
    for entry in resp.entries:
        job = broker_job_table_get(entry.trace_id)
        with job->lock:
            switch (entry.remote_state):
                JOB_PENDING:  ctld_update_remote_state
                JOB_RUNNING:  set remote_start_time/alloc_tres
                              transition RUNNING + ctld_update_remote_state
                terminal*:    set remote_end_time/exit_code
                              if CANCELLED: transition CANCELLED
                              else:        stage_submit_out + transition STAGING_OUT
```

### 6.8 broker 状态机 → Slurm `remote_state` 映射

| broker 状态 | 推送的 `remote_state` | 推送时机 |
|---|---|---|
| INIT / STAGING_IN | 不推送 | broker 内部 |
| STAGED_IN / SUBMITTED | `JOB_PENDING` | sync_ticker 首次拿到 `remote_job_id` |
| RUNNING | `JOB_RUNNING` | sync_ticker 检测远端 RUNNING |
| STAGING_OUT | `JOB_RUNNING` | 同上（不变更） |
| COMPLETED | `JOB_COMPLETE` | TERMINAL_STATE |
| FAILED | `JOB_FAILED` / `JOB_TIMEOUT` / `JOB_NODE_FAIL` 等（沿用 sync_ticker 中的细分映射） | TERMINAL_STATE |
| CANCELLED | `JOB_CANCELLED` | TERMINAL_STATE |

---

## 七、用户场景（落地版）

### 7.1 US-1 `sbatch` 提交跨域作业 (★ v2.0 originator 流程变化最大)

#### A. originator handler (`handler_ctld.c::handle_forward_job`, ★ v2.0)

```c
/* v2.0 注意: ctld 发来的 forward_job 已不带 target_cluster / target_partition / job_desc */
1. brokerd_wire_parse_v2 后的 brokerd_forward_job_msg_t (★ 瘦身后字段)
   必带: src_job_id, src_uid, src_user_name, src_cluster_name,
         src_partition, src_work_dir, script_path, cd_app_name
2. 限流: count_inflight() >= MaxInFlightJobs -> reply BROKERD_ERR_OVERLOAD (9001)
3. broker_job_create + 填 ORIGINATOR 字段:
     job->src_uid           = req->src_uid
     job->src_user_name     = req->src_user_name
     job->src_cluster_name  = req->src_cluster_name
     job->src_partition     = req->src_partition       /* ★ v2.0 新增字段 */
     job->cd_app_name       = req->cd_app_name
     job->script_path       = req->script_path
     job->src_work_dir      = req->src_work_dir
     job->init_phase        = INIT_PHASE_DECIDING       /* ★ v2.0 新增: 进入 DECIDING */
     job->state             = BROKER_JOB_STATE_INIT
4. broker_job_table_add (内部幂等; 已存在则 reply 现状即可)
5. persist_async_request
6. reply RESPONSE_FORWARD_JOB { error_code=0, trace_id }
7. state_machine 下一 tick 接管:
   INIT.DECIDING -> route_decide() -> cap_check() -> INIT.PROBING (8018 test-only)
   -> INIT.SELECTED -> STAGING_IN -> ...
8. 若 init 阶段任何一步耗尽候选 (BROKERD_ERR_ALL_ROUTES_EXHAUSTED 等), state_machine
   走 _on_init_exhausted: 通过 8002 (异步) 通知 ctld? -- v2.0 实际通过 trace_id 反查:
   - 此时 ORIGINATOR 已 reply 过 8002 OK, ctld 端 job 处于"已转发"等待 8003 状态
   - broker 走 ctld_inject_terminal_state(8004) 携带 BROKERD_ERR_ALL_ROUTES_EXHAUSTED
     原因码, ctld 端 cross_region_rpc.c::handle_broker_terminal_state 据此调
     cd_revert_forward_hard() 置 cd_route_exhausted=1
```

#### A.1 INIT.DECIDING 阶段 originator 内部调用 (★ v2.0 新增子状态)

```c
/* src/slurmbrokerd/state_machine.c::_on_init_deciding */
1. route_decide(job, &candidates, &n)
   - 输入: job->src_cluster_name, job->src_partition, job->cd_app_name
   - 内部: routes_loader 已加载 routes.conf, 用 (Src=, AllowApps=) 过滤
   - 输出: route_candidate_t candidates[]
     每条含: route_id, target_broker_addr, remote_cluster_name,
              remote_partition, remote_uid, remote_user_name (由 user_mapping 解析)
   - 失败: 无候选 -> 返回 BROKERD_ERR_NO_VIABLE_ROUTE -> 调
            _on_init_exhausted("no_viable_route")
2. cap_check(job, candidates, &n)
   - 输入: 候选数组
   - 内部: 逐个查 cap_check 本地表 (Phase 1 检查 RemoteMaxInflight, MaxInflight)
   - 输出: 过滤后的可用候选; 若全部满 -> 返回 BROKERD_ERR_CAP_FULL_SOFT_WAIT
            -> ctld_inject_terminal_state 不调用, 下轮重试 (临时性)
3. job->route_candidates = candidates;
   job->route_current_idx = 0;
   job->route_attempted_mask = 0;
   job->init_phase = INIT_PHASE_PROBING;
4. 转入下一 tick: _on_init_probing
```

#### A.2 INIT.PROBING 阶段 originator 内部调用 (★ v2.0 新增子状态)

```c
/* src/slurmbrokerd/state_machine.c::_on_init_probing */
/* 该子态每次只探一个候选, 异步; 等回调到来后再决策 */
1. cand = &job->route_candidates[job->route_current_idx];
2. egress_test_only_async(job, cand, _test_only_cb)
   - 远端 broker 收到 BROKERD_REQUEST_BROKER_TEST_ONLY(8018)
   - 调远端 ctld 的 submit_batch_job(test_only=1)
   - 5s 内回 BROKERD_RESPONSE_BROKER_TEST_ONLY(8019)
3. 等待回调; 不在 tick 内自旋

/* _test_only_cb 回调上下文中执行 */
4. 标记 job->route_attempted_mask |= (1 << job->route_current_idx);
5. if (resp.result == 0) {
       /* 该候选可用 */
       job->selected_route_id = cand->route_id;
       job->target_broker_addr = cand->target_broker_addr;
       job->remote_cluster_name = cand->remote_cluster_name;
       job->remote_partition    = cand->remote_partition;
       job->remote_uid          = cand->remote_uid;
       job->remote_user_name    = cand->remote_user_name;
       job->init_phase = INIT_PHASE_SELECTED;
   } else if (resp.result == 1 /* REJECTED */ ||
              resp.result == 2 /* TIMEOUT */) {
       /* 推进下一个候选, 不退出 PROBING */
       job->route_current_idx++;
       if (job->route_current_idx >= job->n_route_candidates) {
           job->init_phase = INIT_PHASE_EXHAUSTED;
       }
   }
6. persist_async_request (落 init_phase / route_attempted_mask / route_current_idx)
```

#### A.3 INIT.SELECTED 阶段 originator 内部调用 (★ v2.0 新增子状态)

```c
/* src/slurmbrokerd/state_machine.c::_on_init_selected */
1. egress_forward_async(job): 用 selected target_broker_addr 调 8010
2. 收到 8011 ACK 后:
   - 解析 dst_work_dir
   - cap_inc(job->selected_route_id)          /* 正式占住 RemoteMaxInflight 名额 */
   - transition STAGING_IN
3. 8010 失败时, 区分:
   - BROKERD_ERR_NO_USER_MAPPING / BROKERD_ERR_USER_MAPPING_MISMATCH / 9008
     -> 整体 ALL_ROUTES_EXHAUSTED (硬错), 调 _on_init_exhausted
   - 网络瞬态 / 5xx superficial -> 退回 PROBING 重试下一候选
```

#### A.4 INIT.EXHAUSTED 阶段 originator 内部调用 (★ v2.0 新增终态分支)

```c
/* src/slurmbrokerd/state_machine.c::_on_init_exhausted */
1. cap_dec(job)                              /* 若 SELECTED 阶段已 inc, 此处回滚 */
2. ctld_inject_terminal_state(job,
        terminal_kind = TERMINAL_INIT_EXHAUSTED,
        err_code      = BROKERD_ERR_ALL_ROUTES_EXHAUSTED) /* 8004 */
3. transition FAILED ("all_routes_exhausted")
4. broker_job_table_remove
5. persist_async_request (落 state=FAILED + exhausted_reason)
```

#### B. receiver handler (`handler_remote.c::handle_broker_forward_job`)

```c
/* receiver 端 v2.0 改动较少, 仅消费的字段名调整 */
1. brokerd_wire_parse 后的 brokerd_broker_forward_job_msg_t
   (该消息由 originator 端的 SELECTED 阶段已基于路由结果填好 remote_user_name / remote_partition)
2. hop_count > 0 -> reply BROKERD_ERR_HOP_EXCEEDED
3. user_mapping_lookup(src_user_name, src_cluster); 反向校验 remote_user_name 一致
   失败 -> reply BROKERD_ERR_USER_MAPPING_MISMATCH (9003)
4. 已存在 trace_id -> 幂等 reply current ACK + dst_work_dir
5. broker_job_create + 填 RECEIVER 字段 (含 remote_partition)
6. dst_work_dir = "/work/home/<remote_user>/.burst/<src_cluster>/<src_jobid>"
   _create_dst_work_dir: fork+exec sudo -n -u remote_user /bin/sh -c
                        "mkdir -p \"$1\" && chmod 700 \"$1\""
   失败 -> reply 失败错误码
7. broker_job_table_add + persist_async_request
8. reply RESPONSE_BROKER_ACK { error_code=0, trace_id, dst_work_dir }
```

#### C. stage worker（`stage.c::_run_stage_task`）

```c
1. _du_sb(src_user, src_work_dir)
   -> 子进程 sudo -n -u src_user du -sb，超 max_stage_bytes -> transition FAILED
2. fork+execvp:
   sudo -n -u src_user /usr/bin/rsync -av --partial
        --rsync-path "sudo -n -u remote_user rsync"
        -e "ssh -i <stage_ssh_key> -o BatchMode=yes -l <stage_ssh_user>"
        <src_work_dir>/  <remote_broker_host>:<dst_work_dir>/
   stdout/stderr 重定向到 /var/log/slurm/broker_stage/<trace_id>.log
3. brokerd_waitpid_timeout(STAGE_CHILD_TIMEOUT_S=3600)
4. exit==0 -> state_machine_transition(STAGED_IN) + egress_staged_in_async
   exit!=0 -> 仅日志, 让 state_machine _on_staging_in 走重试/FAILED
```

#### D. receiver `STAGED_IN` 处理 (`handler_remote.c::handle_broker_staged_in`, ★ v2.0 微调)

```c
1. broker_job_table_get(trace_id); 已 SUBMITTED 则幂等 reply
2. rewrite_job_script(job, &modified_path)
   - lookup_software_path(src_cluster, cd_app_name, &src_root)  (容错)
   - lookup_software_path(dst_cluster, cd_app_name, &dst_root)  (容错)
   - 行级处理:
       drop --reservation / --allow-remote / --app / --account / -A
       replace -p / --partition= 为 job->remote_partition          /* ★ v2.0: 必须改写 */
   - 全局字符串替换 src_root -> dst_root
   - 写到 <dst_work_dir>/<basename>.cd_modified.sh (mode 0700)
   失败(filesystem) -> transition FAILED + reply BROKERD_ERR_LOOKUP_FAILED
3. _sudo_sbatch(remote_user, dst_work_dir, modified_path)
   - 根据 broker.conf::SubmitMode 选择:
       * mapped_user (默认): fork+execvp sudo -n -u remote_user sbatch <modified_path>
       * root_uid (★ v2.0 新): fork+execvp sbatch --uid=<remote_uid> <modified_path>
                              (要求 broker 进程具有 root SubmitJob 权限)
   - 抓 stdout 解析 "Submitted batch job <N>"
4. 成功 -> remote_job_id=N, transition SUBMITTED
   失败 -> transition FAILED
5. reply RESPONSE_BROKER_SUBMITTED { error_code, trace_id, remote_job_id }
```

### 7.2 US-2 scancel 反向传播

```
ctld(A) → broker(A) :CANCEL(8016)
  handler_ctld::handle_cancel_from_ctld
    - broker_job_table_get(trace_id)
    - cancel_requested = true
    - persist_async_request
    - reply RESPONSE_SLURM_RC(0)

state_machine 下一 tick (cancel 优先级):
  Phase 1 enqueue SM_ACTION_CANCEL(trace_id)
  Phase 2:
    egress_cancel_async(job)            # proto_send_to_peer 8016 fire-and-forget
    state_machine_transition(CANCELLED, "user cancelled")

state_machine 终态 tick:
  ctld_inject_terminal_state(8004)
  broker_job_table_remove

broker(B) handler_remote::handle_broker_cancel
  - broker_job_table_get(trace_id) → 不存在则幂等返回
  - 若 remote_job_id != 0:
       slurm_kill_job(remote_job_id, SIGTERM, KILL_FULL_JOB)
  - cancel_propagated = true
  - state_machine_transition(CANCELLED, "remote_cancel")
  - 不回 reply (fire-and-forget)
```

### 7.3 US-3 `squeue --remote` 字段化展示

broker 端调用关系：

- `sync_ticker._apply_remote_status` 检测到字段变化时调 `ctld_update_remote_state`（8003）
- 终态时 `state_machine` Phase 2 调 `ctld_inject_terminal_state`（8004）
- broker 全程**不调** `slurm_update_job(comment)`

ctld 端的字段写入与 squeue/sacct 展示由整体方案 §4 / §5 约束。

### 7.4 US-4 重启恢复

```
systemd → slurmbrokerd
  main()
    _parse_commandline / _setup_logging / _install_signal_handlers
    daemonize + create_pidfile (除 -D 外)
    broker_init():
       broker_conf_init                 # 解析 broker.conf + LocalUser 行
       slurm_init(NULL)                 # 必须先于 proto_init / serializer
       serializer_g_init(JSON)
       broker_job_table_init
       broker_state_restore             # 从 JSONL 读回所有在途
       persist_thread_start
       proto_init                       # auth_g 准备好
       egress_init
       state_machine_start              # tick 立即开始
       stage_pool_start                 # 4 worker
       state_machine_resume_inflight    # ★ 拨快 STAGING_* 任务时间戳
       sync_ticker_start
       listener_start                   # 最后开门
    main loop: while (!g_shutdown_requested) sleep(1)
    收到 SIGTERM/SIGINT:
       broker_state_save                # 兜底落盘
       broker_fini (反向顺序: listener -> sync_ticker -> stage_pool ->
                     state_machine -> egress -> proto -> persist ->
                     job_table -> user_mapping -> broker_conf ->
                     slurm_fini)
```

幂等性要点（已实现）：

| 调用 | 幂等性来源 |
|---|---|
| `egress_forward_async` 重发 | receiver 端 `handle_broker_forward_job` 检测 trace_id 已存在则回当前 ACK |
| `_sudo_sbatch` 重投 | receiver 端 `handle_broker_staged_in` 检测 `remote_job_id != 0` 则幂等 reply |
| rsync 重跑 | rsync 自带 `--partial`，源端文件未变天然幂等 |
| `slurm_kill_job` 重发 | Slurm 自身允许重复 kill |

---

## 八、模块设计（实际文件清单, ★ v2.0 新增 3 个核心模块）

### 8.1 源码布局 (v2.0 共 21 文件, ★ 新增 routes_loader/route/cap_check)

```
src/slurmbrokerd/
├── slurmbrokerd.c/.h          # main / 信号 / 启停编排 (M01 实质完成)
├── broker_conf.c/.h           # broker.conf 解析 (M02), ★ v2.0 新增 RouteSource / SubmitMode
├── user_mapping.c/.h          # LocalUser=... 行解析 (M02)
├── broker_job.c/.h            # broker_job_t + 全局表 + JSON 序列化 (M03), ★ v2.0 增 init_phase/route_candidates
├── persist.c/.h               # 三文件原子 rename + 30s/事件 checkpoint (M03)
├── proto.c/.h                 # 13 RPC + 14 错误码 + 私有 wire frame + send/recv API (M04, ★ v2.0 扩)
├── proto_pack.c               # 13 个 _pack/_unpack 实现 + dispatcher (M04)
├── listener.c/.h              # 双端口 select + ACL + self-pipe + dispatch (M05)
├── handler_ctld.c/.h          # CtldPort 入口 (M06)
├── handler_remote.c/.h        # PeerPort 入口 + sudo mkdir/sbatch (M07), ★ v2.0 新增 test_only handler
├── egress.c/.h                # 出站 8 wrapper (M08, ★ v2.0 新增 test_only_async)
├── state_machine.c/.h         # 1Hz tick + cancel 优先级 + 两阶段 + resume_inflight (M09)
│                              #   ★ v2.0 新增 INIT 四子状态 (_on_init_deciding/probing/selected/exhausted)
├── stage.c/.h                 # 4 worker pool + sudo+rsync+ssh + du -sb (M10)
├── software.c/.h              # lookup_software.sh 调用器 (M11)
├── util_exec.c/.h             # brokerd_waitpid_timeout 公共 helper (M11)
├── rewrite.c/.h               # SBATCH 行处理 + 路径前缀替换 (M12), ★ v2.0 必须重写 partition
├── sync_ticker.c/.h           # 10s 轮询 + 三阶段 + 失败计数 (M13)
├── routes_loader.c/.h         # ★ v2.0 新增: 解析 routes.conf, 支持 SIGHUP 热加载 (M16)
├── route.c/.h                 # ★ v2.0 新增: route_decide() 决策 + 候选列表生成 (M16)
├── cap_check.c/.h             # ★ v2.0 新增: 容量计数 (MaxInflight / RemoteMaxInflight) (M16)
└── Makefile.am
```

**v2.0 对比 v1.5**: 增 3 模块 (M16, ~600 LoC); proto_pack.c 增 2 个 _pack/_unpack (test_only); state_machine.c 增 4 个内部分支函数 (~250 LoC)。

**对比旧版规划（16 文件）**：

- ✅ 多了 `proto_pack.c`（pack/unpack 与 lifecycle 拆开，便于将来删 LEGACY block）
- ✅ 多了 `util_exec.c/.h`（M11 抽公共 fork+wait helper）
- ✅ ★ v2.0 多 `routes_loader.c` / `route.c` / `cap_check.c` (M16 三件套)
- ❌ 没有独立 `cleanup.c`（M14 废弃）
- 旧版的 `audit.c` / `sbroker.c` / `app_route.c` 在 v2.0 由 `routes_loader.c` + `route.c` 实质对位

### 8.A `routes_loader.c` (★ v2.0 新增)

#### 8.A.1 `routes.conf` 文件格式

```ini
# /etc/slurm/routes.conf
# 每个 [Route] 段一条路由规则; 配对原则: 先按 Src 匹配, 再按 AllowApps 过滤
# 同一 (Src, AllowApps) 可以有多个 Target, broker 在 PROBING 阶段按定义顺序探测

[Route route-cluA-cpuP1-to-cluB-cpuQ]
Src              = cluster_A:cpu_partition_1
AllowApps        = vasp,gromacs,*                  # `*` = 任意 app; 多个用 , 分隔
TargetBroker     = 10.0.2.50:7001                  # 对端 broker peer_addr
TargetCluster    = cluster_B                       # 远端 ClusterName
TargetPartition  = cpu_partition_Q
Priority         = 100                             # 数值越小越优先
RemoteMaxInflight = 64                             # 该 Target 上同时存在的 forward 数上限
TestOnlyTimeout  = 5                               # test-only 探测超时秒数, 缺省 5

[Route route-cluA-cpuP1-to-cluC-cpuR]
Src              = cluster_A:cpu_partition_1
AllowApps        = *
TargetBroker     = 10.0.3.50:7001
TargetCluster    = cluster_C
TargetPartition  = cpu_partition_R
Priority         = 200
RemoteMaxInflight = 32
```

#### 8.A.2 数据结构

```c
/* src/slurmbrokerd/routes_loader.h, v2.0 新增 */
typedef struct route_entry {
    char     *route_id;
    char     *src_cluster_name;
    char     *src_partition;
    char    **allow_apps;             /* NULL=禁用; ["*"]=放通; ["vasp","gromacs"]=白名单 */
    uint16_t  n_allow_apps;
    char     *target_broker_addr;     /* "host:port" */
    char     *target_cluster_name;
    char     *target_partition;
    uint16_t  priority;
    uint32_t  remote_max_inflight;
    uint16_t  test_only_timeout_s;
    struct route_entry *next;
} route_entry_t;

typedef struct {
    pthread_rwlock_t  lock;            /* 读多写少, 用 rwlock 让 route_decide 不互相阻塞 */
    route_entry_t    *head;
    uint32_t          version;          /* 每次 reload 递增, 给 cap_check 失效缓存用 */
    time_t            mtime;
    char             *path;
} routes_table_t;

extern routes_table_t *g_routes;
```

#### 8.A.3 关键函数

```c
extern int  routes_loader_init(const char *path);          /* 启动时同步加载 */
extern int  routes_loader_reload(void);                    /* SIGHUP 触发 */
extern int  routes_loader_lookup_by_src(const char *src_cluster,
                                          const char *src_partition,
                                          const char *app_name,
                                          route_entry_t ***out_matched,
                                          uint16_t *out_n);
extern void routes_loader_fini(void);
```

行解析容错: 任一行解析失败 → broker 启动 fail-stop (避免半配置上线); SIGHUP 时若新文件解析失败 → 保留旧表, log_warn。

### 8.B `route.c` (★ v2.0 新增)

```c
/* src/slurmbrokerd/route.h */
typedef struct route_candidate {
    char     *route_id;
    char     *target_broker_addr;
    char     *remote_cluster_name;
    char     *remote_partition;
    uint16_t  priority;
    uint32_t  remote_max_inflight;
    uint16_t  test_only_timeout_s;
    /* 由 user_mapping 解析 */
    uint32_t  remote_uid;
    char     *remote_user_name;
} route_candidate_t;

/*
 * 根据 (src_cluster, src_partition, src_user_name, cd_app_name) 产出按 priority 升序的候选数组
 * 返回:
 *   SLURM_SUCCESS         有 ≥1 个候选 -> *out 非空, *n_out > 0
 *   BROKERD_ERR_NO_VIABLE_ROUTE   无匹配
 *   BROKERD_ERR_NO_USER_MAPPING   user_mapping 找不到对应 remote_user
 */
extern int route_decide(const char *src_cluster_name,
                         const char *src_partition,
                         const char *src_user_name,
                         const char *cd_app_name,
                         route_candidate_t **out, uint16_t *n_out);
extern void route_candidates_free(route_candidate_t *arr, uint16_t n);
```

#### 8.B.1 决策流程

```
route_decide():
  1. routes_loader_lookup_by_src(src_cluster, src_partition, app) -> matched[]
     (内部已用 rwlock_rdlock 持锁)
  2. 对每一条 matched[i]:
     - user_mapping_lookup(src_user_name, target_cluster_name, &remote_uid, &remote_user_name)
       失败 -> skip (该路由对该用户不可用)
     - 通过则填 route_candidate_t 并加入输出
  3. 按 priority 升序排
  4. *n_out == 0 -> BROKERD_ERR_NO_VIABLE_ROUTE
```

### 8.C `cap_check.c` (★ v2.0 新增)

#### 8.C.1 数据结构

```c
/* src/slurmbrokerd/cap_check.h */
typedef struct {
    char     *route_id;
    uint32_t  remote_max_inflight;
    uint32_t  current_inflight;       /* 已 SELECTED 但未到终态的作业数 */
} cap_slot_t;

typedef struct {
    pthread_mutex_t  lock;
    cap_slot_t      *slots;
    uint32_t         n_slots;
    uint32_t         routes_version;   /* 与 g_routes->version 对齐, 不一致则 rebuild_locked */
    uint32_t         global_max_inflight;    /* = broker.conf MaxInFlightJobs */
    uint32_t         global_current_inflight;
} cap_state_t;

extern cap_state_t g_cap_state;
```

#### 8.C.2 关键函数

```c
/*
 * 在 INIT.DECIDING 阶段调用; 过滤掉容量满的候选; 不实际占用名额
 * 返回:
 *   SLURM_SUCCESS                有 ≥1 候选可用 -> in-place 压缩 cands[]
 *   BROKERD_ERR_OVERLOAD         全局 MaxInFlightJobs 已满
 *   BROKERD_ERR_CAP_FULL_SOFT_WAIT 全部候选的 RemoteMaxInflight 满 (临时性, 不写 cd_route_exhausted)
 */
extern int cap_check_filter(route_candidate_t *cands, uint16_t *n);

/* SELECTED 后正式占住名额 (state_machine._on_init_selected 调用) */
extern int cap_inc(const char *route_id);
/* 任意失败回滚 / 终态时释放 */
extern int cap_dec(const char *route_id);
```

cap_check 与 broker_job_table 的双重计数策略: cap_check 维护"路由级"配额, broker_job_table 维护"全局"在途数; broker_state_restore 时, 遍历 jobs 把每条已 SELECTED 的作业加回对应的 cap_slot.current_inflight (恢复占用)。

### 8.D state_machine.c 内部新增函数 (★ v2.0)

| 函数 | 调用时机 | 主要动作 |
|---|---|---|
| `_on_init_deciding(job)` | tick 见 init_phase==DECIDING | 调 `route_decide` + `cap_check_filter`, 设 candidates[], 进 PROBING |
| `_on_init_probing(job)` | tick 见 init_phase==PROBING && !probe_pending | 调 `egress_test_only_async`, 注册 cb; 等回调推进 |
| `_on_init_selected(job)` | tick 见 init_phase==SELECTED | 调 `cap_inc` + `egress_forward_async`; 收 8011 ACK 后 transition STAGING_IN |
| `_on_init_exhausted(job)` | tick 见 init_phase==EXHAUSTED | 调 `cap_dec` (若已 inc) + `ctld_inject_terminal_state(8004, err=9013)`, transition FAILED |

### 8.2 软件路径解析与脚本改写

#### 8.2.1 `software.c::lookup_software_path()`

```c
/*
 * pipe + fork + execl LookupSoftwareScript <cluster> <app>
 * 父侧:
 *   - select() 等 stdout 可读，超 LookupTimeoutSec
 *   - read() 单行
 *   - brokerd_waitpid_timeout(pid, 1)  (util_exec.h)
 * 校验:
 *   - exit code == 0
 *   - stdout 第 1 字节 == '/'
 * 返回:
 *   SLURM_SUCCESS                     -> *out_path = xstrdup(buf)
 *   BROKERD_ERR_LOOKUP_TIMEOUT
 *   BROKERD_ERR_LOOKUP_FAILED
 */
extern int lookup_software_path(const char *cluster, const char *app,
                                char **out_path);
```

stderr 不重定向，让脚本错误信息直达 broker journal。

#### 8.2.2 `rewrite.c::rewrite_job_script()`

行级处理规则：

| 规则 | 触发条件 | 行为 |
|---|---|---|
| drop reservation | `#SBATCH --reservation=...` | 删除该行 |
| drop cross-domain | `#SBATCH --allow-remote` | 删除该行 |
| drop app | `#SBATCH --app=...` | 删除该行 |
| drop account | `#SBATCH --account=...` 或 `-A` | 删除该行（让远端 sacctmgr 用 default account） |
| replace partition | `#SBATCH -p X` 或 `--partition=X` | 替换为 `target_partition` |
| 路径替换 | 任何含 `src_root` 子串的行 | 全局替换为 `dst_root` |

容错模式：`lookup_software_path` 任一调用失败时，跳过路径替换但继续完成 SBATCH 头处理与文件写出（`SLURM_SUCCESS`），下游 sbatch 仍可执行（用户脚本必须自己用 module/source 加载软件时才会感受到差异）。

输出：`<dst_work_dir>/<basename(script_path)>.cd_modified.sh`，mode 0700。

#### 8.2.3 `lookup_software.sh` 与 `software_routes.conf` 模板

部署模板存在 `etc/lookup_software.sh.example` 与 `etc/software_routes.conf.example`。运维 cp 到正式路径并按需替换。脚本协议：

```
argv[1] = <cluster>
argv[2] = <app>
stdout  = first line is the resolved absolute install path (must start with '/')
exit 0  = success
exit 2  = unknown cluster/app or missing config
exit *  = treated as failure
```

`software_routes.conf` 行格式：`<cluster>:<app>=<absolute_path>`，`#` 起注释。

### 8.3 listener 双端口实现

```c
listener_thread:
    fd_ctld = bind+listen(0.0.0.0, ctld_port)   # SO_REUSEADDR
    fd_peer = bind+listen(0.0.0.0, peer_port)
    pipe(wakeup_pipe)                            # self-pipe trick

    while (running):
        FD_ZERO; FD_SET fd_ctld; fd_peer; wakeup_pipe[0]
        select(maxfd+1, ...)

        # shutdown 信号
        if FD_ISSET(wakeup_pipe[0]): break

        if FD_ISSET(fd_ctld):
            client = _accept_with_acl(fd_ctld, ACL_LOOPBACK_ONLY)
            _handle_ctld_conn(client):
                slurm_msg_t msg; slurm_receive_msg(client, &msg, ...)
                  # 未注册的 msg_type -> SLURM_UNEXPECTED_MSG_ERROR
                dispatch_ctld_msg(&msg)

        if FD_ISSET(fd_peer):
            client = _accept_with_acl(fd_peer, ACL_PEER_DNS_RESOLVED)
            _handle_peer_conn(client):
                slurm_msg_recvfrom_timeout(client, &buffer, PEER_RECV_TIMEOUT)
                brokerd_wire_parse(&buffer, &mtype, &pv, &payload)
                dispatch_remote_msg(mtype, payload, client, &peer_addr)

shutdown:
    listener_running = false
    write(wakeup_pipe[1], "x", 1)
    pthread_join
    close(fd_ctld); close(fd_peer)               # 必须在 join 之后
```

ACL：
- ctld 端口：仅允许 loopback（127.0.0.1 / ::1）；
- peer 端口：把 `RemoteBrokerHost` DNS 解析的 IP 集做白名单，可刷新（缓存 5 分钟）。

---

## 九、数据传输（rsync over SSH）

### 9.1 stage-in 命令模板（落地版）

```
sudo -n -u <src_user> /usr/bin/rsync \
    -av --partial \
    --rsync-path="sudo -n -u <remote_user> rsync" \
    -e "ssh -i <StageSshKey> \
            -o StrictHostKeyChecking=yes \
            -o BatchMode=yes \
            -o ConnectTimeout=30 \
            -l <StageSshUser>" \
    <src_work_dir>/ \
    <RemoteBrokerHost>:<dst_work_dir>/
```

新增项：

- `--partial`：保留中断的部分文件，重启后增量续传；
- `_du_sb` 预检：超 `MaxStageBytes` 直接 transition FAILED，不入队。

### 9.2 worker pool

- `StageWorkerCount` 默认 4；
- 队列 `stage_in_queue` / `stage_out_queue` 由 `pthread_mutex` + `pthread_cond` 保护；
- `stop` 时广播 cond，但若 worker 正在 `waitpid(rsync)`，最坏需等 `STAGE_CHILD_TIMEOUT_S=3600s` 才能退出（已在 stage.h 注释里明示）；
- 完成回调：成功 → `state_machine_transition(STAGED_IN/COMPLETED)` + 仅 stage-in 触发 `egress_staged_in_async`；失败 → 只打日志，让 M09 watchdog 推进重试或 FAILED。

### 9.3 SSH key 部署（运维一次性）

部署清单详见 §11 与 `etc/lookup_software.sh.example` 头部注释。

---

## 十、配置

### 10.1 `/etc/slurm/broker.conf` (★ v2.0 字段集)

```ini
# ============================================
# 集群标识
# ============================================
ClusterName=xian_cluster
BrokerNodeName=broker.xian.example.com

# ============================================
# 服务监听
# ============================================
BrokerCtldPort=8442
BrokerPeerPort=8443

# ============================================
# v2.0 (★ 主要变化): 路由配置外置到 routes.conf
# ============================================
# 路由来源: file (从 routes.conf 解析) | static_legacy (回退到 v1.5 单对端字段)
RouteSource=file
RoutesConfPath=/etc/slurm/routes.conf
# 默认 SIGHUP 触发 routes_loader_reload(); 同时启用文件 mtime 监控
RoutesReloadMode=sighup_or_mtime
# routes.conf mtime 轮询间隔 (秒); 仅 RoutesReloadMode=sighup_or_mtime / mtime 时生效
RoutesMtimePollSec=30

# ============================================
# v2.0 (★ 新): 远端 sbatch 投递身份策略
# ============================================
# mapped_user (默认, v1.5 行为): receiver 端 fork+exec sudo -u <remote_user> sbatch
# root_uid : receiver broker 必须以 root 运行, 使用 sbatch --uid=<remote_uid>
#            (省去 sudoers 配置, 但要求 broker 是受信特权进程)
SubmitMode=mapped_user

# ============================================
# v2.0 (★ 新): 鉴权 ACL 复用 SlurmDBD 的 user/assoc.remote_allowed
# ============================================
# 不在 broker 内部独立维护 ACL; 由 ctld 端在转发前完成 ACL 检查
# 但 broker 端可启用 strict_check 以加固
RemoteAllowedCheck=strict   # strict (默认, broker 二次校验) | none (信任 ctld)

# ============================================
# 全局在途限流 (broker 兜底, 配合 routes.conf 内每路由 RemoteMaxInflight)
# ============================================
MaxInFlightJobs=500
MaxStageBytes=53687091200      # 50 GB

# ============================================
# v1.5 -> v2.0 兼容: RemoteCluster*/DefaultRemotePartition 仅 RouteSource=static_legacy 生效
# ============================================
# RouteSource=file 时下列字段被忽略, 但仍可保留以备回滚
#RemoteClusterName=wz_cluster
#RemoteBrokerHost=broker.wz.example.com
#RemoteBrokerPort=8443
#RemoteMungeKeyPath=/etc/slurm/munge_wz.key
#DefaultRemotePartition=wzhcnormal

# ============================================
# 鉴权
# ============================================
AuthType=auth/munge

# ============================================
# 持久化
# ============================================
StateSaveLocation=/var/spool/slurm/broker
StateFileName=broker_state.jsonl
CheckpointInterval=30

# ============================================
# 状态轮询
# ============================================
PollInterval=10
PollMaxRetries=5

# ============================================
# v2.0 (★ 新): test-only 探测调参
# ============================================
TestOnlyTimeoutSec=5           # 单候选 test-only 超时
TestOnlyMaxCandidates=8         # 同一作业最多尝试的候选数 (越界视为 EXHAUSTED)

# ============================================
# 数据传输
# ============================================
StageRsyncBin=/usr/bin/rsync
StageSshKey=/etc/slurm/broker_id_ed25519
StageSshUser=slurm-broker
StageWorkerCount=4
StageTimeoutPerGB=120

# ============================================
# 软件路径解析
# ============================================
LookupSoftwareScript=/opt/slurm-broker/scripts/lookup_software.sh
LookupTimeoutSec=10

# ============================================
# 数据保留 (保留字段; M14 废弃, broker 当前不删除远端目录)
# ============================================
RemoteWorkDirRetentionHours=24
RemoteWorkDirFailureRetentionDays=7

# ============================================
# 用户映射 —— ★ 与旧版差异：直接内联，不再 Include user_mapping.conf
# ============================================
LocalUser=test1 RemoteCluster=wz_cluster RemoteUser=wz_test1 RemoteUid=20001 RemoteGid=20001
LocalUser=test2 RemoteCluster=wz_cluster RemoteUser=wz_test2 RemoteUid=20002 RemoteGid=20002
# ... 试点用户全部列出
```

#### 10.1.1 v2.0 新增字段语义表

| 字段 | 默认 | 取值 | 语义 |
|---|---|---|---|
| `RouteSource` | `file` | `file` / `static_legacy` | 路由来源: `file` = 读 routes.conf; `static_legacy` = v1.5 行为, 自动构造单条路由 |
| `RoutesConfPath` | `/etc/slurm/routes.conf` | 路径 | 仅 `RouteSource=file` 时生效 |
| `RoutesReloadMode` | `sighup_or_mtime` | `sighup` / `mtime` / `sighup_or_mtime` / `none` | 热加载策略 |
| `RoutesMtimePollSec` | 30 | 正整数 | 文件 mtime 轮询间隔 |
| `SubmitMode` | `mapped_user` | `mapped_user` / `root_uid` | 远端 sbatch 投递身份 |
| `RemoteAllowedCheck` | `strict` | `strict` / `none` | 是否在 broker 二次校验 user/assoc.remote_allowed (需 broker 能查 SlurmDBD) |
| `TestOnlyTimeoutSec` | 5 | 正整数 | 单个 test-only 探测超时 |
| `TestOnlyMaxCandidates` | 8 | 正整数 | 单作业最大候选数, 防止 routes.conf 爆炸 |

### 10.2 用户映射的解析路径

`broker_conf.c` 把 `LocalUser` 注册为 `S_P_LINE`，每条 `LocalUser=...` 行被 `parse_config.c::s_p_parse_line` 解析为一个子表，子表内字段 `RemoteCluster` `RemoteUser` `RemoteUid` `RemoteGid`。`user_mapping.c::user_mapping_load_from_hashtbl` 在 `broker_conf_init` 完成后接管子表，把每条记录写入全局 `g_user_mappings` xhash（key = `local_user|remote_cluster`）。

### 10.3 munge 与跨集群 key

不变：建议 `munged --ttl 86400` 缓解时钟漂移；MVP 推荐 A/B 共享同一份 munge key。

---

## 十一、构建与部署

### 11.1 build 集成

- `configure.ac` L540 已加 `src/slurmbrokerd/Makefile`
- `src/Makefile.am` L21 已加 `slurmbrokerd` 子目录
- `src/slurmbrokerd/Makefile.am` 中 `slurmbrokerd_SOURCES` 列全 18 个文件
- 链接 `libslurm.la`，与 `slurmctld` / `slurmd` 同套构建链

### 11.2 systemd

`etc/slurmbrokerd.service.in` 已就位，由 `etc/Makefile.am::WITH_SYSTEMD_UNITS` 安装到 `/usr/lib/systemd/system/slurmbrokerd.service`。

### 11.3 启动序列（与 §7.4 一致）

```
1. systemd 拉起 slurmbrokerd
2. parse_commandline / setup_logging / install_signal_handlers
3. daemonize + pidfile（除 -D 外）
4. broker_init() 全套子模块按依赖序启动
5. notify systemd READY（如启用 Type=notify）
6. main loop sleep(1) 直到 SIGTERM
```

### 11.4 部署侧文件

```
/usr/sbin/slurmbrokerd
/usr/lib/systemd/system/slurmbrokerd.service

/etc/slurm/
├── broker.conf                        # 单文件，含所有 LocalUser=...
├── broker_id_ed25519                  # SSH key, chmod 600
└── munge.key                          # 跨集群一致

/opt/slurm-broker/
├── scripts/lookup_software.sh         # 由 etc/lookup_software.sh.example 派生
└── conf/software_routes.conf          # 由 etc/software_routes.conf.example 派生

/var/spool/slurm/broker/
├── broker_state.jsonl
├── broker_state.jsonl.tmp             # 写入中
└── broker_state.jsonl.old             # 上一份

/var/log/slurm/
├── slurmbrokerd.log                   # 仅当未走 journald 时
└── broker_stage/<trace_id>.log        # 每个 stage worker 一个
```

### 11.5 运维基线（M15 待补，已知 TODO）

- `etc/broker.conf.example`（缺）
- ★ v2.0 `etc/routes.conf.example`（缺, M16 同时给）
- sudoers 模板（缺）：`broker_user ALL=(ANY) NOPASSWD: /usr/bin/rsync, /usr/bin/sbatch, /bin/mkdir, /bin/chmod, /bin/sh`
- ssh key 分发 README（缺）
- logrotate 配置（缺）：`/var/log/slurm/broker_stage/*.log`

### 11.6 v1.5 → v2.0 升级路径 (★ 新增)

#### 11.6.1 升级前置条件

| 项 | v1.5 要求 | v2.0 升级要求 |
|---|---|---|
| ctld 版本 | MVP-1.5 | **必须先升级 MVP-v2.0** (ctld 侧的 forward_job_msg_t 协议变更) |
| broker_state.jsonl | v1.5 schema | 字段向前兼容: v1.5 jsonl 中无 `init_phase` / `route_candidates`, broker_state_restore() 把缺省值视为 SELECTED 已完成 |
| broker.conf | v1.5 | 必须新增 `RouteSource=file` + `RoutesConfPath=`, 否则启动失败 |
| routes.conf | 不存在 | 必须新建; 至少包含一个 `[Route]` 段覆盖原 v1.5 的 `RemoteCluster*`+`DefaultRemotePartition` |

#### 11.6.2 升级步骤

```bash
# 1. 双 broker 同时停服 (停 listener, 不停 sync_ticker; 让在途作业先轮询完终态)
systemctl stop slurmbrokerd     # A 端
ssh broker-B systemctl stop slurmbrokerd

# 2. 备份 state 与 conf
cp /var/spool/slurm/broker/broker_state.jsonl{,.v1.5.bak}
cp /etc/slurm/broker.conf{,.v1.5.bak}

# 3. 部署 v2.0 二进制
yum upgrade slurmbrokerd-2.0

# 4. 编写 routes.conf (最小迁移示例: 把 v1.5 单对端字段平移)
cat > /etc/slurm/routes.conf <<'EOF'
[Route legacy-default]
Src              = $(hostname -s | sed 's/-broker$//'):*   # 任意源 partition
AllowApps        = *
TargetBroker     = $(awk '/RemoteBrokerHost/{print $2}' /etc/slurm/broker.conf.v1.5.bak):$(awk '/RemoteBrokerPort/{print $2}' /etc/slurm/broker.conf.v1.5.bak)
TargetCluster    = $(awk '/RemoteClusterName/{print $2}' /etc/slurm/broker.conf.v1.5.bak)
TargetPartition  = $(awk '/DefaultRemotePartition/{print $2}' /etc/slurm/broker.conf.v1.5.bak)
Priority         = 100
RemoteMaxInflight = 500
EOF

# 5. 编辑 broker.conf 新增 RouteSource= 段 (详见 §10.1)
vim /etc/slurm/broker.conf
#   + RouteSource=file
#   + RoutesConfPath=/etc/slurm/routes.conf
#   + SubmitMode=mapped_user
#   + RemoteAllowedCheck=strict

# 6. 启动
systemctl start slurmbrokerd
ssh broker-B systemctl start slurmbrokerd

# 7. 校验
journalctl -u slurmbrokerd | grep -E 'routes_loader|route_decide'
scontrol show job <test_jobid>
```

#### 11.6.3 回滚 SOP

```bash
# 1. 停 broker
systemctl stop slurmbrokerd

# 2. 回退二进制到 v1.5
yum downgrade slurmbrokerd-1.5

# 3. 恢复 conf (v1.5 没有 routes.conf, 直接清掉)
cp /etc/slurm/broker.conf.v1.5.bak /etc/slurm/broker.conf

# 4. broker_state.jsonl 兼容性:
#    v2.0 -> v1.5 不自动支持 (新增的 init_phase 字段会被 v1.5 忽略, 但 INIT 阶段未完成的作业
#                              会被 v1.5 当成"普通 INIT"重跑, 可能造成重复转发)
#    安全做法: 在 v1.5 启动前手动 cancel 掉所有处于 INIT 阶段的作业:
jq -r 'select(.state=="INIT") | .src_job_id' /var/spool/slurm/broker/broker_state.jsonl \
    | xargs -I {} ssh slurm-mgr scancel {}

# 5. 启动 v1.5
systemctl start slurmbrokerd
```

---

## 十二、Sprint 进度 (落地状态盘点, ★ v2.0 新增 M16-M19)

| Sprint | 模块 | 状态 |
|---|---|---|
| S1 W1-W2 | M01 进程骨架（slurmbrokerd.c 信号/日志/init/fini） | ✅ 完成（实质，无单独 checklist） |
| S1 W1-W2 | M02 配置（broker.conf 单文件） | ✅ 完成 (★ v2.0 增 RouteSource/SubmitMode/TestOnly* 字段) |
| S1 W1-W2 | M03 持久化（JSONL + 三文件 rename） | ✅ 完成 (★ v2.0 增 init_phase / route_attempted_mask / route_current_idx 字段) |
| S1 W1-W2 | M04 RPC（broker 私有 wire frame + 11 → 13 msg + 9 → 14 error） | ✅ v1.5 完成 / ★ v2.0 新增 8018/8019 + 5 错误码 (9010-9013, 9020) |
| S2 W3-W4 | M05 listener（双端口 + self-pipe + ACL） | ✅ 完成 |
| S2 W3-W4 | M06 ctld 入站 handler | ✅ 完成 (★ v2.0 forward_job payload 瘦身) |
| S2 W3-W4 | M07 remote 入站 handler（含 sudo+sbatch） | ✅ 完成 (★ v2.0 新增 test_only handler + SubmitMode 分支) |
| S2 W3-W4 | M08 egress（5 broker→broker + 2 broker→ctld → 8 总） | ✅ 完成 (★ v2.0 新增 egress_test_only_async) |
| S3 W5-W6 | M09 状态机（1Hz + 两阶段 + cancel 优先级 + resume_inflight） | ✅ 完成 (★ v2.0 INIT 四子状态 _on_init_deciding/probing/selected/exhausted) |
| S3 W5-W6 | M10 stage（4 worker + sudo+rsync + du -sb + --partial） | ✅ 完成 |
| S3 W5-W6 | M11 software（fork+exec lookup + util_exec helper） | ✅ 完成 |
| S3 W5-W6 | M12 rewrite（SBATCH 行处理 + 路径替换 + 容错） | ✅ 完成 (★ v2.0 强制改写 partition= 行) |
| S3 W5-W6 | M13 sync_ticker（10s + 三阶段 + 失败计数） | ✅ 完成 (★ v2.0 首次包附带 remote_cluster_name/remote_partition_name) |
| S3 W5-W6 | M14 cleanup（远端目录定时清理） | ⏸ **暂废弃，不删除用户数据** |
| S4 W7-W8 | M15 部署运维工件（broker.conf.example / sudoers / logrotate） | ⏳ 待补 (★ v2.0 增 routes.conf.example) |
| S4 W7-W8 | **★ v2.0 M16 routes_loader.c + route.c + cap_check.c** | ⏳ v2.0 新增, ~600 LoC, 单元测试覆盖率 ≥ 80% |
| S4 W7-W8 | **★ v2.0 M17 state_machine INIT 子状态** | ⏳ v2.0 新增, 250 LoC + 故障注入测试 |
| S4 W7-W8 | **★ v2.0 M18 proto 8018/8019 + handler_remote::_handle_test_only** | ⏳ v2.0 新增, 200 LoC |
| S4 W7-W8 | **★ v2.0 M19 集成测试: routes.conf 热加载 + test-only 探测 + cap_check 边界** | ⏳ v2.0 新增, 含故障注入 (远端 ctld test_only 超时) |

**跨 PR 依赖（外部）**：
- ctld 同步 PR (★ v2.0 关键)：
  1. `src/common/slurm_protocol_defs.h` 注册 6 个 msg_type 与 payload (8001-8004 + ★ 8018-8019);
  2. `src/slurmctld/cross_region.c` 实现路由侧 hold 逻辑;
  3. `src/slurmctld/cross_region_rpc.c::handle_broker_update_remote_state` 首次包写 remote_cluster_name/remote_partition_name;
  4. `job_record_t::cd_route_exhausted` + state file pack/unpack;
  5. `partition_record_t::cd_allow_remote` + slurm.conf 解析;
  6. SlurmDBD user/assoc.remote_allowed 字段 + sacctmgr CLI;
  之后 broker 这边走机械删除 PR 移除 27+5 处 `LEGACY_M04_TRANSITIONAL`。
- ctld + slurm.conf：新增 `BrokerAddr` / `BrokerPort` 字段；ctld 跨域调度线程实现。

---

## 十三、验收清单

### 13.1 功能（13.1-13.3 沿用旧版，状态标注本 MVP 实现度）

- ✅ US-1 全流程（INIT → STAGING_IN → SUBMITTED → RUNNING → STAGING_OUT → COMPLETED）已可在代码层端到端联通；待 M16 编写 smoke test 实测
- ✅ US-2 scancel 反向（含 receiver 端 `slurm_kill_job`）已实现
- ⏳ US-3 字段化展示需要 ctld + 客户端 PR 配合（broker 侧 8003/8004 已就位）
- ✅ US-4 重启恢复：`broker_state_restore` + `state_machine_resume_inflight` + 全部 4 处幂等性已就位

### 13.2 异常 (★ v2.0 新增 4 项)

- ✅ 远端 broker 不可达：`egress::_retry_n_times` 指数退避；上层 state_machine 超时后转 FAILED
- ✅ rsync 失败：stage worker 不直接转 FAILED，由 M09 `_on_staging_in` watchdog 触发重试 ≤ 3
- ✅ 远端 sbatch 拒绝：handler_remote `_sudo_sbatch` 解析失败 → transition FAILED + reply BROKERD_ERR_REMOTE_SUBMIT_FAILED
- ✅ `lookup_software.sh` 退出非 0：返回 BROKERD_ERR_LOOKUP_FAILED；rewrite 容错继续 SBATCH-only 改写
- ✅ 用户映射缺失：handler_ctld 直接 reply BROKERD_ERR_NO_USER_MAPPING
- ✅ broker 进程 OOM：systemd Restart=on-failure（unit 文件已配）
- ⏳ ★ v2.0 routes.conf 无匹配 → BROKERD_ERR_NO_VIABLE_ROUTE → ctld 置 `cd_route_exhausted=1`
- ⏳ ★ v2.0 test-only 全数被拒 → BROKERD_ERR_ALL_ROUTES_EXHAUSTED → ctld 置 `cd_route_exhausted=1`
- ⏳ ★ v2.0 test-only 5s 超时 → 当前候选放弃, 推进下一个候选; 全部超时 → 同上
- ⏳ ★ v2.0 cap_check 软命中 (RemoteMaxInflight 满) → BROKERD_ERR_CAP_FULL_SOFT_WAIT → ctld 下轮重试

### 13.3 性能 (★ v2.0 新增基准)

- ⏳ 100 跨域作业并发：待 M19 长稳测试
- ⏳ 24h 无内存泄漏：待 valgrind / tcmalloc 跑
- ⏳ ★ v2.0 100 候选 routes.conf 下 route_decide() < 1 ms (rwlock 读路径)
- ⏳ ★ v2.0 SIGHUP routes.conf 热加载 < 100 ms, 期间无 forward_job 阻塞 (rwlock writer 等读者退出)
- ⏳ ★ v2.0 单作业最差 PROBING 耗时上限: TestOnlyMaxCandidates × TestOnlyTimeoutSec = 40 s (默认 8×5)

### 13.4 已知限制 (v2.0 部分缓解)

- 仅 sbatch 跨域，不支持 srun / salloc
- 仅 CLI 作业，不支持 Portal `.portal/job_portal.var`
- `scontrol update` 不支持跨域（用户需 scancel 重投）
- ~~单对端集群 (1 RemoteCluster)~~ → ★ v2.0 解除, 通过 routes.conf 支持任意多对端
- stdout/stderr 仅终态可见
- 单 broker 实例无 HA
- 远端用户必须由运维预先用 `sacctmgr` 配好 default account 与 partition association + `remote_allowed=true`
- **远端 work_dir 不会被自动清理**（M14 废弃后果）；运维需周期性回收
- ★ v2.0 `routes.conf` 不支持基于资源画像 (内存/GPU 数量) 的动态路由; 仅静态 `(src_cluster, src_partition, app_name)` 三元组匹配; 智能化路由仍在 §14.4 V2/V3 演进中

---

## 十四、演进锚点

### 14.1 短期（待 ctld 同事 PR 落地后）

- 删 27 处 `LEGACY_M04_TRANSITIONAL`（机械工作）
- 删 `proto.h::brokerd_forward_job_msg_t` / `brokerd_broker_remote_state_msg_t` / `brokerd_broker_terminal_state_msg_t` / `brokerd_forward_job_resp_msg_t`
- `handler_ctld.c` 改用 `slurm_msg_t.data` 直接读原生结构

### 14.2 中期（向 v0.1 完整版演进）

| 演进方向 | 落地位置 | 影响 ctld / 客户端？ |
|---|---|---|
| 路由能力（多对端 / 平台代理 / 智能化 / Portal） | **见 §14.4 路由能力演进路径（专章）** | 否（仅 ctld 配置侧 SendTo 微调） |
| `lookup_software.sh` 替换为 RDB / API | `software.c::lookup_software_path` 改实现 | 否 |
| 持久化升级到 `pack.c` | `persist.c` JSONL → 二进制 | 否 |
| broker→broker mTLS / HTTPS | 抽象 `peer_transport_ops_t`，加 mTLS 实现 | 否 |
| 实时 stdout 回传 | 新增 `stream.c` 独立线程拉 | 仅 ctld 增加可选 RPC |
| broker HA | 引入 etcd/consul + VIP | 否 |
| Prometheus exporter | 新增 `metrics.c` | 否 |
| sbroker CLI | 新增 `src/sbroker/` 复用 JSONL（最先用于 §14.4 `show routes`） | 否 |

### 14.3 不再变动的契约

为后续演进风险最小，下列字段语义只增不改：

- `proto.h` 中 7 个 PERMANENT msg_type 与 payload 字段顺序（broker↔broker）
- `broker_job.h` 的 JSONL schema（只允许文件末尾追加可选字段）
- `state_machine.h` 的 9 状态 + 转换图
- `software.h::lookup_software_path()` 函数签名
- `route.h::route_decide()` 函数签名（V1 一次性引入后跨 V1/V2/V3 不变；详见 §14.4）
- `route.h::route_request_t` / `route_decision_t` 字段顺序（只允许末尾追加可选字段）
- `BROKERD_REQUEST_PORTAL_SUBMIT`（8005，V3 引入）字段顺序（只增不改）
- 平台 HTTP `POST /v1/route` 请求/响应 schema（只允许追加可选字段）
- `routes.conf` section schema（`[route:<name>]` 内 `match_*` / `target_*` / `priority` 字段，只允许追加新的 `match_*` 维度）
- `broker.conf::RouteSource=` 取值集合（`inline` / `file` / `platform` / `auto`）

---

### 14.4 路由能力演进路径（固定路由 → 智能路由）

#### 14.4.1 设计目标与四阶段

当前 broker 的"路由"能力是 **V0** 状态：在 `broker.conf` 中**硬编码**单一对端集群 + 单一目标分区，所有作业 100% 进同一目的地。下面给出从 V0 到 V3（智能化路由 + portal 入口）的演进路径。**任何阶段的代码改动都被 §1.2 的强约束锁在 `src/slurmbrokerd/` 内，对 `src/common/` / Slurm 原生源码零侵入**。

| 阶段 | 能力 | 决策依据 | 决策实现位置 | broker 内新增模块 |
|---|---|---|---|---|
| **V0**（当前 MVP_new）| 单对端单分区 | `broker.conf::DefaultRemotePartition` 静态值 | `handler_ctld.c::handle_forward_job` 直接读 conf | — |
| **V1** 多对端静态路由 | 1→N 集群，按 `(src_partition, app)` 选 | `broker.conf` 内联 `[Route]` 段 | 新增 `route.c::route_decide_static()` | `route.{c,h}` |
| **V2** 平台代理 | V1 + 平台动态决策 | 平台 `POST /v1/route`（用户权限+集群画像+软件清单），失败降级到 V1 | `route.c` 内分发：先调 platform，失败回 static | `platform_client.{c,h}` |
| **V3** 智能化 + portal | V2 + 主动入口 + 综合打分 | 平台输出"评分 + 推荐"；新增 portal HTTP 入口接受 explicit decision | 新增 `handler_platform.c`；`route.c` 接受 `decision_explicit` 跳过查表 | `handler_platform.{c,h}` |

> **路由真相源在演进中的迁移**：V0 / V1 真相源在 broker.conf；V2 起真相源逐步迁到平台，broker 持有的本地路由表仅作降级兜底；V3 时平台是唯一智能源，broker 退化为无脑执行枢纽。

#### 14.4.2 V0 现状盘点（当前 MVP_new）

代码事实（`handler_ctld.c::handle_forward_job` ~120-150 行）：

- ctld 通过 `BROKERD_REQUEST_FORWARD_JOB` 传入 `target_partition`，broker 直接采纳；
- ctld 不传 `target_cluster`，broker 取 `g_broker_conf.remote_cluster_name`；
- ctld 没传 `target_partition` 时取 `g_broker_conf.default_remote_partition`；
- `broker.conf` 只允许 1 个 `RemoteCluster*` / `DefaultRemotePartition`，多目的地不支持。

V0 的"路由"逻辑总共不到 30 行 C 代码。优点是端到端打通最快；缺点是路由语义没有专属抽象——这是 V1 阶段第一个动作要做的事：**抽出 `route.c`，把这 30 行变成稳定接口**。这是后续 V2 / V3 演进的"零号承重墙"。

#### 14.4.3 决策抽象层（V1 一次性引入，跨阶段不变）

V1 阶段新增 `route.{c,h}`，对外接口稳定，所有后续阶段（V2 / V3）只换实现不换签名。这是把"路由演进"对调用方完全透明化的关键。

```c
/* src/slurmbrokerd/route.h */

typedef struct route_request {
    /* 来自 ctld FORWARD_JOB 或平台 portal_submit */
    const char *src_cluster;
    const char *src_partition;
    const char *src_user;
    const char *app_name;

    /* 资源画像（V2 起被平台用，V0/V1 忽略） */
    uint32_t    cpus;
    uint32_t    mem_mb;
    uint32_t    nodes;
    uint32_t    time_limit_min;

    /* portal 路径 explicit decision */
    bool        decision_explicit;
    const char *explicit_target_cluster;
    const char *explicit_target_partition;
} route_request_t;

typedef struct route_decision {
    char    *target_cluster;
    char    *target_partition;
    char    *target_user;             /* user_mapping 二次填充 */
    char    *src_app_prefix;          /* lookup_software 源端结果 */
    char    *dst_app_prefix;          /* lookup_software 远端结果 */
    int      error_code;              /* 0 = SUCCESS, 复用 9001-9099 broker 私有空间 */
    char    *error_reason;            /* 仅日志 */
} route_decision_t;

/* 跨 V1→V3 不变 */
extern int  route_decide(const route_request_t *req,
                         route_decision_t      *out);
extern void route_decision_free(route_decision_t *out);
```

设计要点：

- `decision_explicit=true` 时（portal 路径），`route_decide` 直接采纳 explicit 字段，**跳过查表 / 查平台**；
- `error_code` 沿用 broker 9001-9099 私有错误空间，新增 `9010 BROKERD_ERR_NO_ROUTE` / `9011 BROKERD_ERR_PLATFORM_UNAVAILABLE`；
- target_user / app_prefix 由 `route.c` 内部串行调用 `user_mapping_lookup` 与 `lookup_software_path` 填充——**decision 是"路由 + 用户映射 + 路径解析"的一站式产出**。调用方（handler_ctld / handler_platform）只看 `route_decision_t`，不再各自调用三个子模块。

#### 14.4.4 V1 多对端静态路由

##### A. 配置载体：独立 `routes.conf` + `broker.conf::RouteSource=` 开关

`broker.conf` 仅持有"配什么源 / 文件路径在哪"的元信息，**真实的路由表在独立的 `/etc/slurm-broker/routes.conf` 文件**。这与 `LocalUser=` 内联到 broker.conf 的做法**刻意区分**——选型依据见后文 §14.4.4-G。

```ini
# /etc/slurm/broker.conf  (V1 起新增字段，与 V0 段并存)
# ============================================
# 多对端集群（V1 起允许多组）
# ============================================
RemoteCluster=wz_cluster   BrokerHost=broker.wz.example.com  BrokerPort=8443  MungeKey=/etc/slurm/munge_wz.key
RemoteCluster=hf_cluster   BrokerHost=broker.hf.example.com  BrokerPort=8443  MungeKey=/etc/slurm/munge_hf.key

# ============================================
# 路由决策来源（关键开关）
# ============================================
# inline   = V0 兼容：仅用 DefaultRemotePartition 单字段（默认值，零迁移成本）
# file     = V1：从 RoutesFile 加载静态规则
# platform = V2：调平台 API；失败按 PlatformFailureFallback 决定
# auto     = V2 推荐：platform 优先 → file → inline 三级降级
RouteSource=file
RoutesFile=/etc/slurm-broker/routes.conf
RoutesReloadOnSighup=yes      # SIGHUP 仅 reload 路由表，不动 broker 主进程
```

##### B. `routes.conf` 完整 schema

`routes.conf` 是独立文件，broker 用 Slurm 自带的 `s_p_parse_file` 直接 `fopen` 读，不走 `Include` 机制——**不破强约束 §1.2**。文件用 INI 风格，每条规则一个 `[route:<name>]` section：

```ini
# /etc/slurm-broker/routes.conf
# 由 broker 加载；SIGHUP 触发 routes_reload()，对在途作业零影响

[route:gpu_priority]
match_src_partition = xahgpu
match_app           = lammps-2Aug2023-intelmpi2018,vasp-6.4-gpu
match_resource_gpu  = ">0"           # 可选：含 GPU 资源的作业
target_cluster      = wz_cluster
target_partition    = wzgpu
priority            = 200            # 数值大者优先

[route:cpu_default]
match_src_partition = xahcnormal
match_app           = lammps-2Aug2023-intelmpi2018,vasp-6.4-ioptcell
target_cluster      = wz_cluster
target_partition    = wzhcnormal
priority            = 100

[route:fallback_normal]
match_src_partition = "*"            # 通配兜底
match_app           = "*"
target_cluster      = wz_cluster
target_partition    = wzhcnormal
priority            = 1              # 最低
```

匹配字段语义：

| 字段 | 必填 | 含义 | 取值 |
|---|---|---|---|
| `match_src_partition` | 是 | 源 partition 匹配 | 全名精确 / `*` 通配 |
| `match_app` | 是 | 应用名匹配 | 逗号分隔列表（任一命中即可）/ `*` 通配 |
| `match_resource_gpu` | 否 | GPU 资源条件 | `">0"` / `"=0"` / `">=4"` 等比较表达式 |
| `match_resource_cpu` | 否 | CPU 资源条件 | 同上 |
| `match_resource_mem_gb` | 否 | 内存条件 | 同上 |
| `match_user` | 否 | 用户名匹配 | 逗号分隔列表 / 留空表示不限 |
| `target_cluster` | 是 | 目标集群名 | 必须在 `broker.conf::RemoteCluster=` 中存在 |
| `target_partition` | 是 | 目标分区 | 不允许 `*`，必须为远端物理分区 |
| `priority` | 是 | 规则优先级 | int，**数值大者先评估** |

匹配算法：所有规则按 `priority` 降序排序，依次尝试 match_* 全部条件，**首条全部命中即用**。无规则命中时按 `RouteSource=auto` 模式继续向下游降级（V2 时降级到 platform 是另一个方向，但当前讨论 V1 内的"file → inline"降级）。

##### C. broker.conf 的 `RouteSource=` 三种模式行为表

| 模式 | 决策来源 | 找不到匹配时 | 用途 |
|---|---|---|---|
| `inline`（默认） | `DefaultRemotePartition` 单字段 | `BROKERD_ERR_NO_ROUTE` | V0 兼容、调试 |
| `file` | `routes.conf` 全表 | 若 `routes.conf` 含 `match_src_partition="*"` 兜底 → 用兜底；否则降级到 `inline`（仅当 `DefaultRemotePartition` 配了）→ 否则 `NO_ROUTE` | V1 主用 |
| `platform` | 仅平台 API | 平台失败按 `PlatformFailureFallback=` 决定（reject / file / inline） | V2 高可信场景 |
| `auto` | 平台 → file → inline 链式降级 | 三级都失败 → `NO_ROUTE` | V2 推荐 |

**三级降级实现位置不变**：在 `route.c::route_decide()` 里做 dispatch，对外签名跨阶段不变（§14.4.3）。

##### D. `route.c::route_decide()` V1 实现骨架（含 RouteSource 分发）

```c
/* src/slurmbrokerd/route.c (V1) */
int route_decide(const route_request_t *req, route_decision_t *out)
{
    if (req->decision_explicit)
        return _decide_explicit(req, out);

    switch (g_broker_conf.route_source) {
    case ROUTE_SOURCE_INLINE:
        return _decide_inline(req, out);

    case ROUTE_SOURCE_FILE:
        return _decide_file(req, out);
        /* _decide_file 内部 NO_ROUTE → 可选回 _decide_inline */

    case ROUTE_SOURCE_PLATFORM:                    /* V2 起 */
        return _decide_platform(req, out);

    case ROUTE_SOURCE_AUTO:                        /* V2 默认 */
        if (_decide_platform(req, out) == SLURM_SUCCESS) return SLURM_SUCCESS;
        if (_decide_file    (req, out) == SLURM_SUCCESS) return SLURM_SUCCESS;
        return _decide_inline(req, out);
    }
    return _decide_inline(req, out);  /* 不应到达 */
}

static int _decide_file(const route_request_t *req, route_decision_t *out)
{
    /* g_route_table 已按 priority 降序持久缓存 */
    foreach (rule in g_route_table) {
        if (!_part_match    (rule->match_src_partition, req->src_partition)) continue;
        if (!_app_match     (rule->match_app,           req->app_name))      continue;
        if (!_res_match_gpu (rule->match_resource_gpu,  req))                continue;
        if (!_res_match_cpu (rule->match_resource_cpu,  req))                continue;
        if (!_res_match_mem (rule->match_resource_mem,  req))                continue;
        if (!_user_match    (rule->match_user,          req->src_user))      continue;

        out->target_cluster   = xstrdup(rule->target_cluster);
        out->target_partition = xstrdup(rule->target_partition);
        info("route hit: rule=[%s] partition=%s app=%s -> %s:%s",
             rule->name, req->src_partition, req->app_name,
             rule->target_cluster, rule->target_partition);
        return _fill_user_mapping_and_app_prefix(req, out);
    }

    out->error_code = BROKERD_ERR_NO_ROUTE;        /* 9010 */
    out->error_reason = xstrdup_printf(
        "no route in routes.conf for src_partition=%s app=%s",
        req->src_partition, req->app_name);
    return out->error_code;
}
```

##### E. 热更新：`SIGHUP` → `routes_reload()`

`routes.conf` 独立文件 + `RoutesReloadOnSighup=yes` 时，broker `signal_handler` 收到 `SIGHUP`：

```text
1. 读 RoutesFile → 临时 g_route_table_new (priority 排序、字段校验)
2. 校验失败（语法/必填缺失/target_cluster 在 RemoteCluster 里找不到）→ 拒绝 reload, 老表继续工作, 写 error 日志
3. 校验通过 → atomic swap (g_route_table_old ↔ g_route_table_new)
4. 旧表延迟 5s 释放（让正在 _decide_file 的线程跑完）
5. info("routes reloaded: N rules, took Xms")
```

整个流程**不动 broker.conf 主配置**，不重启进程，对在途作业零影响——这正是把 routes.conf 与 broker.conf 拆开的核心收益。

##### F. 用户/运维可观测性（CLI）

```bash
# 列出当前生效路由表（按 priority 降序）
$ sbroker show routes
NAME              SRC_PART     APP                                 GPU    TARGET             PRIORITY
gpu_priority      xahgpu       lammps-2Aug2023,vasp-6.4-gpu       >0     wz_cluster:wzgpu   200
cpu_default       xahcnormal   lammps-2Aug2023,vasp-6.4-ioptcell  -      wz_cluster:wzhcn   100
fallback_normal   *            *                                   -      wz_cluster:wzhcn   1

# 测试某个具体作业会命中哪条规则（dry-run，不真转发）
$ sbroker route test --partition xahgpu --app lammps-2Aug2023-intelmpi2018 --gpu 4
[match] rule=gpu_priority -> wz_cluster:wzgpu
        user_mapping: test1 -> wz_test1
        src_app_prefix: /public/software/lammps/2Aug2023-intelmpi2018
        dst_app_prefix: /opt/scnet/apps/lammps/2Aug2023

# 交叉校验 broker routes 与 ctld partition.SendTo 是否一致
$ sbroker route check-ctld
[OK] xahcnormal → wz_cluster:wzhcnormal (ctld SendTo=wzhcnormal@wz_cluster)
[WARN] xahgpu → wz_cluster:wzgpu (ctld SendTo 未配置, 该 partition 不会被 ctld 跨域线程预筛)
```

> **与 ctld 端 `partition.SendTo` 的关系**：ctld `partition.SendTo` **不再是"决策依据"**，而是"路由可达性 hint"——让 ctld 跨域线程能预筛"哪些 partition 有跨域目标"。具体路由由 broker 的 `routes.conf` 决策。SendTo 与 broker `routes.conf` 必须保持一致（**至少 src_partition 这一维一致**），由 `sbroker route check-ctld` 校验工具检查（M19 子任务）。

##### G. 为什么独立 `routes.conf` 而不复用 broker.conf 内联（vs `LocalUser=` 决策对比）

| 维度 | `LocalUser=` 内联到 broker.conf（已落地） | `routes.conf` 独立文件（V1 选型） |
|---|---|---|
| 变更频率 | 试点期一次性配齐，几乎不变 | 月改 / 周改（加 partition 路由、调优先级、增 app） |
| 表达力需求 | 4 字段 KV 单行足够（local→remote 用户映射） | 多维匹配条件 + 优先级 + 通配，需 section 形式 |
| 热更新需求 | 重启 broker 即可（运维窗口可控） | 路由变化频繁，重启不可接受 |
| 运维分工 | 用户映射归 IDM / SRE | 路由策略归调度业务运维，独立文件审计链清晰 |
| 文件锁定 | broker 主配置变更属"重大变更"（端口/线程/超时） | 路由表变更应"小步快跑" |
| 与原生解析的耦合 | `Include` 实现依赖原生函数 → 必须内联（§1.2 强约束） | 独立 `s_p_parse_file` 不走 Include，**不破约束** |

`LocalUser=` 内联是被强约束**逼出来**的，不是首选；`routes.conf` 独立没有这层约束，应该按运维需求选最合适的方式。两者基于不同的运维特性做了不同的选型。

##### H. V1 改动量估算（修订）

| 文件 | 改动量 | 说明 |
|---|---|---|
| `broker_conf.{c,h}` | +60 行 | `RouteSource=` / `RoutesFile=` / `RoutesReloadOnSighup=` / 多 `RemoteCluster=` 行解析 |
| `routes_loader.{c,h}` 新增 | +250 行 | `routes.conf` `s_p_parse_file` 解析 + 字段校验 + priority 排序 |
| `route.{c,h}` 新增 | +400 行 | `route_decide` dispatch + `_decide_inline/_decide_file/_decide_explicit` |
| `routes_reload` 入口 | +80 行 | SIGHUP handler + 原子 swap + 5s 延迟释放 |
| `handler_ctld.c` | -30 +20 行 | "读 conf 字段"换成 `route_decide()` |
| `egress.c` | +30 行 | 多 broker 路由（按 `target_cluster` 选 host:port:munge） |
| `proto.h` | +1 错误码 9010 | `BROKERD_ERR_NO_ROUTE` |
| `src/sbroker/`（新工具） | +400 行 | `show routes` / `route test` / `route check-ctld` 三子命令 |
| 新增配置文件模板 | — | `etc/routes.conf.example` |

V1 阶段不动 RPC 字段顺序，PERMANENT 契约不破。`routes_loader` 新增独立模块的好处是 V2 时 `_decide_file` 仍然复用同一份解析器（因为 V2 把 file 作为降级源时仍然需要它）。

#### 14.4.5 V2 平台代理

##### A. 配置扩展（与 V1 `RouteSource=` 联动）

```ini
# V2 起新增
RouteSource=auto              # 推荐: platform → file → inline 三级降级
                              # 也可用 platform 强制只走平台

PlatformApiUrl=https://platform.example.com/api
PlatformApiToken=<bearer_token>
PlatformTimeoutSec=2

# RouteSource=platform 时的失败行为（auto 模式不需要这条）
PlatformFailureFallback=file  # file = 降级到 routes.conf
                              # inline = 降级到 DefaultRemotePartition
                              # reject = 直接 NO_ROUTE
```

##### B. `route.c::_decide_platform()` 实现思路

```c
static int _decide_platform(const route_request_t *req, route_decision_t *out)
{
    if (!g_broker_conf.platform_api_url) {
        out->error_code = BROKERD_ERR_PLATFORM_UNAVAILABLE;
        return out->error_code;
    }

    int rc = platform_client_route(req, out,
                                   g_broker_conf.platform_timeout_s);
    if (rc == SLURM_SUCCESS)
        return _fill_user_mapping_and_app_prefix(req, out);

    /* 平台不可达 / 超时 / 5xx */
    /* RouteSource=platform 单一模式才检查 PlatformFailureFallback;
     * RouteSource=auto 下由上层 route_decide 自动降级，这里只返错。 */
    if (g_broker_conf.route_source == ROUTE_SOURCE_PLATFORM) {
        switch (g_broker_conf.platform_failure_fallback) {
        case FALLBACK_FILE:    return _decide_file  (req, out);
        case FALLBACK_INLINE:  return _decide_inline(req, out);
        case FALLBACK_REJECT:  /* fallthrough */ ;
        }
    }
    out->error_code = BROKERD_ERR_PLATFORM_UNAVAILABLE;  /* 9011 */
    return out->error_code;
}
```

> 这里把"降级链"集中在 `route_decide()` 顶层（`auto` 模式）和 `_decide_platform()`（`platform + Fallback`）两处，让单一降级路径不会绕回自身造成死循环。

##### C. 平台契约（HTTP）

```text
POST /v1/route
Authorization: Bearer <PlatformApiToken>
Content-Type: application/json

Request:
{
  "src_cluster":"xian_cluster",
  "src_partition":"xahcnormal",
  "src_user":"test1",
  "app":"lammps-2Aug2023-intelmpi2018",
  "cpus":32, "mem_mb":131072, "nodes":1, "time_limit_min":120
}

Response 200:
{
  "target_cluster":"wz_cluster",
  "target_partition":"wzhcnormal",
  "target_user":"wz_test1",                                  // 可选, 缺则走 broker user_mapping
  "src_app_prefix":"/public/software/lammps/2Aug2023-intelmpi2018",  // 可选
  "dst_app_prefix":"/opt/scnet/apps/lammps/2Aug2023",                // 可选
  "decision_id":"<uuid>",                                    // 仅元数据，可写入 broker 日志便于追溯
  "decision_score":0.87
}

Response 4xx/5xx → broker 视 PlatformFailureFallback 降级或拒绝
```

##### D. 决策真相源迁移

V2 上线后路由真相源开始**双轨**：

- **平台**：主源（智能决策、用户权限、集群画像）
- **broker 本地静态表**：兜底源（平台不可达时的最低可用保证）

运维通过 `PlatformFailureFallback=static|reject` 旋钮控制降级策略。集群网络成熟后建议 `reject`，让平台故障显式暴露。

#### 14.4.6 V3 智能化路由 + portal 入口

##### A. 平台决策维度升级（broker 侧无感）

V3 阶段平台 `POST /v1/route` 输入字段不变，输出字段保持向后兼容（只增不改），broker 端 `route.c` 一字不动。**平台端打分维度扩展**：

- 用户跨域权限矩阵（`user × cluster × partition`）
- 集群实时画像（在线 / 负载 / 队列长度 / 排队预估）
- 软件清单（`cluster × app` 是否就绪 + 版本）
- 数据局部性（用户输入数据已经在哪个集群）
- 全局调度策略（公平性 / 配额 / 优先级）

平台输出多维评分排序，broker 拿到的仍然只是 top-1 决策。

##### B. portal 入口（broker 侧需新增）

V3 引入第三类作业路径：用户在平台 UI 提交 → 平台决策 → 平台调 broker 提交 → broker 执行流水线。需要 broker 新增 RPC `8005 BROKERD_REQUEST_PORTAL_SUBMIT`（PERMANENT，平台 → broker）：

```c
typedef struct brokerd_portal_submit_msg {
    char    *src_cluster;        /* 平台标识源；可填 "platform" */
    char    *src_user;
    char    *app_name;
    char    *script_path;        /* 平台暂存路径 (NFS/S3) */

    /* explicit decision */
    char    *target_cluster;
    char    *target_partition;
    char    *target_user;        /* 可空, 走 user_mapping */

    /* 数据 stage 指令 */
    char    *data_in_url;        /* 例 s3://... 或 NFS 绝对路径 */
    char    *data_out_url;

    /* 平台回调 */
    char    *callback_url;       /* portal_job_id 状态推送 */
    char    *portal_job_id;
} brokerd_portal_submit_msg_t;
```

新增 `handler_platform.c::handle_portal_submit` 流程：

```text
1. brokerd_wire_parse 反包
2. 鉴权: 平台 token 校验（不走 munge, 走专用 bearer token）
3. 构造 route_request_t {
       decision_explicit       = true,
       explicit_target_cluster, explicit_target_partition }
4. route_decide()      // 直接采纳, 仅做 user_mapping + lookup_software
5. broker_job_create + role=ORIGINATOR, src_cluster="platform"
6. broker_job_table_add + persist_async_request
7. 入 state_machine, 与现有 sbatch 流水线 100% 复用
8. sync_ticker / state_machine 在状态变化时 HTTP POST callback_url
```

V3 阶段 `broker.conf` 增量：

```ini
# V3 portal 入口（独立监听端口与 token 文件）
EnablePortalEndpoint=yes
PortalEndpointPort=8444
PortalAllowedTokens=/etc/slurm/portal_tokens.conf
PortalCallbackTimeoutSec=5
```

##### C. 数据传输扩展（与本路由演进解耦）

V3 portal 作业的输入数据可能来自 S3 / 平台对象存储 / 用户家目录三种源。`stage.c` 当前仅支持 rsync over SSH，V3 起需要扩展为多 driver 模式（rsync / s3cmd / aws-cli）。该改造与本路由演进解耦，单独里程碑 M22 处理。

#### 14.4.7 三方协作矩阵（按阶段）

| 阶段 | ctld 端职责 | broker 端职责 | 平台端职责 |
|---|---|---|---|
| V0 | 跨域线程 + 传 target_partition（hint）| 接 ctld 转发 + 单目标执行 | — |
| V1 | 跨域线程 + 不传 target（broker 自决）；`partition.SendTo` 改为可达性 hint | `route_decide_static` + 多目标执行 | — |
| V2 | 同 V1 | `route_decide_platform` + V1 降级 | 提供 `POST /v1/route` |
| V3 | 同 V1 | + portal 入口 + 多 stage driver | `/v1/route` 升级 + portal UI + 用户权限/画像/软件清单 |

> **ctld 端在 V0→V3 全程仅一次代码侧微调**（V0→V1 时把 ctld 跨域线程发给 broker 的 `target_partition` 字段从"硬路由"改成"传 hint 或不传"），其余所有改动落在 broker 与平台。这是 broker 作为"业务承载层 + 跨域执行枢纽"的核心价值——**演进风险被收敛在 broker 单一进程内**。

#### 14.4.8 用户可观测性按阶段

| 用户场景 | V0 / V1 | V2 | V3 |
|---|---|---|---|
| **S1 提交前看路由** | `scontrol show partition`（ctld SendTo） + `sbroker show routes` | 平台 UI 配置中心（主） + `sbroker show routes`（兜底） | 平台 UI |
| **S2 提交后看跨域意图** | `squeue` + `scontrol show job`（看 cd_remote_*） | 同左 | 平台 UI 主显示 |
| **S3 实际跨到哪** | `squeue --remote` / `sacct Remote_*` | 同左 | 同左 + 平台 UI |
| **S4 智能预测** | 不支持 | 不支持 | 平台 UI 提交向导 |
| 取消作业 | `scancel <local_jobid>` | 同左 | scancel 或平台 UI |

S1 / S2 / S3 在 V0→V3 始终用 Slurm 标准客户端工具（scontrol / squeue / sacct）就能看清，**broker 不需要做客户端命令扩展**（仅运维用 `sbroker show routes`）。S4 是 V3 平台 UI 的责任，不进 scontrol，避免污染 Slurm 客户端契约。

#### 14.4.9 演进里程碑

| 里程碑 | 阶段 | 模块 / 文件 | 入侵 ctld？ | 入侵 src/common？ | 工作量 |
|---|---|---|---|---|---|
| **M17** 路由抽象层 + RouteSource 开关 | V0→V1 准备 | 新增 `route.{c,h}`；`broker_conf.c` 加 `RouteSource=` / `RoutesFile=`；`handler_ctld.c` 适配；`_decide_inline` 实现（保持 V0 行为） | 否 | 否 | 1.5 周 |
| **M18** routes.conf 加载器 + 多对端 | V1 | 新增 `routes_loader.{c,h}`（独立 `s_p_parse_file` + 字段校验 + priority 排序）；`broker_conf.c` 加多 `RemoteCluster=`；`egress.c` 按 target 路由；`_decide_file` 实现 | 否 | 否 | 1.5 周 |
| **M19** sbroker 工具 + SIGHUP reload | V1 | 新增 `src/sbroker/`（`show routes` / `route test` / `route check-ctld`）；`routes_reload()` SIGHUP 入口 + 原子 swap | 否 | 否 | 1.5 周 |
| **M20** 平台代理 | V2 | 新增 `platform_client.{c,h}`；`_decide_platform` + `RouteSource=auto` / `RouteSource=platform` 分发；`PlatformFailureFallback=` 行为 | 否 | 否 | 2 周 |
| **M21** portal 入口 | V3 | 新增 `handler_platform.{c,h}`；`proto_pack.c` +1 msg_type 8005；`broker.conf` 增 portal 段；rate limit | 否 | 否 | 2 周 |
| **M22** 多 stage driver | V3（独立） | `stage.c` 重构为 driver 模式（rsync / s3cmd / aws-cli） | 否 | 否 | 2 周 |

V0→V3 全链路约 9.5 周，**与 ctld PR 完全解耦**：M17-M19 不需要 ctld 任何改动，M20-M21 仅需要平台侧 API 上线，M22 与 ctld 也无关。

#### 14.4.10 与 §14.3 不变契约的衔接

V1 一次性引入的下列契约从此进入 §14.3 "不再变动"清单：

- `route.h::route_decide()` 函数签名
- `route.h::route_request_t` / `route_decision_t` 字段顺序
- 8005 `BROKERD_REQUEST_PORTAL_SUBMIT` 字段顺序
- 平台 `POST /v1/route` 请求/响应 schema
- **`routes.conf` section schema**（`[route:<name>]` 内 `match_*` / `target_*` / `priority` 字段名）——只允许追加新的 `match_*` 维度（如 `match_resource_*`），不允许删除/重命名已发布字段
- `broker.conf::RouteSource=` 取值集合（`inline` / `file` / `platform` / `auto`）

未来的智能化升级（V3+）只允许在 broker 侧扩展 `route.c` 内部实现、在平台侧打分维度演进，对外契约不动。

---

## 十五、附录

### 15.1 文件清单 (含部署, ★ v2.0 新增 routes.conf 与 3 个 M16 模块)

```
源码 (Slurm 树):
src/slurmbrokerd/  (★ v2.0 共 21 个 .c/.h, 详见 §8.1)
  routes_loader.c/.h    # ★ v2.0 新增
  route.c/.h            # ★ v2.0 新增
  cap_check.c/.h        # ★ v2.0 新增

构建:
configure.ac (L540)        # 加 src/slurmbrokerd/Makefile
src/Makefile.am (L21)       # 加 slurmbrokerd 子目录

部署模板:
etc/slurmbrokerd.service.in
etc/lookup_software.sh.example       # ★ M11 新增
etc/software_routes.conf.example     # ★ M11 新增
etc/routes.conf.example               # ★ v2.0 新增 (M19 待补)

部署目标:
/usr/sbin/slurmbrokerd
/usr/lib/systemd/system/slurmbrokerd.service
/etc/slurm/broker.conf                # 单文件（含 LocalUser= 行, ★ v2.0 增 RouteSource/SubmitMode 段）
/etc/slurm/routes.conf                # ★ v2.0 新增, 路由表
/etc/slurm/broker_id_ed25519
/etc/slurm/munge.key
/opt/slurm-broker/scripts/lookup_software.sh
/opt/slurm-broker/conf/software_routes.conf
/var/spool/slurm/broker/broker_state.jsonl{,.tmp,.old}
/var/log/slurm/slurmbrokerd.log
/var/log/slurm/broker_stage/<trace_id>.log
```

### 15.2 排错 Playbook

#### Q1：跨域作业卡 PENDING(Held)，broker 没收到

```bash
# 1. broker 日志
journalctl -u slurmbrokerd | grep <local_jobid>
# 期望: "trace_id=xxx-<jobid> accepted"

# 2. ctld 端是否走了跨域链路
scontrol show job <jobid> | grep -E '(CrossRegion|RouteExhausted|Reason)'

# 3. broker 端口是否监听
ss -tlnp | grep -E '8442|8443'
```

#### Q1.5 (★ v2.0)：作业被置 RouteExhausted=1, scontrol 显示 reason=BrokerNoRoute

```bash
# 1. broker route_decide 日志
journalctl -u slurmbrokerd | grep -E 'route_decide|NO_VIABLE_ROUTE|ALL_ROUTES_EXHAUSTED'

# 2. 检查 routes.conf 是否覆盖该 src_partition + cd_app_name
cat /etc/slurm/routes.conf | grep -A6 "Src.*= ${src_cluster}:${src_partition}"

# 3. 检查 user_mapping
grep "LocalUser=${user}" /etc/slurm/broker.conf

# 4. 如确认是配置问题, 修复后 SIGHUP + 手工清旗:
systemctl reload slurmbrokerd        # 触发 routes_loader_reload
scontrol update jobid=${jobid} CdRouteExhausted=0
scontrol release ${jobid}
```

#### Q1.6 (★ v2.0)：作业卡在 BROKER_INIT.PROBING, 反复 test-only 但都失败

```bash
# 1. 看远端 broker 的 test_only handler 日志
ssh broker-B 'journalctl -u slurmbrokerd | grep _handle_test_only'

# 2. 远端 ctld 是否拒绝 (资源不足 / ACL / QOS)
ssh slurm-mgr-B 'tail -F /var/log/slurm/slurmctld.log | grep test_only'

# 3. 强制走另一候选: 修改 routes.conf 把当前候选 priority 调低 + SIGHUP
```

#### Q2：broker 收到，但 STAGING_IN 失败

```bash
# 1. 看 stage 日志
ls -lt /var/log/slurm/broker_stage/ | head
cat /var/log/slurm/broker_stage/<trace_id>.log

# 2. 手工验证 rsync
sudo -u test1 /usr/bin/rsync -av --partial \
  --rsync-path "sudo -n -u wz_test1 rsync" \
  -e "ssh -i /etc/slurm/broker_id_ed25519 -l slurm-broker" \
  /work/home/test1/case1/  broker.wz.example.com:/work/home/wz_test1/.burst/xian-test/
```

#### Q3：远端 sbatch 失败

```bash
# 1. broker 日志
journalctl -u slurmbrokerd | grep -E "_sudo_sbatch|REMOTE_SUBMIT_FAILED"

# 2. 手工跑改写后脚本
sudo -u wz_test1 sbatch /work/home/wz_test1/.burst/xian/12345/run.sh.cd_modified.sh

# 3. account/partition association
sacctmgr show association user=wz_test1 \
  format=user,account,partition,defaultaccount%-15
```

#### Q4：跨集群 munge 校验失败

```bash
md5sum /etc/munge/munge.key             # 两边一致？
ps aux | grep munged                    # --ttl 86400？
ssh broker.wz.example.com date; date     # 时钟漂移？
```

#### Q5：状态文件损坏

```bash
sudo systemctl stop slurmbrokerd
sudo cp /var/spool/slurm/broker/broker_state.jsonl{,.broken}
sudo mv /var/spool/slurm/broker/broker_state.jsonl{.old,}
sudo systemctl start slurmbrokerd
```

#### Q6：lookup_software.sh 被频繁 fork

```bash
# 当前无 LRU 缓存；每次 rewrite_job_script 调 2 次。
# 100 forward/s × 2 = 200 fork/s × ~50ms = 10s/s -> CPU 1 核 100%
# 缓解: 把 software_routes.conf 内容做成内置常量；或将 software.c
#       升级为 RDB / API 调用。两者都不破坏 lookup_software_path() 契约。
```

### 15.3 LEGACY_M04_TRANSITIONAL 清理速查

```bash
# 列出所有 27 处
grep -rn LEGACY_M04_TRANSITIONAL src/slurmbrokerd/

# 分布
proto.h        : 12 处
proto_pack.c   :  8 处
proto.c        :  6 处
handler_ctld.c :  1 处
```

清理 PR 完成后这条命令应返回 0。

### 15.4 与旧版文档的字段对照

| 旧版字段/术语 | 新版字段/术语 | 备注 |
|---|---|---|
| `broker_job_t.job_desc` (`job_desc_msg_t *`) | `broker_job_t.app_name` (`char *`) + `broker_job_t.script_path` (`char *`) | 解耦 Slurm 版本 |
| `Include user_mapping.conf` | `LocalUser=...` 行写在 `broker.conf` 内 | 不破原生解析器 |
| `REQUEST_FORWARD_JOB` (msg_type 由 ctld 注册) | `BROKERD_REQUEST_FORWARD_JOB` (8001, LEGACY_M04_TRANSITIONAL) | ctld PR 落地后回归原名 |
| `comment` 字段污染 | 永不写 `comment` | 与旧版强约束一致 |
| 无 `state_machine_resume_inflight` | 重启后调一次，拨快在途 stage 时间戳 | 优化用户感知 |
| 无 `--partial` rsync option | 加上 `--partial`，断点续传 | 配合上面恢复机制 |
| Egress Worker 独立线程 | egress 调用同步运行在调用线程 | 简化线程模型 |
| 无 self-pipe trick | listener / state_machine / sync_ticker 三处都装 | 防 fd 重用竞争 |

---

## 修订记录

| 版本 | 日期 | 变更 | 作者 |
|---|---|---|---|
| MVP | 2026-04-27 | 旧版裁剪文档，规划期产出 | — |
| MVP-1.1 / MVP-1.2 | 2026-04-27 | 旧版迭代（详见旧文件） | — |
| **MVP_new** | 2026-05-03 | **基于已落地代码（M02–M13 + M11 完成、M14 废弃）回写**；§1 强约束规则前置；§3.3 线程拓扑改为 7 线程组；§4.1 `broker_job_t` 移除 `job_desc`、新增 `app_name`；§4.4 新增 `state_machine_resume_inflight`；§5 状态机 tick 改为两阶段 action queue；§6 RPC 拆为 ctld↔broker（4 LEGACY）+ broker↔broker（7 PERMANENT），新增 §6.5 LEGACY 清理流程；§8 文件清单 18 个（新增 `proto_pack.c` `util_exec.c/.h` `software.c/.h`）；§10.1 `broker.conf` 用户映射改为内联 `LocalUser=...` 行；§11.5 标注 M15 缺省项；§12 进度盘点表；§14 新增"短期：清理 LEGACY"演进项；§15.4 新旧字段对照表 | — |
| MVP_new.1 | 2026-05-03 | 新增 §3.4 "Egress Worker 为什么不在线程图里"：解释 M08-T1 取消独立线程的决策依据，列出 7 个出站 RPC 的真实执行线程，给出未来回头独立成 worker pool 的触发条件 | — |
| **MVP_new.2** | 2026-05-05 | **新增 §14.4 "路由能力演进路径（固定路由 → 智能路由）"专章**：定义 V0/V1/V2/V3 四阶段；V1 引入 `route.{c,h}` 决策抽象层（`route_decide()` 跨阶段不变签名）+ `broker.conf` 多对端 / `Route=` 静态表 + `sbroker show routes`；V2 引入 `platform_client.c` 调平台 `POST /v1/route` + 静态表降级；V3 引入 `handler_platform.c` + `8005 BROKERD_REQUEST_PORTAL_SUBMIT` + portal 监听端口；明确路由真相源迁移路径（broker.conf → 平台）、ctld `partition.SendTo` 退化为可达性 hint、三方协作矩阵、用户可观测性按阶段、6 个里程碑（M17-M22 共 9.5 周）；§14.2 中"Portal 作业 / 多对端集群"两行收敛指向新章；§14.3 不变契约清单延伸 4 项（`route_decide` 签名、`route_request_t/decision_t` 字段顺序、8005 字段顺序、`/v1/route` HTTP schema） | — |
| **MVP_new.3** | 2026-05-05 | **§14.4.4 路由配置载体改为独立 `routes.conf` 文件**：撤销原 broker.conf 内联 `Route=` 行的方案（实现便利驱动而非需求驱动）；新增 `RouteSource=inline\|file\|platform\|auto` 开关协调 V0→V1→V2 演进过渡；`routes.conf` 用 INI `[route:<name>]` section 形式，支持 `match_resource_gpu` / `match_user` 多维匹配 + `priority` 显式排序 + 通配兜底；新增 `routes_loader.{c,h}` 独立模块（不走 Slurm 原生 `Include`，不破强约束）；新增 SIGHUP 触发 `routes_reload()` 原子 swap 热更新（5s 延迟释放老表）；§14.4.4-G 新增 "为什么独立而非内联" 决策对比表，与 `LocalUser=` 内联方案做了显式区分；§14.4.5 V2 平台代理与 `RouteSource=auto` 三级降级链联动；§14.4.9 里程碑表 M17/M18/M19 改写体现 `RouteSource` 开关、`routes_loader` 模块、SIGHUP reload 三个新工件；§14.4.10 / §14.3 契约延伸 2 项（`routes.conf` section schema、`RouteSource=` 取值集合） | — |
| **★ MVP-v2.0** | 2026-05-14 | **重大架构变更：路由能力完整下沉到 broker (V1 决策抽象层一次性引入)，ctld 退化为只识别 / hold / 转发 / 回写**。本版与 v1.5 不向前兼容, ctld 必须同步升级。变更点: §1 头部表"v1.x → v2.0 顶层差异"; §2 US-1/US-3/US-4 扩 broker 路由职责, 新增 US-5 routes.conf 热加载; §2.2.A `forward_job` payload 瘦身, 新增 8018/8019 test-only RPC, `update_remote_state` 首次包必带 remote_cluster_name/remote_partition_name; §3.2 端到端时序图增 route_decide/cap_check/test_only_probe 三步; §4.1 `broker_job_t` 增 `src_partition` / `selected_route_id` / `init_phase` / `route_candidates[]` / `route_attempted_mask` / `route_current_idx`, 删 `account` 字段; 新增 `route_candidate_t`; §4.3 JSONL schema 同步扩字段; §5.1/5.2 INIT 状态扩为 DECIDING/PROBING/SELECTED/EXHAUSTED 四子态; 新增 §5.5 详细流程; §6.3 RPC 表增 8018/8019; §6.4 错误码增 9010-9013/9020; 新增 §6.4.1 错误码 → ctld 错误码映射; §6.6 egress API 增 `egress_test_only_async`; 新增 §6.6.1/6.6.2 实现要点; §7.1 originator handler 分 A/A.1/A.2/A.3/A.4 四子态分别实现; receiver handler 反向 user_mapping 校验保留; §8.1 模块清单增 routes_loader.c/route.c/cap_check.c 三件套 (~600 LoC); 新增 §8.A/8.B/8.C/8.D 三大模块详细设计; §10.1 broker.conf 增 RouteSource/RoutesConfPath/RoutesReloadMode/RoutesMtimePollSec/SubmitMode/RemoteAllowedCheck/TestOnlyTimeoutSec/TestOnlyMaxCandidates 8 字段; 新增 §11.6 v1.5 → v2.0 升级路径 + 回滚 SOP; §12 Sprint 表新增 M16-M19 4 个里程碑; §13.2/13.3/13.4 增 v2.0 异常/性能/限制; §15.1 文件清单同步; §15.2 排错 Playbook 增 Q1.5/Q1.6 (RouteExhausted / PROBING 卡住); 全文术语 `cross_domain` → `cross_region` 同步对齐 | — |
