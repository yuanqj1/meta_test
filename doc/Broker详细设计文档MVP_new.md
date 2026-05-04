# Slurm 跨域 Broker (`slurmbrokerd`) MVP 详细设计文档（实现版）

> **版本**：MVP_new（基于已落地代码回写）
> **状态**：实施中—M02 / M03 / M04 / M05 / M06 / M07 / M08 / M09 / M10 / M11 / M12 / M13 已交付；M01 实质完成；M14 暂废弃；M15/M16 待补
> **基线**：`Broker详细设计文档MVP.md`（裁剪版规划）+ `跨域调度MVP整体方案.md`
> **目标**：本文档替代旧版裁剪文档，作为后续维护、培训、验收的**当前真相 (source of truth)**

---

## 一、文档目的与定位

### 1.1 与旧版的关系

旧版 `Broker详细设计文档MVP.md` 是 8 周工期前的"目标设计"，部分接口（`job_desc_msg_t` 透传、`Include user_mapping.conf`、ctld 直接走 native RPC）在落地过程中根据强约束做了重大调整。本文档是落地后的等价物，反映**当前 `src/slurmbrokerd/` 中真实代码的样子**，旧版文档保留作历史。

### 1.2 强约束（最重要——决定一切实现选择的指挥棒）

```
所有改动均在 src/slurmbrokerd/ 下；slurm 原生源码禁止修改；
所有逻辑都放到 src/slurmbrokerd/ 下；如必须扩展原生函数行为，
通过在 src/slurmbrokerd/ 内新增包装函数实现。
```

这条规则贯穿所有模块设计，并直接驱动了下列三个**与旧版不同**的关键技术决策：

| 旧版假设 | 实际落地 | 触发原因 |
|---|---|---|
| `broker_job_t` 内嵌 `job_desc_msg_t *job_desc`，跨集群透传 | 改为 `app_name` + `script_path` 两个平铺字段 | `job_desc_msg_t` 是 Slurm 版本绑定结构；A=24.05 ↔ B=23.11 跨集群必须解耦 |
| 4 个 `ctld↔broker` RPC 直接复用 Slurm 的 `pack_msg`/`unpack_msg` 大 switch | broker 侧用一份私有 wire frame（`'BRKR'` 魔数 + `auth_g_pack` + 自定义 packing）实现，标 `LEGACY_M04_TRANSITIONAL`；待 ctld 同事在 `src/common/` 注册同名 msg_type 后机械删除 | 原生 `slurm_protocol_pack.c` 不准动 |
| `Include /etc/slurm/user_mapping.conf` 二级配置 | 用户映射直接在 `broker.conf` 内联 `LocalUser=...` 行（用 `S_P_LINE`） | `Include` 在 `parse_config.c` 的实现依赖原生函数；改为单文件解析后无须扩展原生解析器 |

### 1.3 broker 端 MVP 必做的对外契约（不可简化）

5 项接口契约与旧版完全一致；落地差异只在**实现路径**。

1. **接收 ctld 转发** — `BROKERD_REQUEST_FORWARD_JOB`（msg_type=8001，`LEGACY_M04_TRANSITIONAL`）
2. **回写 ctld 远端状态** — `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE`（8003，`LEGACY_M04_TRANSITIONAL`）；写 `remote_*` 独立字段，**永不写 `comment`**
3. **通知 ctld 终态** — `BROKERD_REQUEST_BROKER_TERMINAL_STATE`（8004，`LEGACY_M04_TRANSITIONAL`）
4. **改写 source 行** — 受 receiver 端 `rewrite.c` 调 `software.c::lookup_software_path()` 触发，broker 自包含
5. **处理 scancel 反向** — `BROKERD_REQUEST_BROKER_CANCEL`（8016，dual-purpose：ctld→broker 与 broker→broker 共用同号）

> 说明：8001/8003/8004 是 broker 私有占位定义。一旦 ctld 同事的 PR 在 `src/common/slurm_protocol_defs.h` 注册了**同号且同字段顺序**的 msg_type，broker 端这 3 个 `LEGACY_M04_TRANSITIONAL` 标记的代码块会被一次性删除（共 27 处，详见 §6.5）。

---

## 二、MVP 范围

### 2.1 用户故事（不变）

| ID | 故事 | broker 端职责 |
|---|---|---|
| US-1 | sbatch `--cross-domain --app=...` 提交 | 接收 ctld 转发 → stage 输入 → 远端 broker 提交 → 同步状态 → 终态回写 |
| US-2 | scancel 反向传播 | 接收 ctld 取消 → 跨集群传播 → 远端 `slurm_kill_job` |
| US-3 | `squeue --remote` / `sacct` 看跨域字段 | sync_ticker 周期推送 `remote_*` 独立字段 |
| US-4 | broker 重启恢复 | JSONL 持久化 + trace_id 幂等 + `state_machine_resume_inflight` |

### 2.2 范围分层

#### A. 对外接口（必须完整、已经实现）

| # | 项 | 落地位置 |
|---|---|---|
| E1 | ctld → broker `FORWARD_JOB` | `proto.h::brokerd_forward_job_msg_t` + `handler_ctld.c::handle_forward_job` |
| E2 | broker → broker 5 RPC（FORWARD / STAGED_IN / QUERY_STATUS / CANCEL / CLEANUP） | `proto.h::brokerd_broker_*_msg_t` + `handler_remote.c::handle_broker_*` + `egress.c::egress_*_async` |
| E3 | broker → ctld `UPDATE_REMOTE_STATE` 字段化 | `egress.c::ctld_update_remote_state` |
| E4 | broker → ctld `TERMINAL_STATE` | `egress.c::ctld_inject_terminal_state` |
| E5 | broker → 本机 `lookup_software.sh` | `software.c::lookup_software_path` |

#### B. broker 内部实现（实际落地版本）

| # | 旧版规划 | 落地实现 | 结论 |
|---|---|---|---|
| I1 | 9 状态 + 错误分支合并 | 9 状态保留，state_machine 用**两阶段** action queue（Phase1 持锁扫描入队、Phase2 释锁执行 RPC）；`_on_xxx` 9 个分支函数；错误码归并为 `BROKERD_ERR_*` 9 个 | ✅ 完成 |
| I2 | JSONL（每行一个 `broker_job_t`） | 完全 JSONL，`broker_job.c::broker_job_to_json/from_json`；不再嵌入 base64 `job_desc`（已删除该字段） | ✅ 完成、字段精简 |
| I3 | 跨集群 Slurm RPC + 共享 Munge | broker→broker 用 broker **私有 wire frame**（`'BRKR'` 魔数 + `auth_g_pack` + 自定义 payload）；底层仍走 `slurm_msg_sendto`/`slurm_msg_recvfrom_timeout`；auth 复用 Munge 插件（`auth_g_create/pack/verify`） | ✅ 完成、解耦更彻底 |
| I4 | 单对端集群 | `broker_conf_t` 单 `remote_cluster_name` / `remote_broker_host` 字段 | ✅ 不变 |
| I5 | 单 `rewrite.c` 替代 v0.1 三 translator | `rewrite.c` 单文件 + `software.c` 调用 `lookup_software.sh` | ✅ 完成 |
| I6 | `sudo -u <remote_user> mkdir -p` | `handler_remote.c::_create_dst_work_dir` 内嵌 `sudo -n -u ... /bin/sh -c 'mkdir -p && chmod 700'` | ✅ 完成 |
| I7 | `sudo -u <local_user> rsync` | `stage.c::_exec_rsync` 通过 `fork+execvp` 跑 `sudo -n -u <src_user> /usr/bin/rsync ... --partial ...`，stdout/stderr 重定向到 `/var/log/slurm/broker_stage/<trace_id>.log` | ✅ 完成 |
| I8 | sync_ticker 10s | `g_broker_conf.poll_interval` 默认 10s | ✅ 完成 |
| I9-I13 | sbroker / metrics / SIGHUP 热更等不做 | 同旧版，未实现 | ✅ 不做 |

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

### 3.2 端到端时序

```
sbatch (US-1)                                      scancel (US-2)
─────────────                                      ──────────────
1. user sbatch --cross-domain --app=...           1. user scancel <local_jobid>
2. ctld(A) 入队、判定 cross_domain               2. ctld(A) 标 cancel
3. ctld(A) → broker(A) :FORWARD_JOB(8001)        3. ctld(A) → broker(A) :CANCEL(8016)
4. handler_ctld::handle_forward_job              4. handler_ctld::handle_cancel_from_ctld
   - 溢出保护 + user_mapping_lookup                  - 设 cancel_requested = true
   - broker_job_create + broker_job_table_add    5. state_machine 下一 tick (cancel 优先级)
   - state = INIT, persist_async_request               -> egress_cancel_async (8016)
5. state_machine tick (1Hz)                          -> transition CANCELLED
   _on_init: stage_submit_in + transition STAGING_IN
6. stage worker (4 个之一):
   - sudo -u src_user rsync -e ssh ... --partial
   - 完成 → state_machine_transition STAGED_IN
   - egress_staged_in_async (8012)
7. broker(B) handler_broker_staged_in
   - rewrite_job_script (rewrite.c)
   - sudo -u remote_user sbatch *.cd_modified.sh
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

### 4.1 `broker_job_t`（落地版）

```c
/* src/slurmbrokerd/broker_job.h */

#define BROKER_TRACE_ID_LEN 48

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
    char      *remote_user_name;
    uint32_t   remote_uid;
    uint32_t   remote_gid;
    char      *account;                         /* 仅源端日志/追溯，远端 sbatch 时 NOT 传递 */

    /* cluster routing */
    char      *src_cluster;
    char      *dst_cluster;
    char      *target_partition;

    /* working directories */
    char      *src_work_dir;
    char      *dst_work_dir;
    char      *script_path;                     /* originator 绝对路径；receiver 用 basename() */

    /* application identity (drives rewrite + lookup_software.sh) */
    char      *app_name;                        /* ★ 取代旧版的 job_desc_msg_t */

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

> **关键变更（vs 旧版 §4.1）**：
> 1. 删除 `job_desc_msg_t *job_desc;`，新增 `char *app_name;` —— **彻底解耦 Slurm 版本绑定**。receiver 拿 `app_name + script_path + remote_user_name + target_partition + dst_work_dir` 五元组就足以完成本地 sbatch。
> 2. `broker_job.h` 不再 `#include <slurm/slurm.h>`，让该 header 可以在任何 Slurm 版本间复用。
> 3. 因为 `job_desc` 没了，JSON 序列化里也再没有 `job_desc_b64` 字段（旧版 §4.3 的 base64 嵌入设计随之作废）。

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

JSON schema（实际样本）：

```json
{"trace_id":"xian-12345","src_job_id":12345,"remote_job_id":8888,
 "role":0,"hop_count":0,
 "src_user_name":"test1","src_uid":20001,
 "remote_user_name":"wz_test1","remote_uid":20001,"remote_gid":20001,
 "account":"ac_lab",
 "src_cluster":"xian_cluster","dst_cluster":"wz_cluster",
 "target_partition":"wzhcnormal",
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

### 4.4 重启重接力（`state_machine_resume_inflight`）

旧版没有这个机制。新增动机：

> 重启后所有恢复条目继承**旧的 `state_enter_time`**。如果某条目正卡在 `STAGING_IN`，state_machine 的超时是默认 12 分钟（`stage_timeout`），用户感知的 broker 重启延迟会非常糟糕。

实现：`broker_init()` 在 `state_machine_start()` 之后、`listener_start()` 之前调一次 `state_machine_resume_inflight()`，将所有 `STAGING_IN / STAGED_IN / STAGING_OUT` 任务的 `state_enter_time` 拨到"已过期到只剩 1s"，让下一次 tick 立刻走重试路径。`retry_count` 不重置（已超 3 次的就让它转 FAILED）。

---

## 五、状态机（M09 + M10 落地版）

### 5.1 状态跃迁表

| 状态 | 入口动作 | 推进方式 | 失败处理 | 超时（默认） |
|---|---|---|---|---|
| INIT | `broker_job_table_add` + persist | `_on_init`：originator 端 `stage_submit_in` 后 transition STAGING_IN；receiver 端等 STAGED_IN | 直接 FAILED | 60s |
| STAGING_IN | `stage_submit_in` 把任务塞 stage 队列 | stage worker 完成回调 `state_machine_transition(STAGED_IN)` | 重试 ≤ 3 → 否则 FAILED | `du -sb` × `StageTimeoutPerGB` + 600s |
| STAGED_IN | originator 已 8012；receiver 已收到准备 sbatch | originator: `egress_staged_in_async` 收到 8013 → SUBMITTED；receiver: `rewrite_job_script` + `sudo sbatch` → SUBMITTED | 重试 ≤ 3 → FAILED | 30s |
| SUBMITTED | 记录 `remote_job_id` | sync_ticker 拉到 RUNNING | 24h 仍 PENDING → FAILED | 24h |
| RUNNING | — | sync_ticker 拉到终态 | NODE_FAIL 一次重试 | `time_limit + 30 min`（暂未启用，依赖 ctld 字段） |
| STAGING_OUT | `stage_submit_out` 入队 | stage worker 完成回调 → COMPLETED | 重试 ≤ 3 → FAILED（远端目录保留） | 同 STAGING_IN |
| COMPLETED | `ctld_inject_terminal_state` + 出表 | — | — | — |
| FAILED | 同上 | — | — | — |
| CANCELLED | originator: `egress_cancel_async` + `ctld_inject_terminal_state` + 出表；receiver: `slurm_kill_job` + 出表 | — | — | — |

### 5.2 两阶段 tick 模型（避免持锁阻塞）

```
state_machine_tick():
    -- Phase 1: 持 g_broker_jobs_lock --
    foreach job in table:
        if cancel_requested && !cancel_propagated && state ∉ terminal:
            enqueue SM_ACTION_CANCEL(trace_id)
        else switch (state):
            case INIT:        enqueue SM_ACTION_INIT
            case STAGING_IN:  if timeout -> 内联 transition retry/FAILED
            case STAGED_IN:   if timeout -> 内联 transition retry/FAILED
            case SUBMITTED:   if 24h -> 内联 transition FAILED
            case STAGING_OUT: 同 STAGING_IN
            case COMPLETED|FAILED|CANCELLED:
                enqueue SM_ACTION_INJECT_TERMINAL(trace_id)
                enqueue SM_REMOVE(trace_id)
    -- Phase 2: 释锁，逐个执行 enqueued action --
    drain action queue:
        - INIT  -> stage_submit_in + transition STAGING_IN
        - CANCEL-> egress_cancel_async + transition CANCELLED
        - INJECT_TERMINAL -> egress::ctld_inject_terminal_state
    drain remove queue:
        - broker_job_table_remove(trace_id)
```

### 5.3 状态推进点

| 推进点 | 触发方 | 周期/事件 |
|---|---|---|
| INIT → STAGING_IN | state_machine `_on_init` enqueue → Phase 2 `stage_submit_in` | 1s tick |
| STAGING_IN → STAGED_IN | stage worker 完成回调（exit=0） | 事件 |
| STAGED_IN → SUBMITTED | originator: 收 8013；receiver: `sudo sbatch` 成功 | 事件 |
| SUBMITTED → RUNNING | sync_ticker `_apply_remote_status` 拉到 JOB_RUNNING | 10s 周期 |
| RUNNING → STAGING_OUT | sync_ticker 拉到终态 → `stage_submit_out` | 10s 周期 |
| STAGING_OUT → COMPLETED | stage worker 完成回调 | 事件 |
| 任意 → CANCELLED | `cancel_requested` + 下一 tick | 1s 周期 |
| 任意 → FAILED | 各类超时 / 错误响应 | 混合 |

### 5.4 `state_machine_transition` API

任何线程都可以调；内部行为：

1. 持 `job->lock`；
2. `job->state == to` 时直接返回（幂等）；
3. 更新 `state_enter_time = time(NULL)`；
4. 替换 `state_reason`（NULL 清空）；
5. `persist_async_request()` 提示 checkpoint 线程；
6. `info()` 一行日志便于 grep。

> 历史 M07 直接使用的 `broker_job_set_state()` 是简化版（只改字段不持 persist hint）；M09 后约定**新代码统一用 `state_machine_transition`**。

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

### 6.3 消息类型表（11 个）

| msg_type | 名称 | 方向 | 标记 | 落地位置 |
|---|---|---|---|---|
| 8001 | `BROKERD_REQUEST_FORWARD_JOB` | ctld → broker | LEGACY_M04_TRANSITIONAL | `proto_pack.c::_pack_forward_job_msg` |
| 8002 | `BROKERD_RESPONSE_FORWARD_JOB` | broker → ctld | LEGACY_M04_TRANSITIONAL | 同上 |
| 8003 | `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` | broker → ctld | LEGACY_M04_TRANSITIONAL | `proto_pack.c::_pack_broker_remote_state_msg` |
| 8004 | `BROKERD_REQUEST_BROKER_TERMINAL_STATE` | broker → ctld | LEGACY_M04_TRANSITIONAL | `proto_pack.c::_pack_broker_terminal_state_msg` |
| 8010 | `BROKERD_REQUEST_BROKER_FORWARD_JOB` | broker → broker | PERMANENT | `proto_pack.c::_pack_broker_forward_job_msg` |
| 8011 | `BROKERD_RESPONSE_BROKER_ACK` | broker → broker | PERMANENT | 同上 |
| 8012 | `BROKERD_REQUEST_BROKER_STAGED_IN` | broker → broker | PERMANENT | 同上 |
| 8013 | `BROKERD_RESPONSE_BROKER_SUBMITTED` | broker → broker | PERMANENT | 同上 |
| 8014 | `BROKERD_REQUEST_BROKER_QUERY_STATUS` | broker → broker | PERMANENT | 同上 |
| 8015 | `BROKERD_RESPONSE_BROKER_STATUS` | broker → broker | PERMANENT | 同上 |
| 8016 | `BROKERD_REQUEST_BROKER_CANCEL` | ctld → broker / broker → broker（dual-purpose） | PERMANENT | 同上 |
| 8017 | `BROKERD_REQUEST_BROKER_CLEANUP` | broker → broker | PERMANENT | 同上 |

### 6.4 错误码表（broker 私有 9001-9099）

| 代码 | 名称 | 含义 |
|---|---|---|
| 9001 | `BROKERD_ERR_OVERLOAD` | 在途数 ≥ `MaxInFlightJobs` |
| 9002 | `BROKERD_ERR_NO_USER_MAPPING` | `user_mapping_lookup` 返回 NULL |
| 9003 | `BROKERD_ERR_USER_MAPPING_MISMATCH` | receiver 反向校验 `remote_user` 不一致 |
| 9004 | `BROKERD_ERR_HOP_EXCEEDED` | `hop_count > 0`（MVP 严格 ≤ 1） |
| 9005 | `BROKERD_ERR_LOOKUP_FAILED` | `lookup_software.sh` 非 0 退出或路径非 `/` 开头 |
| 9006 | `BROKERD_ERR_LOOKUP_TIMEOUT` | `lookup_software.sh` 超时 |
| 9007 | `BROKERD_ERR_STAGE_FAILED` | rsync 子进程失败 |
| 9008 | `BROKERD_ERR_REMOTE_SUBMIT_FAILED` | receiver 端 sbatch 解析失败或退出非 0 |
| 9009 | `BROKERD_ERR_NOT_FOUND` | trace_id 在表中不存在（cancel/cleanup 幂等返回成功） |

`proto.c::brokerd_strerror()` 将上述映射到中英文双语字符串便于日志阅读。

### 6.5 `LEGACY_M04_TRANSITIONAL` 与未来清理

由于强约束不允许动 `src/common/`，4 个 ctld↔broker 的 RPC（8001 / 8002 / 8003 / 8004）当前由 broker 这边**临时实现**一份 packing 代码，标 `LEGACY_M04_TRANSITIONAL`。当 ctld 工程师在 `src/common/slurm_protocol_defs.h` + `src/common/slurm_protocol_pack.c` 注册等同结构和 msg_type 之后，broker 这边将做一个**纯机械删除 PR**：

- 删 `proto.h` 中 4 个 `BROKERD_REQUEST_*` 宏 + 4 个 payload 结构 + 4 个 free 声明
- 删 `proto_pack.c` 中 4 对 `_pack/_unpack_*_msg` + dispatcher 4 个 case
- 删 `proto.c` 中 4 个 `brokerd_free_*` 实现
- `handler_ctld.c` 改用 `slurm_msg_t.data` 直接读原生结构

当前共 27 处 `LEGACY_M04_TRANSITIONAL` 标记，全文 grep 即可枚举。

### 6.6 `egress` 出站 API（`egress.h`）

| 函数 | 方向 | 实现方式 | 调用方 |
|---|---|---|---|
| `egress_forward_async(job)` | broker→broker 8010 | `proto_send_recv_to_peer` + 内嵌 `_retry_n_times` 指数退避 | `state_machine` `_on_init` |
| `egress_staged_in_async(job)` | broker→broker 8012 | 同上 | stage worker 完成回调 |
| `egress_query_status_sync(ids,n,&resp)` | broker→broker 8014 | 同步阻塞 | `sync_ticker` |
| `egress_cancel_async(job)` | broker→broker 8016 | `proto_send_to_peer`（fire-and-forget，SO_LINGER） | `state_machine` cancel 优先级 |
| `egress_cleanup_async(trace_id)` | broker→broker 8017 | 同上 | 当前未启用（M14 废弃） |
| `ctld_update_remote_state(job)` | broker→ctld 8003 | `slurm_send_recv_controller_rc_msg` | `sync_ticker._apply_remote_status` |
| `ctld_inject_terminal_state(job)` | broker→ctld 8004 | 同上 | `state_machine` 终态 |

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

### 7.1 US-1 `sbatch` 提交跨域作业

#### A. originator handler（`handler_ctld.c::handle_forward_job`）

```c
1. 反包 brokerd_forward_job_msg_t
2. 限流: count_inflight() >= max_inflight -> reply BROKERD_ERR_OVERLOAD
3. user_mapping_lookup(src_user_name, target_cluster) -> NULL 时 BROKERD_ERR_NO_USER_MAPPING
4. broker_job_create + 填字段 (trace_id, role=ORIGINATOR, app_name, script_path...)
5. broker_job_table_add (内部幂等; 已存在则 reply 现状即可)
6. persist_async_request
7. reply RESPONSE_FORWARD_JOB { error_code=0, trace_id }
8. state_machine 下一 tick 接管 INIT → STAGING_IN
```

#### B. receiver handler（`handler_remote.c::handle_broker_forward_job`）

```c
1. brokerd_wire_parse 后的 brokerd_broker_forward_job_msg_t
2. hop_count > 0 -> reply BROKERD_ERR_HOP_EXCEEDED
3. user_mapping_lookup(src_user_name, src_cluster); 校验 remote_user_name 一致
4. 已存在 trace_id -> 幂等 reply current ACK + dst_work_dir
5. broker_job_create + 填 RECEIVER 字段
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

#### D. receiver `STAGED_IN` 处理（`handler_remote.c::handle_broker_staged_in`）

```c
1. broker_job_table_get(trace_id); 已 SUBMITTED 则幂等 reply
2. rewrite_job_script(job, &modified_path)
   - lookup_software_path(src_cluster, app_name, &src_root)  (容错)
   - lookup_software_path(dst_cluster, app_name, &dst_root)  (容错)
   - 行级处理:
       drop --reservation/--cross-domain/--app/--account/-A
       replace -p / --partition= 为 target_partition
   - 全局字符串替换 src_root -> dst_root
   - 写到 <dst_work_dir>/<basename>.cd_modified.sh (mode 0700)
   失败(filesystem) -> transition FAILED + reply BROKERD_ERR_LOOKUP_FAILED
3. _sudo_sbatch(remote_user, dst_work_dir, modified_path)
   - fork+execvp sudo -n -u remote_user sbatch <modified_path>
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

## 八、模块设计（实际文件清单）

### 8.1 源码布局（18 文件）

```
src/slurmbrokerd/
├── slurmbrokerd.c/.h          # main / 信号 / 启停编排 (M01 实质完成)
├── broker_conf.c/.h           # broker.conf 解析 (M02)
├── user_mapping.c/.h          # LocalUser=... 行解析 (M02)
├── broker_job.c/.h            # broker_job_t + 全局表 + JSON 序列化 (M03)
├── persist.c/.h               # 三文件原子 rename + 30s/事件 checkpoint (M03)
├── proto.c/.h                 # 11 RPC + 9 错误码 + 私有 wire frame + send/recv API (M04)
├── proto_pack.c               # 11 个 _pack/_unpack 实现 + dispatcher (M04)
├── listener.c/.h              # 双端口 select + ACL + self-pipe + dispatch (M05)
├── handler_ctld.c/.h          # CtldPort 入口 (M06)
├── handler_remote.c/.h        # PeerPort 入口 + sudo mkdir/sbatch (M07)
├── egress.c/.h                # 出站 7 wrapper (M08)
├── state_machine.c/.h         # 1Hz tick + cancel 优先级 + 两阶段 + resume_inflight (M09)
├── stage.c/.h                 # 4 worker pool + sudo+rsync+ssh + du -sb (M10)
├── software.c/.h              # lookup_software.sh 调用器 (M11)
├── util_exec.c/.h             # brokerd_waitpid_timeout 公共 helper (M11)
├── rewrite.c/.h               # SBATCH 行处理 + 路径前缀替换 (M12)
├── sync_ticker.c/.h           # 10s 轮询 + 三阶段 + 失败计数 (M13)
└── Makefile.am
```

**对比旧版规划（16 文件）**：

- ✅ 多了 `proto_pack.c`（pack/unpack 与 lifecycle 拆开，便于将来删 LEGACY block）
- ✅ 多了 `util_exec.c/.h`（M11 抽公共 fork+wait helper）
- ❌ 没有独立 `cleanup.c`（M14 废弃）
- 旧版的 `audit.c` / `sbroker.c` / `app_route.c` 同样不实现

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
| drop cross-domain | `#SBATCH --cross-domain` | 删除该行 |
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

### 10.1 `/etc/slurm/broker.conf`（实际字段集）

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
# 单对端
# ============================================
RemoteClusterName=wz_cluster
RemoteBrokerHost=broker.wz.example.com
RemoteBrokerPort=8443
RemoteMungeKeyPath=/etc/slurm/munge_wz.key

# ============================================
# 单一目标 partition
# ============================================
DefaultRemotePartition=wzhcnormal

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
# 限流（broker 兜底，主限流走 ctld 端 partition.MaxJobs）
# ============================================
MaxInFlightJobs=500
MaxStageBytes=53687091200      # 50 GB

# ============================================
# 状态轮询
# ============================================
PollInterval=10
PollMaxRetries=5

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
# 数据保留（保留字段；M14 废弃，broker 当前不删除远端目录）
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
- sudoers 模板（缺）：`broker_user ALL=(ANY) NOPASSWD: /usr/bin/rsync, /usr/bin/sbatch, /bin/mkdir, /bin/chmod, /bin/sh`
- ssh key 分发 README（缺）
- logrotate 配置（缺）：`/var/log/slurm/broker_stage/*.log`

---

## 十二、Sprint 进度（落地状态盘点）

| Sprint | 模块 | 状态 |
|---|---|---|
| S1 W1-W2 | M01 进程骨架（slurmbrokerd.c 信号/日志/init/fini） | ✅ 完成（实质，无单独 checklist） |
| S1 W1-W2 | M02 配置（broker.conf 单文件） | ✅ 完成 |
| S1 W1-W2 | M03 持久化（JSONL + 三文件 rename） | ✅ 完成 |
| S1 W1-W2 | M04 RPC（broker 私有 wire frame + 11 msg + 9 error） | ✅ 完成（含 27 处 LEGACY_M04_TRANSITIONAL） |
| S2 W3-W4 | M05 listener（双端口 + self-pipe + ACL） | ✅ 完成 |
| S2 W3-W4 | M06 ctld 入站 handler | ✅ 完成 |
| S2 W3-W4 | M07 remote 入站 handler（含 sudo+sbatch） | ✅ 完成 |
| S2 W3-W4 | M08 egress（5 broker→broker + 2 broker→ctld） | ✅ 完成 |
| S3 W5-W6 | M09 状态机（1Hz + 两阶段 + cancel 优先级 + resume_inflight） | ✅ 完成 |
| S3 W5-W6 | M10 stage（4 worker + sudo+rsync + du -sb + --partial） | ✅ 完成 |
| S3 W5-W6 | M11 software（fork+exec lookup + util_exec helper） | ✅ 完成 |
| S3 W5-W6 | M12 rewrite（SBATCH 行处理 + 路径替换 + 容错） | ✅ 完成 |
| S3 W5-W6 | M13 sync_ticker（10s + 三阶段 + 失败计数） | ✅ 完成 |
| S3 W5-W6 | M14 cleanup（远端目录定时清理） | ⏸ **暂废弃，不删除用户数据** |
| S4 W7-W8 | M15 部署运维工件（broker.conf.example / sudoers / logrotate） | ⏳ 待补 |
| S4 W7-W8 | M16 集成测试（mvp_smoke_test.sh + 故障注入 + 长稳） | ⏳ 待补 |

**跨 PR 依赖（外部）**：
- ctld 同步 PR：在 `src/common/slurm_protocol_defs.h` 注册 4 个 msg_type 与 payload；之后 broker 这边走机械删除 PR 移除 27 处 `LEGACY_M04_TRANSITIONAL`。
- ctld + slurm.conf：新增 `BrokerAddr` / `BrokerPort` 字段；ctld 跨域调度线程实现。

---

## 十三、验收清单

### 13.1 功能（13.1-13.3 沿用旧版，状态标注本 MVP 实现度）

- ✅ US-1 全流程（INIT → STAGING_IN → SUBMITTED → RUNNING → STAGING_OUT → COMPLETED）已可在代码层端到端联通；待 M16 编写 smoke test 实测
- ✅ US-2 scancel 反向（含 receiver 端 `slurm_kill_job`）已实现
- ⏳ US-3 字段化展示需要 ctld + 客户端 PR 配合（broker 侧 8003/8004 已就位）
- ✅ US-4 重启恢复：`broker_state_restore` + `state_machine_resume_inflight` + 全部 4 处幂等性已就位

### 13.2 异常

- ✅ 远端 broker 不可达：`egress::_retry_n_times` 指数退避；上层 state_machine 超时后转 FAILED
- ✅ rsync 失败：stage worker 不直接转 FAILED，由 M09 `_on_staging_in` watchdog 触发重试 ≤ 3
- ✅ 远端 sbatch 拒绝：handler_remote `_sudo_sbatch` 解析失败 → transition FAILED + reply BROKERD_ERR_REMOTE_SUBMIT_FAILED
- ✅ `lookup_software.sh` 退出非 0：返回 BROKERD_ERR_LOOKUP_FAILED；rewrite 容错继续 SBATCH-only 改写
- ✅ 用户映射缺失：handler_ctld 直接 reply BROKERD_ERR_NO_USER_MAPPING
- ✅ broker 进程 OOM：systemd Restart=on-failure（unit 文件已配）

### 13.3 性能

- ⏳ 100 跨域作业并发：待 M16 长稳测试
- ⏳ 24h 无内存泄漏：待 valgrind / tcmalloc 跑

### 13.4 已知限制

- 仅 sbatch 跨域，不支持 srun / salloc
- 仅 CLI 作业，不支持 Portal `.portal/job_portal.var`
- `scontrol update` 不支持跨域（用户需 scancel 重投）
- 单对端集群（1 RemoteCluster）
- stdout/stderr 仅终态可见
- 单 broker 实例无 HA
- 远端用户必须由运维预先用 `sacctmgr` 配好 default account 与 partition association
- **远端 work_dir 不会被自动清理**（M14 废弃后果）；运维需周期性回收

---

## 十四、演进锚点

### 14.1 短期（待 ctld 同事 PR 落地后）

- 删 27 处 `LEGACY_M04_TRANSITIONAL`（机械工作）
- 删 `proto.h::brokerd_forward_job_msg_t` / `brokerd_broker_remote_state_msg_t` / `brokerd_broker_terminal_state_msg_t` / `brokerd_forward_job_resp_msg_t`
- `handler_ctld.c` 改用 `slurm_msg_t.data` 直接读原生结构

### 14.2 中期（向 v0.1 完整版演进）

| 演进方向 | 落地位置 | 影响 ctld / 客户端？ |
|---|---|---|
| Portal 作业 | `rewrite.c` 新增 `translator_portal.c` 分发 | 否 |
| `lookup_software.sh` 替换为 RDB / API | `software.c::lookup_software_path` 改实现 | 否 |
| 多对端集群 | `broker_conf.c` 单字段改 list；`egress.c` 按 `target_cluster` 路由 | 仅 ctld 配置 |
| 持久化升级到 `pack.c` | `persist.c` JSONL → 二进制 | 否 |
| broker→broker mTLS / HTTPS | 抽象 `peer_transport_ops_t`，加 mTLS 实现 | 否 |
| 实时 stdout 回传 | 新增 `stream.c` 独立线程拉 | 仅 ctld 增加可选 RPC |
| broker HA | 引入 etcd/consul + VIP | 否 |
| Prometheus exporter | 新增 `metrics.c` | 否 |
| sbroker CLI | 新增 `src/sbroker/` 复用 JSONL | 否 |

### 14.3 不再变动的契约

为后续演进风险最小，下列字段语义只增不改：

- `proto.h` 中 7 个 PERMANENT msg_type 与 payload 字段顺序（broker↔broker）
- `broker_job.h` 的 JSONL schema（只允许文件末尾追加可选字段）
- `state_machine.h` 的 9 状态 + 转换图
- `software.h::lookup_software_path()` 函数签名

---

## 十五、附录

### 15.1 文件清单（含部署）

```
源码 (Slurm 树):
src/slurmbrokerd/  (18 个 .c/.h)
  详见 §8.1

构建:
configure.ac (L540)        # 加 src/slurmbrokerd/Makefile
src/Makefile.am (L21)       # 加 slurmbrokerd 子目录

部署模板:
etc/slurmbrokerd.service.in
etc/lookup_software.sh.example       # ★ M11 新增
etc/software_routes.conf.example     # ★ M11 新增

部署目标:
/usr/sbin/slurmbrokerd
/usr/lib/systemd/system/slurmbrokerd.service
/etc/slurm/broker.conf                # 单文件（含 LocalUser= 行）
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
scontrol show job <jobid> | grep -E '(CrossDomain|Reason)'

# 3. broker 端口是否监听
ss -tlnp | grep -E '8442|8443'
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
