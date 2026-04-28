# Slurm 跨域 Broker (`slurmbrokerd`) MVP 实现指导

> **版本**：MVP（基于 v0.1 极简实现版裁剪）
> **状态**：实施依据
> **基线**：`Broker详细设计文档.md`（v0.1） + `跨域调度MVP整体方案.md`
> **目标**：4 周内打通整体方案 §9 Sprint 中 broker 端的全部任务，与 ctld + 客户端联调跑通完整闭环

---

## 一、文档目的与定位

本文档是 **v0.1 设计文档** 在 broker 端的**快速实现裁剪版**，作为 MVP 阶段 broker 工程师的**实现指导**。

### 1.1 本文档在 MVP 中的定位

```
┌──────────────────────────────────────────────────────────┐
│  跨域调度MVP整体方案.md（端到端 8 周）                     │
│   ├─ §4 用户交互层  : sbatch/squeue/scontrol/sacct 完整改造 │
│   ├─ §5 ctld 端     : 跨域线程 + 单层 ACL + 字段化 + RPC    │
│   ├─ §6 broker 端   ───────────────────┐                  │
│   ├─ §7 SlurmDBD    : Remote_* 列      │                  │
│   └─ §9 Sprint 计划 : ctld + broker 并行 │                  │
└─────────────────────────────────────────┼──────────────────┘
                                           │ 落地
                              ┌────────────┴───────────┐
                              │  本文档                 │
                              │  (broker 端快速实现版)   │
                              └────────────────────────┘
```

| 整体方案章节 | 本文档对应章节 | 关系 |
|---|---|---|
| §4 用户交互层（sbatch/squeue/scontrol/sacct） | - | 不在 broker 范围；客户端改造由整体方案 §4 定义 |
| §5 调度层（ctld 端） | - | 不在 broker 范围；ctld 改造由整体方案 §5 定义 |
| §6 跨域层（broker 端） | **§3~§13 全部** | **本文档就是 broker 端实现指导** |
| §9 Sprint 1~4 | §12 Sprint 1~4 | 周次完全对齐 |

### 1.2 实现原则（**最重要**）

> **MVP 的"快"只体现在 broker 内部，对外接口必须按整体方案完整落地，不允许二次裁剪。**

| 维度 | 原则 | 是否允许 MVP 裁剪 |
|---|---|---|
| **broker ↔ ctld RPC 接口**（`REQUEST_FORWARD_JOB` / `REQUEST_BROKER_UPDATE_REMOTE_STATE` / `REQUEST_BROKER_TERMINAL_STATE` / `REQUEST_BROKER_CANCEL`） | 字段、语义、序列化按整体方案 §6 完整落地 | ❌ 不允许 |
| **broker ↔ broker 跨集群协议** | Slurm RPC + 共享 Munge（ttl=86400），承诺协议字段稳定 | ❌ 不允许（仅传输层用 munge 是 MVP 取舍，字段不变） |
| **broker → lookup_software.sh 调用约定** | `lookup_software.sh <cluster> <app>` → stdout 一行绝对路径，按整体方案 §6 / §13 落地 | ❌ 不允许 |
| **broker 内部状态机** | 9 个状态名 / 状态转换图按 v0.1，**实现细节**可合并简化 | ✅ 允许 |
| **broker 内部持久化格式** | JSONL 取代 v0.1 二进制 pack，便于排障 | ✅ 允许 |
| **broker 内部线程数** | 6 主线程，砍掉 metrics / audit / sbroker CLI | ✅ 允许 |
| **broker 内部 staging / rewrite 实现** | 用 `sudo -u`，单文件 rewrite.c | ✅ 允许 |

**口诀**：**对外完整、对内简化**。后续 broker 内部演进（HA、重写引擎升级、Munge → mTLS）都不会触发 ctld / 客户端 / 协议改动。

### 1.3 broker 端 MVP 必做的对外契约（不能简化）

以下 5 项是 broker 与外部系统之间**必须完整实现**的契约，是本文档与整体方案对齐的"硬边界"：

1. **接收 ctld 转发** — 实现 `REQUEST_FORWARD_JOB` handler，读取 `cross_domain` / `app_name` / 用户/账户/工作目录 / 完整 `job_desc` 等字段
2. **回写 ctld 远端状态** — 通过新 RPC `REQUEST_BROKER_UPDATE_REMOTE_STATE` 写 `remote_cluster_name` / `remote_partition_name` / `remote_job_id` / `remote_state` / `remote_alloc_tres` / `remote_start_time` / `remote_end_time` / `remote_exit_code` 等独立字段（**不写 `comment`**）
3. **通知 ctld 终态** — 通过新 RPC `REQUEST_BROKER_TERMINAL_STATE` 让 ctld 把影子作业 PENDING(Held) 跳变到 COMPLETED/FAILED/CANCELLED，附 `remote_*` 终态全集
4. **改写 source 行** — 调外部 `lookup_software.sh <cluster> <app>` 解析两端软件路径，按前缀替换脚本中的 source 行（broker 自包含，ctld 不感知）
5. **处理 scancel 反向** — 实现 `REQUEST_BROKER_CANCEL` 跨集群传播 + 远端 `slurm_kill_job`

> 工程师 review 自己的代码时，**这 5 项的接口签名 / 字段全集必须严格对齐 `跨域调度MVP整体方案.md` §6 的描述**，发现不一致以整体方案为准。

> **使用方式**：开发者按本文档实施；遇到本文档**未提**的设计点，回退到 v0.1 文档对应章节查阅。涉及"对外接口"的疑义以整体方案为准。

---

## 二、MVP 范围

### 2.1 MVP 目标用户故事（broker 端的角色）

| ID | 故事 | broker 端职责 | 验收 |
|---|---|---|---|
| US-1 | 用户 `sbatch --cross-domain --app=...` 提交，被自动转发到 B 集群运行 | 接收 ctld 转发 → stage 输入 → 远端 broker 提交 → 同步状态 → 终态回写 | 远端作业可见、可跑、结果回传 |
| US-2 | 用户 `scancel` 影子作业，远端真实作业 30s 内被 kill | 接收 ctld 取消请求 → 跨集群传播 → 远端 `slurm_kill_job` | 远端 sacct 显示 CANCELLED |
| US-3 | 用户 `squeue --remote` / `scontrol show job` / `sacct` 看到跨域作业远端字段 | sync_ticker 通过 `REQUEST_BROKER_UPDATE_REMOTE_STATE` 写 `remote_*` **独立字段**到 ctld（不写 comment） | 字段化输出可见、不污染 user comment |
| US-4 | broker 进程重启，在途跨域作业能继续走完 | JSONL 持久化恢复 + trace_id 幂等 | 无僵尸作业、无重复提交 |

**US-1 ~ US-4 是 MVP 的强需求，全部不可妥协。**

### 2.2 MVP 范围分层（**先看本表再读后续章节**）

> 本表清晰区分"对外接口（必须完整实现）"vs"broker 内部实现（允许简化）"。后续章节的"裁剪"标识都源自本表。

#### A. 对外接口（**必须完整实现，不允许 MVP 裁剪**）

| # | 项 | 要求 | 对应章节 |
|---|---|---|---|
| **E1** | 接收 ctld → broker 转发：`REQUEST_FORWARD_JOB` | 字段全集（cross_domain、app_name、user、account、wckey、cwd、partition、节点/CPU/时间/内存/分区资源全集、`job_script`、env、stdin/stdout/stderr 路径、`trace_id`） | §6.2 |
| **E2** | broker → broker 跨集群转发：`REQUEST_BROKER_FORWARD_JOB` / `REQUEST_BROKER_STAGED_IN` / `REQUEST_BROKER_QUERY_STATUS` / `REQUEST_BROKER_CANCEL` / `REQUEST_BROKER_TERMINAL_STATE` | Slurm RPC over Munge（共享，ttl=86400），字段稳定 | §6.3 |
| **E3** | broker → ctld 远端状态字段化：`REQUEST_BROKER_UPDATE_REMOTE_STATE` | 写 `remote_cluster_name` / `remote_partition_name` / `remote_job_id` / `remote_state` / `remote_alloc_tres` / `remote_start_time` / `remote_end_time` / `remote_exit_code` 独立字段；**不写 `comment`** | §6.4 |
| **E4** | broker → ctld 终态：`REQUEST_BROKER_TERMINAL_STATE` | 让 ctld 影子作业从 PENDING(Held) 跳变到 COMPLETED/FAILED/CANCELLED + `jobcomp_g_record_job_end` 落账；附 `remote_*` 终态全集 | §6.4 |
| **E5** | broker → 本机 `lookup_software.sh` 调用 | `lookup_software.sh <cluster_name> <app_name>` → stdout 输出本地绝对路径；非 0 退出码视为失败并通过 §11 错误处理 | §8.2 |

#### B. broker 内部实现（**允许 MVP 简化**）

| # | v0.1 完整版 | MVP 简化 | 理由 |
|---|---|---|---|
| I1 | 状态机 9 个状态 + 全部错误分支独立处理 | **保持 9 个状态名**，状态机 tick 周期 1s；错误分支合并到 4 类（rsync_failed / submit_rejected / lookup_failed / timeout） | 状态机对外可见，不能少；内部分支可合并 |
| I2 | 持久化用 `pack.c` 二进制 | **JSONL**（每行一个 `broker_job_t` 的 JSON） | 排障可读，写性能 100 在途完全 OK |
| I3 | 跨集群 mTLS / JWT | **复用 Slurm RPC + 共享 Munge**，建议 `munged --ttl 86400` | 不引入新传输层，时钟漂移 24h 内容忍 |
| I4 | 多对端集群拓扑 | **单对端**（`RemoteCluster` 配置仅一条），单虚拟队列 → 单远端 partition | 配置项与 ctld 路由静态化对齐 |
| I5 | 应用路由表 `app_routes.conf` + `translator_portal/translator_cli` 双 translator | **单一 `rewrite.c`**：调 `lookup_software.sh` + 替换 `#SBATCH --partition=` + 按前缀替换 `source ` 行；仅支持 CLI 作业 | translator 模块从 4 个文件缩成 1 个 |
| I6 | 远端 staging 目录用 `mkdir_p` + `setresuid` | **`sudo -u <remote_user> mkdir -p`** | 不写 setresuid 代码 |
| I7 | rsync 切 uid 用 `setresuid(uid)` | **`sudo -u <local_user> rsync ...`** | 同上 |
| I8 | 状态轮询固定 30s | **10s**（更好的用户感知） | MVP 量级 100 在途，10s 完全 OK |
| I9 | sbroker CLI | **不实现**，用 `journalctl` + JSONL 状态文件排障 | 减一个模块 |
| I10 | audit.log / metrics.log 独立文件 | **stdout/syslog（journalctl）**，关键事件（提交/终态/错误）打 INFO 日志即可 | 减两个模块 |
| I11 | 用户映射热更新（SIGHUP） | **重启生效** | MVP 阶段映射表稳定 |
| I12 | 应用元数据从 `comment` JSON 解析 | **不需解析 comment**，broker 直接读 `REQUEST_FORWARD_JOB` 中 ctld 透传的 `app_name` 字段 | 整体方案已字段化 |
| I13 | broker 自身限流 `MaxInFlightJobs` / `MaxInFlightPerUser` | **不实现**，限流由 ctld 端虚拟 partition.MaxJobs 承担，broker 仅做溢出保护（一旦超过 200 在途新作业拒绝） | 单一旋钮，避免双重计费 |

### 2.3 MVP **不做** 的能力（明确告知用户）

| 能力 | 用户感知 | 何时引入 |
|---|---|---|
| 多对端集群 | 跨域目的地固定一个 | v0.1 完整版 |
| 远端 partition 智能选路 | 单虚拟队列 → 单远端 partition 静态映射 | v0.1 完整版 |
| Portal 作业 `.portal/job_portal.var` 改写 | Portal 作业暂不支持跨域 | v0.1 完整版 |
| 实时输出回传 | 作业终态后才能看 stdout/stderr | v0.2 |
| `scontrol update` 跨域作业 | 用户需 scancel 重投 | v0.1 完整版 |
| broker HA | 单实例，宕机期间新作业不能跨域，已在途作业可恢复 | v0.3 |
| 多 hop（A→B→C） | hop_count 严格 ≤ 1 | 永不支持 |
| mTLS / JWT 鉴权 | 走共享 Munge（建议 ttl=86400） | v0.2 |
| sbroker 管理 CLI | 用 `journalctl` + JSONL 文件 | v0.1 完整版 |
| metrics 导出 | 不导出 | v0.1 完整版 |

---

## 三、整体架构（MVP 视图）

### 3.1 系统视图（删减后）

```text
┌─ 集群 A (源端) ─────────────────────────────────────┐
│                                                      │
│  ┌──────────────┐  Slurm RPC + Munge (本机)          │
│  │  slurmctld   │◄─►┌──────────────┐                 │
│  │  跨域调度线程│   │ slurmbrokerd │                 │
│  └──────────────┘   │   (8442/8443)│                 │
│                     └──────┬───────┘                 │
│                            │                         │
└────────────────────────────┼─────────────────────────┘
                             │
        Slurm RPC + 共享 Munge (跨集群, ttl=86400) ─┐
        rsync over SSH (服务账号 key) ──────────────┤
                             │                       │
┌────────────────────────────▼───────────────────────┼─┐
│  集群 B (远端)              ┌──────────────┐       │ │
│                             │ slurmbrokerd │◄──────┘ │
│                             │   (8442/8443)│         │
│                             └──────┬───────┘         │
│                                    │                 │
│                       Slurm RPC + Munge (本机)       │
│                             ▼                        │
│                     ┌──────────────┐                 │
│                     │  slurmctld   │                 │
│                     └──────────────┘                 │
└──────────────────────────────────────────────────────┘
```

### 3.2 端到端时序

```text
sbatch (US-1)                                      scancel (US-2)
─────────────                                      ──────────────
1. 用户 sbatch -p wzhcnormal_virt run.sh          1. 用户 scancel <local_job_id>
2. ctld(A) 收作业, 入队                            2. ctld(A) 把作业标 CANCELLED
3. ctld(A) 跨域调度线程超时检测, 挂起作业          3. ctld(A) 跨域调度线程扫描:
4. ctld(A) → broker(A): REQUEST_FORWARD_JOB           发现 CANCELLED 且未传播
5. broker(A) 入 hash, state=INIT                  4. ctld(A) → broker(A): REQUEST_BROKER_CANCEL
6. broker(A) → broker(B): REQUEST_BROKER_FORWARD  5. broker(A) → broker(B): REQUEST_BROKER_CANCEL
7. broker(B) ACK, state(A)=STAGING_IN             6. broker(B) slurm_kill_job(remote_job_id)
8. broker(A) rsync over SSH → broker(B) staging   7. broker(A) state=CANCELLED
9. broker(A) → broker(B): REQUEST_BROKER_STAGED_IN
10. broker(B) 改写脚本, slurm_submit_batch_job    squeue (US-3)
11. broker(B) → broker(A): SUBMITTED + remote_id  ──────────────
12. state(A)=SUBMITTED                            broker(A) sync_ticker 每 10s:
13. broker(A) sync_ticker 轮询 broker(B) 状态        broker(A) → broker(B): QUERY_STATUS
14. state(A) → RUNNING → COMPLETED                   broker(A) → ctld(A):
15. broker(A) 反向 rsync, state=STAGING_OUT             REQUEST_BROKER_UPDATE_REMOTE_STATE
16. broker(A) → ctld(A): TERMINAL_STATE              写 remote_* 独立字段
17. ctld(A) 影子作业 PENDING→COMPLETED, 写 sacct  用户 squeue --remote 看远端字段
```

### 3.3 线程拓扑（精简至 6 个核心线程）

```text
slurmbrokerd 进程
├── Main Thread          (信号、生命周期)
├── Listener Thread × 1  (单线程同时 accept 8442 和 8443, MVP 不分线程)
├── State Machine Tick   (1s 周期)
├── Egress Worker × 2    (异步出站调用)
├── Stage Worker × 4     (rsync 子进程)
├── Sync Ticker          (10s 周期)
└── Persister + Cleanup  (合一个线程, 30s tick: checkpoint + 清理远端目录)
```

> 相对 v0.1：listener 1 个线程（不分），persister 与 cleanup 合一。可减约 30% 线程切换开销，对 MVP 量级足够。

---

## 四、数据结构（MVP 版 `broker_job_t`）

### 4.1 简化后的核心结构

```c
/* src/slurmbrokerd/broker_job.h */

typedef enum {
    BROKER_STATE_INIT          = 0,
    BROKER_STATE_STAGING_IN    = 1,
    BROKER_STATE_STAGED_IN     = 2,    /* 极短暂, 几乎只在内存中存在 */
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
    /* 标识 */
    char       trace_id[48];        /* "<src_cluster>-<src_job_id>" */
    uint32_t   src_job_id;
    uint32_t   remote_job_id;       /* 0 表示尚未代投 */
    broker_role_t role;
    uint8_t    hop_count;

    /* 用户身份 */
    char      *src_user_name;
    uint32_t   src_uid;
    char      *remote_user_name;
    uint32_t   remote_uid;
    uint32_t   remote_gid;
    char      *account;              /* 源端 account, 仅日志/追溯使用; 远端 sbatch 时清空, 由 sacctmgr 用 remote_user 的 default account */

    /* 集群路由 */
    char      *src_cluster;
    char      *dst_cluster;
    char      *target_partition;     /* MVP: 直接是 broker.conf 中的 DefaultRemotePartition */

    /* 工作目录 */
    char      *src_work_dir;
    char      *dst_work_dir;
    char      *script_path;

    /* sbatch 透传 (使用 Slurm 现成结构) */
    job_desc_msg_t *job_desc;        /* MVP: 整个 pack 后存 buf, 反序列化时复活 */

    /* 状态机 */
    broker_job_state_t state;
    char      *state_reason;
    uint32_t   retry_count;
    time_t     state_enter_time;
    time_t     submit_time;
    time_t     last_poll_time;

    /* 终态账单 */
    time_t     remote_start_time;
    time_t     remote_end_time;
    char      *remote_alloc_tres;
    int32_t    remote_exit_code;

    /* 取消相关 (MVP 新增, 用于反向传播) */
    bool       cancel_requested;     /* ctld 通知过取消 */
    bool       cancel_propagated;    /* 已通知远端 */

    /* 内部 */
    pthread_mutex_t lock;
} broker_job_t;
```

> 相对 v0.1：删除 `app_type`、`app_version`、`submit_way` 字段（MVP 不解析），新增 `cancel_requested`、`cancel_propagated`。

### 4.2 全局表

```c
/* src/slurmbrokerd/broker_job.c */
xhash_t  *g_broker_jobs;           /* key=trace_id, value=broker_job_t* */
list_t   *g_broker_jobs_list;      /* 用于状态机 tick 顺序遍历 */
pthread_mutex_t g_broker_jobs_lock;
```

### 4.3 持久化（JSONL 格式，MVP 专用）

```c
/* src/slurmbrokerd/persist.c */

/* 每 30s 调一次, 全量 dump */
int broker_state_save(void) {
    FILE *fp = fopen(STATE_FILE_TMP, "w");
    if (!fp) return SLURM_ERROR;

    pthread_mutex_lock(&g_broker_jobs_lock);
    list_itr_t *itr = list_iterator_create(g_broker_jobs_list);
    broker_job_t *job;
    while ((job = list_next(itr))) {
        char *json = broker_job_to_json(job);
        fprintf(fp, "%s\n", json);
        xfree(json);
    }
    list_iterator_destroy(itr);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    fflush(fp);
    fsync(fileno(fp));
    fclose(fp);

    rename(STATE_FILE, STATE_FILE_OLD);
    rename(STATE_FILE_TMP, STATE_FILE);
    return SLURM_SUCCESS;
}

/* 启动时调一次 */
int broker_state_restore(void) {
    FILE *fp = fopen(STATE_FILE, "r");
    if (!fp) return SLURM_SUCCESS;     /* 首次启动, 无状态文件 */

    char line[16384];
    while (fgets(line, sizeof(line), fp)) {
        broker_job_t *job = broker_job_from_json(line);
        if (job) {
            xhash_add(g_broker_jobs, job);
            list_append(g_broker_jobs_list, job);
        }
    }
    fclose(fp);
    info("restored %u broker jobs", list_count(g_broker_jobs_list));
    return SLURM_SUCCESS;
}
```

`job_desc_msg_t` 比较复杂，**MVP 直接用 Slurm `pack_job_desc_msg()` 序列化为二进制 buf 后再 base64 嵌入 JSON**：

```json
{"trace_id":"xian-12345","src_job_id":12345,"state":3,"src_user":"test1",
 "remote_user":"wz_test1","remote_job_id":8888,
 "job_desc_b64":"AAAACQAAAAA...(很长的 base64)..."}
```

> 这种混合方式：human-readable 字段方便排错，复杂结构靠 Slurm 自带 pack/unpack 保证正确性。

### 4.4 状态文件路径

```
/var/spool/slurm/broker/broker_state.jsonl       # 当前 checkpoint
/var/spool/slurm/broker/broker_state.jsonl.old   # 上次 checkpoint
/var/spool/slurm/broker/broker_state.jsonl.tmp   # 写入中, 写完原子 rename
```

---

## 五、状态机（MVP 实现）

### 5.1 状态跃迁规则（沿用 v0.1 §7.1 图，仅简化处理）

| 状态 | 入口动作 | 推进方式 | 失败处理 | 超时 |
|---|---|---|---|---|
| INIT | 入 hash 表，写 audit | 状态机 tick 检测 → 触发 egress 推送 | 直接 FAILED | 60s |
| STAGING_IN | 入队 stage worker | stage worker 完成回调 → 推进 | retry ≤ 3 次 | `data_size_GB * 120s + 600s` |
| STAGED_IN | 仅源端有，瞬时态 | 源端 → broker(B) 发 STAGED_IN，等响应 → SUBMITTED | retry ≤ 3，最终 FAILED | 30s |
| SUBMITTED | 记录 remote_job_id | 等 sync_ticker 拉到 RUNNING | 若 24h 仍 PENDING → FAILED | 24h |
| RUNNING | - | sync_ticker 周期更新 | NODE_FAIL → 重试一次 | `time_limit + 30min` |
| STAGING_OUT | 入队 stage worker | stage worker 完成 → COMPLETED | retry ≤ 3，最终 FAILED（保留远端 7 天） | 同 STAGING_IN |
| COMPLETED | 调 ctld UPDATE_JOB；schedule 24h 清理 | - | - | - |
| FAILED | 调 ctld UPDATE_JOB(FAILED)；schedule 7d 清理 | - | - | - |
| CANCELLED | 通知远端 kill；schedule 立即清理 | - | - | - |

### 5.2 状态机 tick 实现（MVP 版）

```c
/* src/slurmbrokerd/state_machine.c */

void state_machine_tick(void) {
    time_t now = time(NULL);
    list_t *to_remove = list_create(NULL);

    pthread_mutex_lock(&g_broker_jobs_lock);
    list_itr_t *itr = list_iterator_create(g_broker_jobs_list);
    broker_job_t *job;

    while ((job = list_next(itr))) {
        pthread_mutex_lock(&job->lock);

        /* 1. 处理用户 cancel (优先级最高) */
        if (job->cancel_requested && !job->cancel_propagated &&
            job->state != BROKER_STATE_CANCELLED &&
            job->state != BROKER_STATE_COMPLETED &&
            job->state != BROKER_STATE_FAILED) {
            egress_cancel_async(job);
            job->cancel_propagated = true;
            transition(job, BROKER_STATE_CANCELLED, "user cancelled");
            goto next;
        }

        /* 2. 状态分发 */
        switch (job->state) {

        case BROKER_STATE_INIT:
            if (job->role == BROKER_ROLE_ORIGINATOR) {
                egress_forward_async(job);    /* 发 REQUEST_BROKER_FORWARD_JOB */
                /* 状态推进由 egress 回调完成 */
            }
            if (now - job->state_enter_time > 60) {
                transition(job, BROKER_STATE_FAILED, "INIT timeout");
            }
            break;

        case BROKER_STATE_STAGING_IN:
            if (now - job->state_enter_time > stage_timeout(job)) {
                if (job->retry_count < 3) {
                    job->retry_count++;
                    transition(job, BROKER_STATE_INIT, "stage retry");
                } else {
                    transition(job, BROKER_STATE_FAILED, "stage timeout");
                }
            }
            break;

        case BROKER_STATE_STAGED_IN:
            /* 已发 STAGED_IN 给远端, 等响应; 超时则重试或 FAILED */
            if (now - job->state_enter_time > 30) {
                if (job->retry_count < 3) {
                    job->retry_count++;
                    egress_staged_in_async(job);    /* 重发 */
                    job->state_enter_time = now;
                } else {
                    transition(job, BROKER_STATE_FAILED, "staged_in timeout");
                }
            }
            break;

        case BROKER_STATE_SUBMITTED:
        case BROKER_STATE_RUNNING:
            /* 远端轮询的状态推进, 由 sync_ticker 完成, 这里只看超时 */
            if (job->state == BROKER_STATE_SUBMITTED &&
                now - job->state_enter_time > 86400) {
                transition(job, BROKER_STATE_FAILED, "remote pending too long");
            }
            break;

        case BROKER_STATE_STAGING_OUT:
            if (now - job->state_enter_time > stage_timeout(job)) {
                if (job->retry_count < 3) {
                    job->retry_count++;
                    stage_submit_out(job);
                    job->state_enter_time = now;
                } else {
                    transition(job, BROKER_STATE_FAILED, "stage_out timeout");
                }
            }
            break;

        case BROKER_STATE_COMPLETED:
        case BROKER_STATE_FAILED:
        case BROKER_STATE_CANCELLED:
            /* 终态: 通知 ctld, 调度清理, 出表 */
            ctld_inject_terminal_state(job);
            schedule_remote_cleanup(job);
            list_append(to_remove, job);
            break;

        default:
            break;
        }
next:
        pthread_mutex_unlock(&job->lock);
    }

    list_iterator_destroy(itr);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    /* 出表 */
    while ((job = list_pop(to_remove))) {
        xhash_delete(g_broker_jobs, job->trace_id, ...);
        broker_job_destroy(job);
    }
    list_destroy(to_remove);
}
```

### 5.3 状态推进点（事件驱动 vs 周期驱动）

| 推进点 | 触发方 | 周期/事件 |
|---|---|---|
| INIT → STAGING_IN | egress_forward_async 收到 ACK | 事件 |
| STAGING_IN → STAGED_IN | stage worker 完成回调 | 事件 |
| STAGED_IN → SUBMITTED | egress_staged_in_async 收到 RESPONSE_BROKER_SUBMITTED | 事件 |
| SUBMITTED → RUNNING | sync_ticker 拉到远端状态变化 | 10s 周期 |
| RUNNING → STAGING_OUT | sync_ticker 拉到远端 COMPLETED/FAILED | 10s 周期 |
| STAGING_OUT → COMPLETED | stage worker 完成回调 | 事件 |
| 任意 → CANCELLED | cancel_requested 标志 + state_machine tick 处理 | 1s 周期 |
| 任意 → FAILED | 各类超时 / 错误响应 | 事件 + 周期混合 |

---

## 六、RPC 消息（MVP 必要集合）

### 6.1 消息类型表

> 完全沿用 v0.1 §6 的消息类型 ID 偏移和 pack/unpack 格式。**MVP 不新增、不修改**。

| 消息类型 | 方向 | MVP 必需 |
|---|---|---|
| `REQUEST_FORWARD_JOB` / `RESPONSE_FORWARD_JOB` | ctld → broker（同机） | ✅ |
| `REQUEST_BROKER_FORWARD_JOB` / `RESPONSE_BROKER_ACK` | broker → broker | ✅ |
| `REQUEST_BROKER_STAGED_IN` / `RESPONSE_BROKER_SUBMITTED` | broker → broker | ✅ |
| `REQUEST_BROKER_QUERY_STATUS` / `RESPONSE_BROKER_STATUS` | broker → broker | ✅ |
| `REQUEST_BROKER_CANCEL` | ctld → broker，broker → broker | ✅ |
| `REQUEST_BROKER_CLEANUP` | broker → broker | ✅ |
| `REQUEST_USER_MAPPING_REFRESH` | ctld → broker | ❌ MVP 不实现，重启生效 |

### 6.2 sync_ticker 批量查询

```c
/* src/slurmbrokerd/sync_ticker.c */

void sync_ticker_run(void) {
    /* 收集所有 ORIGINATOR 角色, 状态在 SUBMITTED/RUNNING 的作业 */
    list_t *to_query = list_create(NULL);
    pthread_mutex_lock(&g_broker_jobs_lock);
    list_itr_t *itr = list_iterator_create(g_broker_jobs_list);
    broker_job_t *job;
    while ((job = list_next(itr))) {
        if (job->role == BROKER_ROLE_ORIGINATOR &&
            (job->state == BROKER_STATE_SUBMITTED ||
             job->state == BROKER_STATE_RUNNING)) {
            list_append(to_query, job);
        }
    }
    list_iterator_destroy(itr);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    if (!list_count(to_query)) return;

    /* 批量构造 trace_ids 数组 */
    uint32_t n = list_count(to_query);
    char **trace_ids = xmalloc(sizeof(char *) * n);
    int i = 0;
    while ((job = list_pop(to_query))) {
        trace_ids[i++] = xstrdup(job->trace_id);
    }

    /* 单次 RPC 查询所有 */
    broker_status_msg_t *resp = NULL;
    int rc = egress_query_status_sync(trace_ids, n, &resp);
    if (rc != SLURM_SUCCESS) {
        warning("query_status failed: %s", slurm_strerror(rc));
        goto out;
    }

    /* 分发更新 */
    for (uint32_t k = 0; k < resp->entry_count; k++) {
        apply_remote_status(&resp->entries[k]);
    }
out:
    /* free trace_ids and resp */
}

static void apply_remote_status(broker_status_entry_t *e) {
    broker_job_t *job = xhash_get(g_broker_jobs, e->trace_id);
    if (!job) return;

    pthread_mutex_lock(&job->lock);
    job->last_poll_time = time(NULL);

    switch (e->remote_state) {
    case JOB_PENDING:
        /* 保持 SUBMITTED；继续推送一次 remote_state=PENDING 让 ctld 更新字段 */
        ctld_update_remote_state(job);
        break;
    case JOB_RUNNING:
        if (job->state == BROKER_STATE_SUBMITTED) {
            job->remote_start_time = e->remote_start_time;
            job->remote_alloc_tres = xstrdup(e->remote_alloc_tres);
            transition(job, BROKER_STATE_RUNNING, "");
            ctld_update_remote_state(job);    /* 推送 remote_* 独立字段 */
        }
        break;
    case JOB_COMPLETE:
    case JOB_FAILED:
    case JOB_TIMEOUT:
    case JOB_CANCELLED:
        job->remote_end_time = e->remote_end_time;
        job->remote_exit_code = e->remote_exit_code;
        if (!job->remote_alloc_tres)
            job->remote_alloc_tres = xstrdup(e->remote_alloc_tres);
        if (e->remote_state == JOB_CANCELLED) {
            transition(job, BROKER_STATE_CANCELLED, "remote cancelled");
        } else {
            stage_submit_out(job);    /* 触发反向 rsync */
            transition(job, BROKER_STATE_STAGING_OUT, "");
        }
        break;
    }
    pthread_mutex_unlock(&job->lock);
}
```

### 6.3 ctld 状态回写（**字段化推送**，对接整体方案 §6.4）

> **MVP 关键决定**：broker → ctld 的状态注入**不写 `comment` 字段**，全部通过新增 RPC 推送独立 `remote_*` 字段。`comment` 是用户字段，broker 永不染指。

#### 6.3.1 RPC 协议契约（与整体方案 §6.4 一致）

新增两个 ctld 端 RPC（由整体方案文档定义、ctld 端实现 handler）：

| RPC | 触发时机 | payload 关键字段 | ctld 行为 |
|---|---|---|---|
| `REQUEST_BROKER_UPDATE_REMOTE_STATE` | sync_ticker 每 10s 检测到状态/资源变化 | `src_job_id`、`trace_id`、`remote_cluster_name`、`remote_partition_name`、`remote_job_id`、`remote_state`、`remote_alloc_tres`、`remote_start_time` | 写入 `job_record_t` 的 `remote_*` 字段；保持影子作业 PENDING(Held) | 
| `REQUEST_BROKER_TERMINAL_STATE` | broker 进入 COMPLETED / FAILED / CANCELLED | 同上 + `remote_end_time`、`remote_exit_code` | 影子作业 PENDING(Held) 跳变为 COMPLETED/FAILED/CANCELLED；写 sacct（`jobcomp_g_record_job_end`）；释放 priority hold |

> **broker 工程师注意**：以上字段名和顺序须与 `跨域调度MVP整体方案.md` §6.4 / `Broker详细设计文档.md` v0.1 §6 完全一致。pack/unpack 在 broker 与 ctld 共用 `src/common/slurm_protocol_pack.c` 的同一段代码。

#### 6.3.2 broker 端发送实现

```c
/* src/slurmbrokerd/egress.c */

#include "src/common/slurm_protocol_defs.h"

/* 推送 remote_* 字段到本地 ctld（取代旧的 ctld_update_comment） */
void ctld_update_remote_state(broker_job_t *job) {
    broker_remote_state_msg_t msg = {0};
    msg.src_job_id              = job->src_job_id;
    msg.trace_id                = job->trace_id;
    msg.remote_cluster_name     = job->dst_cluster;
    msg.remote_partition_name   = job->dst_partition;
    msg.remote_job_id           = job->remote_job_id;
    msg.remote_state            = job->state;            /* broker 状态机映射到 Slurm 状态 */
    msg.remote_alloc_tres       = job->remote_alloc_tres ?: "";
    msg.remote_start_time       = job->remote_start_time;

    slurm_msg_t req_msg;
    slurm_msg_t_init(&req_msg);
    req_msg.msg_type = REQUEST_BROKER_UPDATE_REMOTE_STATE;
    req_msg.data     = &msg;

    int rc;
    if (slurm_send_recv_controller_rc_msg(&req_msg, &rc, working_cluster_rec) < 0
        || rc != SLURM_SUCCESS) {
        warning("trace_id=%s: failed to update ctld remote state, rc=%d",
                job->trace_id, rc);
    }
}

/* 终态时调用：让 ctld 把 PENDING(Held) 影子作业跳变为终态 + 写 sacct */
void ctld_inject_terminal_state(broker_job_t *job) {
    broker_terminal_state_msg_t msg = {0};
    msg.src_job_id              = job->src_job_id;
    msg.trace_id                = job->trace_id;
    msg.remote_cluster_name     = job->dst_cluster;
    msg.remote_partition_name   = job->dst_partition;
    msg.remote_job_id           = job->remote_job_id;
    msg.remote_state            = job->state;
    msg.remote_alloc_tres       = job->remote_alloc_tres ?: "";
    msg.remote_start_time       = job->remote_start_time;
    msg.remote_end_time         = job->remote_end_time;
    msg.remote_exit_code        = job->remote_exit_code;

    slurm_msg_t req_msg;
    slurm_msg_t_init(&req_msg);
    req_msg.msg_type = REQUEST_BROKER_TERMINAL_STATE;
    req_msg.data     = &msg;

    int rc;
    if (slurm_send_recv_controller_rc_msg(&req_msg, &rc, working_cluster_rec) < 0
        || rc != SLURM_SUCCESS) {
        error("trace_id=%s: failed to inject terminal state, rc=%d "
              "(ctld 跨域线程会在下一轮 tick 重试，broker 不重发)",
              job->trace_id, rc);
        return;
    }
    info("trace_id=%s: ctld accepted terminal state remote_jobid=%u exit=%d",
         job->trace_id, job->remote_job_id, job->remote_exit_code);
}
```

#### 6.3.3 broker 状态机 → Slurm 状态映射

| broker 状态 | 推送给 ctld 的 `remote_state` | 何时推送 |
|---|---|---|
| `INIT` | （不推送） | 仅 broker 内部 |
| `STAGING_IN` | （不推送） | 仅 broker 内部 |
| `STAGED_IN` / `SUBMITTED` | `JOB_PENDING` | sync_ticker 首次拿到 `remote_job_id` 时立刻推送一次 |
| `RUNNING` | `JOB_RUNNING` | sync_ticker 检测远端 RUNNING |
| `STAGING_OUT` | `JOB_RUNNING`（还在跑回传） | 同上（不变更） |
| `COMPLETED` | `JOB_COMPLETE` | TERMINAL_STATE |
| `FAILED` | `JOB_FAILED` 或 `JOB_TIMEOUT` | TERMINAL_STATE |
| `CANCELLED` | `JOB_CANCELLED` | TERMINAL_STATE |

> 用户在源端 `squeue --remote` / `scontrol show job` 看到的 `RemoteState=...` 即来自这个映射。

---

## 七、用户场景实现详解

### 7.1 US-1: sbatch 提交跨域作业

**ctld 端职责**（不在本文档详述）：
- 检测虚拟队列、超时、打跨域标签、挂起作业
- 调 broker `REQUEST_FORWARD_JOB`

**broker 端职责**：

```c
/* src/slurmbrokerd/handler_ctld.c */

int handle_forward_job(slurm_msg_t *msg) {
    forward_job_msg_t *req = msg->data;

    /* 1. 溢出保护 (主限流由 ctld 端虚拟 partition.MaxJobs 承担,
     *    broker 仅做兜底, 防止 ctld 误配置或瞬时风暴打爆 broker) */
    if (count_inflight() >= g_broker_conf.max_inflight) {
        return ESLURM_BROKER_OVERLOAD;
    }

    /* 2. 用户映射 */
    user_mapping_t *m = user_mapping_lookup(req->src_user_name, req->target_cluster);
    if (!m) {
        return ESLURM_BROKER_NO_USER_MAPPING;
    }

    /* 3. 创建 broker_job_t */
    broker_job_t *job = broker_job_create();
    snprintf(job->trace_id, sizeof(job->trace_id), "%s-%u",
             g_broker_conf.cluster_name, req->src_job_id);
    job->role             = BROKER_ROLE_ORIGINATOR;
    job->src_job_id       = req->src_job_id;
    job->src_user_name    = xstrdup(req->src_user_name);
    job->src_uid          = req->src_uid;
    job->remote_user_name = xstrdup(m->remote_user);
    job->remote_uid       = m->remote_uid;
    job->remote_gid       = m->remote_gid;
    job->account          = xstrdup(req->account);   /* 仅用于源端追溯/日志, 不会传给远端 sbatch */
    job->src_cluster      = xstrdup(g_broker_conf.cluster_name);
    job->dst_cluster      = xstrdup(req->target_cluster);
    job->target_partition = xstrdup(g_broker_conf.default_remote_partition);
    job->src_work_dir     = xstrdup(req->src_work_dir);
    job->script_path      = xstrdup(req->script_path);
    job->job_desc         = clone_job_desc_msg(req->job_desc);
    job->state            = BROKER_STATE_INIT;
    job->state_enter_time = time(NULL);

    /* 4. 入表 */
    pthread_mutex_lock(&g_broker_jobs_lock);
    xhash_add(g_broker_jobs, job);
    list_append(g_broker_jobs_list, job);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    /* 5. 立即触发持久化 (避免 30s 窗口内重启丢失) */
    persist_async_request();

    info("trace_id=%s job=%u accepted, src_user=%s remote_user=%s",
         job->trace_id, job->src_job_id, job->src_user_name, job->remote_user_name);

    /* 6. 返回 ACK */
    forward_job_resp_msg_t resp = {
        .error_code = 0,
        .trace_id   = job->trace_id,
    };
    slurm_send_response(msg, &resp);

    /* 7. 状态机 tick 会负责 INIT -> STAGING_IN 的推送 */
    return SLURM_SUCCESS;
}
```

**远端 broker 接单**：

```c
/* src/slurmbrokerd/handler_remote.c */

int handle_broker_forward_job(slurm_msg_t *msg) {
    broker_forward_job_msg_t *req = msg->data;

    /* 1. hop 限制 */
    if (req->hop_count > 0) {
        return ESLURM_BROKER_HOP_EXCEEDED;
    }

    /* 2. 反向校验用户映射 */
    user_mapping_t *m = user_mapping_lookup(req->src_user_name, req->src_cluster);
    if (!m || strcmp(m->remote_user, req->remote_user_name) != 0) {
        return ESLURM_BROKER_USER_MAPPING_MISMATCH;
    }

    /* 3. 创建 RECEIVER 角色的 broker_job_t */
    broker_job_t *job = broker_job_create();
    strncpy(job->trace_id, req->trace_id, sizeof(job->trace_id));
    job->role             = BROKER_ROLE_RECEIVER;
    job->hop_count        = req->hop_count + 1;
    job->src_cluster      = xstrdup(req->src_cluster);
    job->src_job_id       = req->src_job_id;
    job->src_user_name    = xstrdup(req->src_user_name);
    job->remote_user_name = xstrdup(req->remote_user_name);
    job->remote_uid       = m->remote_uid;
    job->remote_gid       = m->remote_gid;
    job->target_partition = xstrdup(req->target_partition);
    job->job_desc         = clone_job_desc_msg(req->job_desc);
    job->state            = BROKER_STATE_INIT;
    job->state_enter_time = time(NULL);

    /* 4. 创建远端 staging 目录 */
    char dst_dir[PATH_MAX];
    snprintf(dst_dir, sizeof(dst_dir), "/work/home/%s/.burst/%s/%u",
             job->remote_user_name, job->src_cluster, job->src_job_id);
    job->dst_work_dir = xstrdup(dst_dir);

    /* MVP: 用 sudo -u 创建 */
    char cmd[PATH_MAX + 128];
    snprintf(cmd, sizeof(cmd),
             "sudo -u %s mkdir -p '%s' && sudo -u %s chmod 700 '%s'",
             job->remote_user_name, dst_dir,
             job->remote_user_name, dst_dir);
    if (system(cmd) != 0) {
        broker_job_destroy(job);
        return ESLURM_BROKER_STAGING_DIR_FAILED;
    }

    /* 5. 入表 */
    pthread_mutex_lock(&g_broker_jobs_lock);
    xhash_add(g_broker_jobs, job);
    list_append(g_broker_jobs_list, job);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    /* 6. 返回 ACK */
    broker_ack_msg_t resp = {
        .error_code   = 0,
        .trace_id     = job->trace_id,
        .dst_work_dir = job->dst_work_dir,
    };
    slurm_send_response(msg, &resp);

    info("trace_id=%s received, dst_work_dir=%s", job->trace_id, job->dst_work_dir);
    return SLURM_SUCCESS;
}
```

### 7.2 US-2: scancel 反向传播

**ctld 跨域调度线程**（在跨域调度文档定义；本文档假设其行为）：

```text
ctld 跨域调度线程每 10s tick:
  for each shadow_job in pending_held_with_cd_trace:
    if shadow_job.state == CANCELLED && !shadow_job.cancel_propagated_flag:
      ctld → broker: REQUEST_BROKER_CANCEL { trace_id }
      shadow_job.cancel_propagated_flag = true
```

**broker 处理 cancel**：

```c
/* src/slurmbrokerd/handler_ctld.c */

int handle_cancel_request(slurm_msg_t *msg) {
    broker_cancel_msg_t *req = msg->data;

    pthread_mutex_lock(&g_broker_jobs_lock);
    broker_job_t *job = xhash_get(g_broker_jobs, req->trace_id);
    if (!job) {
        pthread_mutex_unlock(&g_broker_jobs_lock);
        return SLURM_SUCCESS;    /* 已结束或不存在, 视为成功 */
    }

    pthread_mutex_lock(&job->lock);
    job->cancel_requested = true;
    if (req->reason) {
        xfree(job->state_reason);
        job->state_reason = xstrdup(req->reason);
    }
    pthread_mutex_unlock(&job->lock);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    /* 实际推进由状态机 tick 完成 */
    info("trace_id=%s cancel requested", req->trace_id);
    return SLURM_SUCCESS;
}
```

**broker → broker cancel**：

```c
/* src/slurmbrokerd/egress.c */

void egress_cancel_async(broker_job_t *job) {
    broker_cancel_msg_t req = {
        .trace_id = xstrdup(job->trace_id),
        .reason   = xstrdup(job->state_reason ?: "user_cancel"),
    };

    egress_send_async(REQUEST_BROKER_CANCEL, &req,
                      on_cancel_response, job);
}

static void on_cancel_response(broker_job_t *job, int rc, void *resp) {
    if (rc != SLURM_SUCCESS) {
        warning("trace_id=%s: cancel failed remotely: %s",
                job->trace_id, slurm_strerror(rc));
        /* MVP: 即使远端失败, 源端仍标 CANCELLED, 后续运维处理 */
    }
}
```

**远端 broker 处理 cancel**：

```c
/* src/slurmbrokerd/handler_remote.c */

int handle_broker_cancel(slurm_msg_t *msg) {
    broker_cancel_msg_t *req = msg->data;

    pthread_mutex_lock(&g_broker_jobs_lock);
    broker_job_t *job = xhash_get(g_broker_jobs, req->trace_id);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    if (!job) return SLURM_SUCCESS;

    if (job->remote_job_id) {
        /* 调用本地 slurmctld 的 kill */
        job_step_kill_msg_t kill = {
            .job_id = job->remote_job_id,
            .signal = SIGKILL,
            .flags  = KILL_FULL_JOB,
        };
        slurm_kill_job(kill.job_id, kill.signal, kill.flags);
        info("trace_id=%s killed remote_job_id=%u", job->trace_id, job->remote_job_id);
    }

    pthread_mutex_lock(&job->lock);
    job->cancel_requested = true;
    transition(job, BROKER_STATE_CANCELLED, "remote_cancel");
    pthread_mutex_unlock(&job->lock);

    return SLURM_SUCCESS;
}
```

### 7.3 US-3: squeue / scontrol show 看远端状态（**字段化**）

**MVP 方案**：broker 通过 `REQUEST_BROKER_UPDATE_REMOTE_STATE` 推送独立 `remote_*` 字段到 ctld，ctld 写入 `job_record_t`，sbatch / squeue / scontrol / sacct 走整体方案 §4 改造后的字段化展示路径。

源端 broker 在 `apply_remote_status()` 后调 `ctld_update_remote_state(job)`（见 §6.3.2），不写 `comment`。用户：

```bash
# 默认 squeue (影子作业 PENDING(Held))
$ squeue -u test1
JOBID PARTITION     NAME     USER  ST  TIME NODES NODELIST(REASON)
12345 wzhcnormal_v run.sh    test1 PD  0:00     1 (JobHeldUser)

# 看跨域状态: 一个新选项 --remote
$ squeue -u test1 --remote
JOBID PARTITION  NAME    USER  ST  TIME REMOTE_CLUSTER REMOTE_JOBID REMOTE_STATE REMOTE_TRES
12345 wzhcnormal_v run.sh test1 PD  0:00 wz_cluster     8888         RUNNING      cpu=32,mem=128G

# 也可用 -o + %R* 占位符
$ squeue -u test1 -o "%.10i %.4T %.15RC %.10RJ %.10RS %.30RT"
     JOBID   ST  REMOTE_CLUSTER REMOTE_JOB REMOTE_STA REMOTE_TRES
     12345   PD  wz_cluster     8888       RUNNING    cpu=32,mem=128G

# scontrol show job 字段化输出
$ scontrol show job 12345
JobId=12345 ... JobState=PENDING ... Reason=JobHeldUser
   CrossDomain=yes AppName=lammps-2Aug2023-intelmpi2018
   RemoteCluster=wz_cluster RemotePartition=wzhcnormal
   RemoteJobId=8888 RemoteState=RUNNING
   RemoteAllocTRES=cpu=32,mem=128G,node=1
   RemoteStartTime=2026-04-27T17:30:00
   Comment=<用户原始 comment, broker 不污染>
```

**broker 端实现要点**：
- sync_ticker 每 10s 调 `ctld_update_remote_state(job)`（**只在 `remote_state` / `remote_alloc_tres` / `remote_start_time` 变化时推送**，避免无谓 RPC）
- 终态时改调 `ctld_inject_terminal_state(job)`，附 `remote_end_time` / `remote_exit_code`
- broker 全程**不调** `slurm_update_job(comment)`，确保用户 comment 不被覆盖

### 7.4 US-4: broker 重启恢复

**触发**：systemd 拉起 slurmbrokerd 进程。

**步骤**：

```c
/* src/slurmbrokerd/slurmbrokerd.c::main() */

int main(int argc, char **argv) {
    /* ... 解析参数, log_init ... */

    broker_conf_load();
    user_mapping_load();
    g_broker_jobs = xhash_init(...);
    g_broker_jobs_list = list_create(NULL);

    /* 关键: 恢复状态 */
    if (broker_state_restore() != SLURM_SUCCESS) {
        error("state restore failed, exiting");
        exit(1);
    }
    info("restored %u in-flight cross-domain jobs",
         list_count(g_broker_jobs_list));

    /* 启动各线程 */
    threads_start();

    /* 状态机 tick 第一轮会自动:
     * - 对 INIT 作业重新 egress
     * - 对 STAGING_IN 作业检测超时, 走重试或 FAILED
     * - 对 SUBMITTED/RUNNING 作业由 sync_ticker 重新轮询
     * - 对 STAGING_OUT 作业重新触发 stage worker
     * 不需额外 special-case 处理
     */

    notify_systemd_ready();
    main_loop_until_sigterm();
    graceful_shutdown();
    return 0;
}
```

**幂等性关键点**：
- `egress_forward_async` 必须幂等：远端 broker 接单时若 trace_id 已存在，返回当前状态而非 ESLURM_BROKER_DUPLICATE
- `slurm_submit_batch_job` 不幂等：MVP 通过 broker 的 `remote_job_id != 0` 标志位防止重复提交
- rsync 天然幂等

```c
/* 远端 broker 接单时的幂等处理 */
int handle_broker_forward_job(slurm_msg_t *msg) {
    broker_forward_job_msg_t *req = msg->data;

    pthread_mutex_lock(&g_broker_jobs_lock);
    broker_job_t *existing = xhash_get(g_broker_jobs, req->trace_id);
    pthread_mutex_unlock(&g_broker_jobs_lock);

    if (existing) {
        /* 重复请求: 直接返回当前状态的 ACK */
        broker_ack_msg_t resp = {
            .error_code   = 0,
            .trace_id     = existing->trace_id,
            .dst_work_dir = existing->dst_work_dir,
        };
        slurm_send_response(msg, &resp);
        info("trace_id=%s duplicate forward, returned existing", req->trace_id);
        return SLURM_SUCCESS;
    }

    /* ... 正常创建流程 ... */
}
```

---

## 八、模块设计（MVP 版文件清单）

### 8.1 源码布局

```text
src/slurmbrokerd/
├── slurmbrokerd.c          # main(), 启动序列, 信号处理
├── slurmbrokerd.h          # 全局类型导出
├── broker_conf.c/.h        # broker.conf + user_mapping.conf 解析
├── broker_job.c/.h         # broker_job_t, hash 表, JSONL 序列化
├── listener.c/.h           # 8442/8443 单线程 accept + dispatch
├── handler_ctld.c          # 处理 ctld 的 RPC
├── handler_remote.c        # 处理对端 broker 的 RPC
├── egress.c/.h             # 主动出站调用 (broker / 远端 ctld)
├── state_machine.c/.h      # 状态机 tick
├── stage.c/.h              # rsync 子进程管理
├── rewrite.c/.h            # ★ 极简脚本改写 (MVP 单文件, 替换 v0.1 的 translator_*)
├── software.c/.h           # ★ lookup_software.sh 调用器 (fork+exec)
├── user_mapping.c/.h       # 用户映射查询 (启动加载, 不热更)
├── persist.c/.h            # JSONL checkpoint
├── sync_ticker.c/.h        # 10s 轮询远端
├── proto.c/.h              # 自定义 RPC pack/unpack
└── Makefile.am
```

**对比 v0.1**：
- ❌ 删除 `translator.c/.h` `translator_portal.c` `translator_cli.c` → 合并为 `rewrite.c/.h`
- ❌ 删除 `app_route.c/.h` → 改为外部脚本 `lookup_software.sh`，broker 用 `software.c` 包一层调用
- ❌ 删除 `audit.c/.h`（用 syslog/log.c 现成接口）
- ❌ 删除 `sbroker.c`（MVP 无 CLI 工具）
- 总文件数：v0.1 21 个 → MVP **16 个**

### 8.2 软件路径解析（`software.c`） + 极简脚本改写（`rewrite.c`）

#### 8.2.1 `software.c`：调外部 `lookup_software.sh`

`lookup_software.sh` 由 broker 部署时随附（运维管理 `software_routes.conf`），broker 通过 fork+exec 调用，不解析任何路由表，**ctld 端完全不感知**。

```c
/* src/slurmbrokerd/software.c */

/*
 * 调 /opt/slurm-broker/scripts/lookup_software.sh <cluster> <app>
 * 成功: stdout 一行绝对路径, 退出码 0
 * 失败: 退出码 != 0, stderr 给运维诊断
 *
 * 超时: 3s (软件路径查询应该极快)
 * 返回的 *out_path 由调用者 xfree()
 */
int lookup_software_path(const char *cluster, const char *app, char **out_path) {
    int pipefd[2];
    if (pipe(pipefd) < 0) return errno;

    pid_t pid = fork();
    if (pid == 0) {                                    /* child */
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        execl(g_broker_conf.lookup_software_script,
              g_broker_conf.lookup_software_script,
              cluster, app, (char *)NULL);
        _exit(127);
    }
    close(pipefd[1]);

    /* 读 stdout, 同时 waitpid with 3s 超时 */
    char buf[PATH_MAX] = {0};
    ssize_t n = read_with_timeout(pipefd[0], buf, sizeof(buf) - 1, 3000);
    close(pipefd[0]);

    int status;
    if (waitpid_timeout(pid, &status, 3000) < 0) {
        kill(pid, SIGKILL); waitpid(pid, &status, 0);
        return ESLURM_BROKER_LOOKUP_TIMEOUT;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        return ESLURM_BROKER_LOOKUP_FAILED;
    }

    /* 去掉行尾换行 */
    char *nl = strchr(buf, '\n'); if (nl) *nl = '\0';
    if (n <= 0 || buf[0] != '/') return ESLURM_BROKER_LOOKUP_FAILED;

    *out_path = xstrdup(buf);
    return SLURM_SUCCESS;
}
```

#### 8.2.2 `rewrite.c`：脚本改写规则

```c
/* src/slurmbrokerd/rewrite.c */

/*
 * MVP 改写规则 (远端 broker 在 STAGED_IN -> SUBMITTED 之前调用):
 *
 * 1. 调 lookup_software.sh src_cluster <app>  → src_prefix
 *    调 lookup_software.sh dst_cluster <app>  → dst_prefix
 *    (任一失败 → 整作业 FAILED, 不再重试)
 * 2. 在 dst_work_dir 下读取原 script_path (e.g. run.sh)
 * 3. 应用以下行级替换:
 *    a) 替换 #SBATCH -p <X>      → #SBATCH -p <target_partition>
 *    b) 替换 #SBATCH --partition=X → #SBATCH --partition=<target_partition>
 *    c) 删除 #SBATCH --reservation=...
 *    d) 删除 #SBATCH --cross-domain=* / --app=*
 *    e) 删除 #SBATCH --account=* / -A
 *       (源端 account 名在远端通常不存在; 远端 Slurm 会用 remote_user
 *        在 sacctmgr 中的 default account 自动选出 association)
 *    f) 行内字符串替换 src_prefix → dst_prefix
 *       (覆盖 source 行 / module use 行 / 任意写死的绝对路径)
 * 4. 写到 <dst_work_dir>/<basename(script)>.cd_modified.sh
 * 5. job_desc->script 指向修改后文件
 * 6. 同步清空 job_desc->account, 让远端 slurm_submit_batch_job
 *    走"用户默认 account"路径 (不需要 broker.conf 配 DefaultRemoteAccount)
 *
 * 不做的:
 * - 不解析 .portal/job_portal.var (Portal 作业 MVP 不支持)
 * - 不识别"安装目录之外"的路径 (用户应通过 source 引入软件 env.sh)
 */

int rewrite_job_script(broker_job_t *job, char **out_modified_path) {
    char *src_prefix = NULL, *dst_prefix = NULL;

    /* 1. 双向 lookup */
    int rc = lookup_software_path(job->src_cluster, job->app_name, &src_prefix);
    if (rc != SLURM_SUCCESS) {
        error("trace_id=%s: lookup_software FAILED src_cluster=%s app=%s rc=%d",
              job->trace_id, job->src_cluster, job->app_name, rc);
        return rc;
    }
    rc = lookup_software_path(job->dst_cluster, job->app_name, &dst_prefix);
    if (rc != SLURM_SUCCESS) {
        xfree(src_prefix);
        error("trace_id=%s: lookup_software FAILED dst_cluster=%s app=%s rc=%d",
              job->trace_id, job->dst_cluster, job->app_name, rc);
        return rc;
    }

    /* 2. 改写 */
    char src_path[PATH_MAX], dst_path[PATH_MAX];
    snprintf(src_path, sizeof(src_path), "%s/%s",
             job->dst_work_dir, job->script_path);
    snprintf(dst_path, sizeof(dst_path), "%s/%s.cd_modified.sh",
             job->dst_work_dir, basename(job->script_path));

    FILE *fin = fopen(src_path, "r");
    FILE *fout = fopen(dst_path, "w");
    if (!fin || !fout) {
        if (fin) fclose(fin);
        if (fout) fclose(fout);
        xfree(src_prefix); xfree(dst_prefix);
        return ESLURM_BROKER_SCRIPT_NOT_FOUND;
    }

    char line[8192];
    while (fgets(line, sizeof(line), fin)) {
        if (line_matches_partition(line)) {
            fprintf(fout, "#SBATCH --partition=%s\n", job->target_partition);
        } else if (line_matches_drop(line)) {
            /* 跳过 reservation / cross-domain / app */
        } else {
            char *replaced = str_replace_all(line, src_prefix, dst_prefix);
            fputs(replaced ? replaced : line, fout);
            xfree(replaced);
        }
    }
    fclose(fin); fclose(fout);

    /* 改 owner 为 remote_user, 加可执行 */
    char chown_cmd[PATH_MAX + 128];
    snprintf(chown_cmd, sizeof(chown_cmd),
             "sudo chown %u:%u '%s' && sudo chmod 755 '%s'",
             job->remote_uid, job->remote_gid, dst_path, dst_path);
    system(chown_cmd);

    /* 关键: 清空 job_desc 里的 account, 让远端 Slurm 用 remote_user
     * 在 sacctmgr 中的 default account 自动选出 partition association.
     * 源端 account 名 (如 ac_lab1) 在远端通常不存在, 不清空会被远端
     * sacctmgr 拒绝 (AccountingStorageEnforce=associations). */
    xfree(job->job_desc->account);
    job->job_desc->account = NULL;

    info("trace_id=%s: rewrite OK src_prefix=%s dst_prefix=%s out=%s "
         "(remote account cleared, use remote_user default)",
         job->trace_id, src_prefix, dst_prefix, dst_path);

    xfree(src_prefix); xfree(dst_prefix);
    *out_modified_path = xstrdup(dst_path);
    return SLURM_SUCCESS;
}

static bool line_matches_partition(const char *line) {
    /* #SBATCH -p X  或  #SBATCH --partition=X */
    static regex_t re;
    static bool inited = false;
    if (!inited) {
        regcomp(&re, "^[[:space:]]*#SBATCH[[:space:]]+(-p[[:space:]]+|--partition=)",
                REG_EXTENDED);
        inited = true;
    }
    return regexec(&re, line, 0, NULL, 0) == 0;
}

static bool line_matches_drop(const char *line) {
    static regex_t re;
    static bool inited = false;
    if (!inited) {
        regcomp(&re,
                "^[[:space:]]*#SBATCH[[:space:]]+("
                "--reservation|--cross-domain|--app|--account=|-A[[:space:]]+"
                ")",
                REG_EXTENDED);
        inited = true;
    }
    return regexec(&re, line, 0, NULL, 0) == 0;
}
```

#### 8.2.3 `lookup_software.sh` 与 `software_routes.conf` 示例

```bash
#!/bin/bash
# /opt/slurm-broker/scripts/lookup_software.sh
# 用法: lookup_software.sh <cluster_name> <app_name>
# 输出: <绝对路径> (单行)
# 退出: 0=成功, 1=未找到, 2=参数错误

set -euo pipefail
CLUSTER="${1:-}"; APP="${2:-}"
[[ -z "$CLUSTER" || -z "$APP" ]] && { echo "usage: $0 <cluster> <app>" >&2; exit 2; }

CONF="${BROKER_CONF_DIR:-/opt/slurm-broker/conf}/software_routes.conf"
[[ -r "$CONF" ]] || { echo "cannot read $CONF" >&2; exit 1; }

awk -F'|' -v c="$CLUSTER" -v a="$APP" '
    /^#/ || NF < 3 { next }
    $1==c && $2==a { print $3; found=1; exit }
    END            { if (!found) exit 1 }
' "$CONF"
```

```text
# /opt/slurm-broker/conf/software_routes.conf
# 格式: cluster_name|app_name|absolute_path
xian_cluster|lammps-2Aug2023-intelmpi2018|/public/software/lammps/2Aug2023-intelmpi2018
wz_cluster  |lammps-2Aug2023-intelmpi2018|/opt/scnet/apps/lammps/2Aug2023-intelmpi2018
xian_cluster|vasp-6.4.0-intelmpi2018      |/public/software/vasp/6.4.0-intelmpi2018
wz_cluster  |vasp-6.4.0-intelmpi2018      |/opt/scnet/apps/vasp/6.4.0-intelmpi2018
# ... 其他应用
```

> **MVP 自包含原则**：`lookup_software.sh` / `software_routes.conf` **完全归 broker 部署管理**，slurm.conf / ctld 不出现 `LookupSoftwareScript` 配置项。后续 broker 可以无缝把它替换为 RDB 查询、API 调用，只要保持"输入 cluster+app、输出绝对路径"的契约即可。

> **演进锚点**：v0.1 完整版升级时，只需把 `rewrite.c` 替换为 `translator.c` + `translator_portal.c` + `translator_cli.c`，调用方接口不变；`software.c` 演进为支持热加载、批量预解析。

### 8.3 listener 单线程实现

```c
/* src/slurmbrokerd/listener.c */

void *listener_thread(void *arg) {
    int fd_ctld = listen_socket("0.0.0.0", g_broker_conf.ctld_port);
    int fd_peer = listen_socket("0.0.0.0", g_broker_conf.peer_port);

    fd_set readfds;
    int maxfd = (fd_ctld > fd_peer ? fd_ctld : fd_peer) + 1;

    while (!g_shutdown_requested) {
        FD_ZERO(&readfds);
        FD_SET(fd_ctld, &readfds);
        FD_SET(fd_peer, &readfds);

        struct timeval tv = { .tv_sec = 1 };    /* 1s 周期检查 shutdown */
        int rc = select(maxfd, &readfds, NULL, NULL, &tv);
        if (rc <= 0) continue;

        if (FD_ISSET(fd_ctld, &readfds)) {
            int client = accept(fd_ctld, NULL, NULL);
            if (client >= 0) handle_one_request(client, true /*from_ctld*/);
        }
        if (FD_ISSET(fd_peer, &readfds)) {
            int client = accept(fd_peer, NULL, NULL);
            if (client >= 0) handle_one_request(client, false);
        }
    }
    close(fd_ctld); close(fd_peer);
    return NULL;
}

static void handle_one_request(int fd, bool from_ctld) {
    slurm_msg_t msg;
    slurm_msg_t_init(&msg);

    /* slurm_recv_msg 内部完成 munge_decode */
    if (slurm_recv_msg_blocking(fd, &msg) != SLURM_SUCCESS) {
        close(fd);
        return;
    }

    if (from_ctld) {
        dispatch_ctld_msg(&msg);
    } else {
        dispatch_remote_msg(&msg);
    }

    slurm_msg_free(&msg);
    close(fd);
}
```

> MVP 用同步阻塞处理。预估单 broker 在途作业 ≤ 1000，QPS ≤ 100，单线程足够。后续若性能不够，引入连接队列 + worker pool。

---

## 九、数据传输（MVP 版 rsync）

### 9.1 stage-in 命令模板

```bash
sudo -u <src_user> /usr/bin/rsync \
    -rlptDvz \
    --no-owner --no-group \
    --rsync-path="sudo --user=<remote_user> rsync" \
    -e "ssh -i /etc/slurm/broker_id_ed25519 \
            -o StrictHostKeyChecking=yes \
            -o BatchMode=yes \
            -o ConnectTimeout=30 \
            -l slurm-broker" \
    <src_work_dir>/ \
    <remote_broker_host>:<dst_work_dir>/
```

**实现**（`stage.c`）：

```c
int stage_in_run(broker_job_t *job) {
    char rsh[256];
    snprintf(rsh, sizeof(rsh),
             "ssh -i %s -o StrictHostKeyChecking=yes -o BatchMode=yes "
             "-o ConnectTimeout=30 -l %s",
             g_broker_conf.stage_ssh_key, g_broker_conf.stage_ssh_user);

    char rsync_path[256];
    snprintf(rsync_path, sizeof(rsync_path),
             "sudo --user=%s rsync", job->remote_user_name);

    char remote_endpoint[512];
    snprintf(remote_endpoint, sizeof(remote_endpoint),
             "%s:%s/", g_broker_conf.remote_broker_host, job->dst_work_dir);

    char src_endpoint[PATH_MAX];
    snprintf(src_endpoint, sizeof(src_endpoint), "%s/", job->src_work_dir);

    char *argv[] = {
        "sudo", "-u", job->src_user_name,
        "/usr/bin/rsync",
        "-rlptDvz", "--no-owner", "--no-group",
        "--rsync-path", rsync_path,
        "-e", rsh,
        src_endpoint,
        remote_endpoint,
        NULL
    };

    pid_t pid = fork();
    if (pid == 0) {
        execv("/usr/bin/sudo", argv);
        _exit(127);
    }
    int status;
    waitpid(pid, &status, 0);
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0)
        ? SLURM_SUCCESS : SLURM_ERROR;
}
```

### 9.2 SSH key 部署

**前置条件**（运维一次性配置）：

```bash
# 1. 在源端 broker 主机上生成 key (slurm 用户)
sudo -u slurm ssh-keygen -t ed25519 -f /etc/slurm/broker_id_ed25519 -N ""
sudo chmod 600 /etc/slurm/broker_id_ed25519

# 2. 把公钥拷到远端 broker 主机的 slurm-broker 用户 authorized_keys
# (需要远端先创建 slurm-broker 用户)
ssh-copy-id -i /etc/slurm/broker_id_ed25519.pub slurm-broker@broker.wz.example.com

# 3. 远端 sudoers (允许 slurm-broker 切换到任意 wz_* 用户)
echo "slurm-broker ALL=(wz_*) NOPASSWD: /usr/bin/rsync, /bin/mkdir, /bin/chmod" \
  | sudo tee /etc/sudoers.d/slurm-broker

# 4. 源端 sudoers (允许 slurm 用户切换到任意 test* 用户读源数据)
echo "slurm ALL=(test*) NOPASSWD: /usr/bin/rsync" \
  | sudo tee /etc/sudoers.d/slurm-rsync
```

### 9.3 stage-out 命令模板

```bash
sudo -u <src_user> /usr/bin/rsync \
    -rlptDvz \
    --rsync-path="sudo --user=<remote_user> rsync" \
    -e "ssh -i /etc/slurm/broker_id_ed25519 -l slurm-broker" \
    <remote_broker_host>:<dst_work_dir>/ \
    <src_work_dir>/
```

源端拉取，需要 src_user 有写权限到 src_work_dir（天然成立）。

---

## 十、配置（MVP 版）

### 10.1 `/etc/slurm/broker.conf`

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
# 单对端 (MVP)
# ============================================
RemoteClusterName=wz_cluster
RemoteBrokerHost=broker.wz.example.com
RemoteBrokerPort=8443
RemoteMungeKeyPath=/etc/slurm/munge_wz.key   # 可选: 远端 munge key 路径

# ============================================
# 单一目标 partition (MVP: 1:1 虚拟队列映射)
# ============================================
DefaultRemotePartition=wzhcnormal
# 注: 远端 account 不在 broker.conf 中配置.
#     broker 远端提交时会清空 job_desc->account, 由远端 Slurm 自动选用
#     映射用户 (remote_user) 的 default account (sacctmgr 配置).
#     运维需在远端集群预先为每个映射用户配好 default account 与 partition
#     的 association, 详见 §13.4 已知限制.

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
# 限流 (broker 仅做溢出保护, 主限流由 ctld 端 partition.MaxJobs)
# ============================================
MaxInFlightJobs=500                # 单 broker 在途总上限
MaxStageBytes=53687091200          # 50GB

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
# 软件路径解析 (broker 自包含, 不出现在 slurm.conf)
# ============================================
LookupSoftwareScript=/opt/slurm-broker/scripts/lookup_software.sh
LookupTimeoutSec=3

# ============================================
# 数据保留
# ============================================
RemoteWorkDirRetentionHours=24
RemoteWorkDirFailureRetentionDays=7

# ============================================
# 用户映射
# ============================================
Include /etc/slurm/user_mapping.conf
```

### 10.2 `/etc/slurm/user_mapping.conf`

```ini
UserMapping LocalUser=test1 RemoteCluster=wz_cluster RemoteUser=wz_test1 RemoteUid=20001 RemoteGid=20001
UserMapping LocalUser=test2 RemoteCluster=wz_cluster RemoteUser=wz_test2 RemoteUid=20002 RemoteGid=20002
# ... 试点用户全部列出
```

### 10.3 munged 启动参数

`/etc/sysconfig/munge`（CentOS）或 `/etc/default/munge`（Debian）：

```ini
# 把 munge cred ttl 调大到 24h, 缓解集群间时钟漂移
OPTIONS="--ttl 86400"
```

> 仅在跨域 broker 主机上调整。其它 Slurm 组件维持默认。注意：所有跨域涉及的 broker 主机都要改。

### 10.4 跨集群 munge key 同步

```bash
# 在 A 集群的 broker 主机:
sudo cat /etc/munge/munge.key | base64 > /tmp/munge_a.b64
# 安全传输到 B 集群 broker 主机
# 在 B 集群 broker 主机:
base64 -d /tmp/munge_a.b64 > /etc/slurm/munge_a.key
sudo chmod 400 /etc/slurm/munge_a.key
sudo chown munge:munge /etc/slurm/munge_a.key

# 双向同步: A 也需要 B 的 munge key
```

> **MVP 简化**：A 和 B 直接共享同一份 munge key（合并为同一 munge 域）。这样不需要持有"对方的 key"，munged 自身用一份 key 即可。运维实现最简。

---

## 十一、构建与部署

### 11.1 Makefile 集成（沿用 v0.1 §14.1）

`src/slurmbrokerd/Makefile.am`：

```automake
sbin_PROGRAMS = slurmbrokerd

slurmbrokerd_SOURCES = \
    slurmbrokerd.c slurmbrokerd.h \
    broker_conf.c broker_conf.h \
    broker_job.c broker_job.h \
    listener.c listener.h \
    handler_ctld.c \
    handler_remote.c \
    egress.c egress.h \
    state_machine.c state_machine.h \
    stage.c stage.h \
    rewrite.c rewrite.h \
    software.c software.h \
    user_mapping.c user_mapping.h \
    persist.c persist.h \
    sync_ticker.c sync_ticker.h \
    proto.c proto.h

slurmbrokerd_LDADD = \
    $(top_builddir)/src/api/libslurm.la \
    $(top_builddir)/src/common/libcommon.la
```

### 11.2 systemd unit（沿用 v0.1 §14.2）

不变。

### 11.3 启动序列

```text
1. systemd 拉起 slurmbrokerd
2. log_init()
3. broker_conf_load()        # broker.conf
4. user_mapping_load()       # user_mapping.conf
5. broker_state_restore()    # JSONL 重建 hash 表
6. 启动线程 (6 个)
7. notify systemd READY
8. 主循环
```

---

## 十二、Sprint 计划

> **与整体方案对齐**：本章 4 个 Sprint 对应整体方案 §9.1 的 W1-W2 / W3-W4 / W5-W6 / W7-W8，共 4 个 2 周 sprint（broker 工程师视角；ctld + 客户端工程师在同一个 sprint 内并行做对应任务）。两侧汇合点在 W2 末（协议字段定稿）、W4 末（端到端跑通）、W6 末（5 款应用）、W8 末（小流量上线）。

### Sprint 1（W1-W2）：骨架 + 配置 + 持久化 + lookup 调用器

**输出**：

- `slurmbrokerd` 进程能正常启动/停止
- `broker.conf` `user_mapping.conf` 解析正确
- `broker_job_t` JSONL 序列化往返一致
- broker_state_save / restore 跑通
- `lookup_software.sh` 调用器（fork+exec、pipe 读 stdout、超时 3s）—— 数据源 `software_routes.conf` 即可

**验收**：

```bash
# 启动
sudo systemctl start slurmbrokerd
journalctl -u slurmbrokerd -f
# 看到: "broker started, listening on 8442/8443"

# 重启不丢状态 (手工注入 1 个 broker_job_t 后)
sudo kill -9 $(pgrep slurmbrokerd)
sudo systemctl start slurmbrokerd
# 看到: "restored 1 broker jobs"

# lookup_software.sh 可调
$ /opt/slurm-broker/scripts/lookup_software.sh xian_cluster lammps-2Aug2023-intelmpi2018
/public/software/lammps/2Aug2023-intelmpi2018
$ /opt/slurm-broker/scripts/lookup_software.sh xian_cluster nonexistent-app; echo $?
1
```

**关键代码**：

- `slurmbrokerd.c::main()`
- `broker_conf.c`
- `broker_job.c::broker_job_to_json()` `broker_job_from_json()`
- `persist.c::broker_state_save()` `broker_state_restore()`
- `software.c::lookup_software_path(cluster, app, char **out_path)`

### Sprint 2（W3-W4）：跨集群 RPC 通路 + sbatch 端到端 + 字段化状态注入

**输出**：

- ctld → broker 的 `REQUEST_FORWARD_JOB` 处理
- broker → broker 的 `REQUEST_BROKER_FORWARD_JOB` / `STAGED_IN` / `QUERY_STATUS`
- stage worker（rsync over SSH）
- `rewrite.c`：调 `lookup_software.sh` + 替换 `--partition=` + 按前缀替换 `source` 行
- 远端 broker 调 `slurm_submit_batch_job()` 成功
- sync_ticker 周期通过 **`REQUEST_BROKER_UPDATE_REMOTE_STATE`** 推送 `remote_*` 字段（**不写 comment**）
- 终态通过 **`REQUEST_BROKER_TERMINAL_STATE`** 通知 ctld

**验收 US-1**：

```bash
# 在 A 集群提交一个跨域作业 (字段化, 不再用 comment)
sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 -p xahcnormal lammps.slurm
# 看到: 作业 12345 PENDING(Held)

# 30s 内观察远端字段
watch 'squeue -j 12345 --remote'
# REMOTE_STATE 列应依次出现 (REMOTE_CLUSTER 提前可见):
#   wz_cluster -    PENDING -                        (转发中)
#   wz_cluster 8888 PENDING -                        (远端入队)
#   wz_cluster 8888 RUNNING cpu=32,mem=128G          (远端运行)
#   wz_cluster 8888 COMPLETED cpu=32,mem=128G        (终态)

# 作业终态后看
sacct -j 12345 -o JobID,State,ExitCode,Remote_Cluster,Remote_JobId,Remote_AllocTRES,Remote_ExitCode
# 应显示完整字段
ls /work/home/test1/case1/
# 应有 stdout/stderr/output 等远端产生的文件

# 远端脚本 source 行被改写 (两端路径不同)
ssh wz-master 'cat /work/home/wz_test1/.burst/xian_cluster/12345/run.sh.cd_modified.sh' | grep source
# 应看到远端绝对路径 (源端写的是 /public/software/...)
```

**关键代码**：

- `handler_ctld.c::handle_forward_job()`
- `handler_remote.c::handle_broker_forward_job()`
- `handler_remote.c::handle_broker_staged_in()`
- `egress.c::egress_forward_async()` `egress_query_status_sync()`
- `egress.c::ctld_update_remote_state()` `ctld_inject_terminal_state()`（字段化版本，见 §6.3.2）
- `stage.c::stage_in_run()` `stage_out_run()`
- `rewrite.c::rewrite_job_script()`（调 `lookup_software_path()`）
- `sync_ticker.c::sync_ticker_run()`

### Sprint 3（W5-W6）：scancel 反向传播 + 故障兜底 + 应用扩展

**输出**：

- ctld → broker `REQUEST_BROKER_CANCEL` 处理
- broker → broker cancel 传播
- 状态机错误分支：rsync 失败、远端拒绝、`lookup_software.sh` 退出非 0、超时
- broker 重启幂等性（远端不重复提交）
- 配合应用工程师把 4 款新应用（VASP / GROMACS / Gaussian / Fluent）路径加入 `software_routes.conf`

**验收 US-2**：

```bash
# 提交一个长作业 (字段化)
sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 -p xahcnormal --time=01:00:00 long_job.sh

# 等到 RUNNING 后
scancel 12345
# 30s 内观察
sacct -j 12345
# 应显示 CANCELLED

# 远端验证
ssh wz-master 'sacct -j 8888'
# 应显示 CANCELLED
```

**验收 US-4**：

```bash
# 提交 5 个跨域作业
for i in 1 2 3 4 5; do sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 -p xahcnormal run.sh; done

# 等到 STAGING_IN 阶段, 杀 broker
sudo kill -9 $(pgrep slurmbrokerd)
sleep 5
sudo systemctl start slurmbrokerd

# 验证: 5 个作业都继续走完
watch 'squeue -u test1 --remote'
# 最终全部 COMPLETED, 不应有重复的 remote_job_id
```

**关键代码**：

- `handler_ctld.c::handle_cancel_request()`
- `handler_remote.c::handle_broker_cancel()`
- `state_machine.c` 错误分支
- `handler_remote.c::handle_broker_forward_job()` 幂等处理

### Sprint 4（W7-W8）：加固 + 演练 + 交付

**输出**：

- 长稳：100 跨域作业 / 24h 无内存泄漏
- 故障注入：网络抖动 5min、broker kill、远端 ctld 重启、远端磁盘满、`lookup_software.sh` 退出非 0、DBD 失联
- 部署文档：`README.md` + 排错 playbook
- 培训：运维 1 次 + 用户 1 次

**最终验收**：见下章 §13。

**Sprint 总计**：4 周（broker 工程师视角；与 ctld + 客户端工程师并行，整体 8 周交付）。可由 1~2 名 C/Slurm 熟手完成 broker 部分。

---

## 十三、验收清单

### 13.1 功能验收

- [ ] **US-1**：sbatch 跨域作业全流程（INIT → STAGING_IN → SUBMITTED → RUNNING → STAGING_OUT → COMPLETED）跑通
- [ ] **US-1**：远端结果完整回传到源端 work_dir，stdout/stderr 内容正确
- [ ] **US-2**：scancel 后 30s 内远端 kill 完成，sacct 显示 CANCELLED
- [ ] **US-3**：`squeue --remote` / `squeue -o "%RC %RJ %RS %RT"` 看到 RemoteCluster / RemoteJobId / RemoteState / RemoteAllocTRES 实时更新
- [ ] **US-3**：`scontrol show job` 字段化输出 CrossDomain / AppName / Remote_* 行
- [ ] **US-3**：`sacct` 输出 Remote_Cluster / Remote_JobId / Remote_AllocTRES / Remote_ExitCode 列
- [ ] **US-3**：用户原始 `comment` 字段全程不被 broker 覆盖
- [ ] **US-4**：broker 进程在任意状态下被 kill -9 后，重启可继续，不重复提交、不丢失作业

### 13.2 异常验收

- [ ] 远端 broker 不可达：源端重试 3 次后 → FAILED，原因记录到 broker 日志（**不写 comment**），失败信息通过 `REQUEST_BROKER_TERMINAL_STATE` 中 `remote_state=JOB_FAILED` + `remote_exit_code` 传给 ctld
- [ ] rsync stage-in 失败：重试 3 次后 → FAILED
- [ ] 远端 sbatch 拒绝（quota / partition 不存在）：错误回传，作业 → FAILED
- [ ] `lookup_software.sh` 退出非 0：作业 → FAILED，日志中包含 `lookup_failed app=...`
- [ ] 用户映射缺失：返回错误，影子作业留在 PENDING(Held) 等待运维修复
- [ ] broker 进程 OOM 被系统 kill：systemd 拉起后能恢复

### 13.3 性能验收

- [ ] 100 跨域作业并发，broker CPU < 50%（单核），RSS < 500MB
- [ ] 24h 运行无内存泄漏（valgrind 或 tcmalloc 数据印证）
- [ ] 单作业平均开销（broker 自身处理时间，不含 rsync）< 200ms
- [ ] checkpoint 写入 1000 作业耗时 < 1s

### 13.4 已知限制（用户文档明示）

- [ ] **仅支持 sbatch 提交，不支持 srun / salloc 跨域**
- [ ] **作业脚本要求**：用户必须在脚本中以 `source <软件路径>/scripts/env.sh` 形式声明软件依赖；broker 通过 `lookup_software.sh` 把脚本中以源端路径前缀开头的子串替换为远端路径
- [ ] **不支持 Portal 作业跨域**（Portal 的 `.portal/` 元数据不会被改写）
- [ ] **scontrol update 不支持跨域**（需 scancel 重投）
- [ ] **单对端集群**（A → B；如需 A → C 需重新部署）
- [ ] **stdout/stderr 仅终态可见**（运行中无法 tail）
- [ ] **单 broker 实例无 HA**（宕机期间新作业不能跨域）
- [ ] **跨集群链路必须有专网或 VPN**（裸跑公网未做加固）
- [ ] **远端用户授权由运维预先配置**（**部署侧约定**）：
  - 对 `user_mapping.conf` 中每个 `remote_user`，运维需在远端集群用 `sacctmgr` 提前完成：
    - 关联到一个 default account（用于跨域作业记账与对账）
    - 该 account 关联的 partition 列表必须包含 `DefaultRemotePartition`（如 `wzhcnormal`）
    - 视需要配置 GrpTRES / MaxJobs 等限额
  - broker 远端提交时**清空** `job_desc->account`，依赖 Slurm 自动选用上述 default account；任何 association 缺失都会被远端 sacctmgr 拒绝（错误回传 → 影子作业 → FAILED）

---

## 十四、演进 Hook 位置（向 v0.1 完整版 / v0.2 演进）

MVP 完成后，向后演进的**修改边界**已经预留。所有"对外接口"在 MVP 阶段已按整体方案完整落地（字段化状态注入、`lookup_software.sh` 契约、新 RPC），**后续演进只动 broker 内部实现，不动 ctld、不动客户端、不动协议字段**。

### 14.1 长期演进锚点

| 演进方向 | MVP 位置 | 修改方式 | 是否影响 ctld / 客户端 |
|---|---|---|---|
| 接入 Portal 作业 | `rewrite.c` | 新增 `translator_portal.c`，在 `rewrite_job_script` 入口分发 | 否 |
| `lookup_software.sh` 替换为 RDB / API | `software.c::lookup_software_path()` | 改实现，保持"输入 cluster+app、输出绝对路径"的契约 | 否 |
| 引入 app_routes 表（高级路由） | `software.c` + `rewrite.c` | 新增 `app_route.c/.h`，从 SCNet 软件管理平台拉取路由 | 否 |
| 多对端集群 | `broker_conf.c` 单 RemoteCluster 字段；ctld `RemoteDestinations=` 已支持多目的 | broker 端改为 list，`egress.c` 按 `target_cluster` 路由；ctld 端虚拟 partition 配置即可生效 | 仅 ctld 配置（不改代码） |
| 持久化升级到 pack.c | `persist.c` JSONL | 新增 `persist_pack.c`，配置项切换 | 否 |
| 切换到 v0.2 mTLS / HTTPS | `egress.c::egress_send_*` `listener.c` | 抽象出 `peer_transport_ops_t`，加 mTLS 实现，broker.conf 切换；与 ctld 之间仍走本机 munge 不变 | 否 |
| 实时输出回传 | `stage.c` 仅终态触发 | 引入 `stream.c`，独立线程拉 stdout | 仅 ctld 增加一个新 RPC 接收流（可选） |
| broker HA | 单实例 | 引入 etcd/consul + VIP（结构性改造） | 否 |
| Prometheus exporter | 无 metrics | 新增 `metrics.c`，listener 增加 9090 端口 | 否 |
| sbroker CLI | 无 | 新增 `src/sbroker/`，复用 broker JSONL + 新 RPC | 否 |

### 14.2 MVP 之后**不会再动**的 broker 模块

为了让后续演进风险最小，下列模块在 MVP 之后**字段语义只增不改**：

- `proto.c/.h`：与 ctld 之间的 RPC 字段（`REQUEST_FORWARD_JOB` / `REQUEST_BROKER_UPDATE_REMOTE_STATE` / `REQUEST_BROKER_TERMINAL_STATE` / `REQUEST_BROKER_CANCEL`）只增不改
- `broker_job.c/.h`：JSONL schema 末尾只追加可选字段，不调整存量字段语义
- `state_machine.c`：9 个状态名 + 转换图不变
- `software.c::lookup_software_path()` 函数签名：`(cluster, app, char **out_path) → int rc`

> 这意味着 broker 后续大版本升级（甚至完全重写）都**可以原地替换二进制 + 重启**，由 JSONL 持久化保证在途作业不丢；ctld / 客户端 / 用户脚本完全无感。

---

## 十五、附录

### 15.1 文件清单

```
源码 (Slurm 树):
src/slurmbrokerd/
├── slurmbrokerd.c/.h
├── broker_conf.c/.h
├── broker_job.c/.h
├── listener.c/.h
├── handler_ctld.c
├── handler_remote.c
├── egress.c/.h
├── state_machine.c/.h
├── stage.c/.h
├── rewrite.c/.h
├── software.c/.h                 # ★ MVP 新增 (lookup_software.sh 调用器)
├── user_mapping.c/.h
├── persist.c/.h
├── sync_ticker.c/.h
├── proto.c/.h
└── Makefile.am

部署:
/usr/sbin/slurmbrokerd
/usr/lib/systemd/system/slurmbrokerd.service

/etc/slurm/
├── slurm.conf                    # 追加 Include broker.conf
├── broker.conf                   # 含 LookupSoftwareScript=
├── user_mapping.conf
├── broker_id_ed25519             # SSH key, chmod 600
└── munge.key                     # 跨集群一致, 建议 munged --ttl 86400

/opt/slurm-broker/                # ★ MVP 新增, broker 自包含的应用路径数据
├── conf/software_routes.conf
└── scripts/lookup_software.sh

/var/spool/slurm/broker/
├── broker_state.jsonl
├── broker_state.jsonl.old
└── broker_state.jsonl.tmp        # 写入中

/var/log/slurm/
└── slurmbrokerd.log              # journalctl 也能看
```

### 15.2 排错 Playbook

#### Q1：跨域作业卡在 PENDING(Held)，从未触发 broker

```bash
# 1. 确认 ctld 跨域调度线程是否启用
grep CrossDomainEnabled /etc/slurm/slurm.conf

# 2. 确认作业被识别为跨域
scontrol show job <job_id> | grep -E '(CrossDomain|Reason)'
# 应有 CrossDomain=yes 与 Reason=Forwarded_<cluster>

# 3. 确认用户已被授权
sacctmgr show user <user> format=user,comment
# comment 应包含 allow_remote 关键字

# 4. 看 broker 是否收到
journalctl -u slurmbrokerd | grep <job_id>
# 应有 "trace_id=xxx-<jobid> accepted"
```

#### Q2：broker 收到了，但 STAGING_IN 失败

```bash
# 1. 看 broker 日志中的 rsync 退出码
journalctl -u slurmbrokerd | grep "rsync exit"

# 2. 手工跑同样的 rsync 验证
sudo -u test1 /usr/bin/rsync -rlptDvz \
  --rsync-path="sudo --user=wz_test1 rsync" \
  -e "ssh -i /etc/slurm/broker_id_ed25519 -l slurm-broker" \
  /work/home/test1/case1/ \
  broker.wz.example.com:/work/home/wz_test1/.burst/xian-test/
```

#### Q3：远端 sbatch 失败

```bash
# 1. broker 日志会回传错误信息
journalctl -u slurmbrokerd | grep "remote sbatch failed"

# 2. 在远端 broker 主机切到目标用户手工 sbatch 试
sudo -u wz_test1 sbatch -p wzhcnormal /work/home/wz_test1/.burst/xian/12345/run.sh.cd_modified.sh

# 3. 如错误是 "Invalid account or account/partition combination",
#    检查映射用户的 sacctmgr association (默认 account 是否关联到目标 partition)
sacctmgr show association user=wz_test1 format=user,account,partition,defaultaccount%-15
# 期望: 至少有一行 partition=wzhcnormal (或为空, 即对所有 partition 生效)
#       且 defaultaccount 列指向有效 account

# 4. 如缺失, 运维补配:
sudo sacctmgr -i create account ac_burst description="cross-domain pool"
sudo sacctmgr -i create user wz_test1 account=ac_burst defaultaccount=ac_burst
# (按集群 partition ACL 策略调整 partition= 字段)
```

#### Q4：跨集群 munge 校验失败

```bash
# 1. 两边 munge key 一致性
md5sum /etc/munge/munge.key
# 两台机器输出应一致

# 2. munge ttl 是否生效
ps aux | grep munged
# 应看到 --ttl 86400

# 3. 两边时间偏差 (信息性)
ssh broker.wz.example.com date; date
```

#### Q5：状态文件损坏，启动失败

```bash
# 1. 备份现场
sudo cp /var/spool/slurm/broker/broker_state.jsonl /tmp/

# 2. 切到上一份 checkpoint
sudo mv /var/spool/slurm/broker/broker_state.jsonl{,.broken}
sudo mv /var/spool/slurm/broker/broker_state.jsonl{.old,}

# 3. 拉起
sudo systemctl start slurmbrokerd
```

### 15.3 MVP 评测脚本（建议在 Sprint 4 准备）

```bash
#!/bin/bash
# mvp_smoke_test.sh - 跨域 MVP 冒烟测试

set -e

SBATCH_OPT="--cross-domain --app=lammps-2Aug2023-intelmpi2018 -p xahcnormal"

echo "=== Test 1: 单作业 happy path ==="
JOBID=$(sbatch --parsable $SBATCH_OPT -t 00:05:00 hello.sh)
echo "submitted local jobid=$JOBID"
timeout 600 bash -c "
  while true; do
    STATE=\$(sacct -j $JOBID -o State -n -X | head -1 | xargs)
    [ \"\$STATE\" = \"COMPLETED\" ] && break
    sleep 5
  done
"
RJOBID=$(sacct -j $JOBID -o Remote_JobId -n -X | head -1 | xargs)
[ -n "$RJOBID" ] && [ "$RJOBID" != "-" ] || { echo "FAIL: Remote_JobId empty"; exit 1; }
echo "Test 1 PASS  (remote_jobid=$RJOBID)"

echo "=== Test 2: scancel 反向传播 ==="
JOBID=$(sbatch --parsable $SBATCH_OPT -t 01:00:00 long.sh)
sleep 60   # 等 RUNNING
scancel $JOBID
sleep 30
STATE=$(sacct -j $JOBID -o State -n -X | head -1 | xargs)
[ "$STATE" = "CANCELLED" ] || { echo "FAIL: state=$STATE"; exit 1; }
echo "Test 2 PASS"

echo "=== Test 3: broker 重启幂等 ==="
JOBIDS=$(for i in 1 2 3; do sbatch --parsable $SBATCH_OPT -t 00:10:00 hello.sh; done | xargs)
sleep 30
sudo kill -9 $(pgrep slurmbrokerd)
sudo systemctl start slurmbrokerd
sleep 30
for J in $JOBIDS; do
  COUNT=$(ssh wz-master "sacct --name=hello.sh -X -n | grep $J | wc -l")
  [ "$COUNT" = "1" ] || { echo "FAIL: dup remote for $J"; exit 1; }
done
echo "Test 3 PASS"

echo "=== Test 4: comment 不被污染 ==="
JOBID=$(sbatch --parsable $SBATCH_OPT --comment="user_billing=ABC123" -t 00:05:00 hello.sh)
sleep 60
COMMENT=$(scontrol show job $JOBID | sed -n 's/.*Comment=\(.*\)/\1/p')
[ "$COMMENT" = "user_billing=ABC123" ] || { echo "FAIL: comment polluted: $COMMENT"; exit 1; }
echo "Test 4 PASS"

echo "=== ALL PASS ==="
```

---

## 修订记录

| 版本 | 日期 | 变更 | 作者 |
|---|---|---|---|
| MVP | 2026-04-27 | 基于 v0.1 极简实现版裁剪，作为 6 周内交付指导 | - |
| MVP-1.1 | 2026-04-27 | 与整体方案对齐：明确本文档对应整体方案 M1（4 周）；§14 拆分为 14.1 (M2 增量 hook) + 14.2 (v0.1/v0.2 长期演进) + 14.3 (M2 不动模块清单)；Sprint 周数对齐 M1-1~M1-4 | - |
| MVP-1.2 | 2026-04-27 | 按"对外完整、对内简化"原则重组：撤销 M1/M2 阶段对齐表述；§1 重写定位为"broker 端快速实现版"；新增 §1.2 实现原则表 + §1.3 broker 必做对外契约 5 项；§2.2 拆为 A. 对外接口（不允许简化）/ B. broker 内部实现（允许简化）两段；§6.3 升级为字段化推送（`REQUEST_BROKER_UPDATE_REMOTE_STATE` / `REQUEST_BROKER_TERMINAL_STATE`），broker 永不写 comment；§7.3 改为 `squeue --remote` 字段化展示；§8.1/8.2 新增 `software.c` 与 `lookup_software_path()`，`rewrite.c` 升级为按前缀替换 source 行；broker 限流降级为溢出保护，主限流由 ctld 端 `partition.MaxJobs` 承担；§12 Sprint 改为 4 个 2 周 sprint，对齐整体方案 §9.1；§14 删除 M2 段，恢复纯长期演进锚点 | - |
