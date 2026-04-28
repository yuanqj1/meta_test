# Slurm 跨域调度 MVP 整体方案

> **版本**：MVP（整体方案，整合用户交互 / 调度 / 跨域 / 部署）
> **状态**：实施依据
> **基线**：`跨域调度设计文档.md`（架构设计）+ `Broker详细设计文档.md`（v0.1）+ `Broker详细设计文档MVP.md`（broker 实现指导）
> **目标**：6~8 周内交付 SCNet 跨域调度的 MVP，覆盖端到端 sbatch / scancel / squeue / scontrol 完整闭环

---

## 一、文档定位

### 1.1 与现有文档的关系

```text
                    ┌────────────────────────────────────┐
                    │  跨域调度设计文档.md                │
                    │  (整体架构与方案权衡)              │
                    └─────────────┬──────────────────────┘
                                  │
            ┌─────────────────────┴─────────────────────┐
            ▼                                           ▼
┌─────────────────────────┐              ┌─────────────────────────┐
│  Broker详细设计文档.md  │              │  本文档                 │
│  (v0.1 完整版)          │              │  跨域调度MVP整体方案    │
└─────────────────────────┘              │  (端到端 MVP 落地指导)  │
            │                            └─────────────────────────┘
            ▼                                           │
┌─────────────────────────┐                             │
│  Broker详细设计文档MVP  │◄────────────────────────────┘
│  (broker 实现指导)      │           引用
└─────────────────────────┘
```

| 文档 | 关注 | 阅读对象 |
|---|---|---|
| `跨域调度设计文档.md` | 设计权衡、为什么选这个方案 | 架构师、评审 |
| `Broker详细设计文档.md` (v0.1) | broker 完整能力规格 | 二期实施 |
| `Broker详细设计文档MVP.md` | **broker 端**最小可用实现 | broker 开发 |
| **本文档** | **整体 MVP** 落地：ctld + broker + 用户 + 部署 | **整体 PM、ctld 开发、运维** |

### 1.2 本文档覆盖范围

✅ **本文档详述**：

1. 用户交互层 MVP（sbatch 新参数、squeue --remote、scancel、scontrol/sacct Remote_* 输出）
2. Slurm 源码改造范围（sbatch / squeue / scontrol / sacct / job_record / 协议序列化 / SlurmDBD schema）
3. 调度层 (slurmctld) MVP（跨域调度线程、单层 ACL（assoc/user comment）、虚拟队列 MaxJobs 限流、字段化状态注入）
4. 应用软件部署 MVP（broker 自管的 `lookup_software.sh` 接口、`software_routes.conf`，调度系统侧不感知）
5. 端到端集成部署（slurm.conf / broker.conf / sudoers / SSH key / munge.key 全套）
6. 端到端 Sprint 计划（8 周）与验收清单

🔗 **本文档引用而不重复**：

- Broker 内部实现 → 见 `Broker详细设计文档MVP.md`
- 设计动机与方案权衡 → 见 `跨域调度设计文档.md`
- v0.1 协议消息格式 → 见 `Broker详细设计文档.md` §6

### 1.3 与 Broker MVP 文档的分工

| 视角 | 内容 | 文档 |
|---|---|---|
| **对外契约**（ctld ↔ broker、broker ↔ broker、broker ↔ lookup_software.sh）| 必须**完整**实现，本文档与 broker MVP 口径一致 | 本文档 + Broker MVP §6 / §9 |
| **broker 内部实现**（线程数、持久化格式、改写规则、错误重试细节）| MVP 阶段允许大量简化，**只追求"快速跑通"** | Broker MVP §3~§13 |
| **ctld 端实现**（跨域线程、单层 ACL、字段化状态、配置解析、scancel 拦截）| 必须**完整**实现，不做 MVP 二次裁剪 | 本文档 §5 |
| **用户交互层**（sbatch 原生参数、squeue --remote、scontrol/sacct Remote_* 行/列）| 必须**完整**实现 | 本文档 §4 |

> **MVP 的"快"只体现在 broker 端内部实现**，ctld 端 / 用户交互层 / 对外协议接口都按完整方案落地。这样保证 broker 后续替换/重构时不会触发协议或 client 改动。

---

## 二、MVP 整体范围

### 2.1 MVP 用户故事

| ID | 故事 | 涉及组件 |
|---|---|---|
| **US-1** | 用户 `sbatch --cross-domain --app=<全名> -p <分区> run.sh` 提交，自动转发到远端运行，结果回传 | 用户、ctld、broker |
| **US-2** | 用户 `scancel` 影子作业，远端真实作业 30s 内被 kill | 用户、ctld、broker |
| **US-3** | 用户 `squeue --remote` / `scontrol show job` / `sacct` 能看到远端真实状态（字段化输出） | 用户、ctld、broker |
| **US-4** | 用户 `scontrol update partition / priority / time_limit` 影子作业被拒 | 用户、ctld |
| **US-5** | broker 进程重启不影响在途作业 | broker |
| **US-6** | 管理员通过 `slurm.conf` partition + `sacctmgr` user comment 即可启用跨域 | 运维、ctld、broker |
| **US-7** | 跨域作业终态正确写入 sacct，账单与本地物理队列绑定，远端字段（Remote_Cluster / Remote_JobId / Remote_AllocTRES / Remote_ExitCode）独立可查 | ctld、SlurmDBD |

### 2.2 MVP 整体简化项（修订版）

> 本节是与"完整方案"的差异清单。**MVP 阶段允许有限度修改 Slurm 源码**（sbatch / squeue / job_record / 配置解析层），目标是让跨域语义"显式、可结构化查询、可演进"。

| 维度 | 完整方案 | MVP | 理由 |
|---|---|---|---|
| **跨域触发方式** | sbatch 原生 `--cross-domain` 参数 | **保留**：sbatch 新增 `--cross-domain[=yes\|no]` 参数；`job_desc_msg_t` / `job_record` 新增 `cross_domain` 字段；序列化协议同步扩展 | 跨域是显式语义，必须用独立参数表达，禁止借用 `--comment` |
| **应用声明** | sbatch 原生 `--app` `--app-version` 参数 | **仅保留 `--app=<name>`**：app 名约定为"完整字符串"（如 `lammps-2Aug2023-intelmpi2018`），版本信息内嵌；`job_desc_msg_t` / `job_record` 新增 `app_name` 字段 | MVP 不区分 version 维度，运维把版本编码进 app 名即可；少一个字段 / 少一组协议位 |
| **作业脚本类型** | Portal + CLI 双支持 | **仅 CLI 作业**，Portal 二期 | 砍掉一类 translator，先打通 CLI 主链路 |
| **应用路径处理** | broker 内置 `app_routes` 表 + 远端改写 source 行 | **broker 自包含**：broker 端调用外部 `lookup_software.sh <cluster> <app>` 输出本地绝对路径并改写 `source` 行；**调度系统侧（slurm.conf / ctld）完全不感知软件路径** | 路径解析是部署/数据面问题，不归 ctld 管；slurm.conf 不出现 `LookupSoftwareScript`，ctld 不调脚本 |
| **虚拟队列拓扑** | 1 物理 → N 虚拟 → N 远端 | **1 物理 → 1 虚拟 → 1 远端** | 配置最简，单跳路径打通后再扩 |
| **集群画像** | broker 周期采集，ctld 路由决策依赖 | **不采集**，ctld 路由静态指向单一远端 | 砍掉一个模块 |
| **跨域用户授权** | 用户白名单文件 + 账户/QOS ACL + 用户映射存在性校验 | **复用 Slurm 原生 assoc/user `comment` 字段**：运维通过 `sacctmgr modify user X set comment=...` 让 comment 包含关键字 `allow_remote` 即视为放行；ctld 跨域线程读 `assoc_mgr` 中的 user/assoc comment 判断 | 不引入新 conf 文件 / 不写新 ACL 模块；关闭跨域只需 `sacctmgr` 抹掉关键字，运维心智与 priority/QOS 完全一致 |
| **限流配额** | 虚拟队列 MaxJobs + broker 限流 | **仅虚拟队列 `MaxJobs`**：不做 `MaxSubmitJobsPerUser` / `MaxNodes` / `QOS GrpTRES` 等多维限流；broker 也不再实现 `MaxInFlightJobs` | 单一旋钮足够控制总量；多维限流留给二期；与 Slurm 原生 partition.MaxJobs 字段完全一致 |
| **Backfill 集成** | 主调度 + Backfill 都打跨域标 | **仅主调度路径打标**（`schedule()`），Backfill 路径不参与跨域决策 | 主调度足够触达 PENDING 作业，少改一处 |
| **状态注入接口** | 完整字段独立显示（`Remote_*`） | **保留**：`job_record` 新增独立字段：`remote_cluster_name` / `remote_partition_name` / `remote_job_id` / `remote_state` / `remote_alloc_tres` / `remote_start_time` / `remote_end_time` / `remote_exit_code`；序列化协议、`scontrol show job` 输出层、`sacct` 列同步扩展；新增 RPC `REQUEST_BROKER_UPDATE_REMOTE_STATE` | `comment` 是用户字段，不能被强占；账单与排障必须有可结构化查询的字段 |
| **squeue 视图** | 新增 `squeue --remote` 选项 | **保留**：squeue 新增 `--remote` 选项与 `%R_*` format 占位符（如 `%RC` 远端集群、`%RJ` 远端 JobId、`%RS` 远端状态、`%RT` 远端 TRES）；默认列保持兼容 | 用户排障必需，不能要求记忆 `-o "%k"`；`-o` 自定义仍可用 |

> **本次修订核心精神**：调度系统侧只关心"是否跨域 / 跨到哪 / 远端状态"，**不关心软件路径**（broker 自管）；授权与限流尽可能复用 Slurm 已有数据结构（assoc comment、partition.MaxJobs），不引入新配置文件。
>
> **MVP 简化的边界仅限于 broker 内部**：以上所有"对外协议、字段、参数"都在 MVP 阶段一次性落地，确保后续 broker 内部演进（实现升级、HA 改造、协议加密）不再触发 ctld / sbatch / squeue / scontrol / sacct / DBD 的连锁改动。

### 2.3 MVP 不做的（明确告知）

- Portal 作业跨域（`.portal/job_portal.var` 改写）
- 多对端集群（A→B 单向，A→C 不支持）
- 集群画像驱动的智能路由
- 多 hop（A→B→C）
- 实时输出回传（stdout/stderr 仅终态可见）
- broker HA、跨域作业 backfill 调度
- 跨域用户的精细化 ACL（MVP 只看 assoc/user `comment` 是否含 `allow_remote`，不区分作业类型 / app / 时间窗）
- 跨域作业的 QOS / GrpTRES / MaxSubmitJobsPerUser 限流（MVP 仅 `MaxJobs`）
- 跨集群 mTLS（仍走共享 munge，ttl=86400）
- ctld 端软件路径解析（broker 端 `lookup_software.sh` 自管）

---

## 三、端到端架构（MVP 视图）

### 3.1 整体架构图

```text
┌─ 集群 A (源端 xian_cluster) ────────────────────────────────────────────┐
│                                                                          │
│  ┌──────────┐                                                            │
│  │用户 test1│ sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018   │
│  └────┬─────┘        -p xahcnormal run.sh                                │
│       │  (job_desc_msg_t 携带 cross_domain=1, app_name="lammps-2Aug...",│
│       │   独立字段, 不再借 --comment)                                   │
│       ▼                                                                  │
│  ┌────────────────────────────────────────────────────────────┐         │
│  │ slurmctld(A)                                                │         │
│  │   ├─ 主调度器: 正常入队 xahcnormal                         │         │
│  │   ├─ ★ 跨域调度线程 (新增 / 1s tick):                      │         │
│  │   │     1. 扫描 PENDING && job_ptr->cross_domain==1        │         │
│  │   │     2. 单层 ACL: 读 assoc/user comment 含 allow_remote  │         │
│  │   │        → 单一限流: 虚拟队列 MaxJobs 容量足够            │         │
│  │   │     3. 排队 > CrossDomainWaitTime → 打跨域标, 挂起     │         │
│  │   │     4. 调本地 broker REQUEST_FORWARD_JOB               │         │
│  │   │     5. 扫描 CANCELLED 影子作业 → 反向传播              │         │
│  │   │     6. 接收 broker REQUEST_UPDATE_REMOTE_STATE / TERMINAL_STATE   │
│  │   │        → 写 job_ptr->remote_*, squeue/sacct 直接可见  │         │
│  │   └─ ★ scancel/update/release Hook:                        │         │
│  │         拦截影子作业的 update partition / release          │         │
│  └────────┬───────────────────────────────────────────────────┘         │
│           │ Slurm RPC + Munge (本机)                                    │
│           ▼                                                              │
│  ┌──────────────────────────────────────────────────┐                   │
│  │ slurmbrokerd(A) - 见 Broker MVP 文档              │                   │
│  │   软件路径解析: broker 自调 lookup_software.sh     │                   │
│  │   (ctld 不感知, slurm.conf 不出现该脚本)           │                   │
│  └────────┬─────────────────────────────────────────┘                   │
└───────────┼──────────────────────────────────────────────────────────────┘
            │
   Slurm RPC + 共享 Munge (跨集群, ttl=86400) ─┐
   rsync over SSH (服务账号 key) ──────────────┤
            │                                  │
┌───────────┼──────────────────────────────────┼──────────────────────────┐
│ 集群 B (远端 wz_cluster)                      │                          │
│   ┌───────▼──────────────────────────────────┘                          │
│   │ slurmbrokerd(B) - 见 Broker MVP 文档                                │
│   │   接到 STAGED_IN 后调用本端 lookup_software.sh 改写脚本 source 行   │
│   │   (脚本归 broker 维护, 与 slurmctld 解耦)                          │
│   └───────┬───────────────────────────────────────────────────────────┐ │
│           │ Slurm RPC + Munge (本机)                                  │ │
│           ▼                                                            │ │
│   ┌──────────────────────────────────────────────────┐                │ │
│   │ slurmctld(B) - 不感知跨域, 当作本地 sbatch 处理  │                │ │
│   └──────────────────────┬───────────────────────────┘                │ │
│                          │                                            │ │
│                          ▼                                            │ │
│   ┌──────────────────────────────────────────────────────────┐        │ │
│   │ 计算节点 + 软件 (路径由 lookup_software.sh 决定, 不必两端一致) │   │ │
│   └──────────────────────────────────────────────────────────┘        │ │
└───────────────────────────────────────────────────────────────────────┴─┘
```

### 3.2 端到端时序

```text
T0      用户  : sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 \
                       -p xahcnormal run.sh
T0+0    ctld(A): 作业入 xahcnormal 队列, JobId=12345, PENDING
                 job_ptr->cross_domain=1, app_name="lammps-2Aug2023-intelmpi2018"

T0+5    主调度: xahcnormal 资源不足, 作业仍 PENDING
                (本地正常排队中, 暂不触发跨域)

T0+300  跨域线程 tick: 12345 排队 300s ≥ CrossDomainWaitTime
                     job_ptr->cross_domain==1 → 进入授权检查
                     [ACL] assoc/user.comment 含 "allow_remote" ✓
                     [限流] virtual_wznormal MaxJobs 容量充足 ✓
                     [映射] broker 校验 user_mapping(test1→wz_test1) 存在 ✓
                     → 决策跨域
                     priority=0, state_desc="Forwarded_wz_cluster"
                     (Partition 不变, 仍 xahcnormal)

T0+301  ctld(A) → broker(A): REQUEST_FORWARD_JOB
                  payload: { JobId=12345, app_name,
                             target_cluster=wz_cluster,
                             target_partition=wzhcnormal, ... }
T0+301  broker(A): 入 broker_job_t, trace_id="xian_cluster-12345", state=INIT

T0+302  broker(A): 调用 lookup_software.sh xian_cluster lammps-2Aug2023-intelmpi2018
                  → /public/software/lammps/2Aug2023-intelmpi2018  (源端路径)
T0+303  broker(A) → broker(B): REQUEST_BROKER_FORWARD_JOB
T0+304  broker(B): 创建 staging /work/home/wz_test1/.burst/xian_cluster/12345/
                  返回 ACK
T0+305  broker(A): rsync 源数据到远端
T0+360  rsync 完成
T0+361  broker(A) → broker(B): REQUEST_BROKER_STAGED_IN
T0+362  broker(B): 调用 lookup_software.sh wz_cluster lammps-2Aug2023-intelmpi2018
                  → /opt/scnet/apps/lammps/2Aug2023  (远端路径, 与源端可不同)
                  改写脚本中 source 行
                  → slurm_submit_batch_job → remote_job_id=8888
T0+363  broker(A) → ctld(A): REQUEST_BROKER_UPDATE_REMOTE_STATE
                  payload: { JobId=12345, remote_cluster="wz_cluster",
                             remote_job_id=8888, remote_state=PENDING }
T0+363  ctld(A): 写 job_ptr->remote_cluster_name / remote_job_id / remote_state

T0+400  broker(B) 内部 squeue: 8888 → RUNNING, alloc_tres="cpu=32,mem=128G"
T0+410  broker(A) sync_ticker (10s): 拉到 RUNNING
T0+410  broker(A) → ctld(A): REQUEST_BROKER_UPDATE_REMOTE_STATE
                  payload: { remote_state=RUNNING, remote_alloc_tres,
                             remote_start_time }
T0+410  ctld(A): 更新 job_ptr->remote_state / remote_alloc_tres / remote_start_time

       (用户此时 squeue --remote 12345 → 默认就显示 cluster/jobid/state/tres)

T0+3600 远端作业 COMPLETED
T0+3610 broker(A) sync_ticker: 拉到终态
T0+3611 broker(A) state=STAGING_OUT, 反向 rsync
T0+3700 rsync 完成
T0+3700 broker(A) → ctld(A): REQUEST_BROKER_TERMINAL_STATE
                  payload: { JobId=12345, exit_code=0, alloc_tres="cpu=32,mem=128G",
                            start=..., end=..., remote_state=COMPLETED }
T0+3701 ctld(A) 跨域线程接收终态:
                  写 job_ptr->remote_exit_code / remote_end_time
                  影子作业 12345 PENDING(Held) → COMPLETED
                  调 jobcomp_g_record_job_end → sacct 含 Remote_* 列
                  (账单挂在 xahcnormal 物理队列下)
T0+3702 broker(A) → broker(B): REQUEST_BROKER_CLEANUP { delay=86400 }
T0+90000 broker(B): 清理远端 staging 目录
```

### 3.3 关键设计决策（再强调）

| 决策 | 设计 | 影响 |
|---|---|---|
| **作业入口不变** | 用户仍 sbatch 到 `xahcnormal`，partition 全程不变 | 计费天然挂在 `xahcnormal` 下，对账单系统零影响 |
| **影子作业** | 源端作业 PENDING(Held)，priority=0 | 本地调度器不会调度，Slurm 状态机始终一致 |
| **跨域信号显式参数化** | sbatch 新增 `--cross-domain` `--app`；`job_record` 新增 `cross_domain` / `app_name` 字段 | 用户面与协议面均显式表达跨域语义；不再借 `--comment`；版本信息内嵌进 app 名 |
| **远端状态写入独立字段** | `job_record` 新增 `remote_cluster_name` / `remote_job_id` / `remote_state` / `remote_alloc_tres` / `remote_start_time` / `remote_end_time` / `remote_exit_code` | `comment` 字段保持给用户使用；squeue / scontrol / sacct 直接读字段 |
| **`squeue --remote` 视图** | squeue 新增 `--remote` 选项及 `%RC/%RJ/%RS/%RT` 占位符 | 用户排障一行命令，不需要记 `-o "%k"` |
| **单层用户授权** | assoc/user `comment` 字段含 `allow_remote` 关键字即放行 | 复用 Slurm 原生 sacctmgr 能力，零新 conf；启停跨域只需一条 sacctmgr 命令 |
| **单一限流** | 虚拟 partition `MaxJobs` 一个旋钮 | Slurm 原生字段，无需新代码；够用；多维限流留给二期 |
| **终态由 ctld 跳变** | broker 通知 ctld，ctld 把 PENDING(Held) 直接跳到 COMPLETED 并填充 `remote_*` | sacct 账单完整且含远端结构化数据 |
| **软件路径与 ctld 解耦** | broker 自调 `lookup_software.sh <cluster> <app>` 改写脚本；ctld 不感知 | slurm.conf 无 `LookupSoftwareScript`；调度系统升级与软件路由演进互不影响 |

---

## 四、用户交互层 MVP

### 4.1 sbatch 提交跨域作业（原生参数）

#### 4.1.1 sbatch 新增参数

| 参数 | 是否必填 | 说明 |
|---|---|---|
| `--cross-domain[=yes\|no]` | 必填（=yes 表示跨域）| 显式声明此作业允许/请求跨域；缺省视为 `no` |
| `--app=<name>` | 跨域作业必填 | 应用全名（含版本）；约定字符串如 `lammps-2Aug2023-intelmpi2018`、`vasp-5.4.4-ioptcell`；运维通过同名键登记到 `software_routes.conf`（broker 端） |

> 这两个参数是 **sbatch 源码新增**，对应在 `job_desc_msg_t`、Slurm RPC 序列化、`job_record` 中均增加字段（详见 §5.2.2）。MVP 不区分 `app_version`；如需多版本并存，运维直接以全名登记多条软件路由即可。

#### 4.1.2 用户使用方式

```bash
# 基本用法
$ sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 \
         -p xahcnormal run.sh
Submitted batch job 12345

# 也可用 #SBATCH 指令写在脚本里
$ cat run.sh
#!/bin/bash
#SBATCH -J lammps_test
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 02:00:00
#SBATCH --cross-domain
#SBATCH --app=lammps-2Aug2023-intelmpi2018
module purge
source /public/software/lammps/2Aug2023-intelmpi2018/scripts/env.sh
mpirun -n 32 lmp_mpi -in in.case

$ sbatch -p xahcnormal run.sh
Submitted batch job 12345
```

> 用户脚本中的 `source` 行写源端绝对路径即可；远端 broker 在改写脚本时会自动替换为远端等价路径（详见 §6 与 `Broker详细设计文档MVP.md`）。

#### 4.1.3 ctld 端识别跨域作业

ctld 跨域调度线程**直接读结构化字段**，无需解析字符串：

```c
/* src/slurmctld/cross_domain.c */

bool job_is_cross_domain_candidate(struct job_record *job_ptr) {
    return (job_ptr->cross_domain == 1);
}

/* app_name 直接取自 job_ptr 字段, 无字符串解析 */
const char *job_get_app(struct job_record *job_ptr) {
    return job_ptr->app_name;
}
```

### 4.2 squeue 查看跨域作业状态（`--remote` 选项）

**MVP 修改 squeue 源码**，新增 `--remote` 选项与一组 `%R*` 格式占位符。

#### 4.2.1 默认列（不带 `--remote`）

```bash
$ squeue -u test1
JOBID PARTITION     NAME     USER  ST  TIME NODES NODELIST(REASON)
12345 xahcnormal run.sh    test1 PD  0:00     1 (Forwarded_wz_cluster)
```

> 默认列保持兼容；跨域作业的 Reason 显示 `Forwarded_<cluster>`，与本地作业一眼可分辨。

#### 4.2.2 跨域视图（`--remote`）

```bash
$ squeue -u test1 --remote
JOBID PARTITION  NAME    USER  ST  TIME REMOTE_CLUSTER REMOTE_JOBID REMOTE_STATE REMOTE_TRES
12345 xahcnormal run.sh  test1 PD  0:00 wz_cluster     8888         RUNNING      cpu=32,mem=128G
```

`--remote` 等价于 squeue 内置 format：

```text
%.10i %.10P %.10j %.10u %.4T %.10M %.15RC %.12RJ %.12RS %.30RT
```

| 占位符 | 含义 | job_record 字段 |
|---|---|---|
| `%RC` | 远端集群名 | `remote_cluster_name` |
| `%RP` | 远端 partition | `remote_partition_name` |
| `%RJ` | 远端 JobId | `remote_job_id` |
| `%RS` | 远端状态 | `remote_state` |
| `%RT` | 远端分配 TRES | `remote_alloc_tres` |
| `%Rs` | 远端开始时间 | `remote_start_time` |
| `%Re` | 远端结束时间 | `remote_end_time` |
| `%Rx` | 远端 exit_code | `remote_exit_code` |

#### 4.2.3 自定义 format 仍可用

```bash
$ squeue -u test1 -o "%.10i %.4T %.15RC %.10RJ %.10RS"
     JOBID ST  REMOTE_CLUSTER REMOTE_JOB REMOTE_STA
     12345 PD  wz_cluster     8888       RUNNING
```

### 4.3 scontrol show job

`scontrol show job` 输出中**新增独立行展示远端字段**（修改 Slurm 源码 `slurm_print_job_info()`）：

```bash
$ scontrol show job 12345
JobId=12345 JobName=lammps_test
   UserId=test1(10001) GroupId=test1(10001) MCS_label=N/A
   Priority=0 Nice=0 Account=ac_lab1 QOS=normal
   JobState=PENDING Reason=Forwarded_wz_cluster Dependency=(null)
   ...
   Partition=xahcnormal AllocNode:Sid=login01:12345
   ...
   CrossDomain=yes AppName=lammps-2Aug2023-intelmpi2018
   RemoteCluster=wz_cluster RemotePartition=wzhcnormal
   RemoteJobId=8888 RemoteState=RUNNING
   RemoteAllocTRES=cpu=32,mem=128G,node=1
   RemoteStartTime=2026-04-27T17:30:00 RemoteEndTime=Unknown
   RemoteExitCode=N/A
   Comment=<用户原有 comment, 保持原样>
   ...
```

用户感知：
- **CrossDomain=yes** 一眼看出跨域作业
- **Remote*** 行显示远端真实状态
- **Comment** 字段保持给用户自用，不被 broker 占用

### 4.4 scancel 取消跨域作业

用户无任何额外操作：

```bash
$ scancel 12345
$ sleep 30
$ sacct -j 12345
JobID         JobName  Partition    Account  AllocCPUS      State ExitCode
------------ ---------- ---------- ---------- ---------- ---------- --------
12345         run.sh   xahcnormal   ac_lab1     32       CANCELLED      0:0
```

后台流程：
1. ctld(A) 将影子作业标 CANCELLED（本地正常路径）
2. ctld(A) 跨域线程 tick 发现 CANCELLED 影子作业 + `job_ptr->cross_domain_forwarded==1` 且 `cancel_propagated==0`
3. ctld(A) → broker(A): `REQUEST_BROKER_CANCEL`
4. broker(A) → broker(B): cancel 传播
5. broker(B) 调 `slurm_kill_job(remote_job_id)`
6. 30s 内远端作业终止

### 4.5 用户 FAQ（MVP 文档）

| 用户疑问 | 答复 |
|---|---|
| 我怎么提交跨域作业？ | `sbatch --cross-domain --app=<app全名> -p <分区> run.sh`；也可在脚本里加 `#SBATCH --cross-domain` |
| `--app` 该填什么？ | 应用全名（含版本），如 `lammps-2Aug2023-intelmpi2018`；可向运维索取已登记的应用列表 |
| 我的作业为什么 squeue 看到 Reason=Forwarded_wz_cluster？ | 你的作业已被跨域转发到 wz_cluster 集群运行，本地状态保留是为了保留账单 |
| 我看 ST=PD 但作业其实在远端跑了？怎么看真实状态？ | 用 `squeue --remote` 一行就能看到远端集群、JobId、状态、TRES |
| 我能 scancel 我的跨域作业吗？ | 可以，scancel 命令完全照常用 |
| 我能 scontrol update partition 吗？ | 不可以，跨域作业一旦转发不允许改 partition，需要 scancel 后重投 |
| 我没声明 `--cross-domain` 会怎样？ | 作业当作普通本地作业排队，永远不跨域；这是默认行为 |
| 我的跨域权限怎么开通？ | 运维使用 `sacctmgr modify user <你的账号> set comment="...allow_remote..."` 即可；关闭就抹掉这个关键字 |
| `--app` 写错了会怎样？ | broker 调 `lookup_software.sh` 解析失败 → 作业回退本地 PENDING；联系运维补登记 |
| 我的跨域作业失败了怎么办？ | sacct 中 ExitCode 与 RemoteExitCode 同步可见；远端 staging 目录保留 7 天 |
| 远端用什么用户身份跑我的作业？ | 由 `user_mapping.conf` 映射表决定（如 test1 → wz_test1），可联系运维查询 |
| 我的脚本里 source 路径只能在本端用，怎么办？ | 远端 broker 会调用 `lookup_software.sh` 自动改写为对端等价路径，无需用户改脚本 |

---

## 五、调度层 (slurmctld) MVP

> ⚠️ 本章是整体 MVP 文档**最重要**的部分（broker 端已有独立 MVP 文档，ctld 端是首次详述）。

### 5.1 配置项

#### 5.1.1 `/etc/slurm/slurm.conf` 改动

```ini
# ============ 跨域全局开关 ============
CrossDomainEnabled=YES

# 跨域排队等待阈值 (秒)
# 作业排队超过此时间, 才触发跨域流转
CrossDomainWaitTime=300

# Broker 服务的本机通信地址
BrokerHost=127.0.0.1
BrokerPort=8442

# 跨域用户授权关键字 (在 assoc/user comment 中识别)
CrossDomainCommentTag=allow_remote

# ============ 物理队列 + 跨域映射 ============
# SendTo: 该物理队列溢出时往哪个虚拟队列流转
PartitionName=xahcnormal Nodes=node[001-100] DefMemPerCPU=4000 \
    State=UP Default=YES MaxTime=INFINITE \
    SendTo=virtual_wznormal

# ============ 隐式虚拟队列 (跨域出口) ============
# Hidden=YES: 用户 sinfo 看不到
# Remote=yes: 标记为跨域虚拟队列 (Slurm 源码解析层新增字段)
# RemoteDestinations: 映射到的远端集群:partition (MVP 仅 1 个)
# MaxJobs: 唯一限流旋钮 (Slurm 原生字段)
PartitionName=virtual_wznormal Nodes=node[001-100] Hidden=YES \
    State=UP DefMemPerCPU=4000 \
    Remote=yes \
    RemoteDestinations=wzhcnormal@wz_cluster \
    MaxJobs=200

# ============ Broker 配置 (include 进来) ============
Include /etc/slurm/broker.conf
```

> **MVP 配置解析说明**：
> - 新增关键字 `CrossDomainEnabled` / `CrossDomainWaitTime` / `BrokerHost` / `BrokerPort` / `CrossDomainCommentTag` / `Remote=` / `RemoteDestinations=` / `SendTo=` 需要修改 Slurm 配置解析层（`src/common/read_config.c` 与 `src/common/slurm_protocol_pack.c`）。
> - **不再有** `LookupSoftwareScript`（路径解析归 broker）、`CrossDomainAllowedUsersFile`（授权改用 assoc comment）、`AllowApp` / `AllowAccounts` / `AllowQos`（不做应用面/账户面 ACL）、`MaxSubmitJobsPerUser` / `QOS GrpTRES`（限流仅 `MaxJobs`）。
> - `MaxJobs` 是 Slurm 原生 partition 字段，**完全不需新代码**。

#### 5.1.2 跨域用户授权（assoc/user comment）

跨域权限通过 Slurm 原生 sacctmgr 在 user/assoc 的 `comment` 字段中嵌入关键字 `allow_remote` 表达。**没有新增配置文件**：

```bash
# 开通 test1 跨域权限
sudo sacctmgr -i modify user test1 set comment="allow_remote"

# 在已有 comment 上追加 (与既有标签共存)
sudo sacctmgr -i modify user test2 set comment="vip,allow_remote,priority"

# 关闭 test1 跨域 (抹掉关键字)
sudo sacctmgr -i modify user test1 set comment=""

# 查询当前授权用户
sacctmgr show user format=user,defaultaccount,comment%30 | grep allow_remote
```

ctld 跨域线程通过 `assoc_mgr_lock(...)` + `assoc_mgr_get_user_*()` 取 user/assoc 记录，匹配 `CrossDomainCommentTag`（默认 `allow_remote`）即视为允许。**reconfigure 实时生效**，无需重启。

#### 5.1.3 单层 ACL 与单一限流的检查顺序

ctld 跨域线程在 `cd_tick_scan_pending()` 中按以下顺序判断作业是否可跨域：

1. **跨域意图**：`job_ptr->cross_domain == 1`
2. **路由可达**：物理 partition 配 `SendTo=` → 找到虚拟 partition 且 `is_remote==1`
3. **用户授权（单层 ACL）**：作业 user 在 assoc_mgr 中的 `comment` 含 `allow_remote` 关键字
4. **虚拟队列容量（单一限流）**：虚拟 partition 已在该 partition 名下的"已挂单作业数 < MaxJobs"
5. **排队等待阈值**：`now - submit_time ≥ CrossDomainWaitTime`
6. **用户映射存在性**：转发请求到达 broker 后由 broker 校验 `user_mapping.conf`；缺映射则 broker 拒绝，ctld 回滚 `priority`

> 任一项不通过 → 跳过本轮，作业留在本地 PENDING；下一轮 tick 再评估。**不做** account 级 / QOS 级 / 应用级别的额外 ACL。

#### 5.1.4 `/etc/slurm/user_mapping.conf`（broker 与 ctld 共用）

```ini
UserMapping LocalUser=test1 RemoteCluster=wz_cluster RemoteUser=wz_test1 RemoteUid=20001 RemoteGid=20001
UserMapping LocalUser=test2 RemoteCluster=wz_cluster RemoteUser=wz_test2 RemoteUid=20002 RemoteGid=20002
# ... 列出所有跨域用户的映射 ...
```

> ctld 端 MVP 不直接读这个文件（broker 读取并校验），ctld 仅在打跨域标时通过 broker 的 `REQUEST_QUERY_USER_MAPPING` RPC 验证用户存在性。**MVP 跳过这一步**：直接交由 broker 端在 `REQUEST_FORWARD_JOB` 时校验，校验失败则 broker 拒绝，作业留在 PENDING(Held) 等运维介入。

### 5.2 跨域调度线程（核心新增模块）

#### 5.2.1 模块位置

```text
src/slurmctld/
├── cross_domain.c            ★ 新增: 跨域调度线程主体 + 单层 ACL
├── cross_domain.h            ★ 新增
├── cross_domain_rpc.c        ★ 新增: broker RPC handler
└── ... (Slurm 原有文件)

src/common/
├── slurm_protocol_pack.c     修改: job_desc / job_info 序列化 + 新 RPC
├── read_config.c             修改: 新 partition 关键字解析
└── slurmdb_defs.c            修改: sacct 输出新增 Remote_* 列

src/sbatch/
├── opt.c                     修改: 新增 --cross-domain / --app
└── sbatch.c                  修改: 透传到 job_desc_msg_t

src/squeue/
├── opt.c                     修改: 新增 --remote 选项
└── print.c                   修改: 新增 %RC/%RJ/%RS/%RT 占位符

src/scontrol/
└── info_job.c                修改: scontrol show job 输出 Remote_* 行
```

新增 + 修改约 2000 行 C 代码（含 sbatch / squeue / scontrol / sacct / ctld / 协议包装）。

#### 5.2.2 数据结构（job_record 新字段）

**job_record 与 job_desc_msg_t 同步新增字段**（修改 `src/slurmctld/slurmctld.h`、`src/common/slurm.h.in`、`src/common/slurm_protocol_pack.c`）：

```c
/* src/slurmctld/slurmctld.h, struct job_record */

/* === 跨域请求侧字段 (sbatch 提交时填入) === */
uint16_t  cross_domain;            /* 0=否, 1=允许跨域 */
char     *app_name;                /* "lammps-2Aug2023-intelmpi2018"（含版本）*/

/* === 跨域执行侧字段 (broker 回写) === */
char     *remote_cluster_name;     /* "wz_cluster" */
char     *remote_partition_name;   /* "wzhcnormal" */
uint32_t  remote_job_id;           /* 8888 */
uint32_t  remote_state;            /* 复用 Slurm JOB_STATE_* */
char     *remote_alloc_tres;       /* "cpu=32,mem=128G,node=1" */
time_t    remote_start_time;
time_t    remote_end_time;
uint32_t  remote_exit_code;
char     *remote_trace_id;         /* "xian_cluster-12345" 用于排错 */
```

**partition_record 新增字段**：

```c
/* src/slurmctld/slurmctld.h, struct part_record */

uint16_t  is_remote;               /* Remote=yes */
char     *remote_destinations;     /* 原始字符串, 如 "wzhcnormal@wz_cluster" */
char     *remote_cluster;          /* 解析后 */
char     *remote_partition;        /* 解析后 */
char     *send_to;                 /* SendTo= (物理队列才有) */
/* MaxJobs 复用 part_record 已有字段 part_ptr->max_jobs */
```

**跨域线程在内存中用的辅助结构**：

```c
/* src/slurmctld/cross_domain.h */

#ifndef _CROSS_DOMAIN_H
#define _CROSS_DOMAIN_H

#include "src/common/list.h"
#include "src/slurmctld/slurmctld.h"

/* 跨域线程 API */
extern int  cross_domain_init(void);
extern void cross_domain_fini(void);

/* 单层用户授权: 读 assoc/user comment 是否含 CrossDomainCommentTag */
extern bool cd_user_allows_remote(struct job_record *job_ptr);

/* 单一限流: 检查虚拟 partition 当前已挂单跨域作业数 < part->max_jobs */
extern bool cd_vpart_has_capacity(struct part_record *vpart);

/* RPC handler API (供 slurmctld_req.c 调用) */
extern int  handle_broker_update_remote_state(slurm_msg_t *msg);
extern int  handle_broker_terminal_state(slurm_msg_t *msg);

/* update/release 拦截 hook (供 update_job.c 调用) */
extern int  cross_domain_check_update_block(struct job_record *job_ptr,
                                              job_desc_msg_t *job_specs);

#endif /* _CROSS_DOMAIN_H */
```

#### 5.2.3 跨域调度线程主循环

```c
/* src/slurmctld/cross_domain.c */

#include "cross_domain.h"
#include "src/slurmctld/job_scheduler.h"

static pthread_t        cd_thread_id;
static volatile bool    cd_running = false;

static void *_cd_thread(void *arg) {
    info("cross_domain: thread started");
    while (cd_running) {
        cd_tick_scan_pending();      /* 扫描可跨域作业 */
        cd_tick_scan_cancelled();    /* 扫描需反向取消 */
        cd_tick_check_orphans();     /* 检查 broker 是否在线 */
        sleep(1);                    /* 1s 周期, MVP 简单实现 */
    }
    info("cross_domain: thread exited");
    return NULL;
}

int cross_domain_init(void) {
    if (!slurm_conf.cross_domain_enabled) {
        info("cross_domain: disabled");
        return SLURM_SUCCESS;
    }

    cd_load_allowed_users();
    cd_parse_partition_metadata();    /* 解析所有 partition 的 Comment */

    cd_running = true;
    pthread_create(&cd_thread_id, NULL, _cd_thread, NULL);
    return SLURM_SUCCESS;
}

void cross_domain_fini(void) {
    cd_running = false;
    pthread_join(cd_thread_id, NULL);
    cd_free_metadata();
    cd_free_allowed_users();
}
```

#### 5.2.4 扫描可跨域作业（核心逻辑）

```c
static void cd_tick_scan_pending(void) {
    time_t now = time(NULL);
    list_itr_t *itr;
    struct job_record *job_ptr;

    lock_slurmctld(job_read_lock);

    itr = list_iterator_create(job_list);
    while ((job_ptr = list_next(itr))) {
        /* 1. 必须是 PENDING 且未挂起, 也未打过跨域标志 */
        if (!IS_JOB_PENDING(job_ptr) || (job_ptr->priority == 0)) continue;
        if (job_ptr->cross_domain_forwarded) continue;

        /* 2. 字段化判断: 跨域意图 */
        if (job_ptr->cross_domain != 1) continue;

        /* 3. 路由可达: 物理 partition 配 SendTo → 找到 Remote 虚拟 partition */
        struct part_record *phys = find_part_record(job_ptr->partition);
        if (!phys || !phys->send_to) continue;
        struct part_record *vpart = find_part_record(phys->send_to);
        if (!vpart || !vpart->is_remote) continue;

        /* 4. 单层 ACL: 用户授权 (assoc/user comment 含 allow_remote) */
        if (!cd_user_allows_remote(job_ptr)) continue;

        /* 5. 单一限流: 虚拟 partition MaxJobs 容量足够 */
        if (!cd_vpart_has_capacity(vpart)) continue;

        /* 6. 排队时间超过阈值 */
        if (now - job_ptr->details->submit_time < slurm_conf.cross_domain_wait_time)
            continue;

        /* ★ 满足所有条件: 触发跨域 */
        cd_trigger_forward(job_ptr, vpart);
    }
    list_iterator_destroy(itr);

    unlock_slurmctld(job_read_lock);
}
```

```c
/* 用户授权: 直接读 assoc_mgr 中的 user/assoc comment, 匹配 CrossDomainCommentTag */
bool cd_user_allows_remote(struct job_record *job_ptr) {
    if (!job_ptr->assoc_ptr) return false;

    assoc_mgr_lock_t locks = { .user = READ_LOCK, .assoc = READ_LOCK };
    assoc_mgr_lock(&locks);

    const char *tag = slurm_conf.cross_domain_comment_tag ?: "allow_remote";
    bool ok = false;

    /* 优先看 assoc.comment, 找不到再看 user.comment */
    slurmdb_assoc_rec_t *assoc = job_ptr->assoc_ptr;
    if (assoc->comment && strstr(assoc->comment, tag))
        ok = true;
    else {
        slurmdb_user_rec_t *user = assoc_mgr_find_user_rec(assoc->uid);
        if (user && user->comment && strstr(user->comment, tag))
            ok = true;
    }

    assoc_mgr_unlock(&locks);
    return ok;
}

/* 单一限流: 在 job_list 里数一遍该虚拟 partition 上 cross_domain_forwarded
   且未到终态的作业数, 与 part->max_jobs 比 */
bool cd_vpart_has_capacity(struct part_record *vpart) {
    if (!vpart->max_jobs || vpart->max_jobs == INFINITE) return true;

    uint32_t in_flight = 0;
    list_itr_t *itr = list_iterator_create(job_list);
    struct job_record *j;
    while ((j = list_next(itr))) {
        if (!j->cross_domain_forwarded) continue;
        if (IS_JOB_FINISHED(j)) continue;
        if (xstrcmp(j->details ? j->details->req_partitions : NULL,
                    vpart->name) == 0 ||
            (j->remote_partition_name &&
             xstrcmp(j->remote_partition_name, vpart->remote_partition) == 0))
            in_flight++;
    }
    list_iterator_destroy(itr);
    return in_flight < vpart->max_jobs;
}
```

> 限流复用 Slurm 原生的 `part_record.max_jobs` 字段 + 自计数。MVP 不引入 QOS / GrpTRES / MaxSubmitJobsPerUser 等多维限流。

#### 5.2.5 触发跨域转发

```c
static void cd_trigger_forward(struct job_record *job_ptr,
                                struct part_record *vpart) {
    info("cross_domain: forwarding job %u to %s@%s",
         job_ptr->job_id, vpart->remote_partition, vpart->remote_cluster);

    /* 1. 上 job_write_lock 改作业状态 */
    lock_slurmctld(job_write_lock);

    /* 2. priority=0, 脱离调度 */
    job_ptr->priority = 0;
    job_ptr->state_reason = WAIT_HELD_USER;
    xfree(job_ptr->state_desc);
    job_ptr->state_desc = xstrdup_printf("Forwarded_%s", vpart->remote_cluster);

    /* 3. ★ 用独立标志位防止重复, 不再污染 comment */
    job_ptr->cross_domain_forwarded = 1;

    /* 4. 提前填入"决策侧"远端字段 (broker 回写后会被覆盖) */
    xfree(job_ptr->remote_cluster_name);
    job_ptr->remote_cluster_name = xstrdup(vpart->remote_cluster);
    xfree(job_ptr->remote_partition_name);
    job_ptr->remote_partition_name = xstrdup(vpart->remote_partition);

    /* 5. last_sched_eval 更新, 防止 backfill 反复尝试 */
    job_ptr->last_sched_eval = time(NULL);

    unlock_slurmctld(job_write_lock);

    /* 6. 通过本地 RPC 通知 broker (异步) */
    cd_send_forward_to_broker(job_ptr, vpart);

    /* 7. 持久化变更 (lock 释放后才能 schedule_job_save) */
    schedule_job_save();
}
```

> 用 `job_ptr->cross_domain_forwarded` 这个独立 `uint8_t` 字段防止重复转发，是 §2.2 "状态注入用字段不用 comment" 原则在转发幂等控制上的延伸。

#### 5.2.6 调用 broker

```c
static void cd_send_forward_to_broker(struct job_record *job_ptr,
                                       struct part_record *vpart) {
    forward_job_msg_t req;
    memset(&req, 0, sizeof(req));

    req.src_job_id    = job_ptr->job_id;
    req.src_user_name = xstrdup(job_ptr->user_name);
    req.src_uid       = job_ptr->user_id;
    req.account       = xstrdup(job_ptr->account ?: "");
    req.target_cluster   = xstrdup(vpart->remote_cluster);
    req.target_partition = xstrdup(vpart->remote_partition);

    /* ★ 直接读字段, 不再 parse comment; MVP 不传 app_version */
    req.app_name      = xstrdup(job_ptr->app_name ?: "");
    req.submit_way    = xstrdup("cli");      /* MVP: 仅 cli */
    req.src_work_dir  = xstrdup(job_ptr->details->work_dir ?: "");
    req.script_path   = xstrdup_printf("%s/run.sh.cd_orig", req.src_work_dir);

    /* 把作业脚本拷贝到 src_work_dir/run.sh.cd_orig (broker stage 时拷贝原始) */
    cd_dump_job_script(job_ptr, req.script_path);

    /* 把 job_desc_msg 完整 clone 给 broker */
    req.job_desc = clone_job_desc_from_record(job_ptr);

    slurm_msg_t msg;
    slurm_msg_t_init(&msg);
    msg.msg_type = REQUEST_FORWARD_JOB;
    msg.data     = &req;

    slurm_addr_t addr;
    slurm_set_addr(&addr, slurm_conf.broker_port, slurm_conf.broker_host);

    forward_job_resp_msg_t *resp = NULL;
    int rc = slurm_send_recv_msg(&addr, &msg, (slurm_msg_t **)&resp, 30);
    if (rc != SLURM_SUCCESS) {
        error("cross_domain: forward_job to broker failed: %s",
              slurm_strerror(rc));
        /* MVP: broker 不可达时, 回滚 priority, 等下一轮 tick 重试 */
        cd_revert_forward(job_ptr->job_id);
        return;
    }
    if (resp->error_code != 0) {
        error("cross_domain: broker rejected job %u: %s (rc=%d)",
              req.src_job_id, resp->error_message ?: "", resp->error_code);
        /* 常见拒绝场景: 用户映射缺失 → 作业回退为本地 PENDING, 等待运维补映射 */
        cd_revert_forward(job_ptr->job_id);
    } else {
        info("cross_domain: forwarded job %u, trace_id=%s",
             req.src_job_id, resp->trace_id);
        /* trace_id 写到独立字段, 不污染 comment */
        lock_slurmctld(job_write_lock);
        xfree(job_ptr->remote_trace_id);
        job_ptr->remote_trace_id = xstrdup(resp->trace_id);
        unlock_slurmctld(job_write_lock);
    }

    free_forward_job_msg(&req);
    free_forward_job_resp_msg(resp);
}
```

> **MVP 兜底**：broker 不可达或拒绝时，作业保持原状 PENDING（不挂起）。下轮 tick 会重试。这避免了"跨域线程挂起作业但 broker 永远收不到"的死锁。

#### 5.2.7 反向取消传播

```c
static void cd_tick_scan_cancelled(void) {
    list_itr_t *itr;
    struct job_record *job_ptr;

    lock_slurmctld(job_read_lock);

    itr = list_iterator_create(job_list);
    while ((job_ptr = list_next(itr))) {
        /* ★ 字段化判定: 跨域已转发 + 用户 cancel + 未传播 */
        if (!IS_JOB_CANCELLED(job_ptr)) continue;
        if (!job_ptr->cross_domain_forwarded) continue;
        if (job_ptr->cross_domain_cancel_propagated) continue;

        /* 已经在终态 7 天以上, 跳过 (避免无限重试) */
        if (job_ptr->end_time &&
            (time(NULL) - job_ptr->end_time) > 7 * 86400) continue;

        cd_send_cancel_to_broker(job_ptr);
    }
    list_iterator_destroy(itr);

    unlock_slurmctld(job_read_lock);
}

static void cd_send_cancel_to_broker(struct job_record *job_ptr) {
    if (!job_ptr->remote_trace_id) {
        warning("cross_domain: cancelled job %u has no trace_id, cannot propagate",
                job_ptr->job_id);
        return;
    }

    broker_cancel_msg_t req = {
        .trace_id = xstrdup(job_ptr->remote_trace_id),
        .reason   = xstrdup("user_cancel"),
    };

    slurm_msg_t msg;
    slurm_msg_t_init(&msg);
    msg.msg_type = REQUEST_BROKER_CANCEL;
    msg.data     = &req;

    slurm_addr_t addr;
    slurm_set_addr(&addr, slurm_conf.broker_port, slurm_conf.broker_host);

    int rc = slurm_send_only_node_msg(&msg, &addr);    /* 不等响应 */
    if (rc == SLURM_SUCCESS) {
        /* 用独立字段标记已传播 */
        lock_slurmctld(job_write_lock);
        job_ptr->cross_domain_cancel_propagated = 1;
        unlock_slurmctld(job_write_lock);
    } else {
        warning("cross_domain: cancel to broker failed: %s", slurm_strerror(rc));
        /* 下轮 tick 重试 */
    }

    xfree(req.trace_id); xfree(req.reason);
}
```

#### 5.2.8 broker 中间状态回写（remote_state 流转期间）

broker `sync_ticker` 轮询到远端状态变化时（INIT → SUBMITTED → RUNNING 等），通过 `REQUEST_BROKER_UPDATE_REMOTE_STATE` 把字段同步给 ctld：

```c
int handle_broker_update_remote_state(slurm_msg_t *msg) {
    broker_update_remote_state_msg_t *req = msg->data;

    lock_slurmctld(job_write_lock);
    struct job_record *job_ptr = find_job_record(req->src_job_id);
    if (!job_ptr) {
        unlock_slurmctld(job_write_lock);
        return ESLURM_INVALID_JOB_ID;
    }
    if (!job_ptr->cross_domain_forwarded) {
        unlock_slurmctld(job_write_lock);
        return ESLURM_INVALID_JOB_STATE;
    }

    /* ★ 直接写字段, 不再拼 comment */
    if (req->remote_cluster_name) {
        xfree(job_ptr->remote_cluster_name);
        job_ptr->remote_cluster_name = xstrdup(req->remote_cluster_name);
    }
    if (req->remote_partition_name) {
        xfree(job_ptr->remote_partition_name);
        job_ptr->remote_partition_name = xstrdup(req->remote_partition_name);
    }
    if (req->remote_job_id) job_ptr->remote_job_id = req->remote_job_id;
    job_ptr->remote_state = req->remote_state;
    if (req->remote_alloc_tres) {
        xfree(job_ptr->remote_alloc_tres);
        job_ptr->remote_alloc_tres = xstrdup(req->remote_alloc_tres);
    }
    if (req->remote_start_time) job_ptr->remote_start_time = req->remote_start_time;

    unlock_slurmctld(job_write_lock);
    return SLURM_SUCCESS;
}
```

#### 5.2.9 broker 终态回写处理

```c
int handle_broker_terminal_state(slurm_msg_t *msg) {
    broker_terminal_state_msg_t *req = msg->data;

    lock_slurmctld(job_write_lock);
    struct job_record *job_ptr = find_job_record(req->src_job_id);
    if (!job_ptr) {
        unlock_slurmctld(job_write_lock);
        return ESLURM_INVALID_JOB_ID;
    }

    /* 必须是被跨域托管的影子作业 */
    if (job_ptr->priority != 0 || !job_ptr->cross_domain_forwarded) {
        unlock_slurmctld(job_write_lock);
        warning("broker_terminal_state: job %u not in forwarded state",
                req->src_job_id);
        return ESLURM_INVALID_JOB_STATE;
    }

    /* ★ 字段化写入 (本地账单时间 = 远端时间, 保证 sacct 一致) */
    job_ptr->start_time          = req->remote_start_time;
    job_ptr->end_time            = req->remote_end_time;
    job_ptr->exit_code           = req->remote_exit_code;
    job_ptr->remote_start_time   = req->remote_start_time;
    job_ptr->remote_end_time     = req->remote_end_time;
    job_ptr->remote_exit_code    = req->remote_exit_code;
    job_ptr->remote_state        = req->remote_state;
    xfree(job_ptr->remote_alloc_tres);
    job_ptr->remote_alloc_tres   = xstrdup(req->remote_alloc_tres ?: "");

    /* alloc_tres 写到 tres_alloc_str 让 Slurm 原生 sacct 列也能展示 */
    xfree(job_ptr->tres_alloc_str);
    job_ptr->tres_alloc_str = xstrdup(req->remote_alloc_tres ?: "");

    /* 决定终态 */
    uint32_t job_state;
    switch (req->remote_state) {
    case JOB_COMPLETE:   job_state = JOB_COMPLETE; break;
    case JOB_FAILED:     job_state = JOB_FAILED;   break;
    case JOB_CANCELLED:  job_state = JOB_CANCELLED; break;
    case JOB_TIMEOUT:    job_state = JOB_TIMEOUT;  break;
    case JOB_NODE_FAIL:  job_state = JOB_NODE_FAIL; break;
    default:             job_state = JOB_FAILED;   break;
    }

    /* 调 Slurm 内部完成路径写 sacct */
    job_ptr->job_state = job_state | JOB_COMPLETING;
    jobcomp_g_record_job_end(job_ptr);
    job_completion_logger(job_ptr, false);
    job_ptr->job_state = job_state;

    unlock_slurmctld(job_write_lock);

    info("cross_domain: job %u terminal state %s injected by broker",
         req->src_job_id, job_state_string(job_state));

    return SLURM_SUCCESS;
}
```

> **sacct 扩展**：在 `src/sacct/print.c` 与 `src/common/slurmdb_defs.c` 中新增 `Remote_Cluster` / `Remote_JobId` / `Remote_State` / `Remote_AllocTRES` / `Remote_ExitCode` 列；持久化到 SlurmDBD 的 `job_table` 通过 schema 升级新增对应列。账单查询同时可看本地与远端口径，便于运维核账。

### 5.3 update / release / scancel 拦截

#### 5.3.1 update_job 拦截

```c
/* 修改 src/slurmctld/job_mgr.c::update_job() 入口 */

extern int update_job(slurm_msg_t *msg, uid_t uid, bool send_msg) {
    job_desc_msg_t *job_specs = (job_desc_msg_t *) msg->data;
    struct job_record *job_ptr;

    lock_slurmctld(job_write_lock);
    job_ptr = find_job_record(job_specs->job_id);

    /* ★ MVP 新增: 跨域作业的特殊保护 */
    if (job_ptr && job_ptr->cross_domain_forwarded) {
        int rc = cross_domain_check_update_block(job_ptr, job_specs);
        if (rc != SLURM_SUCCESS) {
            unlock_slurmctld(job_write_lock);
            return rc;
        }
    }

    /* ... Slurm 原有逻辑 ... */
}
```

```c
/* src/slurmctld/cross_domain.c */

int cross_domain_check_update_block(struct job_record *job_ptr,
                                     job_desc_msg_t *job_specs) {
    /* 拦 update partition */
    if (job_specs->partition && strcmp(job_ptr->partition, job_specs->partition) != 0) {
        info("cross_domain: rejecting partition change of forwarded job %u",
             job_ptr->job_id);
        return ESLURM_NOT_SUPPORTED;
    }

    /* 拦 release (priority 改非 0) */
    if (job_specs->priority != NO_VAL && job_specs->priority != 0) {
        info("cross_domain: rejecting priority change of forwarded job %u",
             job_ptr->job_id);
        return ESLURM_NOT_SUPPORTED;
    }

    /* time_limit 改不允许 (远端已生效) */
    if (job_specs->time_limit != NO_VAL) {
        info("cross_domain: rejecting time_limit change of forwarded job %u",
             job_ptr->job_id);
        return ESLURM_NOT_SUPPORTED;
    }

    return SLURM_SUCCESS;
}
```

#### 5.3.2 scancel 不需要拦截

`scancel` 走正常路径标记 CANCELLED。跨域线程在下轮 tick 中扫描到，自动反向传播（见 §5.2.7）。

#### 5.3.3 配置 Slurm "Forwarded" 状态文案

跨域线程在 `cd_trigger_forward()` 中设置 `state_reason = WAIT_HELD_USER`（已存在），并把 `state_desc = "Forwarded_<remote_cluster>"`。`squeue` 默认显示 `state_desc`，已能让用户一眼看出是跨域。无需新增 `REASON_*` 枚举。

### 5.4 Slurm 源码改动清单（MVP 全景）

| 模块 | 文件 | 改动 | 估算 LoC |
|---|---|---|---|
| sbatch | `src/sbatch/opt.c`, `src/sbatch/sbatch.c` | 新增 `--cross-domain` / `--app` 参数 | ~60 |
| squeue | `src/squeue/opt.c`, `src/squeue/print.c` | 新增 `--remote` 选项与 `%RC/%RJ/%RS/%RT/%Rs/%Re/%Rx/%RP` 占位符 | ~200 |
| scontrol | `src/scontrol/info_job.c` | `scontrol show job` 输出新增 CrossDomain / Remote_* 行 | ~60 |
| sacct | `src/sacct/print.c`, `src/common/slurmdb_defs.c` | 新增 Remote_* 输出列 | ~120 |
| 协议序列化 | `src/common/slurm_protocol_pack.c`, `src/common/slurm_protocol_defs.h` | `job_desc_msg_t` / `job_info_msg_t` / `slurm_job_info_t` 新字段 pack/unpack；新 RPC 类型 | ~220 |
| 配置解析 | `src/common/read_config.c`, `src/slurmctld/partition_mgr.c` | partition `Remote=` `RemoteDestinations=` `SendTo=` 关键字；slurm.conf 跨域全局配置项（`CrossDomainEnabled` / `CrossDomainWaitTime` / `BrokerHost` / `BrokerPort` / `CrossDomainCommentTag`） | ~120 |
| ctld 跨域线程 + 单层 ACL | `src/slurmctld/cross_domain.c/.h` ★ 新增 | 主循环 + 单层用户授权（assoc comment）+ 单一限流（MaxJobs 自计数）+ 转发 + 取消传播 | ~700 |
| ctld RPC handler | `src/slurmctld/cross_domain_rpc.c` ★ 新增 | UPDATE_REMOTE_STATE / TERMINAL_STATE handler | ~200 |
| ctld update 拦截 | `src/slurmctld/job_mgr.c` | update_job() 入口插入 cross_domain_check_update_block | ~30 |
| ctld 启动 | `src/slurmctld/controller.c` | 启动跨域线程 | ~20 |
| job_record 扩展 | `src/slurmctld/slurmctld.h`, `src/slurmctld/job_mgr.c` 持久化 | 新字段 + state save / restore | ~140 |
| SlurmDBD schema | `src/plugins/accounting_storage/mysql/as_mysql_job.c` | job_table 新增 Remote_* 列 | ~70 |
| **合计** | | | **~1940 LoC** |

> 这是 MVP 阶段对 Slurm 源码改动的真实账。关键是把"用户面 + 协议面 + 数据面"一次性补齐，避免后期返工。Slurm 主调度算法本身不改。**调度侧不出现软件路径解析代码**（broker 侧自管）。

---

## 六、跨域层 (broker) MVP

> 本章不重复细节。完整内容见 `Broker详细设计文档MVP.md`。

### 6.1 关键集成点回顾

| 接口 | broker → ctld | ctld → broker |
|---|---|---|
| `REQUEST_FORWARD_JOB` | - | ctld 触发跨域时调用 |
| `REQUEST_BROKER_UPDATE_REMOTE_STATE` | broker sync_ticker 周期更新 `remote_*` 字段 | - |
| `REQUEST_BROKER_TERMINAL_STATE` | broker 终态回写（含 exit_code / alloc_tres / start / end） | - |
| `REQUEST_BROKER_CANCEL` | - | ctld 跨域线程发现 CANCELLED 时调用 |
| `REQUEST_QUERY_USER_MAPPING` | - | (MVP 不实现，broker 启动时一次性加载 user_mapping.conf) |

> 注：`lookup_software.sh` 是 broker 自身进程内 fork+exec 的本地脚本调用，不走 RPC，**完全不与 slurmctld 发生交互**。

> **重要协议变更**：v0.1 设计文档曾规划用 `REQUEST_UPDATE_JOB` 把远端状态塞到 `comment` 里。MVP 改为 `REQUEST_BROKER_UPDATE_REMOTE_STATE` 直接写独立字段，`comment` 字段保留给用户。同步在 `Broker详细设计文档MVP.md` 更新协议定义。

### 6.2 broker MVP 端口

| 端口 | 用途 |
|---|---|
| 8442 | 接 ctld 的本机 RPC（Munge） |
| 8443 | 接对端 broker 的跨集群 RPC（共享 Munge，ttl=86400） |

### 6.3 broker MVP 与 ctld MVP 协作矩阵

| 场景 | ctld 责任 | broker 责任 |
|---|---|---|
| 作业识别 | 跨域线程读 `job_ptr->cross_domain` 字段判定 | - |
| 用户授权 | 读 assoc/user comment 是否含 `allow_remote` | 仅做 user_mapping 存在性校验（兜底） |
| 限流配额 | 虚拟 partition `MaxJobs` 自计数 | 不做应用层限流 |
| 应用合法性 | 不感知（无 AllowApp） | 调 `lookup_software.sh` 失败即拒绝 |
| 作业 stage-in/out | - | rsync 全过程 + 调 lookup_software.sh 解析路径 |
| 作业脚本改写 | - | 远端 broker 调 lookup_software.sh 重写 source 行 |
| 状态轮询 | - | broker sync_ticker (10s) |
| 状态可见性 | 接收 `REQUEST_BROKER_UPDATE_REMOTE_STATE` 写 `remote_*` 字段 | broker 周期推送 |
| scancel 传播 | 跨域线程扫描 CANCELLED 影子作业, 用 trace_id 调 broker | 转发 cancel 到对端 |
| 终态写入 sacct | 接收 broker 通知, 写 `remote_*` 字段后调 jobcomp_g_record_job_end | 通知 ctld |
| 远端目录清理 | - | broker 24h/7d 后定时清理 |

### 6.4 软件查询脚本（broker 端自管，ctld 不感知）

> 路径由 broker 在 stage-in / 改写脚本时自行调用查询脚本得到。**slurm.conf 不出现该脚本路径**，broker.conf 中通过 `LookupSoftwareScript=...` 配置（详见 `Broker详细设计文档MVP.md`）。

#### 6.4.1 接口契约

```bash
# 调用方式 (MVP 仅 cluster + app 两个参数, app 名含版本)
$ /opt/slurm-broker/scripts/lookup_software.sh <cluster_name> <app_name>

# 示例 (源端)
$ lookup_software.sh xian_cluster lammps-2Aug2023-intelmpi2018
/public/software/lammps/2Aug2023-intelmpi2018

# 示例 (远端可用不同路径)
$ lookup_software.sh wz_cluster lammps-2Aug2023-intelmpi2018
/opt/scnet/apps/lammps/2Aug2023-intelmpi2018

# 失败时
$ lookup_software.sh wz_cluster unknown_app
ERROR: app=unknown_app not registered for cluster=wz_cluster
$ echo $?    # 2
```

| 退出码 | 含义 | broker 行为 |
|---|---|---|
| 0 | 成功，stdout 是绝对路径 | 用此路径改写 source 行 |
| 2 | 应用未注册 | 拒绝跨域，作业回退本地排队（ctld 收到拒绝后 cd_revert_forward） |
| 其它非 0 | 脚本错误 | 同上，但日志告警，运维介入 |

#### 6.4.2 MVP 实现示例（运维提供）

```bash
#!/bin/bash
# /opt/slurm-broker/scripts/lookup_software.sh
# 用 conf 表查路径; 二期可换 HTTP API / DB

CLUSTER="$1"
APP="$2"

CONF=/opt/slurm-broker/conf/software_routes.conf

# conf 行格式: <cluster>|<app>|<absolute_path>
LINE=$(awk -F'|' -v c="$CLUSTER" -v a="$APP" \
       '$1==c && $2==a {print $3; exit}' "$CONF")

if [[ -z "$LINE" ]]; then
    echo "ERROR: app=$APP not registered for cluster=$CLUSTER" >&2
    exit 2
fi

echo "$LINE"
exit 0
```

```ini
# /opt/slurm-broker/conf/software_routes.conf
xian_cluster|lammps-2Aug2023-intelmpi2018|/public/software/lammps/2Aug2023-intelmpi2018
xian_cluster|vasp-5.4.4-ioptcell|/public/software/vasp/5.4.4-ioptcell
wz_cluster|lammps-2Aug2023-intelmpi2018|/opt/scnet/apps/lammps/2Aug2023-intelmpi2018
wz_cluster|vasp-5.4.4-ioptcell|/opt/scnet/apps/vasp/5.4.4-ioptcell
```

> **演进**：脚本接口稳定后，可无侵入升级到查 HTTP API 或 SCNet 软件管理平台，broker 与 Slurm 都不需要改。

---

## 七、应用软件部署 MVP

### 7.1 路径解耦：broker 自管的软件查询

MVP 不再要求两端路径完全一致。**软件路径解析完全由 broker 完成**（slurm.conf / slurmctld 不参与）：

- **源端 broker**：stage-in 时调本地 `lookup_software.sh src_cluster <app>` → 用作 rsync 源 + 校验脚本中 `source` 行
- **远端 broker**：stage-in 完成后调本地 `lookup_software.sh dst_cluster <app>` → 改写脚本 `source` 行 → submit

```text
源端 (xian_cluster): /public/software/lammps/2Aug2023-intelmpi2018/
远端 (wz_cluster):   /opt/scnet/apps/lammps/2Aug2023-intelmpi2018/

用户脚本: source /public/software/lammps/2Aug2023-intelmpi2018/scripts/env.sh
         ↓ 远端 broker 改写后 ↓
         source /opt/scnet/apps/lammps/2Aug2023-intelmpi2018/scripts/env.sh
```

> 改写规则：远端 broker 用 `lookup_software.sh src_cluster <app>` 得到源端前缀 P_src，再用 `lookup_software.sh dst_cluster <app>` 得到远端前缀 P_dst；将脚本中所有以 `P_src` 开头的子串替换为 `P_dst`。脚本中其他 source 行（用户私有数据集等）不动。

### 7.2 MVP 首批支持的应用

| 软件 | 登记 app 名（含版本） | 备注 |
|---|---|---|
| LAMMPS | `lammps-2Aug2023-intelmpi2018` | |
| VASP | `vasp-5.4.4-ioptcell_intelmpi2017` | |
| GROMACS | `gromacs-2024.1-gcc930_intelmpi2017` | |
| Gaussian | `gaussian-16a.03` | 受限发行 |
| Fluent | `fluent-2024r1` | 商业 license 跨域需协商 |

> 5 款覆盖 SCNet 高频应用 80% 量。每款均需在两端 broker 的 `software_routes.conf` 登记路径，MVP 验收要求每款跑一个标准 case。

### 7.3 部署 Checklist

```text
集群 A (源端) broker:
[ ] 应用已部署到任意路径, env.sh 可 source
[ ] 至少一个测试 case 在 A 集群本地跑通
[ ] 在 A broker software_routes.conf 添加 xian_cluster|<app全名>|<path>
[ ] 也添加 wz_cluster|<app全名>|<远端 path> (改写时要用)

集群 B (远端) broker:
[ ] 应用部署到任意路径 (无需与 A 一致), env.sh 可 source
[ ] 同一个测试 case 在 B 集群本地跑通
[ ] 在 B broker software_routes.conf 添加 wz_cluster|<app全名>|<path>
[ ] 也添加 xian_cluster|<app全名>|<源端 path>

跨域验证:
[ ] 用户在 A 集群 sbatch --cross-domain --app=<app全名> 提交
[ ] B 集群作业 source 远端路径成功执行 (检查 stdout 中 env 解析正确)
[ ] 输出文件成功回传到 A 集群
[ ] sacct 中 Remote_AllocTRES / Remote_ExitCode 字段填写正确
```

### 7.4 应用脚本 MVP 范例

#### LAMMPS 跨域作业脚本

```bash
#!/bin/bash
#SBATCH -J lammps_cd_test
#SBATCH -N 1
#SBATCH --ntasks-per-node=32
#SBATCH -t 02:00:00
#SBATCH --cross-domain
#SBATCH --app=lammps-2Aug2023-intelmpi2018

module purge
# 此处直接写源端路径; 远端 broker 会自动改写
source /public/software/lammps/2Aug2023-intelmpi2018/scripts/env.sh
mpirun -n 32 lmp_mpi -in in.case
```

提交：

```bash
$ sbatch -p xahcnormal lammps.slurm
```

---

## 八、整体配置与部署

### 8.1 一次性部署 Checklist（按顺序执行）

#### 集群 A（源端 xian_cluster）

```bash
# 1. 创建 broker 服务账号
sudo useradd -r -s /sbin/nologin slurm-broker

# 2. 生成 broker SSH key (用于跨集群 rsync)
sudo -u slurm ssh-keygen -t ed25519 -f /etc/slurm/broker_id_ed25519 -N ""
sudo chmod 600 /etc/slurm/broker_id_ed25519

# 3. 配置 sudoers 允许 slurm 切换到任意普通用户
echo "slurm ALL=(test*) NOPASSWD: /usr/bin/rsync" | sudo tee /etc/sudoers.d/slurm-rsync

# 4. munged 启动参数: ttl=86400
sudo sed -i 's|^OPTIONS=.*|OPTIONS="--ttl 86400"|' /etc/sysconfig/munge
sudo systemctl restart munge

# 5. 编译并安装 (含 sbatch/squeue/scontrol/sacct 跨域改造 + ctld 跨域线程 + slurmbrokerd)
cd slurm-source-cd-mvp
./configure --prefix=/usr --sysconfdir=/etc/slurm
make -j8
sudo make install

# 6. 部署调度系统配置
sudo cp slurm.conf /etc/slurm/                # 含 CrossDomainEnabled / Remote= / SendTo= / MaxJobs
sudo cp user_mapping.conf /etc/slurm/         # broker 与 ctld 共用

# 7. 部署 broker 自有的软件查询脚本与路由表 (调度系统不感知)
sudo mkdir -p /opt/slurm-broker/scripts /opt/slurm-broker/conf
sudo cp lookup_software.sh /opt/slurm-broker/scripts/
sudo chmod 755 /opt/slurm-broker/scripts/lookup_software.sh
sudo cp software_routes.conf /opt/slurm-broker/conf/
sudo cp broker.conf /opt/slurm-broker/conf/   # broker.conf 中通过 LookupSoftwareScript=... 配置脚本路径

# 8. 创建状态目录
sudo mkdir -p /var/spool/slurm/broker
sudo chown slurm:slurm /var/spool/slurm/broker

# 9. 配置跨域用户授权 (用 sacctmgr, 不再有 cd_allowed_users.conf)
sudo sacctmgr -i modify user test1 set comment="allow_remote"
sudo sacctmgr -i modify user test2 set comment="allow_remote,priority"

# 10. 启动服务 (sbatch / squeue 等 client 已是改造后版本)
sudo systemctl restart slurmctld
sudo systemctl enable --now slurmbrokerd

# 11. 把 munge.key 同步到 B 集群 (运维操作, 见下)
```

#### 集群 B（远端 wz_cluster）

```bash
# 1. 创建 broker 服务账号 + 接受 A 的 SSH 公钥
sudo useradd -r -m -s /bin/bash slurm-broker
sudo mkdir -p /home/slurm-broker/.ssh
sudo cp <A-broker-pubkey>.pub /home/slurm-broker/.ssh/authorized_keys
sudo chown -R slurm-broker:slurm-broker /home/slurm-broker/.ssh
sudo chmod 700 /home/slurm-broker/.ssh
sudo chmod 600 /home/slurm-broker/.ssh/authorized_keys

# 2. 配置 sudoers 允许 slurm-broker 切换到任意 wz_* 用户
cat <<EOF | sudo tee /etc/sudoers.d/slurm-broker
slurm-broker ALL=(wz_*) NOPASSWD: /usr/bin/rsync, /bin/mkdir, /bin/chmod, /bin/chown
EOF

# 3-10. 同 A 的步骤 4 ~ 10
#  - B 集群 sbatch / squeue 不需要跨域改造 (B 不是入口集群),
#    但 broker 端仍需要 lookup_software.sh + software_routes.conf 来改写脚本.
#  - 也可以选择两端编译同一份 Slurm, 简化运维.
#  - 步骤 9 (sacctmgr 授权) 在 B 端不需要执行 (B 不是入口).

# 10. 共享 munge.key
# 在 A 集群:
ssh slurm-broker@wz-broker.example.com "sudo cp /etc/munge/munge.key /etc/munge/munge.key.local.bak"
sudo cat /etc/munge/munge.key | ssh slurm-broker@wz-broker.example.com "sudo tee /etc/munge/munge.key > /dev/null"
ssh slurm-broker@wz-broker.example.com "sudo chmod 400 /etc/munge/munge.key && sudo systemctl restart munge"

# 警告: 这一步会让 A 和 B 的 munge 域合并, 需评估对其它 Slurm 通信的影响.
# 推荐方案: 使用专用的"跨域 munge daemon"实例, 仅服务于 broker, 不与原生 munge 冲突.
# (具体实施细节见 broker MVP 文档 §10.4)
```

#### 双向连通性验证

```bash
# A → B 的 SSH (rsync 通道)
ssh -i /etc/slurm/broker_id_ed25519 slurm-broker@wz-broker.example.com "echo OK"

# A → B 的 Slurm RPC (broker peer 端口 8443)
nc -zv wz-broker.example.com 8443

# 跨集群 munge 校验
echo "test" | munge | ssh slurm-broker@wz-broker.example.com "unmunge"
# 应输出: test
```

### 8.2 应用部署 Checklist（每个应用）

```bash
# 在 A 和 B 两个集群上分别执行 (路径可不同, 由运维自定)
# MVP 不区分 app_version, app 全名作为唯一键
APP=lammps-2Aug2023-intelmpi2018

# A 集群 (路径示例 /public/software/lammps/2Aug2023-intelmpi2018)
A_PATH=/public/software/lammps/2Aug2023-intelmpi2018
sudo mkdir -p ${A_PATH}
# ... 拷贝/编译软件到此路径 ...
sudo cp env.sh ${A_PATH}/scripts/env.sh
source ${A_PATH}/scripts/env.sh && which lmp_mpi

# B 集群 (路径可以不同, 例如 /opt/scnet/apps/...)
B_PATH=/opt/scnet/apps/lammps/2Aug2023-intelmpi2018
sudo mkdir -p ${B_PATH}
sudo cp env.sh ${B_PATH}/scripts/env.sh
source ${B_PATH}/scripts/env.sh && which lmp_mpi

# 在 A 和 B 两端的 broker software_routes.conf 都登记两端路径
cat <<EOF | sudo tee -a /opt/slurm-broker/conf/software_routes.conf
xian_cluster|${APP}|${A_PATH}
wz_cluster|${APP}|${B_PATH}
EOF

# 验证脚本可正确解析 (broker 自检)
/opt/slurm-broker/scripts/lookup_software.sh xian_cluster ${APP}
# → /public/software/lammps/2Aug2023-intelmpi2018
/opt/slurm-broker/scripts/lookup_software.sh wz_cluster ${APP}
# → /opt/scnet/apps/lammps/2Aug2023-intelmpi2018
```

### 8.3 用户与权限 Checklist

```bash
# 每个跨域用户 (以 test1 为例)
# A 集群:
sudo useradd -m -d /work/home/test1 test1

# B 集群: 创建映射用户 (UID/GID 不必与 A 一致, 但需在 user_mapping.conf 中正确登记)
sudo useradd -m -d /work/home/wz_test1 -u 20001 wz_test1

# 把映射加到 user_mapping.conf (A 和 B 都要有)
echo "UserMapping LocalUser=test1 RemoteCluster=wz_cluster RemoteUser=wz_test1 RemoteUid=20001 RemoteGid=20001" \
  | sudo tee -a /etc/slurm/user_mapping.conf

# 用 sacctmgr 给 test1 开通跨域权限 (仅 A 集群入口需要, 复用 Slurm 原生 comment 字段)
sudo sacctmgr -i modify user test1 set comment="allow_remote"

# 验证
sacctmgr show user test1 format=user,comment
# 看到 comment 列含 "allow_remote" 即可

# 重启 broker 让 user_mapping 映射生效 (sacctmgr 由 SlurmDBD 实时刷新, 无需重启)
sudo systemctl restart slurmbrokerd
```

---

## 九、整体 Sprint 计划（8 周）

### 9.1 总览

> **节奏特征**：ctld 端 / 用户交互层 / 协议接口都按本文档完整设计**一次性**落地；broker 端按 `Broker详细设计文档MVP.md` 的"快速实现版"落地，4 周完成。两侧并行推进，6 周末汇合做端到端联调。

```text
W1-2 (Sprint 1):  ctld 跨域线程骨架 + Slurm 客户端字段化骨架 + broker 进程骨架
                  ├─ ctld 端: cross_domain.c/.h、partition 解析新关键字、
                  │           job_record / job_desc_msg_t 新字段、协议序列化扩展、
                  │           assoc/user comment ACL hook (空实现先 stub)
                  ├─ Client : sbatch / squeue / scontrol / sacct 新参数与新占位符骨架
                  └─ broker : 进程框架 + JSONL 持久化 + listener (broker MVP §Sprint 1)

W3-4 (Sprint 2):  ctld 跨域线程完整逻辑 + broker 跨集群通路 + sbatch 端到端
                  ├─ ctld 端: cd_tick_scan_pending / cd_trigger_forward 完整逻辑
                  │           + REQUEST_BROKER_UPDATE_REMOTE_STATE / TERMINAL_STATE handler
                  │           + 单层 ACL (assoc comment) + 单一限流 (partition.MaxJobs)
                  ├─ broker : REQUEST_FORWARD_JOB / BROKER_FORWARD_JOB / STAGED_IN / QUERY_STATUS
                  │           + rsync stage worker + lookup_software.sh 调用器
                  │           + sync_ticker → REQUEST_BROKER_UPDATE_REMOTE_STATE 推送字段
                  │           + REQUEST_BROKER_TERMINAL_STATE 通知 ctld (broker MVP §Sprint 2)
                  └─ App    : LAMMPS 在两端不同路径部署 + software_routes.conf 双向登记
                  ★ 验收 US-1: 字段化路径端到端跑通

W5-6 (Sprint 3):  scancel + scontrol/sacct 字段化输出 + 异常处理 + 应用扩展
                  ├─ ctld 端: cd_tick_scan_cancelled + cross_domain_check_update_block
                  │           + scontrol show job 输出 CrossDomain/Remote_* 行
                  │           + sacct 新增 Remote_* 列 + DBD schema 升级
                  │           + squeue --remote 占位符接 broker 字段
                  ├─ broker : REQUEST_BROKER_CANCEL 处理与传播
                  │           + 错误状态机分支 (rsync 失败 / 远端拒绝 / lookup 失败 / 超时)
                  │           + broker 重启幂等性 (broker MVP §Sprint 3)
                  └─ App    : VASP / GROMACS / Gaussian / Fluent 上线
                  ★ 验收 US-2 / US-3 / US-4 / US-5

W7-8 (Sprint 4):  长稳压测 + 部署文档 + 培训交付
                  ├─ 100 并发 24h 长稳 (broker MVP §Sprint 4)
                  ├─ 故障注入: 网络抖动 / broker kill / 远端 ctld 重启 / 远端磁盘满 / DBD 失联
                  ├─ 部署文档 + Ansible playbook + 排错 playbook
                  ├─ 运维培训 1 次 + 用户培训 1 次
                  └─ 灰度切流: 5 用户 → 1 周 → 50 用户 → 1 月 → 全量
```

> 人力假设：1~2 名熟悉 Slurm C 代码的工程师（含 ctld 端与客户端改造）+ 1 名 broker 实现工程师（按 broker MVP 文档实施，可以是同一人）+ 1 名运维。

### 9.2 各 Sprint 详细任务

#### Sprint 1（W1-W2）：骨架并行

**ctld 端**：
- [ ] 新建 `src/slurmctld/cross_domain.c/.h`：跨域线程主循环骨架 + 1s tick
- [ ] partition 配置解析新增 `Remote=` `RemoteDestinations=` `SendTo=` 关键字（`src/common/read_config.c` + `src/slurmctld/partition_mgr.c`）
- [ ] slurm.conf 全局新增 `CrossDomainEnabled` / `CrossDomainWaitTime` / `BrokerHost` / `BrokerPort` / `CrossDomainCommentTag`
- [ ] `update_job` 入口插入空 `cross_domain_check_update_block()`
- [ ] controller.c 启动跨域线程 stub
- [ ] `cd_user_allows_remote()` 骨架（先返回 true，便于联调）

**Slurm 客户端骨架**：
- [ ] `job_desc_msg_t` / `job_record` / `slurm_job_info_t` 新增字段（`cross_domain` / `app_name` / `remote_cluster_name` / `remote_partition_name` / `remote_job_id` / `remote_state` / `remote_alloc_tres` / `remote_start_time` / `remote_end_time` / `remote_exit_code` / `remote_trace_id` / `cross_domain_forwarded` / `cross_domain_cancel_propagated`）
- [ ] 协议序列化（pack/unpack）扩展，确保旧客户端兼容
- [ ] sbatch 新增 `--cross-domain[=yes|no]` / `--app=<全名>` 参数解析（`src/sbatch/opt.c` `sbatch.c`）
- [ ] squeue 新增 `--remote` 选项 + `%RC/%RJ/%RS/%RT/%RP/%Rs/%Re/%Rx` 占位符骨架（先打印 "-" 也可）
- [ ] scontrol show job 输出 CrossDomain / Remote_* 行骨架
- [ ] sacct 新增 Remote_* 列骨架
- [ ] DBD `job_table` schema 升级新增 Remote_* 列

**broker 端**（按 broker MVP §Sprint 1）：
- [ ] `slurmbrokerd` 进程框架 + listener + `broker.conf` / `user_mapping.conf` 解析
- [ ] `broker_job_t` JSONL 持久化往返
- [ ] `lookup_software.sh` 调用器（broker 内部 fork+exec 包装，先返回 stub）
- [ ] systemd 服务可启停

**Sprint 1 验收**：

```bash
# 1. sbatch 新参数能透传到 ctld 并落到 job_record
$ sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 -p test sleep.sh
Submitted batch job 100
$ scontrol show job 100 -d | grep -E '(CrossDomain|AppName|Remote)'
CrossDomain=yes AppName=lammps-2Aug2023-intelmpi2018
RemoteCluster=(null) RemoteJobId=0 RemoteState=N/A
...

# 2. squeue --remote 选项可用 (远端字段空)
$ squeue --remote -j 100
JOBID PARTITION NAME ... REMOTE_CLUSTER REMOTE_JOBID REMOTE_STATE REMOTE_TRES
  100 test     ...      -              -            -            -

# 3. partition 新关键字解析 OK
$ scontrol show partition virtual_wznormal | grep -i Remote
Remote=YES RemoteDestinations=wzhcnormal@wz_cluster

# 4. broker 启停 OK + lookup_software.sh 可调
$ systemctl status slurmbrokerd
Active: active (running)
$ /opt/slurm-broker/scripts/lookup_software.sh xian_cluster lammps-2Aug2023-intelmpi2018
/public/software/lammps/2Aug2023-intelmpi2018
```

#### Sprint 2（W3-W4）：跨域线程完整逻辑 + sbatch 端到端（US-1）

**ctld 端**：
- [ ] `cd_tick_scan_pending()` 完整实现（路由可达 + assoc comment 含 `allow_remote` + virtual partition.MaxJobs 容量 + 等待阈值）
- [ ] `cd_trigger_forward()` 完整实现（priority=0 / state_desc / cross_domain_forwarded 标志位 / 触发 broker 调用）
- [ ] `cd_send_forward_to_broker()` 调 broker（透传 job_desc + app_name + 用户/账户/工作目录）
- [ ] `handle_broker_update_remote_state()`：写 `remote_*` 独立字段
- [ ] `handle_broker_terminal_state()`：影子作业 PENDING(Held) → COMPLETED/FAILED/CANCELLED + `jobcomp_g_record_job_end`
- [ ] `cd_user_allows_remote()` 真正读 assoc_mgr user/assoc comment

**broker 端**（按 broker MVP §Sprint 2）：
- [ ] `REQUEST_FORWARD_JOB` / `REQUEST_BROKER_FORWARD_JOB` / `REQUEST_BROKER_STAGED_IN` / `REQUEST_BROKER_QUERY_STATUS` 全套
- [ ] rsync stage worker（`sudo -u <src_user> rsync ...`）
- [ ] `rewrite.c` 调 `lookup_software.sh src_cluster <app>` 和 `lookup_software.sh dst_cluster <app>`，按前缀替换脚本中的 source 行 + 替换 `#SBATCH --partition=`
- [ ] 远端 broker `slurm_submit_batch_job()`
- [ ] sync_ticker 调 `REQUEST_BROKER_UPDATE_REMOTE_STATE` 推送字段（不写 comment）
- [ ] 终态调 `REQUEST_BROKER_TERMINAL_STATE`

**应用部署**：
- [ ] LAMMPS 在两端**不同路径**部署 + `software_routes.conf` 双向登记（验证 broker 改写 source 行能力）

**Sprint 2 验收（US-1 端到端）**：

```bash
# 在 A 集群提交跨域作业 (不再用 comment, 直接原生参数)
$ sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 -p xahcnormal lammps.slurm
Submitted batch job 12345

# 监控
$ watch 'squeue --remote -j 12345'
# 应观察到状态依次:
#   PD ... wz_cluster -    PENDING -                       (转发中)
#   PD ... wz_cluster 8888 PENDING -                       (远端入队)
#   PD ... wz_cluster 8888 RUNNING cpu=32,mem=128G        (远端运行)
#   CD ... wz_cluster 8888 COMPLETED cpu=32,mem=128G      (终态)

# 终态检查 (字段化, 不需要 -o "%k")
$ sacct -j 12345 -o JobID,State,ExitCode,Remote_Cluster,Remote_JobId,Remote_AllocTRES,Remote_ExitCode
JobID    State     ExitCode Remote_Cluster Remote_JobId Remote_AllocTRES Remote_ExitCode
12345    COMPLETED 0:0      wz_cluster     8888         cpu=32,mem=128G  0:0

# 远端脚本被改写
$ ssh wz-master 'cat /work/home/wz_test1/.burst/xian_cluster/12345/run.sh.cd_modified.sh' | grep source
source /opt/scnet/apps/lammps/2Aug2023-intelmpi2018/scripts/env.sh
# (源端写的是 /public/software/lammps/2Aug2023-intelmpi2018/scripts/env.sh)

# scontrol show job 字段化
$ scontrol show job 12345 | grep -E '(CrossDomain|Remote|Comment)'
   CrossDomain=yes AppName=lammps-2Aug2023-intelmpi2018
   RemoteCluster=wz_cluster RemotePartition=wzhcnormal
   RemoteJobId=8888 RemoteState=COMPLETED
   RemoteAllocTRES=cpu=32,mem=128G,node=1
   RemoteStartTime=2026-04-27T17:30:00 RemoteEndTime=2026-04-27T18:30:00
   RemoteExitCode=0:0
   Comment=<用户原始 comment 不变>

# 文件回传检查
$ ls /work/home/test1/case1/
# 应看到 stdout/stderr/log/output 文件
```

#### Sprint 3（W5-W6）：scancel + scontrol update + 异常 + 应用扩展（US-2/3/4/5）

**ctld 端**：
- [ ] `cd_tick_scan_cancelled()` 完整实现
- [ ] `cd_send_cancel_to_broker()` 调 `REQUEST_BROKER_CANCEL`
- [ ] `cross_domain_check_update_block()` 拦截 update partition / priority / time_limit

**broker 端**（按 broker MVP §Sprint 3）：
- [ ] `REQUEST_BROKER_CANCEL` 处理 + 跨集群传播
- [ ] 状态机错误分支：rsync 重试 3 次 / 远端拒绝 / lookup_software.sh 失败 / 超时
- [ ] broker 重启幂等（trace_id 去重）
- [ ] 24h / 7d 远端 staging 目录自动清理

**应用扩展**：
- [ ] VASP / GROMACS / Gaussian / Fluent 在两端不同路径部署 + `software_routes.conf` 登记

**Sprint 3 验收**：

```bash
# US-2: scancel
$ sbatch --cross-domain --app=lammps-2Aug2023-intelmpi2018 -p xahcnormal -t 01:00:00 lammps.slurm
$ sleep 600   # 等到 RUNNING
$ scancel <id>
$ sleep 30
$ sacct -j <id> -o State; ssh wz-master "sacct -j <remote_id> -o State"
CANCELLED ...
CANCELLED ...

# US-3: squeue --remote / scontrol show / sacct (见 Sprint 2 验收)
$ squeue -u test1 --remote   # 一行命令看完整状态

# US-4: scontrol update 拒绝
$ scontrol update jobid=<id> partition=other
slurm_update error: Operation not supported
$ scontrol update jobid=<id> priority=100
slurm_update error: Operation not supported
$ scontrol update jobid=<id> timelimit=02:00:00
slurm_update error: Operation not supported

# US-5: broker 重启
$ for i in 1 2 3 4 5; do sbatch --cross-domain --app=... -p xahcnormal lammps.slurm; done
$ sleep 30
$ sudo kill -9 $(pgrep slurmbrokerd) && sleep 5 && sudo systemctl start slurmbrokerd
# 验证: 5 作业全部走完, 远端无重复 jobid

# 4 款新应用
$ for APP in vasp-... gromacs-... gaussian-... fluent-...; do
    sbatch --cross-domain --app=$APP -p xahcnormal $APP.slurm
  done
# 全部 COMPLETED
```

#### Sprint 4（W7-W8）：长稳 + 文档 + 灰度

- [ ] 100 跨域作业并发, 24h 稳定, 无内存泄漏（valgrind / tcmalloc）
- [ ] broker CPU < 50%（单核），RSS < 500MB
- [ ] 故障注入：
  - [ ] 跨集群网络抖动 5min 自愈
  - [ ] broker kill -9 任意时刻可恢复
  - [ ] 远端 ctld 重启
  - [ ] 远端磁盘满
  - [ ] DBD 失联（终态记录补写）
  - [ ] `lookup_software.sh` 退出码非 0
- [ ] 部署文档（含 §8 全部 checklist 自动化为 Ansible playbook 可选）
- [ ] 用户文档（sbatch 跨域参数、`squeue --remote`、scancel、`scontrol show job`、sacct Remote_* 列）
- [ ] 排错 playbook
- [ ] 运维培训 1 次 + 用户培训 1 次
- [ ] 灰度切流：5 用户 → 1 周 → 50 用户 → 1 月 → 全量

### 9.3 工期与依赖

| Sprint | ctld + Client 工作量 | broker 工作量 | 依赖关系 |
|---|---|---|---|
| 1 (W1-W2) | 字段骨架 + 协议序列化（中） | 进程骨架 + 持久化（小） | 协议字段约定先行（W1 第 1 天） |
| 2 (W3-W4) | 跨域线程逻辑 + RPC handler（大） | 跨集群 RPC + lookup_software + rsync（大） | broker → ctld 的 RPC 字段在 W3 中前对齐 |
| 3 (W5-W6) | scontrol/sacct 输出 + 拦截（中） | cancel + 错误分支 + 幂等（中） | 应用上线依赖 broker rewrite 成熟（W5 末） |
| 4 (W7-W8) | 集成验证 + 文档 | 长稳 + 故障注入 | — |

> **关键里程碑**：W2 末协议字段定稿、W4 末第一个跨域作业跑通、W6 末 5 款应用全部跑通、W8 末小流量上线。

---

## 十、整体验收清单

### 10.1 功能验收（7 个用户故事）

- [ ] **US-1**：`sbatch --cross-domain --app=<app> --app-version=<ver>` 提交后 30s 跨域触发，端到端 RUNNING → COMPLETED 跑通
- [ ] **US-1**：远端 stdout/stderr/output 完整回传到源端 work_dir
- [ ] **US-2**：scancel 30s 内远端 kill 完成
- [ ] **US-3**：`squeue --remote` 直接显示 RemoteCluster / RemoteJobId / RemoteState / RemoteAllocTRES 列
- [ ] **US-3**：`scontrol show job` 显示 CrossDomain / AppName / AppVersion / Remote* 多行
- [ ] **US-3**：`sacct` 输出含 Remote_Cluster / Remote_JobId / Remote_AllocTRES / Remote_ExitCode 列
- [ ] **US-4**：scontrol update partition / release / time_limit 影子作业被拒
- [ ] **US-5**：broker kill -9 后重启不丢作业、不重复提交
- [ ] **US-6**：管理员通过 slurm.conf（partition Remote/SendTo + CrossDomain 全局参数）+ `sacctmgr modify user X set comment="...allow_remote..."` + broker.conf / user_mapping.conf / software_routes.conf 即可启用跨域
- [ ] **US-7**：sacct 终态记录正确，账单挂在源端 xahcnormal 物理队列；远端字段同步落地

### 10.2 应用验收（5 款应用）

- [ ] LAMMPS：跨域跑 in.lj 标准 benchmark，结果正确
- [ ] VASP：跨域跑标准 case，OUTCAR 正确
- [ ] GROMACS：跨域跑 mdrun 测试 case，结果正确
- [ ] Gaussian：跨域跑 g16 标准 case，输出正确
- [ ] Fluent：跨域跑 jou 文件，结果正确

### 10.3 异常验收

- [ ] 远端 broker 不可达：源端作业保持 PENDING 等待，5min 后 broker 恢复自动继续
- [ ] rsync 失败 3 次：作业 → FAILED，failure_reason 字段记录原因
- [ ] 远端 sbatch 拒绝（quota）：错误回传，作业 → FAILED
- [ ] 用户映射缺失：broker 拒绝，作业回滚为本地 PENDING（priority 恢复），等运维补映射后下轮重试
- [ ] 应用不在 AllowApp：作业不被跨域，留在本地继续排队
- [ ] 账户不在 AllowAccounts / QOS 不在 AllowQos：作业不被跨域，留在本地继续排队
- [ ] 跨域用户不在白名单：作业不被跨域，留在本地继续排队
- [ ] 虚拟队列 MaxJobs/QOS 容量满：作业等到容量空出
- [ ] `lookup_software.sh` 退出码非 0：broker 拒绝，作业回滚为本地 PENDING
- [ ] munge 跨集群校验失败（时钟漂移 > 24h）：错误日志清晰，运维可定位
- [ ] 网络抖动 5min：作业状态保持，恢复后自动继续

### 10.4 性能验收

- [ ] 100 跨域作业并发，broker CPU < 50%（单核），RSS < 500MB
- [ ] 24h 稳定运行无泄漏（valgrind/tcmalloc 数据印证）
- [ ] ctld 跨域线程对原生调度无性能影响（主调度延迟 < 10ms 增量）
- [ ] checkpoint 写入 1000 作业 < 1s

### 10.5 已知限制（用户文档明示）

- [ ] **仅 sbatch 跨域**，不支持 srun / salloc
- [ ] **仅 CLI 作业**，Portal 作业暂不支持
- [ ] **应用范围**：5 款（LAMMPS/VASP/GROMACS/Gaussian/Fluent）；新增应用需在 software_routes.conf 登记
- [ ] **目的地**：单对端集群（A → B），不支持 A → C
- [ ] **作业脚本**：source 行的路径前缀必须是 `lookup_software.sh` 已登记的路径，否则 broker 不会改写
- [ ] **scontrol update 不支持**：跨域作业不允许改 partition / priority / time_limit
- [ ] **stdout/stderr 仅终态可见**，运行中无法 tail
- [ ] **单 broker 实例**，无 HA
- [ ] **跨集群链路必须有专网或 VPN**
- [ ] **必须使用改造后的 sbatch / squeue / scontrol / sacct**（旧客户端虽能用，但 `--cross-domain` `--remote` 等选项无效）

---

## 十一、风险与缓解

| 风险 | 严重度 | 缓解 |
|---|---|---|
| 跨集群 munge 时钟漂移 > 24h | 高 | munge ttl=86400 + 运维监控 NTP；超过 24h 触发告警 |
| 共享 munge key 扩大信任域 | 高 | MVP 仅在跨域 broker 间共享；二期升级 v0.2 mTLS |
| Slurm 源码改动 → 升级合并冲突 | 高 | 把所有改动收敛在新增文件 + 入口 hook 行（最小入侵）；维护一份针对官方 Slurm 的 patch series，每次升级 rebase |
| `lookup_software.sh` 不可用 / 路由表错误 | 中 | broker 启动时调一次自检；运行时失败回退本地排队；告警监控 |
| 用户映射表维护成本 | 中 | MVP 阶段用户量 < 100，手工维护可接受；v0.3 引入用户池化 |
| ctld 跨域线程影响主调度性能 | 中 | 1s 周期，扫描使用 read_lock；持续监控调度延迟 |
| broker 单点 | 中 | systemd 自动拉起 + JSONL 持久化兜底；二期引入 HA |
| 远端 staging 目录磁盘耗尽 | 中 | 24h 自动清理 + MaxStageBytes 限流（50GB/作业） |
| 虚拟队列 ACL 配置错误（AllowAccounts 漏配） | 中 | 部署 checklist 含 ACL 自检；上线前用 fake 用户回归测试 |
| 跨集群链路抖动导致状态轮询失败 | 低 | 退避重试（10s → 60s → 300s），不轻易判 FAILED |
| 用户忘加 `--cross-domain` | 低 | 默认值是 no，作业自动走本地路径，不会出错；可在 `Reason` 里加用户友好提示 |

---

## 十二、向 v0.1 完整版/v0.2 演进路线

| 演进项 | MVP 状态 | 升级路径 |
|---|---|---|
| Portal 作业支持 | 不支持 | 新增 `translator_portal.c`，broker 端读 `.portal/job_portal.var` |
| 多对端集群 | 单对端 | broker.conf 增加 RemoteCluster 列表；虚拟 partition `RemoteDestinations=` 填多个，ctld 按策略选一个 |
| 集群画像驱动路由 | 静态指向 | 新增 broker 集群画像采集 + ctld 路由决策（在 `cd_trigger_forward` 之前插入） |
| 软件查询脚本演进 | 本地 conf 表 | 升级到 HTTP API / 软件管理平台，脚本接口不变 |
| 用户面参数 | sbatch `--cross-domain`/`--app`/`--app-version`、squeue `--remote`、scontrol/sacct Remote_* | （已交付，后续仅做样式优化） |
| 跨集群 mTLS 鉴权 | 共享 munge | 升级 broker 协议到 v0.2 |
| 用户池化映射 | 静态 1:1 (user_mapping.conf) | broker 引入用户池管理器，映射动态分配 |
| 跨域 ACL 精细化 | 用户白名单 + AllowAccounts/Qos | 升级到 SlurmDBD assoc 级 ACL（每个 (user, account, qos) 独立授权） |
| broker HA | 单实例 | VIP + 共享存储 |
| 实时输出回传 | 终态触发 | 引入流式 streaming 模块 |
| Backfill 跨域调度 | 仅主调度 | Backfill 路径同样调用 cd_check_acl + cd_trigger_forward |

---

## 十三、附录

### 13.1 完整文件清单

```text
源码 (Slurm 源码树):
src/sbatch/
├── opt.c                       修改 (新增 --cross-domain / --app / --app-version)
└── sbatch.c                    修改 (透传到 job_desc_msg_t)

src/squeue/
├── opt.c                       修改 (新增 --remote)
└── print.c                     修改 (新增 %RC/%RJ/%RS/%RT/%RP/%Rs/%Re/%Rx)

src/scontrol/
└── info_job.c                  修改 (输出 CrossDomain / Remote_* 行)

src/sacct/
└── print.c                     修改 (新增 Remote_* 列)

src/common/
├── slurm_protocol_pack.c       修改 (新字段 pack/unpack + 新 RPC 类型)
├── slurm_protocol_defs.h       修改 (新 RPC 枚举)
├── read_config.c               修改 (新关键字解析)
└── slurmdb_defs.c              修改 (Remote_* 列定义)

src/slurmctld/
├── slurmctld.h                 修改 (job_record + part_record 新字段)
├── cross_domain.c              ★ MVP 新增 (~700 LoC)
├── cross_domain.h              ★ MVP 新增
├── cross_domain_acl.c          ★ MVP 新增 (~250 LoC, 三层 ACL)
├── cross_domain_rpc.c          ★ MVP 新增 (~200 LoC)
├── job_mgr.c                   修改 (插入 update 拦截 + 新字段持久化)
├── proc_req.c                  修改 (注册新 RPC handler)
├── partition_mgr.c             修改 (Remote/AllowApp/RemoteDestinations 处理)
└── controller.c                修改 (启动跨域线程 + 加载白名单)

src/plugins/accounting_storage/mysql/
└── as_mysql_job.c              修改 (job_table schema + Remote_* 写入)

src/slurmbrokerd/                 见 Broker MVP 文档 §15.1
                                 (新增 lookup_software 调用器, 改写 source 行)

部署到系统:
/usr/bin/sbatch                  ★ 改造后版本
/usr/bin/squeue                  ★ 改造后版本
/usr/bin/scontrol                ★ 改造后版本
/usr/bin/sacct                   ★ 改造后版本
/usr/sbin/slurmctld              ★ 改造后版本
/usr/sbin/slurmbrokerd           ★ 新增

配置:
/etc/slurm/
├── slurm.conf                  修改 (新增跨域配置 + Include broker.conf)
├── broker.conf                 ★ MVP 新增
├── user_mapping.conf           ★ MVP 新增 (broker 与 ctld 共用)
├── software_routes.conf        ★ MVP 新增 (lookup 脚本数据源, broker 端)
├── scripts/lookup_software.sh  ★ MVP 新增 (软件查询脚本, broker 端)
├── broker_id_ed25519           ★ MVP 新增 (rsync SSH key)
└── munge.key                   修改 (跨集群一致, 建议 ttl=86400)
说明: 用户跨域授权已改为 sacctmgr 设 user/assoc comment 含 "allow_remote",
      不再有独立的 cd_allowed_users.conf 文件

状态:
/var/spool/slurm/broker/        ★ MVP 新增

日志:
/var/log/slurm/slurmctld.log    跨域日志混在其中
/var/log/slurm/slurmbrokerd.log  ★ MVP 新增
```

### 13.2 配置文件模板汇总

> 模板文件已在前文 §5.1 / §8 / Broker MVP §10 给出；此处不重复。

### 13.3 用户文档建议大纲

```text
跨域作业用户指南.md
├── 1. 什么是跨域作业
├── 2. 提交跨域作业 (sbatch --cross-domain --app=<...> --app-version=<...>)
├── 3. 查看跨域作业状态 (squeue --remote, scontrol show job, sacct Remote_*)
├── 4. 取消跨域作业 (scancel)
├── 5. 跨域作业的限制
│   - 仅 CLI 作业
│   - 支持的 5 款应用 (软件路径由 lookup_software 自动适配)
│   - 不支持 scontrol update partition / priority / time_limit
│   - 仅一个目的地集群
├── 6. 常见问题 FAQ
└── 7. 故障排查
```

### 13.4 运维 playbook 大纲

```text
跨域调度运维手册.md
├── 1. 日常运维
│   - 查看跨域作业状态 (sbroker show 或 journalctl)
│   - 用户映射表更新流程 (重启 broker 生效)
│   - 应用上下线流程
│   - 跨域用户白名单管理
├── 2. 故障排查
│   - 跨域不触发 (排查 ctld 跨域线程)
│   - rsync 失败 (排查 SSH key, sudoers)
│   - 远端 sbatch 失败 (排查 munge 一致性, 远端 partition)
│   - munge 跨集群校验失败 (排查 NTP, ttl)
├── 3. 性能调优
│   - PollInterval 调整
│   - StageWorkerCount 调整
│   - MaxInFlightJobs 调整
└── 4. 灾难恢复
    - broker 状态文件损坏
    - 跨集群网络中断
    - 单边集群停机
```

---

## 修订记录

| 版本 | 日期 | 变更 | 作者 |
|---|---|---|---|
| MVP-1.0 | 2026-04-27 | 整体方案 MVP，覆盖用户/调度/跨域/部署全链路 | - |
| MVP-1.1 | 2026-04-27 | 修订 §2.2 七项关键设计：sbatch/squeue 显式参数（不再借用 comment）；状态注入字段化（job_record 新增 Remote_*）；三层 ACL（用户白名单 + AllowAccounts/AllowQos + user_mapping）；限流由虚拟队列承担；软件路径外部化为 lookup_software.sh；squeue --remote 子命令；连带 §3 ~ §13 全文调整；Sprint 由 8 周扩为 10 周 | - |
| MVP-1.2 | 2026-04-27 | 按"ctld 端按完整方案落地、broker 端按快速实现版落地"调整文档分工：撤销 M1/M2 两阶段拆分；新增 §1.3 与 broker MVP 文档分工说明（对外契约 / ctld / 客户端必须完整，broker 内部允许简化）；§2.1 用户故事去除阶段列；§2.2 简化项表恢复单列 MVP 取舍并明确边界仅在 broker 内部；§2.3 还原为单一不做清单；§9 Sprint 计划恢复为 8 周单一计划，4 个 2 周 sprint 按 ctld + broker 并行推进 | - |
