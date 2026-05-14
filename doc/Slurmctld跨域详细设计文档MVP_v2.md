# Slurm 跨域调度 — slurmctld 端详细设计文档 (MVP-v2.0)

> **文档版本**: **MVP-v2.0** (**路由能力完整下沉到 broker**: ctld 只负责"识别 / hold / 转发 / 回写", 不再做"路由二次校验 / 容量 / AllowApp ACL"; 删除 `partition.SendTo` / `partition.AllowApp` / `CrossRegionCommentTag`; 新增 `partition.AllowRemote` + `user.remote_allowed` / `assoc.remote_allowed` + `job_record_t.cd_route_exhausted`)
> **上一版**: MVP-1.5 (修复 scancel 抢跑场景下远端字段永远写不进的过度幂等漏洞; §7.4 拆 A/B 分支 + 引入 `cd_terminal_received` 真幂等 + `_cd_dbd_modify_remote_fields` 走 modify-only SQL 通道)
> **更早**: MVP-1.4 撤销主调度 hook, 跨域线程全表扫描; MVP-1.3 纯物理队列路由 `SendTo` + 可选 `AllowApp`
> **基线版本**: Slurm **24.05.8** (`SLURM_24_05_PROTOCOL_VERSION`)
> **配套文档**:
>   - 上游需求与端到端架构 → `跨域调度-概要设计.md`
>   - broker 内部实现 → `Broker详细设计文档MVP_v2.md`
>   - 协议消息字段 → `Broker详细设计文档MVP_v2.md` §2.6
>   - 路由演进背景 → `跨域路由能力演进设计.md`
>   - v1.5→v2.0 差异蓝图 → `跨域调度详设-差异变更说明.md`
> **范围**: 仅 slurmctld 进程 + 共享给客户端的 `libslurm` / `libslurmfull` 改动 (sbatch / squeue / scancel / scontrol / sacct / **sacctmgr**), 以及 SlurmDBD `as_mysql_job` / `as_mysql_user` / `as_mysql_assoc` schema 升级
> **不在本文范围**: broker 进程内部、`routes.conf` 路由表加载与失效切换、test-only 探测、`lookup_software.sh`、rsync stage、计算节点改造
> **目标读者**: ctld 模块开发、客户端模块开发、协议评审人、SlurmDBD 维护者

---

> **v1.5 → v2.0 顶层差异 (一句话总结)**:
>
> | 条目 | MVP-1.5 (ctld 主导路由) | **MVP-v2.0 (broker 主导路由)** |
> |---|---|---|
> | 远端集群 / 物理队列决策 | ctld 解析 `partition.SendTo` 一对一直连 | **broker 加载 `routes.conf` + test-only 探测, 多远端可选** |
> | 应用软件白名单 (AllowApp) | ctld 解析 `partition.AllowApp` + 入库匹配 `cd_app_name` | **broker `routes.conf` 段 `AllowApps=` + 远端 ctld 真实拒收 / 接收** |
> | 跨域权限来源 | user/assoc.`comment` 包含 `allow_remote` 子串 | **`user.remote_allowed` / `assoc.remote_allowed` 布尔列 (sacctmgr)** |
> | 本地分区是否允许溢出 | 由 `partition.SendTo` 是否非空隐式决定 | **`partition.AllowRemote=yes\|no` 显式开关** |
> | 在途容量软限流 | ctld `cd_phys_cross_region_has_capacity()` 复用 `max_jobs` | **下沉到 broker `cap_check.c` 由 broker 全局视角统一限流** |
> | 路由失败语义 | "ACL 失败 / 容量满 / SendTo 不可达" 三类回滚 | **统一为"broker 报 9010 NO_ROUTE / 9011 TEST_ONLY_REJECTED" + ctld 写 `cd_route_exhausted=1` 不再重试** |
> | 协议 `forward_job_msg_t` payload | 含 `target_cluster` / `target_partition` / `account` / 完整 `job_desc` | **瘦身: 仅 jobid / submit_uid / cd_app_name / 本地 partition_name / cluster_name; 远端字段由 broker 决策** |
> | `cross_region_rpc.c::handle_broker_update_remote_state` | 仅写状态 | **首次状态包还需写 `cd_remote_cluster_name` / `cd_remote_partition_name`** (broker 决策后告知 ctld) |
>
> **总体取舍**: ctld 不知道也不关心远端拓扑, 只把作业"扔给 broker"; broker 是唯一的"跨域路由决策中心"。这一变化把 ctld 的 LoC 从 ~2200 降到 ~1700, 把 broker 的复杂度从"转发桥"提升到"跨域调度大脑"。

---

## 0. 文档使用方式

### 0.1 与整体方案 / broker MVP 文档的分工

| 文档 | 关注点 | 必须完整实现? |
|---|---|---|
| `跨域调度-概要设计.md` | 端到端流程、用户故事、Sprint、验收、跨进程职责划分 | ✅ |
| `Broker详细设计文档MVP_v2.md` | broker 内部模块 + **路由决策** + **test-only 探测** + JSONL 持久化 + stage worker + rewrite | ✅ broker 端 |
| `跨域调度详设-差异变更说明.md` | v1.5 → v2.0 增 / 删 / 改逐条对照, 评审准据 | ✅ (评审用) |
| **本文档** | slurmctld 进程 + 客户端 + DBD 的 24.05.8 适配级实现 | ✅ ctld 端 (不允许 MVP 二次裁剪) |

> 整体方案是 *"做成什么样"*; broker MVP 是 *"broker 模块怎么写代码"*; 本文档是 *"ctld / 客户端 / DBD 怎么对着 24.05.8 源码改"*。

### 0.2 适配原则 (24.05.8 必须遵守的 5 条)

1. **协议版本守恒**: 任何 pack/unpack 改动都用 `if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION)` 包起来; 旧客户端 (≤ 23.11) 收/发跨域字段时按"老协议"行为, 跨域字段 pack 0/NULL, unpack 跳过。
2. **锁体系守恒**: 严格使用 `slurmctld_lock_t` 结构 (24.05 包含 7 个域: `config / job / node / part / fed / pmask / user`), 写跨域线程必须按"先行后域"声明锁集, 杜绝跨锁顺序。
3. **`job_record_t` 字段排序谨慎**: 24.05 已经把 `job_record_t` 塞到将近 250 个字段, 新字段一律放结构体**末尾** + 同步更新 `_dump_job_state()` / `_load_job_state()` / `_pack_*_job_msg()` / `_unpack_*_job_msg()` 五处。
4. **`assoc_mgr` 访问规范**: 读 user/assoc 的新增 `remote_allowed` 字段必须走 `assoc_mgr_lock` 公共锁, 不能直接 deref; **不再**走 `comment` 子串模糊匹配 (v2.0 删除 `CrossRegionCommentTag`)。
5. **主调度 / backfill 完全不改**: `job_scheduler.c` / `backfill.c` 保持 24.05.8 上游原样, 不插任何 hook; 跨域识别 / hold / 转发 / 回滚**全部**由跨域线程周期性**全表扫描** `job_list` 独立完成 — `read_lock` 收集待处理 jobid → 逐个 `write_lock` 完成 hold (`priority=0`) + 用户级 ACL (`user/assoc.remote_allowed`) + 分区开关 (`partition.AllowRemote`) + RPC 发起; 绝不嵌套 `schedule()` 或 `select_g_*`。选择全表扫描 (O(N)) 而非主调度打标 + 事件索引的原因: ① **单一数据源** (`job_list` 本身), 无需维护额外索引, 无事件 hook 漏挂风险; ② **不依赖**主调度的 `default_queue_depth` 覆盖范围, 百万级堆积队列尾部作业同样可被识别; ③ 改动面最小, 仅 `cross_region.c` 一个新文件, 与 24.11/25.05 rebase 无冲突点。
6. **ctld 不做路由决策 (v2.0 新增约束)**: ctld 端**不再**解析"远端集群名 / 远端物理分区名 / AllowApp 白名单", 一律由 broker 决策后回写。ctld 端 `cd_remote_cluster_name` / `cd_remote_partition_name` **不**由 ctld 计算, 而是由 broker 通过 `REQUEST_BROKER_UPDATE_REMOTE_STATE` **首次状态包**告知 ctld 写入 (§7.3)。

### 0.3 改动总览数据 (v2.0)

| 类别 | 文件数 | 新增 LoC | 修改 LoC | 相对 v1.5 增减 | 备注 |
|---|---|---|---|---|---|
| ctld 跨域核心 | 4 (新) | **~780** | — | **−300** | `cross_region.c/.h`, `cross_region_rpc.c/.h` — 路由二次校验/AllowApp ACL/在途容量限流全部下沉到 broker, 仅保留"识别 + hold + 转发 + 回写" |
| ctld 集成 hook | 5 (改) | — | ~80 | −30 | controller / proc_req / job_mgr / partition_mgr / read_config (★ `job_scheduler.c` / `backfill.c` 不动) |
| 协议序列化 | 2 (改) | — | ~180 | −80 | `slurm_protocol_pack.c`, `slurm_protocol_defs.h` (`forward_job_msg_t` payload 瘦身) |
| 客户端 (含新增 sacctmgr) | 9 (改) | ~80 | ~480 | +20 | sbatch / squeue / scontrol / sacct + **sacctmgr `remote_allowed` 子命令** + slurm_opt |
| DBD | 3 (改) | — | ~150 | +30 | `as_mysql_job.c` + `as_mysql_user.c` + `as_mysql_assoc.c` (新增 `remote_allowed` 列) |
| 库级头文件 | 4 (改) | — | ~90 | +10 | `slurm.h.in`, `slurmctld.h`, `slurmdbd_defs.h`, `slurmdb.h` (assoc/user 结构新字段) |
| **合计** | **27 文件** | **~860** | **~980** | **~1840 LoC (−360 vs v1.5)** | ctld 复杂度显著下降, 大部分逻辑迁到 broker |

> 减少的 ~360 LoC 主要在: ① `_cd_partition_allows_app()` 全删 (~60 行); ② `cd_phys_cross_region_has_capacity()` 全删 (~80 行); ③ `_cd_partition_sendto_parse()` 全删 (~50 行); ④ `_build_forward_msg()` payload 缩水 (~70 行); ⑤ partition 端 `cd_send_to` / `cd_remote_cluster` / `cd_remote_partition` / `cd_allow_apps` 解析与 pack/unpack (~100 行)。
> 新增的 ~80 LoC 主要在: ① `sacctmgr remote_allowed` 子命令 (~60 行); ② `cd_route_exhausted` 字段 / scontrol 输出 (~20 行)。

### 0.4 顶层目录树 (相对 Slurm 24.05.8 源码根)

```text
src/
├── common/
│   ├── slurm_protocol_pack.c            modify [§5.4 / §10]
│   ├── slurm_protocol_pack.h            modify [§5.4]
│   ├── slurm_protocol_defs.h            modify [§5.4 / §11.1]
│   ├── slurm_protocol_defs.c            modify [§5.4]
│   ├── read_config.c                    modify [§4]
│   └── slurmdb_defs.c                   modify [§9]
├── slurmctld/
│   ├── slurmctld.h                      modify [§5.1 / §5.2]
│   ├── controller.c                     modify [§7]
│   ├── proc_req.c                       modify [§10.2]
│   ├── job_mgr.c                        modify [§5.3 / §8]
│   ├── partition_mgr.c                  modify [§4.3]
│   ├── job_state_save.c                 modify [§5.3]
│   ├── update_job.c                     modify (24.05 拆出来) [§8]
│   ├── cross_region.c                   ★ NEW [§6]
│   ├── cross_region.h                   ★ NEW
│   ├── cross_region_rpc.c               ★ NEW [§7]
│   └── cross_region_rpc.h               ★ NEW
├── sbatch/
│   ├── opt.c                            modify [§5.5]
│   └── sbatch.c                         modify [§5.5]
├── squeue/
│   ├── opt.c                            modify [§5.6]
│   └── print.c                          modify [§5.6]
├── scontrol/
│   └── info_job.c                       modify [§5.7 / §8]
├── sacct/
│   ├── print.c                          modify [§9.4]
│   └── options.c                        modify [§9.4]
├── plugins/accounting_storage/mysql/
│   ├── as_mysql_job.c                   modify [§9.1 / §9.2]
│   ├── as_mysql_jobacct_process.c       modify [§9.3]
│   ├── as_mysql_user.c                  modify [§9.6]   ★ NEW remote_allowed 列
│   └── as_mysql_assoc.c                 modify [§9.6]   ★ NEW remote_allowed 列
├── sacctmgr/                            ★ NEW [§5.8]
│   └── user_functions.c                 modify (set remote_allowed=)
└── api/
    └── slurm_opt.c                      modify (--allow-remote / --app) [§5.5]

slurm/
├── slurm.h.in                           modify [§5.1 / §5.2]
└── slurmdb.h                            modify [§9.3 / §9.6]   ★ user/assoc 结构新增 remote_allowed
```

> 24.05.8 已经把 `update_job_str()` / `update_job_msg()` 抽到独立路径; 本文档涉及"update 拦截"的部分都基于这一布局。

---

## 1. 全局架构 (ctld 视角)

### 1.1 进程内模块图

```text
┌──────────────────────── slurmctld 进程 ────────────────────────────┐
│                                                                    │
│  ┌────────────┐   sbatch      ┌─────────────────┐  job_record_t   │
│  │  RPC 入口   │──────────────►│ submit_job()    │──► 入 job_list  │
│  │ slurmctld_ │  REQUEST_     │ (job_mgr.c)     │   (Slurm 原生)  │
│  │   req()    │  SUBMIT_BATCH │                 │                 │
│  └─────┬──────┘  _JOB         └─────────────────┘                 │
│        │                                                          │
│        │ ★ 新增 dispatch                                           │
│        │ REQUEST_BROKER_UPDATE_REMOTE_STATE                       │
│        │ REQUEST_BROKER_TERMINAL_STATE                            │
│        ▼                                                          │
│  ┌─────────────────────────┐                                      │
│  │ cross_region_rpc.c      │                                      │
│  │  handle_broker_update_  │                                      │
│  │   remote_state()        │                                      │
│  │  handle_broker_terminal │                                      │
│  │   _state()              │                                      │
│  └─────────────────────────┘                                      │
│                                                                   │
│  ┌──────────── 主调度线程 ─────────────────────────────────┐      │
│  │ schedule() / _schedule()   ★ 完全不动 (上游原样)         │      │
│  │ backfill  / _attempt_backfill()  ★ 完全不动 (上游原样)   │      │
│  │ (跨域逻辑与主调度完全解耦, 无 hook)                      │      │
│  └─────────────────────────────────────────────────────────┘      │
│                                                                   │
│  ┌──────────── ★ 跨域线程 (cross_region.c) ──────────────────┐   │
│  │  cd_thread()  CrossRegionScanInterval (默认 5s) tick:      │   │
│  │    cd_tick_scan_pending()      ★ 全表扫描                  │   │
│  │      • read_lock 内遍历 job_list, 收集候选 jobid 到小集合: │   │
│  │          cd_cross_region==1 && IS_JOB_PENDING              │   │
│  │          && priority>0 && !cd_forwarded                    │   │
│  │          && !cd_route_exhausted   ★ v2.0 新增              │   │
│  │          && now-submit_time>=wait_time                     │   │
│  │          && 本地分区 AllowRemote==1   ★ v2.0 新增           │   │
│  │      • 分批上限 CD_MAX_HANDLE_PER_ROUND (默认 500)          │   │
│  │      • 释放 R 锁后逐个 cd_handle_pending_job_locked(jid):  │   │
│  │          1) ★ hold (priority=0, state="CrossRegionQueued") │   │
│  │          2) 用户级 ACL: user/assoc.remote_allowed==1       │   │
│  │             ★ v2.0: 不再做 AllowApp / 容量 / SendTo 二次校验 │   │
│  │          3) 切 Forwarded_*, 释放 W 锁                       │   │
│  │             ★ v2.0: 不在 ctld 端写 cd_remote_cluster_name  │   │
│  │              / cd_remote_partition_name, 等 broker 首次状态包 │   │
│  │          4) 无锁调 broker REQUEST_FORWARD_JOB              │   │
│  │             失败回滚: 还原 priority, 清 cd_forwarded;      │   │
│  │             若 broker 返回 9010 NO_ROUTE / 9013 ALL_EXHAUSTED │   │
│  │             则置 cd_route_exhausted=1, 永久跳出扫描        │   │
│  │    cd_tick_scan_cancelled()  → scancel 反向传播            │   │
│  │    cd_tick_check_orphans()   → broker 心跳监控             │   │
│  │  cd_send_forward_to_broker() → REQUEST_FORWARD_JOB         │   │
│  │  cd_send_cancel_to_broker()  → REQUEST_BROKER_CANCEL       │   │
│  │  cd_mark_route_exhausted()   → ★ v2.0 新增, 写             │   │
│  │       cd_route_exhausted=1 + state_desc                    │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌──────────── update / scancel 拦截 ────────┐                    │
│  │ update_job_msg() 入口插一行                │                    │
│  │   → cross_region_check_update_block()      │                    │
│  └─────────────────────────────────────────────┘                  │
│                                                                   │
│  ┌──────────── job_state save/restore ──────┐                     │
│  │ pack/unpack 含 cross_region / app_name /  │                    │
│  │ remote_*  字段                            │                    │
│  └───────────────────────────────────────────┘                    │
└──────────────────────────────────────────────────────────────────┘
              │ libslurm / libslurmfull (协议)
              ▼
        ┌──────────┐  RPC + Munge (本机)
        │ broker(A)│
        └──────────┘
```

### 1.2 模块职责

| 模块 | 职责 | 新增 / 修改 |
|---|---|---|
| `cross_region.c` | 跨域线程主体 (识别 / hold / 用户级 ACL `remote_allowed` / `partition.AllowRemote` 开关 / **转发请求 broker** / **接收 broker 回报** / 失败回滚 + 标记 `cd_route_exhausted`); **不再做** `AllowApp` ACL、跨域在途容量限流、`SendTo` 远端拓扑解析 | NEW |
| `cross_region_rpc.c` | broker→ctld RPC handler: `UPDATE_REMOTE_STATE` (首次状态包还会**写入** `cd_remote_cluster_name` / `cd_remote_partition_name`) + `TERMINAL_STATE` | NEW |
| `controller.c` | 启动 / 关闭跨域线程 hook | MODIFY |
| `proc_req.c` | 在 `slurmctld_req()` 大 switch 加 2 个 case (UPDATE_REMOTE_STATE / TERMINAL_STATE) | MODIFY |
| `job_mgr.c` | `job_record_t` 字段持久化 (含 v2.0 新增 `cd_route_exhausted`) + update 拦截 + 转发载荷 clone | MODIFY |
| `partition_mgr.c` | `part_record_t` 新字段持久化 (v2.0 只剩 `cd_allow_remote` 1 个字段) + reconfig 安全 | MODIFY |
| `read_config.c` | slurm.conf 跨域全局键 + partition `AllowRemote` 关键字 | MODIFY |
| `slurm_protocol_pack.c` | 5 类消息 pack/unpack; **v2.0 简化**: `forward_job_msg_t` 删除 `target_cluster` / `target_partition` / `account` / 完整 `job_desc` | MODIFY |

> **v2.0 已删除模块职责**: ctld 端**完全不**承担 (1) 远端集群 / 物理队列名解析与下发, (2) 应用软件白名单 `AllowApp` 匹配, (3) 跨域在途容量配额 (`max_jobs` 复用) 软等待。三者全部下沉到 broker (详见 `Broker详细设计文档MVP_v2.md` §2.5 / §2.7)。

### 1.3 跨域线程与主调度的关系 (职责拆分, MVP-v2.0)

> **核心原则**:
>
> 1. **主调度 / backfill 完全不动**: `_schedule()` 和 `_attempt_backfill()` 保持 24.05.8 上游代码不变, 跨域逻辑与主调度**完全解耦**。
> 2. **跨域线程只做"识别 + hold + 转发请求 + 回写"**: 固定周期 (`CrossRegionScanInterval`, 默认 5s) tick 全表扫描 `job_list`, 自行识别候选 → hold → 用户级 ACL → 调 broker → 回滚, 一条龙闭环。**v2.0 起**: 远端集群 / 物理队列 / 应用白名单 / 在途容量等"路由决策"职责**全部**下沉到 broker。
> 3. **本地优先**: 作业在 `submit_time + cross_region_wait_time` 之前 `priority > 0`, 主调度按正常逻辑尝试本地调度; 只有当 wait_time 过期仍 PENDING, 跨域线程才将其 hold 并转发。

| 阶段 | 责任方 | 动作 (v2.0) | 涉及字段 |
|---|---|---|---|
| **识别候选 + hold** | 跨域线程 `CrossRegionScanInterval` tick (`cd_tick_scan_pending` → `cd_handle_pending_job_locked`) | 扫全表筛 (`cd_cross_region==1 && IS_JOB_PENDING && priority>0 && !cd_forwarded && !cd_route_exhausted && now-submit_time>=wait_time && 本地分区.AllowRemote==1`) → hold (`priority=0`, `state_desc="CrossRegionQueued"`) → 用户级 ACL (`user/assoc.remote_allowed==1`) | 写 `priority=0`, `state_desc`, `cd_forwarded=1` (但**不**写 `cd_remote_*`, 由 broker 回写) |
| **转发请求** | 跨域线程 (释放锁后) | 调 broker `REQUEST_FORWARD_JOB` (无锁 RPC); payload 仅含 `jobid / submit_uid / cd_app_name / 本地 partition_name / 本地 cluster_name` | — |
| **路由 + 探测** | **broker** (本文档不展开, 详见 broker 详设) | 读 `routes.conf` → 用 `cap_check.c` 软限流 → 逐远端 `TEST_ONLY` 探测 → 选定 first OK 远端 | — |
| **首次状态回写 (新)** | broker → ctld RPC `UPDATE_REMOTE_STATE` | **首次状态包**带 `target_cluster_name` / `target_partition_name`, ctld 写入 `cd_remote_cluster_name` / `cd_remote_partition_name` (§7.3) | 写 `cd_remote_cluster_name`, `cd_remote_partition_name`, `cd_remote_state` |
| **失败回滚** | 跨域线程 | broker 同步返回 `9010 NO_ROUTE` / `9013 ALL_EXHAUSTED` → 还原 `priority` + 清 `cd_forwarded` + 置 `cd_route_exhausted=1` (一次性, 不再重试) | 写 `cd_route_exhausted=1`, `state_desc`="CrossRegionExhausted" |
| **持续状态回写** | broker → ctld RPC | `UPDATE_REMOTE_STATE` / `TERMINAL_STATE` | 写 `cd_remote_state`, `cd_remote_jobid` 等 |

**与 MVP-1.5 的差异 (本版本变更)**:

- **删除 ctld 端"路由二次校验"**: ctld 不再用 `partition.SendTo` 决定远端 `cluster@partition`; broker 端 `routes.conf` 是唯一路由真相源。
- **删除 ctld 端 `AllowApp` ACL**: ctld 不再用 `partition.AllowApp` 拦截 `cd_app_name`; broker 端 `routes.conf` 段 `AllowApps=` 接管。
- **删除 ctld 端在途容量软限流**: ctld 不再调用 `cd_phys_cross_region_has_capacity()`; broker 端 `cap_check.c` 接管。
- **新增 `cd_route_exhausted` 字段**: broker 报告"全部远端探测失败"后, ctld 一次性永久标记, 跳出扫描, 避免百万次重试 RPC。
- **`cd_remote_cluster_name` / `cd_remote_partition_name` 由 broker 首次状态包写入**: ctld 在转发请求前**不**填这两个字段, 转发完成后由 broker 通过 `UPDATE_REMOTE_STATE` 告知。
- **新增 `partition.AllowRemote=yes|no` 显式开关**: 替代 v1.5 隐式的"是否配置 `SendTo`"判断。
- **新增 `user.remote_allowed` / `assoc.remote_allowed` 布尔列**: 替代 v1.5 `user/assoc.comment` 子串匹配, 不再需要 `CrossRegionCommentTag` 配置。

**为什么选择全表扫描 (而非事件索引 / 主调度打标)**:

| 方案 | 优点 | 缺点 | MVP-1.4 取舍 |
|---|---|---|---|
| 主调度 hook 打标 (MVP-1.2/1.3) | 响应快 (~1s), 复用主调度遍历 | ⛔ **受 `default_queue_depth` 限制, 百万级堆积时尾部作业无法被识别**; 状态机多一个中间态 | ❌ 撤销 |
| 事件驱动索引 (`cd_pending_heap` + submit/complete/cancel hook) | O(log N) 插入, 延迟 ≤ tick | 需要新数据结构 + 3~5 处事件 hook; 状态一致性风险 (漏挂即漏调度); ctld 重启要重建索引 | ❌ 结构复杂度不可控 |
| **全表扫描 (MVP-1.4)** | **单一数据源 (`job_list`), 实现最简, 无 O(N) 之外的复杂度, 无状态一致性风险, 百万作业全覆盖** | O(N) 扫描, 但默认 5s 一次, 持 `job=R` 锁 < 100ms, 可接受 | ✅ 采用 |

**持锁开销实测参考** (100w 作业, 内部字段字节比较):

- `cd_tick_scan_pending` `read_lock` 单次持锁 ~30-80ms (取决于实际 `cd_cross_region==1` 比例, 典型 5%)
- 5s 周期下 `job_list` 的读锁占用 < 2%, 对主调度 / RPC 线程的阻塞可忽略
- 百万作业规模下单机可支撑; 若出现 `cd_cross_region==1` 比例畸高 (> 50%) 场景, 建议运维调大 `CrossRegionScanInterval` 到 10~30s
- **v2.0 性能注记**: 由于扫描过滤新增 `!cd_route_exhausted` 短路条件, 一旦 broker 报告"全部远端不可达", 该作业永久不再进入扫描热路径, 百万级"长期失败堆积"场景下扫描开销不会随时间退化。

**跨域线程不调用** `schedule()` / `select_g_*` / 不分配资源; 仅做字段写 + broker RPC。

---

## 2. 配置层

### 2.1 `slurm.conf` 新增全局键 (v2.0)

| 键 | 类型 | 默认 | 必需? | 说明 |
|---|---|---|---|---|
| `CrossRegionEnabled` | bool | NO | ❌ | 总开关; NO 时跨域线程不启动, 所有跨域字段保持 NULL/0 |
| `CrossRegionWaitTime` | uint32 | 300 | ❌ | 作业排队 ≥ 该秒数才考虑跨域 |
| `CrossRegionScanInterval` | uint16 | 5 | ❌ | 跨域线程全表扫描周期 (秒); 取值范围 1~300, 百万级队列建议 10~30 |
| `CrossRegionMaxHandlePerRound` | uint16 | 500 | ❌ | 单轮扫描最多处理的"新达阈值"候选作业上限; 防百万级瞬时堆积拥塞 write_lock |
| `BrokerHost` | string | `127.0.0.1` | ✅(开启时) | broker 监听 ctld RPC 的地址 |
| `BrokerPort` | uint16 | 8442 | ✅(开启时) | broker ctld 端口 |
| ~~`CrossRegionCommentTag`~~ | — | — | — | **v2.0 删除**: 改用 `user.remote_allowed` / `assoc.remote_allowed` 显式布尔列, 不再走 `comment` 子串模糊匹配 |

### 2.2 `slurm.conf` 新增 partition 子键 (v2.0 大幅瘦身)

> **v2.0 重大变化**: ctld 端不再承担"远端集群 / 物理队列 / AllowApp 白名单"路由决策 (下沉到 broker `routes.conf`), 因此 partition 子键从 v1.5 的 `SendTo` / `AllowApp` 两个键简化为 `AllowRemote` 一个布尔开关。

| 键 | 类型 | 适用 | 默认 | 说明 |
|---|---|---|---|---|
| `AllowRemote` | bool (`yes`/`no`) | **本地物理分区** | `no` | **★ v2.0 新增**。该本地物理分区是否允许其作业在排队超时后被跨域线程转发到 broker。`no`/未配置 时即使用户携带 `--allow-remote`, 也**不允许**转发 (走本地排队)。该开关替代 v1.5 "是否配置 `SendTo`" 的隐式判断。 |
| ~~`SendTo`~~ | — | — | — | **v2.0 删除**: 远端拓扑由 broker `routes.conf` 段 `[Route]` 内 `LocalPartition=` → `RemoteCluster=` / `RemotePartitions=` 多远端候选给出, ctld 端不再保存"一对一映射"。 |
| ~~`AllowApp`~~ | — | — | — | **v2.0 删除**: 应用软件白名单下沉到 broker `routes.conf` 段 `AllowApps=` (按路由维度而非分区维度), ctld 不再对 `cd_app_name` 做强制校验。 |
| ~~`Remote`~~ | — | — | — | 早在 MVP-1.3 已废弃 (原虚拟出口分区标记)。 |
| ~~`RemoteDestinations`~~ | — | — | — | 早在 MVP-1.3 已废弃。 |

**配置示例 (v2.0)**:

```text
PartitionName=xahcnormal Nodes=node[001-100] Default=YES DefMemPerCPU=4000 MaxTime=INFINITE State=UP \
    AllowRemote=yes
```

> 远端集群与物理队列的对应关系不再写在 ctld `slurm.conf`, 而是写在 broker 的 `routes.conf`:
>
> ```ini
> # broker /etc/slurmbroker/routes.conf  (示意, 详见 broker 详设 §2.10)
> [Route route_xahc_to_wz]
> LocalCluster      = xahc
> LocalPartition    = xahcnormal
> RemoteCluster     = wz_cluster
> RemotePartitions  = wznormal, wzlarge
> AllowApps         = LAMMPS, lammps-2Aug2023-intelmpi2018
> Priority          = 100
> ```

### 2.3 `read_config.c` 解析适配 (24.05.8 风格, v2.0)

24.05 用 `s_p_options_t` 表驱动解析。在 `slurm_conf_options[]` 末尾加 6 条 (v2.0 比 v1.5 少 1 条, 去掉 `CrossRegionCommentTag`):

```c
/* src/common/read_config.c — _slurm_conf_options 表新增 */
{"CrossRegionEnabled",             S_P_BOOLEAN},
{"CrossRegionWaitTime",            S_P_UINT32},
{"CrossRegionScanInterval",        S_P_UINT16},
{"CrossRegionMaxHandlePerRound",   S_P_UINT16},
{"BrokerHost",                     S_P_STRING},
{"BrokerPort",                     S_P_UINT16},
/* v2.0 已删: {"CrossRegionCommentTag", S_P_STRING}  */
```

在 partition 表 `_partition_options[]` 中 (v2.0 比 v1.5 减少 2 条, 增加 1 条):

```c
/* v2.0 新增 */
{"AllowRemote",         S_P_BOOLEAN},
/* v2.0 已删: {"SendTo", S_P_STRING}, {"AllowApp", S_P_STRING} */
```

读取后写入 `slurm_conf_t`:

```c
/* slurm/slurm.h.in: struct slurm_conf 末尾加字段 (维持 ABI 安全, 用 reserved 槽 / 末尾增) */
uint16_t  cross_region_enabled;
uint32_t  cross_region_wait_time;
uint16_t  cross_region_scan_interval;            /* 默认 5s */
uint16_t  cross_region_max_handle_per_round;     /* 默认 500 */
char     *broker_host;
uint16_t  broker_port;
/* v2.0 已删: char *cross_region_comment_tag; */
```

> 24.05.8 的 `slurm_conf_t` 已经接近 200 字段, 新加字段必须放最末尾, 并同步在 `init_slurm_conf()` 与 `free_slurm_conf()` 中处理 init/free。

### 2.4 reconfigure 行为 (v2.0)

- `scontrol reconfigure` 走 `_handle_reconfig_req()` → `read_slurm_conf()`。本设计的全局键全部允许 reconfig 实时生效:
  - `CrossRegionEnabled` 由 NO→YES: 触发 `cross_region_init()` (若线程未起则起)。
  - YES→NO: 调 `cross_region_fini()` 优雅停。已转发作业按字段保持, broker 仍能回写终态。
  - 其他键: 跨域线程下个 tick 自动用新值。
- partition `AllowRemote=` 在 reconfig 时由 `_build_part_bitmap()` 一并刷新 `part_record_t::cd_allow_remote`; 不需要重启。
- **broker 端路由表** (`routes.conf`) 的热更新**不**受 ctld `scontrol reconfigure` 影响, 由 broker 自己监听 SIGHUP (详见 broker 详设 §2.10 `RoutesReloadOnSighup`)。

---

## 3. partition_record_t 扩展 (v2.0 大幅瘦身)

> **v2.0 重大变化**: 从 v1.5 的 4 个 `cd_*` 字段缩减为 **1 个** `cd_allow_remote` 布尔字段。远端集群名 / 物理队列名 / 应用白名单**不再**保存在 ctld 端 partition, 全部下沉到 broker `routes.conf`。

### 3.1 字段 (v2.0)

```c
/* src/slurmctld/slurmctld.h, struct part_record 末尾追加 */
typedef struct part_record {
    /* ... 24.05.8 已有字段 (略, 共 ~80 字段) ... */

    /* === 跨域 MVP-v2.0 新增 (路由下沉到 broker 后仅保留一个开关) === */
    uint8_t   cd_allow_remote;        /* AllowRemote=yes|no, 0=no(默认) / 1=yes */
} part_record_t;
```

> **v2.0 已删字段** (v1.5 → v2.0 迁移):
>
> | v1.5 字段 | 状态 | 去向 |
> |---|---|---|
> | `char *cd_send_to` | **删除** | broker `routes.conf` 段 `LocalPartition=` → `RemoteCluster=` / `RemotePartitions=` |
> | `char *cd_remote_cluster` | **删除** | 同上 (broker 路由决策时填进 `forward_job_msg_t`) |
> | `char *cd_remote_partition` | **删除** | 同上 |
> | `char *cd_allow_apps` | **删除** | broker `routes.conf` 段 `AllowApps=` |
> | (复用) `max_jobs` 跨域在途限流 | **不再复用** | broker `cap_check.c` 内独立计数 |
>
> **新增字段命名理由**: 前缀 `cd_` 表明属于跨域功能, 但仅保留"是否允许"语义, 实际路由由 broker 决策。

### 3.2 解析时机 (`partition_mgr.c::_load_part_state()` / `build_part_bitmap()`, v2.0)

```c
/* src/slurmctld/partition_mgr.c — _list_find_part() / _build_part_internal() 末尾 */
static void _cd_partition_init(part_record_t *p, s_p_hashtbl_t *tbl)
{
    bool allow_remote = false;
    s_p_get_boolean(&allow_remote, "AllowRemote", tbl);
    p->cd_allow_remote = allow_remote ? 1 : 0;

    /* v2.0 已删: SendTo / AllowApp 解析与 "<remote>@<cluster>" 切分 */
}
```

> reconfig 路径同样调用 `_cd_partition_init()`, 切换 `AllowRemote=` 实时生效, 不需要重启。

### 3.3 `scontrol show partition` 输出 (v2.0)

修改 `src/slurm/slurm_print.c::slurm_print_partition_info()` 末尾追加:

```c
fprintf(out, "   AllowRemote=%s\n",
        part->cd_allow_remote ? "YES" : "NO");

/* v2.0 已删: SendTo / AllowApp 回显 */
```

`partition_info_msg_t` 同步在协议层 pack/unpack 新字段 (见 §5.4)。

### 3.4 part 持久化兼容 (v2.0)

`pack_part()` / `unpack_part()` (在 `slurm_protocol_pack.c`) 增加版本分支:

```c
extern void slurm_pack_partition_info_members(partition_info_t *part,
                                              uint16_t protocol_version,
                                              buf_t *buffer)
{
    /* ... 已有字段 pack ... */

    if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
        pack8(part->cd_allow_remote, buffer);
        /* v2.0 已删: pack cd_send_to / cd_allow_apps / cd_remote_cluster /
         *           cd_remote_partition 共 4 处 packstr */
    }
    /* < 24.05.8 旧客户端不感知, 跳过即可 */
}

static int _unpack_partition_info_members(partition_info_t *part,
                                          buf_t *buffer,
                                          uint16_t protocol_version)
{
    if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
        safe_unpack8(&part->cd_allow_remote, buffer);
    } else {
        part->cd_allow_remote = 0;
    }
    /* v2.0 已删: 4 处 safe_unpackstr_xmalloc */
}
```

> **协议兼容性说明**: 由于 v2.0 的 `partition_info_t` pack 字段数比 v1.5 少 3 个 (1 个 `pack8` 替换 4 个 `packstr`), v2.0 客户端 / 服务端必须**整体**升级。混部时:
>
> - 24.05.8 v1.5 ctld + 24.05.8 v2.0 client: `scontrol show partition` 会读到额外的 `cd_send_to` 等字段, 但 v2.0 client 不解析, 安全。
> - 24.05.8 v2.0 ctld + 24.05.8 v1.5 client: v1.5 client 期望 4 个 packstr, 会 unpack 失败 → 拒绝混部, 必须同步升级。

---

## 4. job_record_t / job_desc_msg_t 扩展

### 4.1 `job_record_t` 新字段 (`src/slurmctld/slurmctld.h`)

```c
/* === 跨域请求侧 (sbatch 提交时填) === */
uint16_t  cd_cross_region;            /* 0=否, 1=允许 (用户意图) */
char     *cd_app_name;                /* "lammps-2Aug2023-intelmpi2018" */

/* === 跨域决策侧 (ctld 跨域线程在转发时填) === */
uint8_t   cd_forwarded;               /* 转发幂等标志 */
uint8_t   cd_cancel_propagated;       /* scancel 反向传播幂等标志: 我已通知 broker 去 kill */
uint8_t   cd_terminal_received;       /* TERMINAL_STATE 幂等标志: broker 已回告我远端真实终态字段 */
uint8_t   cd_route_exhausted;         /* ★ v2.0 新增: broker 报告"全部远端不可达" 后 ctld 一次性置位,
                                         * 跨域线程扫描时短路跳过, 避免百万级 RPC 重试。
                                         * 仅当 broker 同步返回 9010 NO_ROUTE / 9013 ALL_EXHAUSTED 时设. */
char     *cd_remote_trace_id;         /* "xian_cluster-12345" */

/* === 跨域执行侧 (broker 回写, v2.0 起 cluster/partition 也由 broker 首次状态包写入) === */
char     *cd_remote_cluster_name;     /* "wz_cluster"  ★ v2.0: 由 broker UPDATE_REMOTE_STATE 首次包写入 */
char     *cd_remote_partition_name;   /* "wzhcnormal"  ★ v2.0: 同上 */
uint32_t  cd_remote_job_id;
uint32_t  cd_remote_state;            /* 复用 JOB_STATE_* */
char     *cd_remote_alloc_tres;       /* "cpu=32,mem=128G,node=1" */
time_t    cd_remote_start_time;
time_t    cd_remote_end_time;
uint32_t  cd_remote_exit_code;
```

> **字段位置规则**: 必须放在 `job_record_t` **结构体末尾**, 不要插中间; 24.05.8 多个 macro/iter 假设字段顺序稳定。
> **命名前缀**: 全部用 `cd_` (cross-domain → cross-region, 命名不变以保持二进制兼容) 防止和 Slurm 已有的 `comment` / `start_time` / `exit_code` 等基础字段混淆。
>
> **v2.0 新增字段 `cd_route_exhausted` 语义详解**:
>
> | 触发条件 | broker 返回码 | ctld 行为 |
> |---|---|---|
> | broker 没有任何匹配 `routes.conf` 路由 | `9010 NO_ROUTE` | 立即置 `cd_route_exhausted=1`, 还原 priority |
> | broker 所有候选远端 `TEST_ONLY` 均失败 | `9013 ALL_EXHAUSTED` | 同上 |
> | broker 转发链路网络/超时 | `9012 TEST_ONLY_TIMEOUT` | **不**置位, ctld 下个 tick 仍可重试 |
> | broker 容量软限流, 暂时不可转 | `9020 CAP_FULL_SOFT_WAIT` | **不**置位, ctld 下个 tick 仍可重试 |
>
> 即: `cd_route_exhausted=1` 仅表达"配置/拓扑层面不可达", 不针对临时性故障。

#### 4.1.1 `cd_route_exhausted` 与运维手动清除

- **运维手动重置**: 当 broker 端运维新增 / 修复 `routes.conf` 后, 旧的"被钉死"作业不会自动重试。管理员可通过 `scontrol update jobid=<JID> CdRouteExhausted=0` 强制清除, 该作业进入下个 tick 重新参与扫描。
- 实现: `src/slurmctld/update_job.c::update_job_msg()` 增加一个 `update_job_msg_t::cd_route_exhausted_reset` (uint8_t) 字段, 仅 root/operator 可设。详见 §8 update 拦截改造。

### 4.2 `job_desc_msg_t` 同步新增 (`slurm/slurm.h.in`)

```c
typedef struct job_descriptor {
    /* ... 24.05 已有 ~150 字段 ... */
    uint16_t  cross_region;        /* 客户端原生参数 */
    char     *app_name;            /* 客户端原生参数 */
} job_desc_msg_t;
```

`slurm_init_job_desc_msg()` 中初始化:

```c
void slurm_init_job_desc_msg(job_desc_msg_t *job_desc_msg)
{
    memset(job_desc_msg, 0, sizeof(*job_desc_msg));
    /* ... 已有 default 设置 ... */
    job_desc_msg->cross_region = 0;
    job_desc_msg->app_name     = NULL;
}

void slurm_free_job_desc_msg(job_desc_msg_t *msg)
{
    if (!msg) return;
    /* ... 已有 free ... */
    xfree(msg->app_name);
    xfree(msg);
}
```

### 4.3 `slurm_job_info_t` (squeue / scontrol 共用) 同步新增 (`slurm/slurm.h.in`)

```c
typedef struct slurm_job_info {
    /* ... 已有字段 ... */

    /* === 跨域字段 (透传给客户端) === */
    uint16_t  cross_region;
    char     *app_name;
    char     *cd_remote_cluster_name;
    char     *cd_remote_partition_name;
    uint32_t  cd_remote_job_id;
    uint32_t  cd_remote_state;
    char     *cd_remote_alloc_tres;
    time_t    cd_remote_start_time;
    time_t    cd_remote_end_time;
    uint32_t  cd_remote_exit_code;
    char     *cd_remote_trace_id;
    uint8_t   cd_route_exhausted;          /* ★ v2.0 新增, scontrol show job 用于回显 RouteExhausted=YES/NO */
} slurm_job_info_t;
```

### 4.4 协议 pack/unpack (`slurm_protocol_pack.c`)

> **关键**: 24.05.8 的 `_pack_job_desc_msg()` / `_unpack_job_desc_msg()` 都按 protocol_version 分支; 新字段必须放在 24.05 分支末尾, 旧版本走 fallback。

#### 4.4.1 job_desc_msg 方向

```c
static void _pack_job_desc_msg(job_desc_msg_t *job_desc_ptr,
                                buf_t *buffer,
                                uint16_t protocol_version)
{
    /* ... 24.05 现有 pack ... */

    if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
        pack16(job_desc_ptr->cross_region, buffer);
        packstr(job_desc_ptr->app_name,     buffer);
    }
    /* 旧 client 提交时不带, 服务端 unpack 时给 0 / NULL */
}

static int _unpack_job_desc_msg(job_desc_msg_t **job_desc_buffer_ptr,
                                buf_t *buffer,
                                uint16_t protocol_version)
{
    /* ... 24.05 现有 unpack ... */

    if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
        safe_unpack16(&job_desc_ptr->cross_region, buffer);
        safe_unpackstr_xmalloc(&job_desc_ptr->app_name, &uint32_tmp, buffer);
    } else {
        job_desc_ptr->cross_region = 0;
        job_desc_ptr->app_name = NULL;
    }
    return SLURM_SUCCESS;

unpack_error:
    slurm_free_job_desc_msg(job_desc_ptr);
    *job_desc_buffer_ptr = NULL;
    return SLURM_ERROR;
}
```

#### 4.4.2 job_info / squeue 方向 (v2.0)

```c
extern void _pack_job_info_members(struct job_record *dump_job_ptr,
                                    buf_t *buffer,
                                    uint16_t protocol_version)
{
    /* ... 已有 pack ... */

    if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
        pack16(dump_job_ptr->cd_cross_region, buffer);
        packstr(dump_job_ptr->cd_app_name, buffer);
        packstr(dump_job_ptr->cd_remote_cluster_name, buffer);
        packstr(dump_job_ptr->cd_remote_partition_name, buffer);
        pack32(dump_job_ptr->cd_remote_job_id, buffer);
        pack32(dump_job_ptr->cd_remote_state, buffer);
        packstr(dump_job_ptr->cd_remote_alloc_tres, buffer);
        pack_time(dump_job_ptr->cd_remote_start_time, buffer);
        pack_time(dump_job_ptr->cd_remote_end_time, buffer);
        pack32(dump_job_ptr->cd_remote_exit_code, buffer);
        packstr(dump_job_ptr->cd_remote_trace_id, buffer);
        pack8(dump_job_ptr->cd_route_exhausted, buffer);   /* ★ v2.0 新增 */
    }
}
```

`slurm_job_info_t` 客户端结构同步加 `cd_route_exhausted` 字段, `_unpack_job_info_members()` 对称读取; v1.x 协议路径下设为 0。

`_unpack_job_info_members()` 镜像 + 旧版本 fallback 全部 0/NULL。

#### 4.4.3 partition_info 方向

见 §3.4。

### 4.5 状态文件 (job_state) 兼容 (v2.0)

`src/slurmctld/job_mgr.c::_dump_job_state()` 与 `_load_job_state()` 是 ctld 重启 restore 用的本地序列化, 走 `SLURM_PROTOCOL_VERSION`。同样在末尾追加版本分支:

```c
static void _dump_job_state(struct job_record *dump_job_ptr, buf_t *buffer)
{
    /* ... 已有 pack ... */
    pack16(dump_job_ptr->cd_cross_region, buffer);
    packstr(dump_job_ptr->cd_app_name, buffer);
    pack8(dump_job_ptr->cd_forwarded, buffer);
    pack8(dump_job_ptr->cd_cancel_propagated, buffer);
    pack8(dump_job_ptr->cd_terminal_received, buffer);
    pack8(dump_job_ptr->cd_route_exhausted, buffer);   /* ★ v2.0 新增 */
    packstr(dump_job_ptr->cd_remote_trace_id, buffer);
    packstr(dump_job_ptr->cd_remote_cluster_name, buffer);
    packstr(dump_job_ptr->cd_remote_partition_name, buffer);
    pack32(dump_job_ptr->cd_remote_job_id, buffer);
    pack32(dump_job_ptr->cd_remote_state, buffer);
    packstr(dump_job_ptr->cd_remote_alloc_tres, buffer);
    pack_time(dump_job_ptr->cd_remote_start_time, buffer);
    pack_time(dump_job_ptr->cd_remote_end_time, buffer);
    pack32(dump_job_ptr->cd_remote_exit_code, buffer);
}

static int _load_job_state(struct job_record *job_ptr, buf_t *buffer,
                           uint16_t protocol_version)
{
    /* ... 已有 unpack ... */
    if (protocol_version >= SLURM_24_05_V2_PROTOCOL_VERSION /* v2.0 内部小版本 */) {
        safe_unpack16(&job_ptr->cd_cross_region, buffer);
        safe_unpackstr_xmalloc(&job_ptr->cd_app_name, &uint32_tmp, buffer);
        safe_unpack8(&job_ptr->cd_forwarded, buffer);
        safe_unpack8(&job_ptr->cd_cancel_propagated, buffer);
        safe_unpack8(&job_ptr->cd_terminal_received, buffer);
        safe_unpack8(&job_ptr->cd_route_exhausted, buffer);   /* ★ v2.0 新增 */
        /* ... 其余远端字段 unpack ... */
    } else if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
        /* v1.x state 文件: 没有 cd_route_exhausted, 设为 0 */
        safe_unpack16(&job_ptr->cd_cross_region, buffer);
        safe_unpackstr_xmalloc(&job_ptr->cd_app_name, &uint32_tmp, buffer);
        safe_unpack8(&job_ptr->cd_forwarded, buffer);
        safe_unpack8(&job_ptr->cd_cancel_propagated, buffer);
        safe_unpack8(&job_ptr->cd_terminal_received, buffer);
        job_ptr->cd_route_exhausted = 0;
        /* ... 其余远端字段 unpack ... */
    } else {
        /* < 24.05 state 文件: 跨域字段全 0/NULL */
    }
}
```

> **v1.5 → v2.0 状态文件升级路径**:
>
> - **首次升级**: 旧 state 文件 protocol_version 等于 v1.5 (`SLURM_24_05_PROTOCOL_VERSION`), unpack 时新字段 `cd_route_exhausted` 取默认值 `0`, 即升级后所有作业默认"未耗尽路由", 第一次 tick 会按 v2.0 路径调 broker 重新走一遍。
> - **回滚兼容**: 若必须从 v2.0 回滚到 v1.5, 需运维先用 `scontrol cancel all` 或重启 ctld 丢 state, 否则 v1.5 ctld unpack 时多读 1 字节会偏移 → 此情况 SOP 见 §15.4。

`_load_job_state()` 通过当前 ctld 版本判断, 24.05 之后的 state 文件包含新字段; 升级时旧 state 文件没有 → 走 `if (state_protocol_version >= ...)` 分支, 旧字段全部赋 NULL/0。

> **关键**: 跨大版本升级 (例如从 23.11 升级到 24.05.8 + cross-domain), `state_protocol_version < SLURM_24_05_PROTOCOL_VERSION` 时, 跨域字段直接置零。这保证升级零数据风险。

### 4.6 JobAcctGather / job_completion 接入

终态写入 sacct 由 `jobcomp_g_record_job_end(job_ptr)` 触发, 已经能拿到 `job_ptr->cd_*` 字段。在 `src/plugins/jobacct_gather/.../*.c` 与 `src/plugins/jobcomp/*` 不需要改; SlurmDBD 端 schema 升级见 §9。

---

## 5. 客户端改动

### 5.1 `slurm_opt_t` 表注册 (`src/api/slurm_opt.c`)

24.05 已经统一所有 `--*` 选项到 `slurm_opt_t` + `static const struct option_def[]` 表。新增 2 项:

```c
/* src/api/slurm_opt.c — slurm_options[] 末尾追加 */
{
    .name      = "allow-remote",
    .has_arg   = optional_argument,
    .val       = LONG_OPT_CROSS_REGION,
    .set_func  = arg_set_cross_region,
    .get_func  = arg_get_cross_region,
    .reset_func = arg_reset_cross_region,
},
{
    .name      = "app",
    .has_arg   = required_argument,
    .val       = LONG_OPT_APP,
    .set_func  = arg_set_app_name,
    .get_func  = arg_get_app_name,
    .reset_func = arg_reset_app_name,
},
```

实现 (同文件):

```c
static int arg_set_cross_region(slurm_opt_t *opt, const char *arg)
{
    if (!arg || !xstrcasecmp(arg, "yes") || !xstrcasecmp(arg, "true") || !xstrcasecmp(arg, "1"))
        opt->cross_region = 1;
    else if (!xstrcasecmp(arg, "no") || !xstrcasecmp(arg, "false") || !xstrcasecmp(arg, "0"))
        opt->cross_region = 0;
    else {
        error("Invalid --allow-remote value: %s (yes|no)", arg);
        return SLURM_ERROR;
    }
    return SLURM_SUCCESS;
}

static int arg_set_app_name(slurm_opt_t *opt, const char *arg)
{
    xfree(opt->app_name);
    opt->app_name = xstrdup(arg);
    return SLURM_SUCCESS;
}

/* arg_get_* / arg_reset_* 对称实现, 略 */
```

`slurm_opt_t` 结构本身在 `src/common/slurm_opt.h` 末尾加:

```c
typedef struct {
    /* ... 24.05 已有 ... */
    uint16_t  cross_region;
    char     *app_name;
} slurm_opt_t;
```

### 5.2 sbatch 透传 (`src/sbatch/sbatch.c`)

24.05 sbatch 在 `_set_options()` 之后, `_set_submit_dir_env()` 前调 `slurm_opt_to_job_desc()`。无需改 sbatch 主流程, 只需在 `slurm_opt_to_job_desc()` (在 `src/api/slurm_opt.c`) 加映射:

```c
extern int slurm_opt_to_job_desc(slurm_opt_t *opt_local, job_desc_msg_t *desc, ...)
{
    /* ... 已有映射 ... */
    desc->cross_region = opt_local->cross_region;
    desc->app_name     = xstrdup(opt_local->app_name);
    return SLURM_SUCCESS;
}
```

### 5.3 `#SBATCH` 指令解析

24.05 `_get_next_opt()` 会扫描脚本头, 自动复用 `slurm_options[]`。所以 `#SBATCH --allow-remote` / `#SBATCH --app=lammps-2Aug2023-intelmpi2018` 自动支持, 零额外改动。

### 5.4 sbatch 帮助文本

`src/sbatch/sbatch.c::_usage()` 加两行:

```text
      --allow-remote[=yes|no]    Allow cross-domain forwarding of this job
      --app=<name>               Application full name (with version) for cross-domain
```

### 5.5 squeue `--remote` 选项 + `%R*` 占位符

#### 5.5.1 选项注册 (`src/squeue/opt.c`)

```c
static struct option long_options[] = {
    /* ... 已有 ... */
    {"remote", no_argument, 0, LONG_OPT_REMOTE},
    {0, 0, 0, 0}
};

case LONG_OPT_REMOTE:
    params.cross_region_view = true;
    if (!params.format) {
        params.format = xstrdup(
            "%.10i %.10P %.10j %.10u %.4T %.10M "
            "%.15RC %.12RJ %.12RS %.30RT");
    }
    break;
```

`squeue_params_t` 加字段 `bool cross_region_view;` (`src/squeue/squeue.h`)。

#### 5.5.2 占位符表 (`src/squeue/print.c`)

24.05 的 squeue print 用 `field_t` 表 + 分发函数。在 `job_format_options[]` 末尾加 8 项:

```c
{"RC",  "REMOTE_CLUSTER",   _print_job_cd_remote_cluster,      8 },
{"RP",  "REMOTE_PARTITION", _print_job_cd_remote_partition,    8 },
{"RJ",  "REMOTE_JOBID",     _print_job_cd_remote_job_id,       8 },
{"RS",  "REMOTE_STATE",     _print_job_cd_remote_state,        8 },
{"RT",  "REMOTE_TRES",      _print_job_cd_remote_alloc_tres,  16 },
{"Rs",  "REMOTE_START",     _print_job_cd_remote_start,       16 },
{"Re",  "REMOTE_END",       _print_job_cd_remote_end,         16 },
{"Rx",  "REMOTE_EXIT",      _print_job_cd_remote_exit_code,    8 },
```

实现:

```c
int _print_job_cd_remote_cluster(job_info_t *job, int width, bool right_justify, char *suffix)
{
    if (job == NULL)
        _print_str("REMOTE_CLUSTER", width, right_justify, true);
    else
        _print_str(job->cd_remote_cluster_name ?: "-", width, right_justify, true);
    if (suffix) printf("%s", suffix);
    return SLURM_SUCCESS;
}

int _print_job_cd_remote_state(job_info_t *job, int width, bool right_justify, char *suffix)
{
    if (job == NULL)
        _print_str("REMOTE_STATE", width, right_justify, true);
    else
        _print_str(job->cd_remote_job_id
                   ? job_state_string(job->cd_remote_state)
                   : "-",
                   width, right_justify, true);
    if (suffix) printf("%s", suffix);
    return SLURM_SUCCESS;
}
/* 其它对称实现, 略 */
```

> **注意**: 24.05 squeue 的 `field_size` 字段必须填合理默认, 防止终端列宽错乱。

### 5.6 scontrol show job 输出 (`src/scontrol/info_job.c`)

修改 `slurm_print_job_info()` (实际在 `src/api/job_info.c::_print_job_struct()`) 末尾追加:

```c
xstrcat(out, "   ");
xstrfmtcat(out, "CrossRegion=%s ", job_ptr->cross_region ? "yes" : "no");
if (job_ptr->app_name)
    xstrfmtcat(out, "AppName=%s ", job_ptr->app_name);
xstrfmtcat(out, "RouteExhausted=%s",            /* ★ v2.0 新增 */
           job_ptr->cd_route_exhausted ? "YES" : "NO");
xstrcat(out, "\n");

if (job_ptr->cd_remote_cluster_name) {
    xstrcat(out, "   ");
    xstrfmtcat(out, "RemoteCluster=%s ",  job_ptr->cd_remote_cluster_name);
    xstrfmtcat(out, "RemotePartition=%s\n",
               job_ptr->cd_remote_partition_name ?: "(null)");

    xstrcat(out, "   ");
    xstrfmtcat(out, "RemoteJobId=%u ",
               job_ptr->cd_remote_job_id);
    xstrfmtcat(out, "RemoteState=%s\n",
               job_ptr->cd_remote_job_id
                   ? job_state_string(job_ptr->cd_remote_state)
                   : "N/A");

    if (job_ptr->cd_remote_alloc_tres && *job_ptr->cd_remote_alloc_tres) {
        xstrcat(out, "   ");
        xstrfmtcat(out, "RemoteAllocTRES=%s\n",
                   job_ptr->cd_remote_alloc_tres);
    }

    xstrcat(out, "   ");
    char tbuf[32];
    if (job_ptr->cd_remote_start_time) {
        slurm_make_time_str(&job_ptr->cd_remote_start_time, tbuf, sizeof(tbuf));
        xstrfmtcat(out, "RemoteStartTime=%s ", tbuf);
    } else
        xstrcat(out, "RemoteStartTime=Unknown ");

    if (job_ptr->cd_remote_end_time) {
        slurm_make_time_str(&job_ptr->cd_remote_end_time, tbuf, sizeof(tbuf));
        xstrfmtcat(out, "RemoteEndTime=%s\n", tbuf);
    } else
        xstrcat(out, "RemoteEndTime=Unknown\n");

    xstrcat(out, "   ");
    if (job_ptr->cd_remote_end_time)
        xstrfmtcat(out, "RemoteExitCode=%u:%u\n",
                   WEXITSTATUS(job_ptr->cd_remote_exit_code),
                   WTERMSIG(job_ptr->cd_remote_exit_code));
    else
        xstrcat(out, "RemoteExitCode=N/A\n");

    if (job_ptr->cd_remote_trace_id) {
        xstrcat(out, "   ");
        xstrfmtcat(out, "RemoteTraceId=%s\n", job_ptr->cd_remote_trace_id);
    }
}
```

> **重要**: `Comment=` 行保持原样, 不要插入跨域内容。

### 5.7 sacct `Remote_*` 列见 §9.4。

### 5.8 sacctmgr `remote_allowed` 子命令 (★ v2.0 新增)

**目标**: 管理员通过命令行管理 `user.remote_allowed` / `assoc.remote_allowed` 布尔列, 替代 v1.5 在 user/assoc.comment 中嵌入 `allow_remote` 子串的模糊匹配方式。

#### 5.8.1 命令语法

```bash
# 新建用户时同时启用跨域权限
sacctmgr add user name=test1 account=public_acct remote_allowed=true

# 修改已存在用户的跨域权限
sacctmgr modify user test1 set remote_allowed=true
sacctmgr modify user test1 set remote_allowed=false

# 修改 assoc 维度 (即用户在某个 account 下是否允许跨域, 比 user 粒度更细)
sacctmgr modify user test1 account=hpc_acct set remote_allowed=true

# 查询
sacctmgr show user test1 format=user,account,remote_allowed
sacctmgr show assoc where user=test1 format=cluster,account,user,remote_allowed
```

#### 5.8.2 实现位置 (`src/sacctmgr/`)

| 文件 | 改动 |
|---|---|
| `sacctmgr.c::_set_cond_options()` | 增加 `--remote_allowed` 过滤选项识别 |
| `user_functions.c::_set_user_rec()` | 解析 `remote_allowed=` 关键字, 写入 `slurmdb_user_rec_t::remote_allowed` |
| `assoc_functions.c::_set_assoc_rec()` | 解析 `remote_allowed=` 关键字, 写入 `slurmdb_assoc_rec_t::remote_allowed` |
| `print_fields.c` | 注册 `PRINT_REMOTE_ALLOWED` 列 (uint16 → "YES"/"NO") |

#### 5.8.3 `slurmdb.h` 结构扩展

```c
typedef struct slurmdb_user_rec {
    /* ... 24.05 已有字段 ... */
    uint16_t remote_allowed;     /* ★ v2.0 新增: 0=否, 1=允许 */
} slurmdb_user_rec_t;

typedef struct slurmdb_assoc_rec {
    /* ... 24.05 已有字段 ... */
    uint16_t remote_allowed;     /* ★ v2.0 新增: 0=否, 1=允许; 优先级高于 user 维度 */
} slurmdb_assoc_rec_t;
```

#### 5.8.4 ACL 生效优先级 (跨域线程消费)

ctld 跨域线程在 §6.7 用户 ACL 校验时按以下顺序判定:

```text
if (assoc.remote_allowed 显式设置过)  → 用 assoc.remote_allowed
else if (user.remote_allowed)         → 用 user.remote_allowed
else                                   → 视为 0 (默认拒绝)
```

实现见 §6.7。

> **迁移注意**: v1.5 部署若已在 `user.comment` 中存有 `allow_remote` 子串, 升级到 v2.0 后该 comment 仍保留, 但 ctld 不再读取 (语义无效)。运维需通过 §15.3 提供的脚本批量迁移: 把所有 `comment LIKE '%allow_remote%'` 的 user 设置 `remote_allowed=1`。

---

## 6. ctld 跨域线程 — `cross_region.c`

> 本章是本文档的核心。给出 24.05.8 适配下的完整实现骨架。

### 6.1 头文件

```c
/* src/slurmctld/cross_region.h */
#ifndef _SLURMCTLD_CROSS_REGION_H
#define _SLURMCTLD_CROSS_REGION_H

#include "slurm/slurm.h"
#include "src/slurmctld/slurmctld.h"

/* 生命周期 */
extern int  cross_region_init(void);
extern void cross_region_fini(void);
extern void cross_region_reconfig(void);   /* reconfigure 时调 */

/* ★ MVP-1.4: 不再对外暴露主调度 hook; 跨域识别/打标/hold/决策/转发
 * 全部由跨域线程内部的 cd_tick_scan_pending() + cd_handle_pending_job_locked()
 * 闭环完成, job_scheduler.c / backfill.c 不调用本头文件任何 API。 */

/* RPC handler (注册到 proc_req.c) */
extern int  handle_broker_update_remote_state(slurm_msg_t *msg);
extern int  handle_broker_terminal_state(slurm_msg_t *msg);

/* update_job 入口拦截 (job_mgr.c update_job_msg() 入口调) */
extern int  cross_region_check_update_block(job_record_t *job_ptr,
                                             job_desc_msg_t *job_specs,
                                             uid_t caller_uid);

/* 工具 (供其它模块调用) */
extern bool cd_user_allows_remote(job_record_t *job_ptr);   /* 读 assoc/user.remote_allowed */
extern void cd_mark_route_exhausted(job_record_t *job_ptr,  /* ★ v2.0 新增 */
                                    const char *reason);
/* v2.0 已删: cd_partition_allows_app (AllowApp 下沉到 broker)
 * v2.0 已删: cd_phys_cross_region_has_capacity (在途限流下沉到 broker) */

/* 错误码 */
extern char *cd_strerror(int rc);

#endif /* _SLURMCTLD_CROSS_REGION_H */
```

### 6.2 模块全局

```c
/* src/slurmctld/cross_region.c */
#include "cross_region.h"
#include "src/common/slurmdb_defs.h"
#include "src/common/assoc_mgr.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/job_mgr.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/proc_req.h"
#include "src/slurmctld/state_save.h"

/* 默认值 (可被 slurm.conf 覆盖, 运行期从 slurm_conf.cross_region_scan_interval
 * / cross_region_max_handle_per_round 读取) */
#define CD_DEFAULT_SCAN_INTERVAL_SEC       5
#define CD_DEFAULT_MAX_HANDLE_PER_ROUND    500
#define CD_TICK_INTERVAL_SEC               1   /* 次级 tick (cancel / orphan) 1s */
#define CD_BROKER_RPC_TIMEOUT_SEC          30

static pthread_t        cd_thread_id = 0;
static volatile bool    cd_running   = false;
static pthread_mutex_t  cd_init_lock = PTHREAD_MUTEX_INITIALIZER;
```

### 6.3 生命周期

```c
extern int cross_region_init(void)
{
    pthread_mutex_lock(&cd_init_lock);
    if (cd_running) {
        pthread_mutex_unlock(&cd_init_lock);
        return SLURM_SUCCESS;
    }

    if (!slurm_conf.cross_region_enabled) {
        info("cross_region: disabled by slurm.conf");
        pthread_mutex_unlock(&cd_init_lock);
        return SLURM_SUCCESS;
    }

    if (!slurm_conf.broker_host || !slurm_conf.broker_port) {
        error("cross_region: BrokerHost / BrokerPort missing");
        pthread_mutex_unlock(&cd_init_lock);
        return SLURM_ERROR;
    }

    cd_running = true;
    slurm_thread_create(&cd_thread_id, _cd_thread, NULL);
    info("cross_region: thread started, broker=%s:%u, wait_time=%us",
         slurm_conf.broker_host, slurm_conf.broker_port,
         slurm_conf.cross_region_wait_time);
    pthread_mutex_unlock(&cd_init_lock);
    return SLURM_SUCCESS;
}

extern void cross_region_fini(void)
{
    pthread_mutex_lock(&cd_init_lock);
    if (!cd_running) {
        pthread_mutex_unlock(&cd_init_lock);
        return;
    }
    cd_running = false;
    pthread_mutex_unlock(&cd_init_lock);

    if (cd_thread_id) {
        pthread_join(cd_thread_id, NULL);
        cd_thread_id = 0;
    }
    info("cross_region: thread stopped");
}

extern void cross_region_reconfig(void)
{
    /* reconfig 后由 controller.c 触发, 决定是否需要起停 */
    if (slurm_conf.cross_region_enabled && !cd_running)
        cross_region_init();
    else if (!slurm_conf.cross_region_enabled && cd_running)
        cross_region_fini();
    /* enabled 不变时, tick 自然采新值 */
}
```

> 24.05 的 `slurm_thread_create()` 是 Slurm 内置的 thread helper, 自动 set name/stack。

### 6.4 主循环

```c
static void *_cd_thread(void *arg)
{
    time_t last_scan = 0;
    info("cross_region: tick loop running "
         "(scan=%us, cancel_check=%us, max_handle=%u)",
         slurm_conf.cross_region_scan_interval ?: CD_DEFAULT_SCAN_INTERVAL_SEC,
         CD_TICK_INTERVAL_SEC,
         slurm_conf.cross_region_max_handle_per_round ?: CD_DEFAULT_MAX_HANDLE_PER_ROUND);

    while (cd_running) {
        time_t now = time(NULL);
        uint16_t scan_interval = slurm_conf.cross_region_scan_interval
                                     ?: CD_DEFAULT_SCAN_INTERVAL_SEC;

        /* 主扫描: 周期可调, 默认 5s — O(N) 全表识别跨域候选 */
        if (now - last_scan >= (time_t)scan_interval) {
            cd_tick_scan_pending();
            last_scan = now;
        }

        /* 次级 tick: cancel 传播 / broker 心跳, 1s 够用 (通常 O(已转发作业数)) */
        cd_tick_scan_cancelled();
        cd_tick_check_orphans();

        sleep(CD_TICK_INTERVAL_SEC);
    }
    return NULL;
}
```

> **设计要点**:
>
> - **主扫描 (`cd_tick_scan_pending`) 周期独立可调**: 百万级队列下可在 `slurm.conf` 里把 `CrossRegionScanInterval=15` (或更大) 以进一步摊薄持锁开销; 小集群 (< 1w 作业) 可保持默认 5s 甚至设 1s。
> - **次级 tick 保持 1s**: `cd_tick_scan_cancelled` 只遍历 `cd_forwarded==1 && IS_JOB_CANCELLED && !cd_cancel_propagated` 的作业, 这个集合通常远小于 job_list 总量, 1s 扫描没问题; 保持快响应让 `scancel` 的反向传播延迟 ≤ 1s。
> - `sleep(1)` 固定每秒唤醒一次, 通过内部时间戳判断是否到主扫描周期 — 这样无需 `pthread_cond_timedwait` 复杂同步, 也方便 `cd_running=false` 后最多 1s 退出。

### 6.5 全表扫描 — `cd_tick_scan_pending()` (★ 跨域线程的唯一入口, v2.0 简化)

> **v2.0 核心简化**: 候选过滤条件中, 把 v1.5 的"本地分区 `SendTo` 已解析"换成"本地分区 `AllowRemote==1`", 并新增"`!cd_route_exhausted`"短路条件。其余流程与 v1.5 一致。
>
> **v2.0 关键变更**:
> - 候选过滤新增 `!cd_route_exhausted` 短路 → 避免百万次重试 RPC
> - 候选过滤删除 `partition.cd_send_to / cd_remote_cluster / cd_remote_partition` 三联判断, 改为 `partition.cd_allow_remote==1`
> - Phase A `part=R` 锁仍需要 (读 `cd_allow_remote`)
> - **不再**做"路由二次校验" Step (broker 承担)

#### 6.5.1 设计契约

| 项 | 描述 |
|---|---|
| 调用方 | `_cd_thread()` 主循环 (§6.4), 周期 = `CrossRegionScanInterval` (默认 5s) |
| Phase A (收集) 锁 | `job=R, part=R` (需读 `part_ptr->cd_allow_remote`) |
| Phase A 持锁时间 | 100w 作业全表字段比较实测 < 100ms (见 §12.2) |
| Phase B (处理) 锁 | 每个 jobid 独立 `job=W, part=R`, 详见 §6.9 `cd_handle_pending_job_locked` |
| Phase B 间隔 | 串行, 每个约 1ms write_lock + 可选 broker RPC (30s 超时, 无锁) |
| 分批上限 | `CrossRegionMaxHandlePerRound` (默认 500) — 单轮 tick 最多处理 N 个作业, 剩余下一轮 tick 继续 |
| 幂等性 | 候选条件包含 `!cd_forwarded` 与 `!cd_route_exhausted`, 下轮扫描天然跳过已转发 / 已耗尽的作业 |

#### 6.5.2 实现

```c
/* src/slurmctld/cross_region.c */

static void cd_tick_scan_pending(void)
{
    if (!slurm_conf.cross_region_enabled)
        return;

    slurmctld_lock_t job_read_lock = {
        .conf  = NO_LOCK,
        .job   = READ_LOCK,
        .node  = NO_LOCK,
        .part  = READ_LOCK,   /* 读 part_ptr->cd_allow_remote (v2.0) */
        .fed   = NO_LOCK,
        .pmask = NO_LOCK,
        .user  = NO_LOCK,
    };

    list_t *to_handle = list_create(xfree_ptr);
    time_t now = time(NULL);
    uint32_t wait = slurm_conf.cross_region_wait_time;
    uint32_t max_handle = slurm_conf.cross_region_max_handle_per_round
                              ?: CD_DEFAULT_MAX_HANDLE_PER_ROUND;

    uint32_t scanned = 0, candidates = 0;
    DEF_TIMERS;

    START_TIMER;
    lock_slurmctld(job_read_lock);

    list_itr_t *itr = list_iterator_create(job_list);
    job_record_t *job_ptr;

    while ((job_ptr = list_next(itr))) {
        scanned++;

        /* === 快速过滤 (按字段选择性由强到弱排序, 提前短路) === */
        if (job_ptr->cd_cross_region != 1)   continue;   /* 绝大多数作业 */
        if (job_ptr->cd_forwarded)           continue;
        if (job_ptr->cd_route_exhausted)     continue;   /* ★ v2.0 新增: 一次性钉死 */
        if (!IS_JOB_PENDING(job_ptr))        continue;
        if (job_ptr->priority == 0)          continue;   /* admin hold / 已被 cd 线程 hold */

        /* === wait_time 达标 === */
        if (!job_ptr->details
            || (now - job_ptr->details->submit_time) < (time_t)wait)
            continue;

        /* === 本地分区允许跨域 (v2.0: 由 AllowRemote 开关决定) === */
        part_record_t *phys = job_ptr->part_ptr;
        if (!phys || !phys->cd_allow_remote)
            continue;
        /* v2.0 已删: SendTo / cd_remote_cluster / cd_remote_partition 三联可达性预检
         * (路由由 broker 决策, ctld 不再解析远端拓扑) */

        /* === 命中: 入 to_handle 小集合 === */
        uint32_t *jid = xmalloc(sizeof(uint32_t));
        *jid = job_ptr->job_id;
        list_append(to_handle, jid);
        candidates++;

        if (candidates >= max_handle)
            break;   /* 分批防拥塞, 剩余下轮 tick 继续 */
    }
    list_iterator_destroy(itr);
    unlock_slurmctld(job_read_lock);
    END_TIMER;

    if (candidates || (scanned > 0 && slurm_conf.debug_flags & DEBUG_FLAG_TRACE_JOBS))
        info("cross_region: scan scanned=%u candidates=%u held_time=%s",
             scanned, candidates, TIME_STR);

    /* === Phase B: 释放 R 锁后逐个 write_lock 处理 ===
     * 每个 cd_handle_pending_job_locked 自己申请 job=W / part=R 并在释放后
     * 调 broker; 串行处理避免对 ctld 其它路径的锁风暴。 */
    while (list_count(to_handle)) {
        uint32_t *jid = list_pop(to_handle);
        cd_handle_pending_job_locked(*jid);
        xfree(jid);

        /* 运行期可被 cross_region_fini 打断, 尽早退出 */
        if (!cd_running) break;
    }
    list_destroy(to_handle);
}
```

#### 6.5.3 设计要点

- **Phase A 严格只读**: 不分配任何 job_record_t 字段, 不调用任何可能加锁的 helper (包括 `find_part_record` — 用 `job_ptr->part_ptr` 直取); 持锁时间可预测。
- **Phase A 过滤顺序重要**: `cd_cross_region != 1` 放最前, 在通常只有 5~10% 作业带 `--allow-remote` 的场景下, 90% 以上的作业只做一次 `uint16_t` 比较就被过滤, 实际遍历开销远小于理论 O(N)。
- **Phase B 无锁退出路径**: `cd_running=false` 时主循环会尽快退出; Phase B 每处理一个 jobid 后检查, 保证 ctld shutdown 不被拖。
- **分批策略**: 百万级队列同时达阈值的极端场景下, 一轮 tick 最多处理 500 个作业; 后续每 5s tick 再处理 500 个 → 1w 作业约 100s 消化完, 10w 作业约 17min, 受 §6.8 `MaxJobs` 限流进一步平滑。
- **本地优先语义保留**: 扫描前作业 `priority > 0`, 主调度完全可能在任何时刻把它起来; 一旦起来 `IS_JOB_PENDING` 为假, 下轮扫描自然跳过。主调度与跨域线程无需任何共享状态。
- **幂等**: 单轮内若某作业被多次 append (不可能, 每作业最多 match 一次), 或跨轮重复命中 (Phase B 执行后 `cd_forwarded=1`, 下轮必然跳过), 都不会产生副作用。

### 6.7 ACL — 用户侧 (v2.0 大幅简化)

> **v2.0 变化**: 跨域放行**仅**走"账号维度", 不再有"分区应用软件白名单"二次校验 — 后者已下沉到 broker `routes.conf` 段 `AllowApps=`。
>
> **v2.0 用户 ACL 改造**: 从 v1.5 的"读 user/assoc.comment 子串包含 `allow_remote`"改为"读 user/assoc.remote_allowed 显式布尔列"。

#### 6.7.1 用户 / assoc — `cd_user_allows_remote()` (v2.0)

```c
extern bool cd_user_allows_remote(job_record_t *job_ptr)
{
    if (!job_ptr->assoc_ptr)
        return false;

    assoc_mgr_lock_t locks = {
        .assoc = READ_LOCK,
        .qos   = NO_LOCK,
        .res   = NO_LOCK,
        .user  = READ_LOCK,
        .wckey = NO_LOCK,
        .tres  = NO_LOCK,
        .file  = NO_LOCK,
    };

    bool ok = false;

    assoc_mgr_lock(&locks);

    slurmdb_assoc_rec_t *assoc = job_ptr->assoc_ptr;

    /* 优先看 assoc.remote_allowed (粒度更细, 同一用户在不同 account 下可不同) */
    if (assoc->remote_allowed) {
        ok = true;
        goto unlock;
    }

    /* fallback 看 user.remote_allowed */
    slurmdb_user_rec_t user_req = { .uid = assoc->uid, .name = assoc->user };
    slurmdb_user_rec_t *user_rec = NULL;
    if (assoc_mgr_fill_in_user(NULL, &user_req, ACCOUNTING_ENFORCE_ASSOCS,
                                &user_rec, true) == SLURM_SUCCESS
        && user_rec && user_rec->remote_allowed) {
        ok = true;
    }

unlock:
    assoc_mgr_unlock(&locks);

    if (!ok && (slurm_conf.debug_flags & DEBUG_FLAG_TRACE_JOBS))
        debug2("cross_region: user %s assoc/user.remote_allowed=0",
               job_ptr->user_name ?: "?");
    return ok;
}
```

> 24.05 `assoc_mgr_fill_in_user()` 接口要求传 `slurmdb_user_rec_t` request stub; 实际签名以 24.05.8 头文件为准 (`src/common/assoc_mgr.h`)。如果版本变动, 替换为 `assoc_mgr_get_user_rec_uid()` 私有 helper。
>
> **v1.5 → v2.0 迁移**: v1.5 的 `slurm_conf.cross_region_comment_tag` 全局键已删除, 不再读取 `user/assoc.comment` 子串。运维须在升级前通过 §15.3 提供的 SQL 脚本批量把 `comment LIKE '%allow_remote%'` 的记录置 `remote_allowed=1`。

#### 6.7.2 [已删除] 分区应用软件白名单

v1.5 `cd_partition_allows_app()` 函数完全删除 (~60 LoC 净减)。应用白名单语义由 broker `routes.conf` 段 `AllowApps=` 替代:

```ini
# broker /etc/slurmbroker/routes.conf  (示意, 详见 broker 详设)
[Route route_xahc_to_wz]
LocalCluster      = xahc
LocalPartition    = xahcnormal
RemoteCluster     = wz_cluster
RemotePartitions  = wznormal
AllowApps         = LAMMPS, lammps-2Aug2023-intelmpi2018   ; ★ 取代 ctld 端 partition.AllowApp
```

ctld 端 §6.9 `cd_handle_pending_job_locked()` **不再**调用 `cd_partition_allows_app()`; 即使作业不带 `--app` 也允许转发, 由 broker 在路由阶段拒收 (返回 `9011 TEST_ONLY_REJECTED`)。

### 6.8 [已删除] 跨域在途容量软限流

v1.5 `cd_phys_cross_region_has_capacity()` 函数完全删除 (~80 LoC 净减)。容量软限流职责下沉到 broker `cap_check.c`:

| 维度 | v1.5 ctld 实现 | v2.0 broker 实现 |
|---|---|---|
| 本地分区跨域在途上限 | 复用 `part_record.max_jobs` 字段 | broker `routes.conf` 段 `MaxInflight=` |
| 远端集群可承载并发 | 不感知 | broker `routes.conf` 段 `RemoteMaxInflight=` + 心跳探测远端负载 |
| 软限流行为 | ctld 跨域线程"软等待" (不动字段, 下轮重试) | broker `cap_check` 返回 `9020 CAP_FULL_SOFT_WAIT`, ctld 不置 `cd_route_exhausted`, 下轮 tick 重试 |
| 硬拒绝行为 | (v1.5 无此概念) | broker 返回 `9010 NO_ROUTE` (没有任何匹配路由), ctld 置 `cd_route_exhausted=1` |

ctld 端 §6.9 `cd_handle_pending_job_locked()` **不再**调用 `cd_phys_cross_region_has_capacity()`; 容量决策完全交给 broker。

### 6.9 跨域线程处理单 job — `cd_handle_pending_job_locked()` (★ hold + 决策 + 转发)

> **MVP-1.4 变更**:
> - 函数名由原 `cd_handle_marked_job_locked()` 改为 `cd_handle_pending_job_locked()`, 对应"扫描筛候选" 而非"消费打标" 的新定位
> - 入口处移除对 `cd_eligible_for_remote` 的检查 (该字段已删除)
> - **Step 顺序重排**: hold (`priority=0`) 从原 Step 1 挪到现 Step 4, 即"所有 check 通过后才 hold"; 原因见 Step 3 软等待对扫描条件的影响 (§12.2)
>
> 该函数在 write_lock 内按以下顺序完成:
> 1. **路由二次校验** (`SendTo` 仍存在且远端 cluster/partition 已解析)
> 2. **ACL**: `cd_user_allows_remote()` **且** `cd_partition_allows_app(phys, cd_app_name)`
> 3. **限流** (软等待: 直接 unlock 返回, 不动任何字段, 下轮扫描重试)
> 4. **决策通过 → hold + 切 `Forwarded_*` + 写远端字段 + `cd_forwarded = 1`** (本步骤唯一会动 priority 的点)
> 5. 释放 write_lock, **无锁**调用 broker RPC
> 6. RPC 失败 → `cd_revert_forward` 还原 priority

```c
static void cd_handle_pending_job_locked(uint32_t job_id)
{
    slurmctld_lock_t job_write_lock = {
        .conf = NO_LOCK, .job = WRITE_LOCK, .node = NO_LOCK,
        .part = READ_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
    };

    lock_slurmctld(job_write_lock);

    job_record_t *job_ptr = find_job_record(job_id);
    if (!job_ptr
        ||  job_ptr->cd_cross_region != 1     /* 用户中途 update 清意图 (不常见) */
        ||  job_ptr->cd_forwarded             /* 已转发过 */
        || !IS_JOB_PENDING(job_ptr)           /* 已被本地起来 / 已 cancel */
        ||  job_ptr->priority == 0) {         /* 已被其它路径 hold (admin hold 等) */
        unlock_slurmctld(job_write_lock);
        return;
    }

    /* === Step 1: 本地分区 AllowRemote 复检 (write_lock 内, 防 reconfig 漂移) ===
     *  v2.0: 不再做 SendTo / remote_cluster / remote_partition 三联可达性预检
     *        (路由由 broker 决策, ctld 不感知远端拓扑)
     */
    part_record_t *phys = find_part_record(job_ptr->partition);
    if (!phys || !phys->cd_allow_remote) {
        unlock_slurmctld(job_write_lock);
        cd_revert_for_partition_disallow(job_id);   /* 记 state_desc, 不改 priority */
        return;
    }

    /* === Step 2: 用户/assoc 维度 ACL ===
     *  v2.0: 不再做分区应用软件白名单 (AllowApp 已下沉到 broker routes.conf)
     */
    if (!cd_user_allows_remote(job_ptr)) {
        unlock_slurmctld(job_write_lock);
        cd_revert_for_acl_failed(job_id);           /* 记 state_desc, 不改 priority */
        return;
    }

    /* === v2.0 已删: Step 3 在途容量软限流
     *   (cd_phys_cross_region_has_capacity() 已删除, 容量下沉到 broker)
     *   软限流命中 (broker 返回 9020 CAP_FULL_SOFT_WAIT) 在 Step 7 处理。
     */

    /* === Step 3: ★ 全部 check 通过, 一次性完成 hold + 切 Forwarded_* ===
     *  v2.0 重要差异: 不再写 cd_remote_cluster_name / cd_remote_partition_name,
     *  这两个字段由 broker 首次状态包 (UPDATE_REMOTE_STATE) 回写 (见 §7.3)
     */
    info("cross_region: holding job %u for remote forward "
         "(was priority=%u)", job_id, job_ptr->priority);
    job_ptr->priority      = 0;
    job_ptr->state_reason  = WAIT_HELD_USER;
    xfree(job_ptr->state_desc);
    job_ptr->state_desc    = xstrdup("CrossRegionForwarding");
    job_ptr->cd_forwarded  = 1;
    job_ptr->last_sched_eval = time(NULL);

    /* v2.0 已删: 此处写 cd_remote_cluster_name / cd_remote_partition_name */

    /* === Step 4: 拷贝转发所需的最小快照, 释放写锁 === */
    forward_job_msg_t *req = _build_forward_msg(job_ptr);   /* v2.0: 不再传 phys */

    unlock_slurmctld(job_write_lock);
    schedule_job_save();

    /* === Step 5: 无锁调 broker (broker 内部做路由 + test-only + 实际投递) === */
    int rc = cd_send_forward_to_broker(req);
    free_forward_job_msg(req);

    /* === Step 6: 按 broker 返回码分类回滚 ===
     *  v2.0 新增: 区分"硬不可达"(置 cd_route_exhausted) vs "临时性故障"(下轮重试)
     */
    if (rc != SLURM_SUCCESS) {
        switch (rc) {
        case ESLURM_CR_NO_VIABLE_ROUTE:           /* 9010 broker 无任何匹配路由 */
        case ESLURM_CR_TEST_ONLY_REJECTED:        /* 9011 所有远端拒收 */
        case ESLURM_CR_ALL_ROUTES_EXHAUSTED:      /* 9013 所有候选远端探测均失败 */
            cd_revert_forward_hard(job_id, rc);   /* 置 cd_route_exhausted=1 */
            break;
        case ESLURM_CR_CAP_FULL_SOFT_WAIT:        /* 9020 软限流, 下轮重试 */
        case ESLURM_CR_TEST_ONLY_TIMEOUT:         /* 9012 broker 暂时性超时 */
        default:
            cd_revert_forward(job_id, rc);        /* 不置 cd_route_exhausted */
            break;
        }
    }
}
```

> **设计要点 (v2.0)**:
> - **持锁时间**: 整个 Step 1~3 都在一次 write_lock 内, 实测 < 1ms (无 IO, 仅字段写)。
> - **本地优先窗口**: Phase A read_lock 扫描 → Phase B write_lock 处理之间会释放锁; 这个窗口内主调度可能本地起作业, write_lock 拿到后 `IS_JOB_PENDING` 已假, 自然走 early-return 分支, 不会发生"已 RUNNING 又被 hold"。
> - **hold 推迟到所有 check 通过之后**: 保留 v1.5 的关键不变量 — Step 1/2 失败时 priority 保持 > 0, 作业留在扫描候选池; 仅 Step 3 才置 0。
> - **远端字段延迟回写**: ctld 转发请求时**不**填 `cd_remote_cluster_name` / `cd_remote_partition_name`; broker 决策完成、test-only 探测成功后, 通过 `REQUEST_BROKER_UPDATE_REMOTE_STATE` 首次状态包带过来, ctld 在 §7.3 写入。这一变化使 ctld 不再需要"远端拓扑" 知识。
> - **四类回滚策略 (v2.0 新增"硬回滚")**:
>   - **分区 AllowRemote=0 / 用户 ACL 失败 (Step 1 / 2)**: 立即 revert, 只写 `state_desc` 反馈用户, **不改 priority**, **不置 cd_route_exhausted**; 作业下轮扫描会重新命中, 直到用户修复 ACL / 运维改 `AllowRemote=yes`
>   - **broker 硬不可达 (Step 6, 9010 / 9011 / 9013)**: 已 hold, 调 `cd_revert_forward_hard()` 还原 priority + 清 `cd_forwarded` + 清 `cd_remote_*` + **置 `cd_route_exhausted=1`**; 该作业一次性钉死, 不再参与扫描
>   - **broker 临时性故障 (Step 6, 9012 / 9020)**: 已 hold, 调 `cd_revert_forward()` 还原 priority + 清 `cd_forwarded` + 清 `cd_remote_*`; 下轮扫描会重试
>   - **broker 其它 RPC 失败**: 同上, 走临时性故障路径; 由 broker 心跳监控 (`cd_tick_check_orphans`) 兜底

### 6.10 构造给 broker 的请求 — `_build_forward_msg()` (v2.0 大幅瘦身)

> **v2.0 关键变化**: payload 从 v1.5 的 9 个字段 + 完整 `job_desc_msg_t` 克隆, 缩水为 6 个字段 + 无 `job_desc`。broker 端 originator handler 拿到 jobid 后自己向源 ctld 反查实际 `job_desc` (或读 ctld 已 dump 的 script + sbatch options)。

```c
static forward_job_msg_t *_build_forward_msg(job_record_t *job_ptr)
{
    forward_job_msg_t *req = xmalloc(sizeof(*req));
    req->src_job_id        = job_ptr->job_id;
    req->src_uid           = job_ptr->user_id;
    req->src_gid           = job_ptr->group_id;
    req->src_user_name     = xstrdup(job_ptr->user_name);
    req->src_cluster_name  = xstrdup(slurm_conf.cluster_name);  /* ★ v2.0 新增 */
    req->src_partition     = xstrdup(job_ptr->partition);       /* ★ v2.0 改为本地 partition */
    req->src_work_dir      = xstrdup(job_ptr->details
                                        ? job_ptr->details->work_dir : "");
    req->script_path       = xstrdup_printf("%s/run.sh.cd_orig",
                                            req->src_work_dir);
    req->cd_app_name       = xstrdup(job_ptr->cd_app_name ?: "");

    /* 把作业脚本从 ctld 内存拷到 src_work_dir/run.sh.cd_orig */
    cd_dump_job_script_locked(job_ptr, req->script_path);

    /* v2.0 已删:
     *   - req->target_cluster   (broker 决策)
     *   - req->target_partition (broker 决策)
     *   - req->account          (远端 sacctmgr default account 接管)
     *   - req->job_desc 整体克隆 (~150 字段);
     *     改由 broker 自己向源 ctld GET_JOB_INFO 拿 / 或读 dump 的 script
     */

    return req;
}
```

> `cd_dump_job_script_locked()` 复用 `src/slurmctld/job_mgr.c::get_job_script()` 已有逻辑, 把内存脚本写盘。
>
> **v2.0 简化收益**: `_build_forward_msg()` 从 v1.5 ~80 LoC (含 `clone_job_desc_from_record` 调用与 account 字段清理) 缩为 ~25 LoC; `forward_job_msg_t` 协议 pack/unpack 字段从 9+1(整体 job_desc) 缩为 6 个原子字段。

### 6.11 调用 broker — `cd_send_forward_to_broker()`

```c
static int cd_send_forward_to_broker(forward_job_msg_t *req)
{
    slurm_msg_t req_msg, resp_msg;
    slurm_msg_t_init(&req_msg);
    slurm_msg_t_init(&resp_msg);

    req_msg.msg_type = REQUEST_FORWARD_JOB;
    req_msg.data     = req;

    slurm_addr_t addr;
    memset(&addr, 0, sizeof(addr));
    if (slurm_set_addr(&addr, slurm_conf.broker_port,
                        slurm_conf.broker_host) < 0) {
        error("cross_region: invalid BrokerHost=%s",
              slurm_conf.broker_host);
        return SLURM_ERROR;
    }

    int rc = slurm_send_recv_msg(&addr, &req_msg, &resp_msg,
                                  CD_BROKER_RPC_TIMEOUT_SEC);
    if (rc != SLURM_SUCCESS) {
        error("cross_region: send_recv to broker failed: %s",
              slurm_strerror(rc));
        return rc;
    }

    if (resp_msg.msg_type != RESPONSE_FORWARD_JOB) {
        error("cross_region: unexpected resp msg_type=%u", resp_msg.msg_type);
        slurm_free_msg_members(&resp_msg);
        return ESLURM_INVALID_RPC;
    }

    forward_job_resp_msg_t *resp = resp_msg.data;
    if (resp->error_code != 0) {
        error("cross_region: broker rejected job %u rc=%d (%s)",
              req->src_job_id, resp->error_code,
              slurm_strerror(resp->error_code));
        rc = resp->error_code;
    } else {
        /* 写 trace_id 到字段 (短锁) */
        slurmctld_lock_t job_write_lock = {
            .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
            .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
        };
        lock_slurmctld(job_write_lock);
        job_record_t *jp = find_job_record(req->src_job_id);
        if (jp) {
            xfree(jp->cd_remote_trace_id);
            jp->cd_remote_trace_id = xstrdup(resp->trace_id);
        }
        unlock_slurmctld(job_write_lock);

        info("cross_region: forwarded job %u trace_id=%s",
             req->src_job_id, resp->trace_id);
    }
    slurm_free_msg_members(&resp_msg);
    return rc;
}
```

### 6.12 失败回滚 — `cd_revert_forward()` / `cd_revert_forward_hard()` / 变体 (v2.0)

> **v2.0 回滚分类 (新增 hard 回滚)**:
>
> | 场景 | 触发点 | `priority` 是否已被改过? | 回滚动作 |
> |---|---|---|---|
> | ① broker 临时性失败 (9012 / 9020 / 网络) | §6.9 Step 6 broker 临时性返回码 | ✅ 已被 Step 3 置 0 | 还原 priority + 清 `cd_forwarded` + 清 `cd_remote_*` + 写 state_desc (软等待) |
> | ② broker **硬不可达** (9010 / 9011 / 9013) ★ v2.0 新增 | §6.9 Step 6 硬错误 | ✅ 已被 Step 3 置 0 | 还原 priority + 清 `cd_forwarded` + 清 `cd_remote_*` + **置 `cd_route_exhausted=1`** + 写 state_desc |
> | ③ 分区 AllowRemote=0 | §6.9 Step 1 失败 | ❌ 未被改过 | 仅写 state_desc, 下轮扫描会重试 |
> | ④ 用户 ACL 失败 | §6.9 Step 2 remote_allowed=0 | ❌ 未被改过 | 同 ③ |
>
> **v2.0 关键不变量**: `priority` 仅在 §6.9 Step 3 "所有 check 通过" 的路径上被置 0; Step 1/2 失败时不动 priority。因此只有 broker 返回失败 (`cd_revert_forward` 或 `cd_revert_forward_hard`) 需要还原 priority, 另外两个变体只负责写 state_desc 给用户反馈。

#### 6.12.1 通用 `_cd_revert_locked()` (内部 helper, broker 失败路径用)

```c
/* job_write_lock 已持; reason_fmt 写入 state_desc 用 */
static void _cd_revert_locked(job_record_t *job_ptr,
                               bool mark_exhausted,
                               const char *reason_fmt, ...)
{
    /* 1. 清转发标 */
    job_ptr->cd_forwarded = 0;

    /* 2. 还原 priority (跨域线程之前在 Step 3 置为 0) */
    if (job_ptr->details && job_ptr->details->priority_array)
        job_ptr->priority = job_ptr->details->priority_array[0];
    else
        job_ptr->priority = NO_VAL;
    if (job_ptr->priority == NO_VAL || job_ptr->priority == 0)
        job_ptr->priority = 1;        /* 兜底让主调度可见 */

    /* 3. 写状态原因, 让用户 squeue 可见 */
    job_ptr->state_reason = WAIT_NO_REASON;
    xfree(job_ptr->state_desc);
    va_list ap;
    va_start(ap, reason_fmt);
    job_ptr->state_desc = vxstrdup_printf(reason_fmt, ap);
    va_end(ap);

    /* 4. 清空路由 / 远端字段 (但保留 cd_cross_region == 1, 用户意图不变) */
    xfree(job_ptr->cd_remote_cluster_name);
    xfree(job_ptr->cd_remote_partition_name);
    xfree(job_ptr->cd_remote_trace_id);

    /* 5. ★ v2.0: 硬不可达时一次性钉死, 跳出扫描热路径 */
    if (mark_exhausted)
        job_ptr->cd_route_exhausted = 1;

    job_ptr->last_sched_eval = time(NULL);
}
```

#### 6.12.2 `cd_revert_forward()` — broker 临时性失败 (软重试)

```c
static void cd_revert_forward(uint32_t job_id, int rc)
{
    slurmctld_lock_t job_write_lock = {
        .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
    };
    lock_slurmctld(job_write_lock);
    job_record_t *job_ptr = find_job_record(job_id);
    if (job_ptr)
        _cd_revert_locked(job_ptr, false /* mark_exhausted */,
                          "CrossRegionRejected:%d", rc);
    unlock_slurmctld(job_write_lock);
    schedule_job_save();
}
```

#### 6.12.3 `cd_revert_forward_hard()` — broker 硬不可达 (★ v2.0 新增)

```c
/* ★ v2.0: broker 报告"配置/拓扑层面不可达", 一次性钉死该作业, 不再扫描重试 */
static void cd_revert_forward_hard(uint32_t job_id, int rc)
{
    slurmctld_lock_t job_write_lock = {
        .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
    };
    lock_slurmctld(job_write_lock);
    job_record_t *job_ptr = find_job_record(job_id);
    if (job_ptr) {
        _cd_revert_locked(job_ptr, true /* mark_exhausted */,
                          "CrossRegionExhausted:%d (broker rejected)", rc);
        info("cross_region: job %u marked route_exhausted=1 (rc=%d)",
             job_id, rc);
    }
    unlock_slurmctld(job_write_lock);
    schedule_job_save();

    /* DBD 同步: 把 cd_route_exhausted=1 落库, sacct -X 可见
     * (借用 _cd_dbd_modify_remote_fields 同样的 modify-only 通道, 详见 §9.5) */
    _cd_dbd_modify_route_exhausted(job_id);
}
```

#### 6.12.4 `cd_mark_route_exhausted()` — 公开 API (供 update_job 等其它路径调用)

```c
/* 头文件公开 API; 例如运维 SQL 故障转移触发的批量标记 */
extern void cd_mark_route_exhausted(job_record_t *job_ptr, const char *reason)
{
    if (!job_ptr || job_ptr->cd_route_exhausted)
        return;
    job_ptr->cd_route_exhausted = 1;
    xfree(job_ptr->state_desc);
    job_ptr->state_desc = xstrdup(reason ?: "CrossRegionExhausted");
    job_ptr->last_sched_eval = time(NULL);
}
```

#### 6.12.5 `cd_revert_for_partition_disallow()` — 分区 AllowRemote=0

```c
static void cd_revert_for_partition_disallow(uint32_t job_id)
{
    slurmctld_lock_t job_write_lock = {
        .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
    };
    lock_slurmctld(job_write_lock);
    job_record_t *job_ptr = find_job_record(job_id);
    if (job_ptr) {
        xfree(job_ptr->state_desc);
        job_ptr->state_desc   = xstrdup("CrossRegionPartitionDisallow");
        job_ptr->state_reason = WAIT_NO_REASON;
        job_ptr->last_sched_eval = time(NULL);
    }
    unlock_slurmctld(job_write_lock);
}
```

#### 6.12.6 `cd_revert_for_acl_failed()` — assoc/user.remote_allowed=0

```c
static void cd_revert_for_acl_failed(uint32_t job_id)
{
    slurmctld_lock_t job_write_lock = {
        .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
    };
    lock_slurmctld(job_write_lock);
    job_record_t *job_ptr = find_job_record(job_id);
    if (job_ptr) {
        xfree(job_ptr->state_desc);
        job_ptr->state_desc   = xstrdup("CrossRegionAclDenied");
        job_ptr->state_reason = WAIT_NO_REASON;
        job_ptr->last_sched_eval = time(NULL);
    }
    unlock_slurmctld(job_write_lock);
}
```

> **设计要点 (v2.0)**:
> - 分区 AllowRemote=0 / 用户 ACL 失败的回滚**不改 priority、不置 cd_route_exhausted**, 作业 `priority > 0` 状态不变, 下轮扫描仍会命中, 持续反馈 state_desc; 用户 / 运维修复 ACL / `AllowRemote=yes` 后, 下一轮扫描会自动走通 Step 1/2 继续转发。
> - broker 临时性失败 (`cd_revert_forward`) 必须经 `_cd_revert_locked` 完整回滚, 否则作业陷入"priority=0 + cd_forwarded=0" 的漏网态。
> - **broker 硬不可达 (`cd_revert_forward_hard`) 是 v2.0 新增的关键路径**: 必须同时 (1) 还原 priority + 清 `cd_forwarded`、(2) 置 `cd_route_exhausted=1` 让扫描跳过、(3) 同步落库, 三步缺一不可。
> - **运维清除"钉死" 的作业**: 通过 `scontrol update jobid=<JID> CdRouteExhausted=0` (§8 update 拦截改造), 该作业 `cd_route_exhausted=0`, 下轮扫描重新命中。

### 6.13 反向取消传播 — `cd_tick_scan_cancelled()` + `cd_send_cancel_to_broker()`

> **关键设计**: `cd_cancel_propagated` 与 `cd_terminal_received` 是**两个独立的幂等标志**, 各自管理一段不同的链路:
>
> | 标志 | 语义 | 置 1 时机 | 含义 |
> |---|---|---|---|
> | `cd_cancel_propagated` | "我已通知 broker 去 kill 远端" | `cd_send_cancel_to_broker` 拿到 RPC SUCCESS 时 | 阻止跨域线程重复发 `REQUEST_BROKER_CANCEL` |
> | `cd_terminal_received` | "broker 已回告我远端真实终态字段" | §7.4 `handle_broker_terminal_state` 完成所有写操作时 | 阻止重复消费 `REQUEST_BROKER_TERMINAL_STATE` (broker 重启 resume_inflight 等) |
>
> 用户 `scancel` → ctld 原生路径将 `job_state = JOB_CANCELLED` (作业立即终态), 但 `cd_terminal_received` 仍为 0 — 跨域线程发出 `REQUEST_BROKER_CANCEL` 后 broker 还会异步回送 `REQUEST_BROKER_TERMINAL_STATE` 来补全 `cd_remote_alloc_tres` / `cd_remote_exit_code` / `cd_remote_end_time` 三个字段, ctld §7.4 走"已终态补写"分支处理 (详见 §7.4 分支 B)。

```c
static void cd_tick_scan_cancelled(void)
{
    slurmctld_lock_t job_read_lock = {
        .job = READ_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
    };

    list_t *to_cancel = list_create(NULL);
    lock_slurmctld(job_read_lock);

    list_itr_t *itr = list_iterator_create(job_list);
    job_record_t *job_ptr;
    while ((job_ptr = list_next(itr))) {
        if (!IS_JOB_CANCELLED(job_ptr))            continue;
        if (!job_ptr->cd_forwarded)                 continue;
        if (job_ptr->cd_cancel_propagated)          continue;
        if (!job_ptr->cd_remote_trace_id)           continue;
        if (job_ptr->end_time
            && (time(NULL) - job_ptr->end_time) > 7 * 86400)
            continue;       /* 老作业不重试 */

        cd_pending_cancel_t *pc = xmalloc(sizeof(*pc));
        pc->job_id  = job_ptr->job_id;
        pc->trace_id = xstrdup(job_ptr->cd_remote_trace_id);
        list_append(to_cancel, pc);
    }
    list_iterator_destroy(itr);
    unlock_slurmctld(job_read_lock);

    while (list_count(to_cancel)) {
        cd_pending_cancel_t *pc = list_pop(to_cancel);
        cd_send_cancel_to_broker(pc);
        xfree(pc->trace_id);
        xfree(pc);
    }
    list_destroy(to_cancel);
}

static void cd_send_cancel_to_broker(cd_pending_cancel_t *pc)
{
    broker_cancel_msg_t req = {
        .src_job_id = pc->job_id,
        .trace_id   = xstrdup(pc->trace_id),
        .reason     = xstrdup("user_cancel"),
    };

    slurm_msg_t msg;
    slurm_msg_t_init(&msg);
    msg.msg_type = REQUEST_BROKER_CANCEL;
    msg.data     = &req;

    slurm_addr_t addr;
    slurm_set_addr(&addr, slurm_conf.broker_port, slurm_conf.broker_host);

    int rc = slurm_send_only_node_msg(&msg, &addr);   /* 不等响应 */
    xfree(req.trace_id); xfree(req.reason);

    if (rc == SLURM_SUCCESS) {
        slurmctld_lock_t job_write_lock = {
            .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
            .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
        };
        lock_slurmctld(job_write_lock);
        job_record_t *jp = find_job_record(pc->job_id);
        if (jp) jp->cd_cancel_propagated = 1;
        unlock_slurmctld(job_write_lock);
    } else {
        warning("cross_region: cancel propagation failed: %s",
                slurm_strerror(rc));
    }
}
```

### 6.14 broker 心跳监控 — `cd_tick_check_orphans()`

```c
static time_t cd_last_broker_ok = 0;
#define CD_BROKER_DEAD_THRESHOLD_SEC 600

static void cd_tick_check_orphans(void)
{
    /* 简单实现: 试 ping broker, 失败累计; 超阈值仅日志, 不降级业务 */
    static time_t last_ping = 0;
    time_t now = time(NULL);
    if (now - last_ping < 60) return;        /* 60s 一次 */
    last_ping = now;

    slurm_addr_t addr;
    slurm_set_addr(&addr, slurm_conf.broker_port, slurm_conf.broker_host);

    int fd = slurm_open_msg_conn(&addr);
    if (fd < 0) {
        if (now - cd_last_broker_ok > CD_BROKER_DEAD_THRESHOLD_SEC
            && cd_last_broker_ok)
            error("cross_region: broker unreachable for %lds",
                  (long)(now - cd_last_broker_ok));
        return;
    }
    close(fd);
    cd_last_broker_ok = now;
}
```

> MVP 阶段不做"broker 长时间不可达 → 主动 FAILED 跨域作业"的降级逻辑; 由用户/运维介入。后续可在此 hook 加策略。

---

## 7. broker → ctld RPC handler — `cross_region_rpc.c`

### 7.1 注册到 `slurmctld_req()` 大 switch (`src/slurmctld/proc_req.c`)

```c
extern void slurmctld_req(slurm_msg_t *msg)
{
    /* ... 24.05 已有 case ... */

    /* === 跨域 MVP 新增 === */
    case REQUEST_BROKER_UPDATE_REMOTE_STATE:
        rc = handle_broker_update_remote_state(msg);
        slurm_send_rc_msg(msg, rc);
        break;

    case REQUEST_BROKER_TERMINAL_STATE:
        rc = handle_broker_terminal_state(msg);
        slurm_send_rc_msg(msg, rc);
        break;

    /* ... default ... */
}
```

> 24.05 `slurmctld_req()` 已经接近 200 case, 加 2 个紧凑 case 不破坏可读性。

### 7.2 RPC 鉴权: 仅本机调用

```c
static int _cd_check_caller(slurm_msg_t *msg)
{
    /* broker 与 ctld 同机部署, msg->auth_uid 必须是 SlurmUser */
    uid_t uid = g_slurm_auth_get_uid(msg->auth_cred);
    if (uid != slurm_conf.slurm_user_id) {
        error("cross_region RPC from non-SlurmUser uid=%u, rejected", uid);
        return ESLURM_USER_ID_MISSING;
    }
    return SLURM_SUCCESS;
}
```

### 7.3 `handle_broker_update_remote_state()` (v2.0: 首次状态包写 cluster/partition)

> **v2.0 关键变化**: 在 v1.5, ctld 在 §6.9 Step 4 已写好 `cd_remote_cluster_name` / `cd_remote_partition_name` (从 `partition.SendTo` 解析), broker `UPDATE_REMOTE_STATE` 只是 idempotent overwrite。在 v2.0, ctld **不知道**远端拓扑, **首次** `UPDATE_REMOTE_STATE` 必须由 broker 告诉 ctld "这个作业实际投递到了哪个 cluster@partition", ctld 才填字段。后续状态更新只更新 `remote_state` / `alloc_tres` / `remote_job_id` 等。
>
> 实现上, broker 端 RPC payload `remote_cluster_name` / `remote_partition_name` 始终带过来 (即使非首次), ctld 端直接覆盖即可, 与 v1.5 实现接近, 仅去掉了 ctld 端"已写过就跳过"的硬性约束。

```c
extern int handle_broker_update_remote_state(slurm_msg_t *msg)
{
    int rc = _cd_check_caller(msg);
    if (rc != SLURM_SUCCESS) return rc;

    broker_update_remote_state_msg_t *req = msg->data;
    if (!req || !req->src_job_id) return ESLURM_INVALID_JOB_ID;

    slurmctld_lock_t job_write_lock = {
        .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = NO_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = NO_LOCK,
    };
    lock_slurmctld(job_write_lock);

    job_record_t *job_ptr = find_job_record(req->src_job_id);
    if (!job_ptr) {
        unlock_slurmctld(job_write_lock);
        return ESLURM_INVALID_JOB_ID;
    }
    if (!job_ptr->cd_forwarded) {
        unlock_slurmctld(job_write_lock);
        warning("cross_region: update_remote_state for non-forwarded job %u",
                req->src_job_id);
        return ESLURM_INVALID_JOB_STATE;
    }

    /* ★ v2.0: 首次包 broker 决策的远端目的地写入 ctld; 后续包重复覆盖也无害
     *         (broker 不会中途切目标, 字符串一致). */
    bool first_state_pack = (job_ptr->cd_remote_cluster_name == NULL);

    if (req->remote_cluster_name) {
        xfree(job_ptr->cd_remote_cluster_name);
        job_ptr->cd_remote_cluster_name = xstrdup(req->remote_cluster_name);
    }
    if (req->remote_partition_name) {
        xfree(job_ptr->cd_remote_partition_name);
        job_ptr->cd_remote_partition_name = xstrdup(req->remote_partition_name);
    }
    if (req->trace_id) {
        xfree(job_ptr->cd_remote_trace_id);
        job_ptr->cd_remote_trace_id = xstrdup(req->trace_id);
    }
    if (req->remote_job_id)
        job_ptr->cd_remote_job_id = req->remote_job_id;
    job_ptr->cd_remote_state = req->remote_state;
    if (req->remote_alloc_tres) {
        xfree(job_ptr->cd_remote_alloc_tres);
        job_ptr->cd_remote_alloc_tres = xstrdup(req->remote_alloc_tres);
    }
    if (req->remote_start_time)
        job_ptr->cd_remote_start_time = req->remote_start_time;

    /* ★ v2.0: 首次包时, 写 state_desc 让用户 squeue/scontrol 立刻看到远端位置 */
    if (first_state_pack) {
        xfree(job_ptr->state_desc);
        job_ptr->state_desc = xstrdup_printf("Forwarded_%s_%s",
            job_ptr->cd_remote_cluster_name ?: "?",
            job_ptr->cd_remote_partition_name ?: "?");
        info("cross_region: job %u routed to %s@%s by broker",
             req->src_job_id,
             job_ptr->cd_remote_partition_name ?: "?",
             job_ptr->cd_remote_cluster_name ?: "?");
    }

    unlock_slurmctld(job_write_lock);
    schedule_job_save();
    return SLURM_SUCCESS;
}
```

> **协议字段约束**: broker 端 (详见 broker 详设 §2.7 originator handler) 在第一次发起 `UPDATE_REMOTE_STATE` 时**必须**带上 `remote_cluster_name` 和 `remote_partition_name`; 后续状态更新可重复带 (幂等), 但**不允许**中途改变这两个字段的值 — 一旦绑定就不变, 直到作业终态。

### 7.4 `handle_broker_terminal_state()` (核心)

> **设计要点 (MVP-1.5 修订)**: 早期版本在入口处用 `IS_JOB_FINISHED(job_ptr)` 短路返回 OK 实现幂等, 这是**过度幂等**——它会把"二次 transition 防护"和"远端字段首次补写"两件不同的事一起挡掉, 导致 `scancel` 抢跑场景下 `cd_remote_exit_code` / `cd_remote_alloc_tres` / `cd_remote_end_time` 永远写不进去, 财务/审计字段全空。MVP-1.5 拆成 A/B 两个分支并改用 `cd_terminal_received` 做真幂等 (详见 §7.4.1)。

```c
extern int handle_broker_terminal_state(slurm_msg_t *msg)
{
    int rc = _cd_check_caller(msg);
    if (rc != SLURM_SUCCESS) return rc;

    broker_terminal_state_msg_t *req = msg->data;
    if (!req) return ESLURM_INVALID_JOB_ID;

    slurmctld_lock_t job_write_lock = {
        .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = READ_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = READ_LOCK,
    };
    lock_slurmctld(job_write_lock);

    job_record_t *job_ptr = find_job_record(req->src_job_id);
    if (!job_ptr) {
        unlock_slurmctld(job_write_lock);
        return ESLURM_INVALID_JOB_ID;
    }
    if (!job_ptr->cd_forwarded) {
        unlock_slurmctld(job_write_lock);
        warning("cross_region: terminal_state for non-forwarded job %u",
                req->src_job_id);
        return ESLURM_INVALID_JOB_STATE;
    }

    /* === 真幂等: 这条 TERMINAL_STATE 之前已成功消费过 ===
     * 区别于早期 IS_JOB_FINISHED 短路 — 后者会把 scancel-抢跑场景下
     * "首次补写远端字段" 的请求误判为重复, 导致 sacct 远端字段全空。
     */
    if (job_ptr->cd_terminal_received) {
        unlock_slurmctld(job_write_lock);
        debug("cross_region: terminal_state for job %u already consumed, ignore",
              req->src_job_id);
        return SLURM_SUCCESS;
    }

    /* 决定走分支 A 还是分支 B —
     * already_finished 通常意味着用户在 broker 终态前就 scancel 抢跑了。 */
    bool already_finished = IS_JOB_FINISHED(job_ptr);

    /* === Step 1: 写远端字段 (A/B 分支共有, cd_remote_* 是字段不是状态) === */
    job_ptr->cd_remote_start_time = req->remote_start_time;
    job_ptr->cd_remote_end_time   = req->remote_end_time;
    job_ptr->cd_remote_exit_code  = req->remote_exit_code;
    job_ptr->cd_remote_state      = req->remote_state;
    if (req->remote_alloc_tres) {
        xfree(job_ptr->cd_remote_alloc_tres);
        job_ptr->cd_remote_alloc_tres = xstrdup(req->remote_alloc_tres);
    }

    if (!already_finished) {
        /* === 分支 A: 本地仍 PENDING(Held), 走完整终态写入 ===
         * 这是普通跨域作业的正常路径 (远端正常 RUNNING -> COMPLETED) */

        if (job_ptr->priority != 0) {
            /* 不应到达: cd_forwarded==1 时跨域线程已 hold; 防御性日志 */
            warning("cross_region: terminal_state but priority=%u (expected 0) job %u",
                    job_ptr->priority, req->src_job_id);
        }

        /* (a) 同步本地账单时间, 让 sacct 时间一致 */
        job_ptr->start_time = req->remote_start_time;
        job_ptr->end_time   = req->remote_end_time;
        job_ptr->exit_code  = req->remote_exit_code;

        /* (b) tres_alloc_str 让 Slurm 原生统计列也有值 */
        xfree(job_ptr->tres_alloc_str);
        job_ptr->tres_alloc_str = xstrdup(req->remote_alloc_tres ?: "");

        /* (c) 决定终态 */
        uint32_t job_state;
        switch (req->remote_state) {
        case JOB_COMPLETE:   job_state = JOB_COMPLETE;  break;
        case JOB_FAILED:     job_state = JOB_FAILED;    break;
        case JOB_CANCELLED:  job_state = JOB_CANCELLED; break;
        case JOB_TIMEOUT:    job_state = JOB_TIMEOUT;   break;
        case JOB_NODE_FAIL:  job_state = JOB_NODE_FAIL; break;
        default:             job_state = JOB_FAILED;    break;
        }

        /* (d) 走 Slurm 内部完成路径写 sacct (jobcomp 插件 + SlurmDBD)
         *    24.05 推荐:
         *       job_ptr->job_state = state | JOB_COMPLETING;
         *       jobcomp_g_record_job_end(job_ptr);
         *       job_completion_logger(job_ptr, false);
         *       job_ptr->job_state = state; */
        job_ptr->job_state = job_state | JOB_COMPLETING;
        jobcomp_g_record_job_end(job_ptr);
        job_completion_logger(job_ptr, false);
        job_ptr->job_state = job_state;

        /* (e) 终态保持 priority=0, 防止任何残留路径误启 */
        job_ptr->priority = 0;

        info("cross_region: job %u terminal=%s remote_jobid=%u exit=%u (branch A)",
             req->src_job_id, job_state_string(job_state),
             req->remote_job_id, req->remote_exit_code);

    } else {
        /* === 分支 B: 本地已终态 (scancel 抢跑), 仅补写远端字段 ===
         *
         * 本地 scancel 路径已先把 job_state 改 JOB_CANCELLED 并触发了一次
         * jobcomp_g_record_job_end + job_completion_logger (写 sacct, 触发
         * jobcomp 插件, 推 dbd_job_complete_msg_t). 当时 cd_remote_*
         * 字段大半还是空的 (尤其 alloc_tres / exit_code / end_time).
         *
         * 现在远端真实终态到了, 必须把那三个字段补到 sacct 的 cd_remote_*
         * 列, 但不能再调 jobcomp_g_record_job_end / job_completion_logger:
         *   - jobcomp filetxt 会重复记录一行 (脏)
         *   - SlurmDBD 的 dbd_job_complete_msg 第二次到达, 不同插件版本
         *     行为不一致, 部分会覆盖 end_time / state, 部分会触发依赖
         *     作业二次解锁 (危险)
         *
         * 改走 dbd_job_modify 通道 (详见 §7.4.1 + §9.3): 仅 SQL UPDATE
         * 八个 cd_remote_* 列, 不动 state, 不触发 jobcomp 插件. */
        _cd_dbd_modify_remote_fields(job_ptr);

        info("cross_region: job %u local-terminal arrived first; remote fields "
             "backfilled (remote_state=%s remote_exit=%u branch B)",
             req->src_job_id,
             job_state_string(req->remote_state),
             req->remote_exit_code);
    }

    /* === Step 2: 真幂等标记置位, 防 broker 重发 === */
    job_ptr->cd_terminal_received = 1;

    unlock_slurmctld(job_write_lock);
    schedule_job_save();
    return SLURM_SUCCESS;
}
```

> **关键细节 (分支 A)**: 24.05 的 `jobcomp_g_record_job_end()` + `job_completion_logger()` 会触发:
> - jobcomp 插件 (filetxt / elasticsearch / kafka 等) 记录终态
> - SlurmDBD `slurm_send_step_msg()` 推 `dbd_job_complete_msg_t`
> - `as_mysql_job_complete()` 写 `job_table` (含 §9 的 `Remote_*` 列)
> - 触发 dependent job 解锁 (本场景不适用, 跨域作业一般独立)
>
> **关键细节 (分支 B)**: `_cd_dbd_modify_remote_fields()` (§7.4.1) 走 24.05 已有的 modify-only 通道, 仅 SQL UPDATE 八列, 不会触发上述任何副作用。

### 7.4.1 `_cd_dbd_modify_remote_fields()` — 分支 B 字段补写专用通道

```c
/* src/slurmctld/cross_region_rpc.c
 *
 * 仅供 §7.4 分支 B 调用: 当本地作业已终态 (例如用户 scancel 抢跑)
 * 而 broker 后到的 TERMINAL_STATE 仍需补全 cd_remote_* 字段时使用.
 *
 * 设计约束:
 *   - 不能调 jobcomp_g_record_job_end (会触发 jobcomp 插件二次写入)
 *   - 不能调 job_completion_logger    (会触发依赖作业二次解锁等副作用)
 *   - 仅走 acct_storage_g_job_modify  (24.05 的 modify-only SQL 通道,
 *     与 scontrol update 共用底座, 由 SlurmDBD 翻译为单条 SQL UPDATE)
 *
 * 调用上下文: job=W lock 内, 同 §7.4. 不申请新锁.
 */
static void _cd_dbd_modify_remote_fields(job_record_t *job_ptr)
{
    dbd_job_modify_msg_t modify = { 0 };
    modify.job_id                 = job_ptr->job_id;
    modify.cd_remote_cluster      = job_ptr->cd_remote_cluster_name;
    modify.cd_remote_partition    = job_ptr->cd_remote_partition_name;
    modify.cd_remote_jobid        = job_ptr->cd_remote_job_id;
    modify.cd_remote_state        = job_ptr->cd_remote_state;
    modify.cd_remote_alloc_tres   = job_ptr->cd_remote_alloc_tres;
    modify.cd_remote_exit_code    = job_ptr->cd_remote_exit_code;
    modify.cd_remote_start        = job_ptr->cd_remote_start_time;
    modify.cd_remote_end          = job_ptr->cd_remote_end_time;

    /* 24.05 已有此入口: src/slurmctld/proc_req.c 中 scontrol update 也用它.
     * 我们仅填 cd_remote_* 8 字段, 原生 base 字段全 0 → DBD 端只 UPDATE
     * 已填的列 (24.05 as_mysql_modify_job 行为一致). */
    int rc = acct_storage_g_job_modify(acct_db_conn, &modify);
    if (rc != SLURM_SUCCESS) {
        warning("cross_region: dbd modify remote fields failed for job %u: %s",
                job_ptr->job_id, slurm_strerror(rc));
        /* 不视为致命: ctld 内存里的 cd_remote_* 字段已经写对了,
         * 用户 scontrol show job 仍能看到; 只是 sacct 的列暂未刷新.
         * 下次 SlurmDBD reconnect 后由其重试机制接力. */
    }
}
```

> 实现选择 `acct_storage_g_job_modify` 而非自己拼 SQL, 是因为它能复用 24.05 已有的 SlurmDBD 重试 / 队列 / mTLS 链路, 不需要再走一遍鉴权。`dbd_job_modify_msg_t` 结构体扩展与 SQL UPDATE 模板见 §9.2.1。

### 7.5 RPC payload 结构 (`slurm_protocol_defs.h` 末尾, v2.0)

```c
typedef struct {
    uint32_t  src_job_id;
    char     *trace_id;
    char     *remote_cluster_name;     /* v2.0: 由 broker 决策后告知 ctld 写入 */
    char     *remote_partition_name;   /* v2.0: 同上 */
    uint32_t  remote_job_id;
    uint32_t  remote_state;
    char     *remote_alloc_tres;
    time_t    remote_start_time;
} broker_update_remote_state_msg_t;

typedef struct {
    uint32_t  src_job_id;
    char     *trace_id;
    char     *remote_cluster_name;
    char     *remote_partition_name;
    uint32_t  remote_job_id;
    uint32_t  remote_state;
    char     *remote_alloc_tres;
    time_t    remote_start_time;
    time_t    remote_end_time;
    uint32_t  remote_exit_code;
} broker_terminal_state_msg_t;

typedef struct {
    uint32_t  src_job_id;
    char     *trace_id;
    char     *reason;
} broker_cancel_msg_t;

/* ★ v2.0 forward_job_msg_t 大幅瘦身 (从 v1.5 9 字段 + nested job_desc → 6 个原子字段) */
typedef struct {
    uint32_t  src_job_id;
    uint32_t  src_uid;
    uint32_t  src_gid;
    char     *src_user_name;
    char     *src_cluster_name;        /* ★ v2.0 新增: 本地 cluster 名, 用于 broker 查 routes.conf */
    char     *src_partition;           /* ★ v2.0 改名: 从 target_partition 改为本地 partition, 同上 */
    char     *src_work_dir;
    char     *script_path;
    char     *cd_app_name;             /* ★ v2.0 改名 (原 app_name), 用于 broker AllowApps 匹配 */
    /* v2.0 已删:
     *   - char *target_cluster        (broker 决策)
     *   - char *target_partition      (broker 决策)
     *   - char *account               (远端 sacctmgr default account 接管)
     *   - job_desc_msg_t *job_desc    (broker 自己 GET_JOB_INFO 反查)
     */
} forward_job_msg_t;

typedef struct {
    uint32_t  error_code;              /* 0 / 9010 / 9011 / 9013 / 9020 等 */
    char     *error_message;
    char     *trace_id;
    /* ★ v2.0: 这里**不**带 selected_cluster / selected_partition;
     *         broker 决策结果通过后续 UPDATE_REMOTE_STATE 异步告知 ctld */
} forward_job_resp_msg_t;
```

`pack_*_msg()` / `unpack_*_msg()` 在 `slurm_protocol_pack.c` 增加, 字段顺序与上述 `typedef` 一致, 全部走 `if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION)` 包装。

> **v1.5→v2.0 protocol 不兼容点**: `forward_job_msg_t` payload 字段名 / 顺序 / 总数都发生变化, **ctld 与 broker 必须同步升级**。混部场景:
>
> - v1.5 ctld + v2.0 broker: broker 收到 `target_cluster=wz_cluster` 时忽略它走自己的 routes.conf 决策 (反向兼容); 但 broker 必须能 unpack v1.5 payload, 不推荐。
> - v2.0 ctld + v1.5 broker: v1.5 broker 期望 `target_cluster` 字段, 但 v2.0 不发, broker 会读到空串 → 路由失败; **必须同步升级**。

### 7.6 RPC 类型枚举 (`slurm_protocol_defs.h`, v2.0)

```c
/* 选 8000-8099 段, 与 slurmrestd / slurmdbd 现有 ID 区隔 */
#define REQUEST_FORWARD_JOB                  8001
#define RESPONSE_FORWARD_JOB                 8002
#define REQUEST_BROKER_UPDATE_REMOTE_STATE   8003
#define REQUEST_BROKER_TERMINAL_STATE        8004
#define REQUEST_BROKER_CANCEL                8005
/* 8006~8017 broker→broker (broker MVP §2.6) */
/* ★ v2.0 新增 (ctld 端不直接对接, 但保留命名一致性): */
#define REQUEST_BROKER_TEST_ONLY             8018   /* broker→broker test-only 探测 */
#define RESPONSE_BROKER_TEST_ONLY            8019
```

> ctld 进程**不**注册 `REQUEST_BROKER_TEST_ONLY` 的 handler — 这是 broker↔broker 之间的协议, 走 broker 本地的 receiver handler。ctld 仅需在 `slurm_protocol_defs.h` 把 ID 占住, 防 future 复用冲突。

> 与 broker MVP §6.1 的 ID 表保持一致, 由 broker 端工程师与 ctld 端工程师共同评审 PR (M04-T1)。

---

## 8. update_job / scancel 拦截

### 8.1 `update_job_msg()` 入口 hook (`src/slurmctld/job_mgr.c`)

24.05.8 中 `update_job_msg()` (实际函数: `update_job()` 或 `update_job_str()`, 以 24.05.8 为准) 是 `scontrol update jobid=...` 的入口。

```c
extern int update_job(slurm_msg_t *msg, uid_t uid, bool send_msg)
{
    job_desc_msg_t *job_specs = (job_desc_msg_t *) msg->data;

    slurmctld_lock_t job_write_lock = {
        .job = WRITE_LOCK, .conf = NO_LOCK, .node = NO_LOCK,
        .part = READ_LOCK, .fed = NO_LOCK, .pmask = NO_LOCK, .user = READ_LOCK,
    };
    lock_slurmctld(job_write_lock);

    job_record_t *job_ptr = find_job_record(job_specs->job_id);

    /* === 跨域 MVP 拦截 === */
    if (job_ptr && job_ptr->cd_forwarded) {
        int rc = cross_region_check_update_block(job_ptr, job_specs, uid);
        if (rc != SLURM_SUCCESS) {
            unlock_slurmctld(job_write_lock);
            if (send_msg) slurm_send_rc_msg(msg, rc);
            return rc;
        }
    }
    /* === 跨域 MVP 拦截结束 === */

    /* ... 24.05 已有 _update_job_internal() 调用 ... */
}
```

### 8.2 拦截规则实现 (`cross_region.c`)

```c
extern int cross_region_check_update_block(job_record_t *job_ptr,
                                            job_desc_msg_t *job_specs,
                                            uid_t caller_uid)
{
    /* root / SlurmUser 不拦 (运维强制操作) */
    if (caller_uid == 0 || caller_uid == slurm_conf.slurm_user_id)
        return SLURM_SUCCESS;

    /* 拦 partition 改动 */
    if (job_specs->partition
        && xstrcmp(job_ptr->partition, job_specs->partition)) {
        info("cross_region: reject partition change of forwarded job %u",
             job_ptr->job_id);
        return ESLURM_NOT_SUPPORTED;
    }

    /* 拦 release (priority 改非 0) */
    if (job_specs->priority != NO_VAL && job_specs->priority != 0) {
        info("cross_region: reject priority change of forwarded job %u",
             job_ptr->job_id);
        return ESLURM_NOT_SUPPORTED;
    }

    /* 拦 time_limit */
    if (job_specs->time_limit != NO_VAL) {
        info("cross_region: reject time_limit change of forwarded job %u",
             job_ptr->job_id);
        return ESLURM_NOT_SUPPORTED;
    }

    /* 24.05 还允许 update --comment / --gres 等; 跨域作业全部允许 */
    return SLURM_SUCCESS;
}

/* ★ v2.0 新增: 处理 scontrol update jobid=<JID> CdRouteExhausted=0 重置 */
extern int cross_region_check_update_reset(job_record_t *job_ptr,
                                            job_desc_msg_t *job_specs,
                                            uid_t caller_uid)
{
    /* 仅 root / SlurmUser / operator 可清除 cd_route_exhausted */
    if (caller_uid != 0 && caller_uid != slurm_conf.slurm_user_id
        && !validate_operator(caller_uid))
        return ESLURM_ACCESS_DENIED;

    if (job_specs->cd_route_exhausted_reset == 0xFF /* sentinel: 未设 */)
        return SLURM_SUCCESS;

    if (job_specs->cd_route_exhausted_reset == 0 && job_ptr->cd_route_exhausted) {
        job_ptr->cd_route_exhausted = 0;
        xfree(job_ptr->state_desc);
        job_ptr->state_desc = xstrdup("CrossRegionRetryByAdmin");
        job_ptr->last_sched_eval = time(NULL);
        info("cross_region: job %u route_exhausted cleared by uid=%u",
             job_ptr->job_id, caller_uid);
    } else if (job_specs->cd_route_exhausted_reset == 1 && !job_ptr->cd_route_exhausted) {
        /* 极少用: 管理员手动钉死 */
        cd_mark_route_exhausted(job_ptr, "CrossRegionExhaustedByAdmin");
    }

    return SLURM_SUCCESS;
}
```

> v2.0 在 `job_desc_msg_t` 末尾新增 `uint8_t cd_route_exhausted_reset` (sentinel 默认 `0xFF` = 未设置); scontrol 端 `parse_command_line()` 解析 `CdRouteExhausted=0|1` 时填入。`update_job()` 调用顺序:
>
> ```c
> /* === 跨域 MVP 拦截 === */
> if (job_ptr && job_ptr->cd_forwarded) {
>     int rc = cross_region_check_update_block(...);
>     if (rc) goto out;
> }
> if (job_ptr) {
>     int rc = cross_region_check_update_reset(job_ptr, job_specs, uid);   /* ★ v2.0 新增 */
>     if (rc) goto out;
> }
> ```

### 8.3 scancel 不需要拦截

`scancel` → `REQUEST_KILL_JOB` → `slurm_kill_job_msg()` → `_signal_job()` 改 `job_state = JOB_CANCELLED`, 这条路径**不动**。

跨域线程下一轮 `cd_tick_scan_cancelled()` 自动检测 `IS_JOB_CANCELLED(job_ptr) && job_ptr->cd_forwarded && !job_ptr->cd_cancel_propagated` 触发反向取消。

> **设计要点**: 不在 scancel 路径同步发送 broker RPC, 避免在 client 等待 RPC 时阻塞 scancel; 全部走异步 tick。

#### 8.3.1 远端真实终态字段的异步补写 (MVP-1.5)

`scancel` 触发本地 `JOB_CANCELLED` 时, ctld 原生路径会**立刻**调一次 `jobcomp_g_record_job_end + job_completion_logger` 写 sacct, 此时 `cd_remote_*` 字段大半还是空的:

| 字段 | scancel 当下值 | 真实值 |
|---|---|---|
| `cd_remote_cluster_name` / `cd_remote_partition_name` | 已填 (跨域线程 Step 4 写入) | 同 |
| `cd_remote_job_id` | 已填 (broker UPDATE_REMOTE_STATE 已推) | 同 |
| `cd_remote_state` | `JOB_RUNNING` (cancel 前最后一次 update) | `JOB_CANCELLED` (远端实际) |
| `cd_remote_alloc_tres` | 可能空, 也可能是 RUNNING 中的快照 | 远端真实 alloc |
| `cd_remote_exit_code` | `0` (从未写过) | 远端真实退出码 (一般是 SIGTERM=15) |
| `cd_remote_end_time` | `0` | 远端真实结束时间 |

异步补写时序:

```text
T0  scancel → ctld 原生 JOB_CANCELLED, sacct 第一次写 (cd_remote_* 大半空)
    cd_terminal_received = 0
T1  跨域线程 cd_tick_scan_cancelled (1s)
    → REQUEST_BROKER_CANCEL → broker;  cd_cancel_propagated = 1
T2  broker → 远端 slurm_kill_job; 拉到远端真实终态 (CANCELLED, exit=15)
T3  broker → ctld REQUEST_BROKER_TERMINAL_STATE (携带远端真实字段)
T4  §7.4 进入分支 B (already_finished==true):
       - 写 cd_remote_* 字段
       - _cd_dbd_modify_remote_fields → SQL UPDATE 8 列 (不触发 jobcomp)
       - cd_terminal_received = 1
T5  最终一致: sacct 里 State=CANCELLED 且 Remote_ExitCode=15 / Remote_AllocTRES /
              Remote_End 全部齐全
```

> **关键**: 用户 `sacct -j 12345 --format=...,Remote_*` 在 T0 立刻执行可能看到远端字段空, 几秒后 (T4 完成后) 再查就齐了。这是异步补写的固有特征, 文档与运维手册需要同时说明。

#### 8.3.2 边界场景: scancel 与远端自然终态竞争

```text
T0  远端作业自然 COMPLETED (broker 还没来得及推 update)
T1  user scancel
T2  ctld JOB_CANCELLED + 写 sacct (本地视角认为是用户取消)
T3  broker sync_ticker 拉到远端 COMPLETED (此时 broker 看到的是真实终态)
T4  broker → ctld REQUEST_BROKER_TERMINAL_STATE { remote_state=JOB_COMPLETE,
                                                  remote_exit_code=0 }
T5  §7.4 分支 B 补写: cd_remote_state=JOB_COMPLETE, cd_remote_exit_code=0
```

**最终 sacct 显示**: 本地 `State=CANCELLED`, 远端 `Remote_State=COMPLETED Remote_ExitCode=0`。**这是设计的预期**——本地视角与远端视角对同一作业可以有不同结论, 这正是把 `Remote_*` 拆成独立列的核心价值; 财务对账只看 `Remote_AllocTRES` × `(Remote_End - Remote_Start)` 即可。

### 8.4 用户体验

```bash
$ scontrol update jobid=12345 partition=otherpart
slurm_update error: Operation not supported

$ scontrol update jobid=12345 priority=100
slurm_update error: Operation not supported

$ scontrol update jobid=12345 timelimit=02:00:00
slurm_update error: Operation not supported

$ scancel 12345     # OK, 30s 内远端被 kill
```

---

## 9. SlurmDBD 与 sacct

### 9.1 `job_table` schema 升级 (`as_mysql_job.c`)

24.05 `as_mysql_job.c::_check_table_columns()` 在每次 ctld 注册集群时校验/补建 schema。在 `job_table_fields[]` 末尾追加列:

```c
/* src/plugins/accounting_storage/mysql/as_mysql_job.c
 * job_table_fields[] 表末尾追加 */
{ "cd_remote_cluster",   "tinytext"          },
{ "cd_remote_partition", "tinytext"          },
{ "cd_remote_jobid",     "int unsigned default 0" },
{ "cd_remote_state",     "smallint unsigned default 0" },
{ "cd_remote_alloc_tres","text"              },
{ "cd_remote_exit_code", "int unsigned default 0" },
{ "cd_remote_start",     "bigint unsigned default 0" },
{ "cd_remote_end",       "bigint unsigned default 0" },
```

> 24.05 schema 升级机制 (`as_mysql_check_tables()`) 会自动 `ALTER TABLE ... ADD COLUMN` 没有的列, 不需要手写 ALTER。

### 9.2 INSERT / UPDATE 写入 (`as_mysql_job.c::as_mysql_job_complete()`)

24.05 通过 `dbd_job_complete_msg_t` 接收 ctld 推过来的终态。在 `slurmdbd_defs.c::pack_dbd_job_complete_msg()` / `unpack_dbd_job_complete_msg()` 增加 8 字段, 对应:

```c
typedef struct {
    /* ... 24.05 已有 ... */
    char     *cd_remote_cluster;
    char     *cd_remote_partition;
    uint32_t  cd_remote_jobid;
    uint16_t  cd_remote_state;
    char     *cd_remote_alloc_tres;
    uint32_t  cd_remote_exit_code;
    time_t    cd_remote_start;
    time_t    cd_remote_end;
} dbd_job_complete_msg_t;
```

`as_mysql_job_complete()` SQL UPDATE 末尾追加 SET clause:

```c
xstrfmtcat(query,
    ", cd_remote_cluster='%s'"
    ", cd_remote_partition='%s'"
    ", cd_remote_jobid=%u"
    ", cd_remote_state=%u"
    ", cd_remote_alloc_tres='%s'"
    ", cd_remote_exit_code=%u"
    ", cd_remote_start=%lu"
    ", cd_remote_end=%lu",
    msg->cd_remote_cluster ?: "",
    msg->cd_remote_partition ?: "",
    msg->cd_remote_jobid,
    msg->cd_remote_state,
    msg->cd_remote_alloc_tres ?: "",
    msg->cd_remote_exit_code,
    (unsigned long)msg->cd_remote_start,
    (unsigned long)msg->cd_remote_end);
```

#### 9.2.1 字段补写通道 (`as_mysql_modify_job()`, MVP-1.5 新增)

§7.4.1 `_cd_dbd_modify_remote_fields()` 走的不是 `as_mysql_job_complete` 路径, 而是 24.05 已有的 modify-only 通道 `as_mysql_modify_job()`。它的载荷类型是 `dbd_job_modify_msg_t`, 与 `dbd_job_complete_msg_t` **不同**, 必须独立扩展:

```c
/* src/common/slurmdbd_defs.h
 * dbd_job_modify_msg_t 末尾追加 (与 dbd_job_complete_msg_t 字段顺序一致) */
typedef struct {
    /* ... 24.05 已有 (主要服务于 scontrol update) ... */

    /* === 跨域 MVP-1.5 新增: 仅供 ctld §7.4 分支 B 调用 === */
    char     *cd_remote_cluster;
    char     *cd_remote_partition;
    uint32_t  cd_remote_jobid;
    uint16_t  cd_remote_state;
    char     *cd_remote_alloc_tres;
    uint32_t  cd_remote_exit_code;
    time_t    cd_remote_start;
    time_t    cd_remote_end;
} dbd_job_modify_msg_t;
```

`pack_dbd_job_modify_msg()` / `unpack_dbd_job_modify_msg()` 同步在 24.05 协议版本分支末尾追加 8 个字段的 pack/unpack, 与 §4.5 一致。

`as_mysql_modify_job()` 既要兼容 scontrol update 的原有字段, 又要按需写入 cd_remote_*。**关键差异**: scontrol update 提交时 cd_remote_* 字段为 0/NULL → 按"NULL 跳过, 非 NULL/非零才进 SET"原则:

```c
/* src/plugins/accounting_storage/mysql/as_mysql_job.c
 * as_mysql_modify_job() 末尾追加 (按字段判 NULL/0 决定是否进 SET 子句) */
if (msg->cd_remote_cluster && *msg->cd_remote_cluster) {
    xstrfmtcat(query, ", cd_remote_cluster='%s'", msg->cd_remote_cluster);
}
if (msg->cd_remote_partition && *msg->cd_remote_partition) {
    xstrfmtcat(query, ", cd_remote_partition='%s'", msg->cd_remote_partition);
}
if (msg->cd_remote_jobid) {
    xstrfmtcat(query, ", cd_remote_jobid=%u", msg->cd_remote_jobid);
}
if (msg->cd_remote_state) {     /* 0 == 未知, 不覆盖 */
    xstrfmtcat(query, ", cd_remote_state=%u", msg->cd_remote_state);
}
if (msg->cd_remote_alloc_tres && *msg->cd_remote_alloc_tres) {
    xstrfmtcat(query, ", cd_remote_alloc_tres='%s'", msg->cd_remote_alloc_tres);
}
if (msg->cd_remote_exit_code) {
    xstrfmtcat(query, ", cd_remote_exit_code=%u", msg->cd_remote_exit_code);
}
if (msg->cd_remote_start) {
    xstrfmtcat(query, ", cd_remote_start=%lu",
               (unsigned long)msg->cd_remote_start);
}
if (msg->cd_remote_end) {
    xstrfmtcat(query, ", cd_remote_end=%lu",
               (unsigned long)msg->cd_remote_end);
}
```

> **副作用约束**: `as_mysql_modify_job()` 单线 SQL UPDATE, 不触发 jobcomp 插件, 不发 dbd_job_complete_msg, 不解锁依赖作业 — 这正是 §7.4 分支 B 选它的核心原因。
>
> **行为对照**:
>
> | 通道 | 触发 | jobcomp 插件 | 依赖作业解锁 | sacct 列写入 | 状态机跃迁 |
> |---|---|---|---|---|---|
> | `as_mysql_job_complete()` (§9.2) | 作业终态 | ✅ 写一行 | ✅ 触发 | ✅ 全列 | ✅ → COMPLETING |
> | `as_mysql_modify_job()` (§9.2.1) | scontrol update / §7.4 分支 B | ❌ | ❌ | ✅ 仅指定列 | ❌ |

### 9.3 `slurmdb_job_rec_t` 透传给 sacct (`slurmdb.h`)

```c
typedef struct {
    /* ... 24.05 已有 ... */
    char     *cd_remote_cluster;
    char     *cd_remote_partition;
    uint32_t  cd_remote_jobid;
    uint16_t  cd_remote_state;
    char     *cd_remote_alloc_tres;
    uint32_t  cd_remote_exit_code;
    time_t    cd_remote_start;
    time_t    cd_remote_end;
} slurmdb_job_rec_t;
```

`as_mysql_jobacct_process.c::_setup_job_cond_limits()` 与 `_get_job_cond()` 已经按字段名映射, 加上面 8 列后会自动出现在 sacct 查询结果中。

### 9.4 sacct 输出列

`src/sacct/print.c` 的 `fields[]` 表末尾加 8 项:

```c
{"Remote_Cluster",   "remote_cluster",   _print_remote_cluster   },
{"Remote_Partition", "remote_partition", _print_remote_partition },
{"Remote_JobId",     "remote_jobid",     _print_remote_jobid     },
{"Remote_State",     "remote_state",     _print_remote_state     },
{"Remote_AllocTRES", "remote_alloc_tres",_print_remote_alloc_tres},
{"Remote_ExitCode",  "remote_exit_code", _print_remote_exit_code },
{"Remote_StartTime", "remote_start",     _print_remote_start     },
{"Remote_EndTime",   "remote_end",       _print_remote_end       },
```

`src/sacct/options.c::DEFAULT_FIELDS` 不改 (保持兼容); 用户用 `-o JobID,State,Remote_Cluster,...` 显式选。

#### 9.4.1 `_print_remote_state()` 实现

```c
static void _print_remote_state(slurmdb_job_rec_t *job, int field_size, bool right)
{
    char *str = job->cd_remote_jobid
        ? job_state_string(job->cd_remote_state)
        : "-";
    char tmp[32];
    snprintf(tmp, sizeof(tmp), "%s", str);
    field_print_str(tmp, field_size, right);
}
```

其余对称实现, 略。

### 9.5 用户使用示例

```bash
$ sacct -j 12345 -o JobID,State,ExitCode,Remote_Cluster,Remote_JobId,Remote_AllocTRES,Remote_ExitCode
       JobID      State ExitCode Remote_Cluster Remote_JobId Remote_AllocTRES Remote_ExitCode
------------ ---------- -------- -------------- ------------ ---------------- ---------------
       12345  COMPLETED      0:0     wz_cluster         8888 cpu=32,mem=128G              0:0
```

### 9.6 user / assoc 表 `remote_allowed` 列 (★ v2.0 新增)

> v2.0 引入显式布尔列替代 v1.5 在 `user.comment` / `assoc.comment` 中嵌入 `allow_remote` 子串的模糊匹配。

#### 9.6.1 `user_table` schema 升级 (`as_mysql_user.c`)

```c
/* src/plugins/accounting_storage/mysql/as_mysql_user.c
 * user_table_fields[] 末尾追加 */
{ "remote_allowed",  "tinyint unsigned default 0" },
```

#### 9.6.2 `<cluster>_assoc_table` schema 升级 (`as_mysql_assoc.c`)

> Slurm 24.05 每个 cluster 有独立 `<cluster>_assoc_table` (例: `xahc_assoc_table`); schema 由 `as_mysql_assoc.c::_check_table_columns()` 统一管理。

```c
/* src/plugins/accounting_storage/mysql/as_mysql_assoc.c
 * assoc_table_fields[] 末尾追加 */
{ "remote_allowed",  "tinyint unsigned default 0" },
```

#### 9.6.3 sacctmgr 入站 RPC 字段 (`slurmdbd_defs.c`)

`dbd_user_msg_t` / `dbd_assoc_msg_t` 已经在 slurmdb.h 扩展过 `remote_allowed` (§5.8.3), `pack/unpack` 在 24.05 分支末尾追加:

```c
/* src/common/slurmdbd_defs.c::pack_dbd_user_msg() */
if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
    /* ... 已有 ... */
    pack16(user->remote_allowed, buffer);   /* ★ v2.0 新增 */
}

/* src/common/slurmdbd_defs.c::pack_dbd_assoc_msg() */
if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
    /* ... 已有 ... */
    pack16(assoc->remote_allowed, buffer);  /* ★ v2.0 新增 */
}
```

#### 9.6.4 assoc_mgr 缓存同步

`src/common/assoc_mgr.c::_post_user_list()` / `_post_assoc_list()` 在拷贝 user/assoc 结构时把 `remote_allowed` 也拷过来; 这是 ctld 跨域线程 `cd_user_allows_remote()` (§6.7.1) 真正读到的字段。

```c
/* src/common/assoc_mgr.c::_post_user_list() 内部 */
new_user->remote_allowed = user->remote_allowed;

/* src/common/assoc_mgr.c::_post_assoc_list() 内部 */
new_assoc->remote_allowed = assoc->remote_allowed;
```

#### 9.6.5 v1.5 → v2.0 数据迁移脚本

```sql
-- 升级前对所有 cluster 库执行 (SOP 详见 §15.3):
UPDATE user_table u
   SET u.remote_allowed = 1
 WHERE u.comment LIKE '%allow_remote%';

UPDATE xahc_assoc_table a   -- 每个 cluster 库都要跑一次
   SET a.remote_allowed = 1
 WHERE a.comment LIKE '%allow_remote%';
```

> 迁移完成后 v1.5 的 `comment` 子串保持原样 (运维可自行清理), v2.0 ctld 不再读取该字段。

---

## 10. 协议序列化总表

### 10.1 新增/修改的 pack/unpack 入口 (v2.0)

| 函数 | 文件 | 改动 | 字段数 (v2.0) | 字段数 (v1.5) |
|---|---|---|---|---|
| `_pack_job_desc_msg` / `_unpack_job_desc_msg` | `slurm_protocol_pack.c` | 末尾追加 24.05 分支 (`cross_region` + `app_name` + `cd_route_exhausted_reset`) | +3 | +2 |
| `_pack_job_info_members` / `_unpack_job_info_members` | `slurm_protocol_pack.c` | 末尾追加 24.05 分支 (含 `cd_route_exhausted`) | +12 | +11 |
| `slurm_pack_partition_info_members` / `slurm_unpack_partition_info_members` | `slurm_protocol_pack.c` | 末尾追加 24.05 分支, **v2.0 仅 1 个布尔** (`cd_allow_remote`) | **+1** | +4 |
| `_pack_forward_job_msg` / `_unpack_forward_job_msg` | `slurm_protocol_pack.c` | NEW (REQUEST_FORWARD_JOB), **v2.0 瘦身** | **6** | 11 |
| `_pack_forward_job_resp_msg` / `_unpack_forward_job_resp_msg` | `slurm_protocol_pack.c` | NEW | 3 | 3 |
| `_pack_broker_update_remote_state_msg` / 反 | `slurm_protocol_pack.c` | NEW | 8 | 8 |
| `_pack_broker_terminal_state_msg` / 反 | `slurm_protocol_pack.c` | NEW | 10 | 10 |
| `_pack_broker_cancel_msg` / 反 | `slurm_protocol_pack.c` | NEW | 3 | 3 |
| `pack_dbd_job_complete_msg` / 反 | `src/common/slurmdbd_defs.c` | 末尾追加 24.05 分支 (cd_remote_* 8 字段) | +8 | +8 |
| `pack_dbd_user_msg` / 反 | `src/common/slurmdbd_defs.c` | 末尾追加 24.05 分支 (`remote_allowed`) ★ v2.0 新增 | **+1** | 0 |
| `pack_dbd_assoc_msg` / 反 | `src/common/slurmdbd_defs.c` | 末尾追加 24.05 分支 (`remote_allowed`) ★ v2.0 新增 | **+1** | 0 |

> **v1.5 → v2.0 协议总字段变化**: `partition_info_members` -3 (删 SendTo/AllowApp/cd_remote_cluster/cd_remote_partition, 加 cd_allow_remote 共 -3); `forward_job_msg` -5 (删 target_cluster/target_partition/account/job_desc + 加 src_cluster_name/src_partition 改名 + 字段重组); `job_info_members` +1 (`cd_route_exhausted`); `dbd_user_msg` / `dbd_assoc_msg` 各 +1。**净影响**: 总协议字段从 v1.5 的 ~46 个字段降到 v2.0 的 ~40 个字段。

### 10.2 `pack_msg()` / `unpack_msg()` 大 switch 增 case

`src/common/slurm_protocol_pack.c::pack_msg()` 大 switch:

```c
case REQUEST_FORWARD_JOB:
    _pack_forward_job_msg((forward_job_msg_t *)msg->data,
                           buffer, msg->protocol_version);
    break;
case RESPONSE_FORWARD_JOB:
    _pack_forward_job_resp_msg(...);
    break;
case REQUEST_BROKER_UPDATE_REMOTE_STATE:
    _pack_broker_update_remote_state_msg(...);
    break;
case REQUEST_BROKER_TERMINAL_STATE:
    _pack_broker_terminal_state_msg(...);
    break;
case REQUEST_BROKER_CANCEL:
    _pack_broker_cancel_msg(...);
    break;
```

`unpack_msg()` 镜像。

### 10.3 协议版本规则

| 客户端版本 | 服务端版本 | 跨域字段是否传送 | 服务端如何处理 |
|---|---|---|---|
| 24.05.8 (改造) | 24.05.8 (改造) | ✅ | 完整 |
| 23.11 (官方) | 24.05.8 (改造) | ❌ (不感知) | 收到 job_desc 时 cross_region=0, 视为本地作业 |
| 24.05.8 (改造) | 23.11 (官方) | ✅ pack | 服务端 unpack 跳过未知字段 (Slurm 协议天然兼容) |

> 关键: Slurm 24.05 的 `safe_unpack*` 系列遇到旧 protocol_version 自动跳过新字段, 不会 fatal。

---

## 11. 错误码体系 / 状态机回滚

### 11.1 新增 `ESLURM_*` 错误码 (`slurm_errno.h` / `slurm_strerror.c`, v2.0)

```c
/* 跨域专用错误码, 占用 5000~5099 段 */
#define ESLURM_CR_BROKER_UNREACHABLE           5001
#define ESLURM_CR_BROKER_REJECTED              5002
#define ESLURM_CR_USER_NO_MAPPING              5003
#define ESLURM_CR_USER_NOT_AUTHORIZED          5004
/* v2.0 已删: ESLURM_CR_CROSS_REGION_CAP_FULL  5005  (容量下沉到 broker) */
#define ESLURM_CR_NOT_SUPPORTED_UPDATE         5006
#define ESLURM_CR_LOOKUP_SOFTWARE_FAILED       5007
#define ESLURM_CR_REMOTE_SUBMIT_FAILED         5008
#define ESLURM_CR_INVALID_REMOTE_TRACE         5009
/* ★ v2.0 新增 (与 broker 9010~9020 段一一对应, 经 ctld 转换或透传) */
#define ESLURM_CR_NO_VIABLE_ROUTE              5010   /* 对应 broker 9010 NO_ROUTE */
#define ESLURM_CR_TEST_ONLY_REJECTED           5011   /* 对应 broker 9011 */
#define ESLURM_CR_TEST_ONLY_TIMEOUT            5012   /* 对应 broker 9012 */
#define ESLURM_CR_ALL_ROUTES_EXHAUSTED         5013   /* 对应 broker 9013 */
#define ESLURM_CR_CAP_FULL_SOFT_WAIT           5020   /* 对应 broker 9020 (临时, 不置 cd_route_exhausted) */
#define ESLURM_CR_PARTITION_DISALLOW_REMOTE    5030   /* ctld 本地: partition.AllowRemote=no */
```

`slurm_strerror()` 加映射文本, 提供中英文 (按 Slurm 现有风格)。

> v2.0 错误码语义按"是否触发 `cd_route_exhausted=1`" 分两类:
>
> | 类别 | 错误码 | 是否硬钉死作业 |
> |---|---|---|
> | **硬不可达 (置 `cd_route_exhausted=1`)** | 5010 / 5011 / 5013 | ✅ |
> | **临时性故障 (下轮扫描可重试)** | 5001 / 5002 / 5003 / 5004 / 5007 / 5008 / 5012 / 5020 / 5030 | ❌ |

### 11.1.1 错误码处理流程示意

```text
broker REQUEST_FORWARD_JOB 返回 rc:
  rc==0 ─────────────► 转发成功, 等待 broker UPDATE_REMOTE_STATE 异步回写
  rc==5010/5011/5013 ► cd_revert_forward_hard() → cd_route_exhausted=1 (永久跳出扫描)
  rc==5020/5012 ─────► cd_revert_forward() (下轮扫描重试)
  rc==其它 ──────────► cd_revert_forward() (默认临时性故障, 兜底)
```

### 11.2 跨域字段状态机 (v2.0)

```text
                 ┌──────────────────────────────────────┐
                 │ cd_cross_region == 0                 │  普通本地作业
                 └──────────────────────────────────────┘
                            │
                            │ user sbatch --allow-remote
                            ▼
   ┌──────────────────────────────────────────────────────────┐
   │ cd_cross_region == 1                                     │  PENDING
   │ cd_forwarded == 0                                        │  (主调度按正常逻辑
   │ cd_route_exhausted == 0                                  │   尝试本地调度)
   │ priority > 0                                             │
   └────────────────────────────┬─────────────────────────────┘
                                │
                                │ 等待 now - submit_time >= wait_time
                                │ (期间作业可能被主调度本地起起来 → RUNNING
                                │  跨域线程下轮扫描自然跳过 → 本地优先)
                                │
                                │ wait_time 达标且仍 PENDING:
                                │ 跨域线程 cd_tick_scan_pending() 在 Phase A 匹配命中
                                │ (条件: !cd_route_exhausted && partition.cd_allow_remote==1)
                                │ → Phase B cd_handle_pending_job_locked() write_lock 内:
                                │     Step 1: partition.AllowRemote 复检 (失败 → revert_for_partition_disallow, 不动 priority)
                                │     Step 2: 用户 ACL remote_allowed (失败 → revert_for_acl_failed, 不动 priority)
                                │     Step 3: ★ hold (priority=0) + 切 Forwarded_* (但不写 cd_remote_*)
                                │     Step 4/5: 释放锁; 无锁调 broker
                                │     Step 6:  按 broker rc 分流:
                                │              - rc=0      → 转发成功, 等首次 UPDATE_REMOTE_STATE 写 cd_remote_*
                                │              - rc=5020/5012 → cd_revert_forward (临时, 下轮重试)
                                │              - rc=5010/5011/5013 → cd_revert_forward_hard ★ 置 cd_route_exhausted=1
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │ cd_forwarded == 1                                        │  PENDING(Held)
   │ priority = 0                                             │
   │ state_desc = "CrossRegionForwarding"                     │
   │ cd_remote_cluster_name / cd_remote_partition_name 仍 NULL│  ★ v2.0 关键差异
   └────────────────────────────┬─────────────────────────────┘
                                │ broker UPDATE_REMOTE_STATE 首次包到达:
                                │ 写 cd_remote_cluster_name / cd_remote_partition_name
                                │ + cd_remote_trace_id; state_desc 改为 "Forwarded_<P>_<C>"
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │ cd_forwarded == 1                                        │  PENDING(Held)
   │ cd_remote_cluster_name / cd_remote_partition_name 已填   │
   │ cd_remote_trace_id 已填                                  │
   └────────────────────────────┬─────────────────────────────┘
                                │ broker UPDATE_REMOTE_STATE (周期推, 第 2+ 次)
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │ cd_remote_state = PENDING | RUNNING                      │
   │ cd_remote_job_id / cd_remote_alloc_tres                  │
   └────────────────────────────┬─────────────────────────────┘
                                │ broker TERMINAL_STATE
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │ JobState = COMPLETED / FAILED / CANCELLED                │  本地终态 (sacct)
   │ cd_remote_state / cd_remote_end_time / cd_remote_exit    │
   └──────────────────────────────────────────────────────────┘

   任意阶段 user scancel:
   ┌──────────────────────────────────────────────────────────┐
   │ JobState = CANCELLED  (本地立即终态, sacct 第一次写)     │
   │ cd_remote_alloc_tres / exit_code / end_time 仍可能为空   │
   │ cd_cancel_propagated = 1 (after broker_cancel RPC OK)    │
   │ cd_terminal_received = 0  (远端字段尚未补全)             │
   └────────────────────────────┬─────────────────────────────┘
                                │ broker kill 远端 + 拉到真实终态
                                │ broker → ctld TERMINAL_STATE
                                ▼
   ┌──────────────────────────────────────────────────────────┐
   │ §7.4 分支 B: already_finished==true → 仅补字段            │
   │   cd_remote_state / alloc_tres / exit_code / end_time 写齐│
   │   _cd_dbd_modify_remote_fields → SQL UPDATE 仅 cd_remote_*│
   │   cd_terminal_received = 1                                │
   │ ★ 不再调 jobcomp_g_record_job_end / job_completion_logger │
   │   (本地 scancel 路径已经触发过, 不可重入)                  │
   └──────────────────────────────────────────────────────────┘

   失败回滚 (v2.0):
     - partition 禁跨域 / 用户 ACL 失败 (Step 1/2): state_desc="CrossRegion{PartitionDisallow|AclDenied}",
       priority 保持 > 0, 下轮扫描自动再试
     - broker 临时性失败 (Step 6, rc=5012/5020): _cd_revert_locked(false) — cd_forwarded=0,
                             priority 还原 (来自 priority_array[0]), state_desc="CrossRegionRejected:<rc>",
                             下轮扫描命中后重新走 Step 1~6
     - broker 硬不可达 (Step 6, rc=5010/5011/5013): _cd_revert_locked(true) — 同上 +
                             ★ cd_route_exhausted=1, 一次性钉死,
                             不再参与扫描; 运维 scontrol update CdRouteExhausted=0 可解钉
```

> **关键观察 (MVP-1.4)**:
>
> 1. **状态机被简化了**: 相比 MVP-1.2 去掉了"已打标待 hold"中间态 (对应原 `cd_eligible_for_remote==1 && priority>0`)。作业只有两个稳定状态: "`cd_cross_region==1` 候选中" 与 "`cd_forwarded==1` 已转发", 跃迁由跨域线程的扫描 + Phase B 一次性完成。
> 2. **本地优先靠什么保证**: 扫描前作业 `priority > 0`, 主调度完全可见并可能随时将其本地启动 — 跨域线程持续扫描到 wait_time 过期为止。与"主调度先打标"方案相比, 响应时间差异仅为 `CrossRegionScanInterval` (默认 5s), 而 wait_time 通常 300s, 影响可忽略。
> 3. **限流软等待的正确性**: Step 3 限流命中时不动任何字段, 作业 `priority>0 && !cd_forwarded && cd_cross_region==1` 保持不变, 下一轮 `cd_tick_scan_pending` 会再次命中 (前提是路由 + ACL 之前也通过)。
>
> **关键观察 (MVP-1.5 新增)**:
>
> 4. **取消传播与终态字段补写解耦**: `cd_cancel_propagated` 与 `cd_terminal_received` 是两个独立标志, 各自管理一段链路:
>    - `cd_cancel_propagated` = "我已通知 broker 去 kill" (出方向: ctld → broker)
>    - `cd_terminal_received` = "broker 已回告我远端真实终态字段" (入方向: broker → ctld)
>
>    用户 `scancel` 让本地 `JOB_CANCELLED` 立刻成立但 `cd_terminal_received` 仍为 0, 跨域线程后续异步发 `REQUEST_BROKER_CANCEL`, broker 异步回送 `REQUEST_BROKER_TERMINAL_STATE`, §7.4 走分支 B 补写远端字段。两条链路相互独立, 任何一边失败都不影响另一边的最终一致性。
> 5. **§7.4 真幂等的判据是 `cd_terminal_received` 而非 `IS_JOB_FINISHED`**: 早期版本用 `IS_JOB_FINISHED` 短路是过度幂等, 会让 scancel 抢跑场景下远端字段永远写不进去 — MVP-1.5 修复了这个漏洞 (详见 §7.4 / §8.3.1)。

### 11.3 失败回滚契约表 (v2.0)

| 触发点 | 回滚动作 | 新 state_desc | 是否置 `cd_route_exhausted=1` | 用户可见 |
|---|---|---|---|---|
| **partition.AllowRemote=no** (Step 1, v2.0 新) | `cd_revert_for_partition_disallow()` | "CrossRegionPartitionDisallow" | ❌ | 立刻 |
| **用户 ACL 失败** (Step 2, v2.0 新语义) | `cd_revert_for_acl_failed()` | "CrossRegionAclDenied" | ❌ | 立刻 |
| broker 不可达 (RPC 超时) | `cd_revert_forward(rc=ESLURM_CR_BROKER_UNREACHABLE)` | "CrossRegionRejected:5001" | ❌ | 立刻 |
| broker 拒绝 (用户映射缺失) | 同上, rc=5003 | "CrossRegionRejected:5003" | ❌ | 立刻 |
| broker 拒绝 (lookup_software 失败) | 同上, rc=5007 | "CrossRegionRejected:5007" | ❌ | 立刻 |
| **broker 报告 NO_VIABLE_ROUTE** (★ v2.0 新) | `cd_revert_forward_hard(rc=5010)` | "CrossRegionExhausted:5010" | ✅ | 立刻 |
| **broker 报告 TEST_ONLY_REJECTED** (★ v2.0 新) | `cd_revert_forward_hard(rc=5011)` | "CrossRegionExhausted:5011" | ✅ | 立刻 |
| **broker 报告 ALL_ROUTES_EXHAUSTED** (★ v2.0 新) | `cd_revert_forward_hard(rc=5013)` | "CrossRegionExhausted:5013" | ✅ | 立刻 |
| broker 报告 CAP_FULL_SOFT_WAIT (临时容量) | `cd_revert_forward(rc=5020)` | "CrossRegionRejected:5020" | ❌ (下轮重试) | 立刻 |
| broker 报告 TEST_ONLY_TIMEOUT (探测临时超时) | `cd_revert_forward(rc=5012)` | "CrossRegionRejected:5012" | ❌ | 立刻 |
| 远端 sbatch 拒绝 (后阶段) | broker 自己 FAILED 后通过 TERMINAL_STATE 通知 ctld | JobState=FAILED, remote_exit_code | n/a (已转发, 流程外) | 终态 |
| broker 心跳长期不可达 | 仅日志告警, 不主动 FAILED 已转发作业 | - | ❌ | 运维 |
| **本地 scancel 抢跑 + broker 终态后到 (MVP-1.5 起)** | §7.4 分支 B `_cd_dbd_modify_remote_fields` 仅 SQL UPDATE cd_remote_* 八列, 不重入 jobcomp | JobState 保持 CANCELLED; sacct Remote_* 列由空补齐为远端真实值 | n/a | 异步补齐 (T4 ≈ scancel 后 30s 内) |

---

## 12. 锁与并发分析

### 12.1 锁矩阵 (v2.0)

| 操作 | conf | job | node | part | fed | pmask | user | 备注 |
|---|---|---|---|---|---|---|---|---|
| **`cd_tick_scan_pending`** Phase A 全表扫描收集 | NO | R | NO | R | NO | NO | NO | ★ 核心: O(N) 遍历 `job_list` 按 `cd_cross_region==1 && !cd_forwarded && !cd_route_exhausted && priority>0 && wait_time 达标 && partition.cd_allow_remote==1` 过滤; 单次持锁百万作业 < 100ms, 典型 10-50ms; 分批上限 `CrossRegionMaxHandlePerRound` 控制单轮 Phase B 压力 |
| **`cd_handle_pending_job_locked`** Phase B 单 job 处理 | NO | W | NO | R | NO | NO | R | 短锁; 内部 Step 1~3 完成 AllowRemote/用户 ACL/hold/字段写, 实测 < 1ms; ★ v2.0 增 user=R (读 user/assoc.remote_allowed) |
| `cd_send_forward_to_broker` (网络 IO) | (无锁) | (无锁) | - | - | - | - | - | 必须 release 锁后调用; 阻塞最长 30s |
| 写 trace_id 短锁 (legacy) | NO | W | NO | NO | NO | NO | NO | v2.0 起 trace_id 改由 broker UPDATE_REMOTE_STATE 异步回写 (§7.3), 此处可省 |
| `cd_revert_forward` (临时性) | NO | W | NO | NO | NO | NO | NO | 调用 `_cd_revert_locked(false)`; priority 还原, **不**置 `cd_route_exhausted` |
| **`cd_revert_forward_hard`** ★ v2.0 新增 | NO | W | NO | NO | NO | NO | NO | 调用 `_cd_revert_locked(true)`; priority 还原 + **置 `cd_route_exhausted=1`** + 调 `_cd_dbd_modify_route_exhausted` 同步落库 |
| `cd_revert_for_partition_disallow` / `_acl_failed` | NO | W | NO | NO | NO | NO | NO | 仅写 state_desc (priority 未被动过) |
| **`cd_mark_route_exhausted`** ★ v2.0 新增 | (调用方持锁) | - | - | - | - | - | - | 公开 API, 供 update_job / 运维路径调用 |
| `cd_tick_scan_cancelled` 决策 | NO | R | NO | NO | NO | NO | NO | 1s tick, 仅扫 `cd_forwarded==1 && IS_JOB_CANCELLED` |
| `cd_send_cancel_to_broker` 写 propagated | NO | W | NO | NO | NO | NO | NO | |
| `handle_broker_update_remote_state` ★ v2.0 含首次 cluster/partition 回写 | NO | W | NO | NO | NO | NO | NO | |
| `handle_broker_terminal_state` 分支 A (本地 PENDING) | NO | W | NO | R | NO | NO | R | 调 jobcomp_g_record_job_end + job_completion_logger; 终态写入 sacct |
| `handle_broker_terminal_state` 分支 B (本地已终态) | NO | W | NO | R | NO | NO | R | 仅调 `_cd_dbd_modify_remote_fields`; **不**调 jobcomp / completion_logger |
| `_cd_dbd_modify_remote_fields` (MVP-1.5+) | (调用方持 job=W) | - | - | - | - | - | - | 由 §7.4 分支 B 持锁内调用; 自身只调 `acct_storage_g_job_modify`, 不申请新锁 |
| `_cd_dbd_modify_route_exhausted` ★ v2.0 新增 | (调用方持 job=W) | - | - | - | - | - | - | 由 `cd_revert_forward_hard` 持锁内调用; 单字段 SQL UPDATE |
| `cross_region_check_update_block` | (调用方持锁) | - | - | - | - | - | - | `update_job_msg` 已持 job=W |
| `cross_region_check_update_reset` ★ v2.0 新增 | (调用方持锁) | - | - | - | - | - | - | 处理 `CdRouteExhausted=0\|1` 重置 |

> **v2.0 变化**: ① 删除 `cd_phys_cross_region_has_capacity` 行 (下沉到 broker); ② 删除 `cd_partition_allows_app` 行 (下沉到 broker); ③ `cd_handle_pending_job_locked` 锁集增加 `user=R` (读 user/assoc.remote_allowed); ④ 新增 `cd_revert_forward_hard` / `cd_mark_route_exhausted` / `cross_region_check_update_reset` / `_cd_dbd_modify_route_exhausted` 四个条目。
>
> **本版本扫描函数 part 锁仍是 R**: 虽然 v2.0 partition 端只剩 `cd_allow_remote` 1 个字段, 仍需要 `part=R` 锁防止 reconfig 期间字段被并发改写。

### 12.2 与 Slurm 主调度的并发风险

| 风险 | 缓解 |
|---|---|
| **`cd_tick_scan_pending` Phase A 持 `job=R` 遍历 100w 作业** | 典型场景 `cd_cross_region==1` 作业占比 < 10%, 其余作业只做一次 `uint16_t` 比较就过滤掉; 实测 100w 作业全表扫描 ≈ 30-80ms; 默认扫描周期 5s → 读锁占比 < 2%, 主调度 / RPC 线程可忍受; 百万级堆积且候选占比高的场景可通过 `CrossRegionScanInterval=15` 进一步摊薄 |
| **扫描周期 = 5s, 最坏情况下跨域响应延迟** | 对比 `CrossRegionWaitTime` 默认 300s, 5s 抖动可忽略; 极端强实时需求用户可设 `CrossRegionScanInterval=1` (以 5x 持锁占比换 5x 响应) |
| **单轮 tick 命中大量作业导致 Phase B 串行拥塞** | 分批上限 `CrossRegionMaxHandlePerRound` (默认 500): 单轮 Phase B 最多 500 次 write_lock 字段写 (≈ 500ms) + 500 次 broker RPC (RPC 本身可多路复用或 broker 侧异步 ack 优化, 本文档未强制); 超过部分留给下一轮 tick, 自动背压; 配合 §6.8 `MaxJobs` 限流, 整体转发速率受集群资源与 broker 能力双重约束 |
| **主调度 / backfill 完全不感知跨域字段** | MVP-1.4 已撤销所有 hook, `_schedule()` / `_attempt_backfill()` 上游逻辑 0 改动; 跨域字段仅在 `job_mgr.c` 持久化路径 + `proc_req.c` RPC handler + `update_job` 拦截 hook 被读写, 与主调度热路径完全解耦 |
| `cd_send_forward_to_broker()` 阻塞 30s | 在锁外发起, 主调度 / 扫描线程均不受影响 |
| `cd_handle_pending_job_locked()` 持 `job=W` | 单次持锁仅做字段写入 (含 hold + 状态切换), 实际耗时 < 1ms; 此期间主调度 / backfill 等待可接受 |
| RPC handler `handle_broker_terminal_state` 分支 A 调用 `jobcomp_g_record_job_end` 嵌套锁 | 24.05 jobcomp 插件已 lock-aware, 沿用 Slurm 原生终态写入路径不嵌套 |
| **scancel 抢跑后 broker 终态再到 (分支 B) 的 jobcomp 重入风险 (MVP-1.5)** | 分支 B 不调 `jobcomp_g_record_job_end / job_completion_logger`, 改走 `acct_storage_g_job_modify` 单线 SQL UPDATE; 既保证远端字段补写, 又彻底避开 jobcomp 插件 / 依赖作业解锁的二次副作用 |
| **broker 重发 TERMINAL_STATE (broker 重启 / resume_inflight)** | §7.4 入口检查 `cd_terminal_received==1` 真幂等返回 SUCCESS, 不重复写任何字段 |
| **ctld 重启 / backup 切 primary 后跨域扫描的重入** | 扫描函数无状态, 完全由 `job_list` 字段驱动; 重启后已转发 (`cd_forwarded=1`) 作业会被 Phase A 过滤跳过; 未转发候选会在第一次扫描被重新评估 — 无需任何额外恢复逻辑 |

### 12.3 可重入校验

- 跨域线程主循环每 1s tick, 不允许重入 (单线程)。
- `handle_broker_update_remote_state` 是 RPC 线程池调用, 多实例并发, 通过 `find_job_record` 后操作单个 job_ptr; Slurm 的全局锁体系本身保证安全。
- `cd_revert_forward()` 与 `handle_broker_update_remote_state()` 的竞态: revert 后 broker 可能仍发 update; handler 检测 `!cd_forwarded` 直接返回 `ESLURM_INVALID_JOB_STATE`, 安全。

---

## 13. 启动顺序与 controller.c / schedule.c / backfill.c 集成

### 13.1 启动 hook (`controller.c`)

```c
/* src/slurmctld/controller.c::main() */

/* 在 _slurmctld_listen_thread() 启动后, schedule() 启动前: */
if (cross_region_init() != SLURM_SUCCESS) {
    fatal("cross_region_init failed");
}

/* 主循环 ... */

/* 退出时: */
cross_region_fini();
```

### 13.2 reconfigure hook

```c
/* src/slurmctld/controller.c::_slurmctld_reconfig() 末尾 */
cross_region_reconfig();
```

### 13.3 主调度 (`job_scheduler.c` / `schedule.c`) — ★ MVP-1.4 完全不动

MVP-1.4 撤销了 MVP-1.2 在 `_schedule()` 主循环中插入的 `cross_region_main_sched_mark()` hook, `job_scheduler.c` / `schedule.c` 保持 24.05.8 上游代码**不做任何修改**。

**理由**:

- **覆盖不全**: Slurm 原生 `_schedule()` 受 `SchedulerParameters=default_queue_depth` (默认 100) 限制, 单轮调度最多 evaluate 前 100 个 PENDING 作业; 队列头部 100 个作业若因分区无资源 / QoS 等原因被整体跳过, 后面的作业 (百万级堆积时占绝大多数) 永远不会进入 `_schedule()` 热路径, 也就永远不会被主调度 hook 识别为跨域候选。
- **单一数据源更稳定**: 把跨域识别完全收敛到跨域线程的全表扫描, `job_list` 是唯一真相源, 无"主调度打标 + 跨域线程消费"的跨线程状态同步问题。
- **响应时间可接受**: 主调度打标最快 ~1-2s (周期性 `batch_sched_delay`), 跨域线程扫描默认 5s; 对比 `CrossRegionWaitTime=300s` 默认值, 5s 抖动可忽略。
- **rebase 到上游新版本更简单**: 24.11 / 25.05 的 `_schedule()` 重构只需关注自身逻辑, 无需照顾跨域 hook; 跨域线程的扫描函数只依赖 `job_record_t` 字段定义 (基本稳定) 与 `job_list` 迭代器 (已存在十余年 ABI 稳定)。

### 13.4 Backfill (`backfill.c`) — ★ MVP-1.4 完全不动

同 §13.3, `_attempt_backfill()` 不插入任何 hook, `backfill.c` 保持上游原样。

> backfill 是"在不影响优先级作业的前提下填空闲资源"的辅助路径, 一个跨域意图作业若被 backfill 本地起来也是合理结果 (本地优先); 跨域线程扫描后续 tick 看到作业 `!IS_JOB_PENDING` 会自然跳过。

### 13.5 backup ctld

24.05.8 backup ctld 在切换为 primary 时会调 `_init_after_takeover()`。在该函数末尾追加:

```c
cross_region_init();
```

> backup → primary 切换后, broker 可能已经发了几条 `UPDATE_REMOTE_STATE` 给原 primary; 重启的 primary 会从 state save 文件 restore 跨域字段 (`cd_cross_region` / `cd_forwarded` / `cd_remote_*` / `cd_remote_trace_id`), 保证连续性。broker 端的 sync_ticker 下个 tick 自然会重新推送最新状态。跨域线程**无需特殊 recovery 逻辑** — 第一次 `cd_tick_scan_pending` 执行时, 已转发 (`cd_forwarded=1`) 的作业被 Phase A 过滤跳过, 未转发候选会被重新评估。

---

## 14. 测试矩阵

### 14.1 单元测试 (在 `testsuite/expect/` 目录新增, v2.0)

| 测试编号 | 名称 | 内容 |
|---|---|---|
| test_cr1.1 | 配置解析 | `slurm.conf` 含 `CrossRegionEnabled=YES` 等, ctld 启动后 `scontrol show config` 能看到 |
| test_cr1.2 | partition 解析 (v2.0 新) | `AllowRemote=yes` 解析后 `scontrol show partition` 输出 `AllowRemote=YES`; 旧 `SendTo=`/`AllowApp=` 应解析失败或被忽略 |
| test_cr1.3 | sbatch 透传 | `sbatch --allow-remote --app=foo sleep.sh` 后 `scontrol show job` 看到 `CrossRegion=yes AppName=foo RouteExhausted=NO` |
| test_cr1.4 | squeue --remote 占位符 | 跨域作业 squeue --remote 输出 8 列正确 |
| test_cr1.5 | scontrol show job (v2.0 新增 RouteExhausted) | 跨域字段全部显示含 `RouteExhausted=NO/YES`, comment 不污染 |
| test_cr1.6 | sacct Remote_* | 终态后 `sacct -o Remote_Cluster,Remote_JobId,Remote_ExitCode` 有值 |
| **test_cr1.7** ★ v2.0 新增 | sacctmgr remote_allowed 子命令 | `sacctmgr modify user u1 set remote_allowed=true`; 然后 `sacctmgr show user u1 format=user,remote_allowed` 输出 YES |
| **test_cr1.8** ★ v2.0 新增 | scontrol update CdRouteExhausted=0 | 模拟 `cd_route_exhausted=1` 状态后, root 执行 `scontrol update jobid=X CdRouteExhausted=0` 能清除; 普通用户拒绝 |

### 14.2 集成测试 (mock broker, v2.0)

| 测试编号 | 名称 | 内容 |
|---|---|---|
| test_cr2.1 | 跨域触发 | mock broker, 提交作业 + 等 wait_time + 观察 `cd_forwarded=1` |
| test_cr2.2 | broker 拒绝回滚 (临时) | mock broker 返回 `ESLURM_CR_USER_NO_MAPPING` (5003), 验证 priority 还原 + **cd_route_exhausted 仍为 0** |
| test_cr2.3 | broker UPDATE 首次包写远端 (★ v2.0 新) | mock broker 在 UPDATE_REMOTE_STATE 首次包带 `remote_cluster_name=wz_cluster / remote_partition_name=wznormal`, 验证 ctld 写入 `cd_remote_cluster_name` 与 state_desc 改为 `Forwarded_wznormal_wz_cluster` |
| test_cr2.4 | broker TERMINAL_STATE | mock 推 COMPLETE, 验证作业进 sacct |
| test_cr2.5 | scancel 反向 | 提交跨域 + 转发 + scancel + 观察 mock broker 收到 CANCEL |
| test_cr2.6 | update partition 拦截 | scontrol update 拦截后报 `ESLURM_NOT_SUPPORTED` |
| test_cr2.7 | ctld 重启 restore | 跨域字段从 state save 文件恢复 (含 v2.0 新增 `cd_route_exhausted`) |
| test_cr2.8 | ACL 关闭 (v2.0 新) | sacctmgr 改 `remote_allowed=false`, 新作业 ACL 失败 (state_desc=CrossRegionAclDenied), 不触发转发 |
| **test_cr2.9** ★ v2.0 新增 | partition AllowRemote=no | partition 不带 `AllowRemote=yes`, 提交 `--allow-remote` 作业, 跨域线程扫描 Phase A 直接过滤掉, `cd_forwarded` 永远为 0 |
| **test_cr2.10** ★ v2.0 新增 | broker NO_ROUTE 硬失败 | mock broker 返回 `ESLURM_CR_NO_VIABLE_ROUTE` (5010), 验证 `cd_route_exhausted=1` 被置位, 下轮扫描 Phase A 跳过, 作业不再尝试 |
| **test_cr2.11** ★ v2.0 新增 | broker ALL_ROUTES_EXHAUSTED | mock broker 模拟"3 次 test-only 后全部拒收", 返回 5013, 验证同上 |
| **test_cr2.12** ★ v2.0 新增 | broker CAP_FULL_SOFT_WAIT 软等待 | mock broker 返回 5020, 验证作业 `cd_route_exhausted=0`, 下轮扫描重试, 第 N 轮 mock broker 改为 OK 后作业成功转发 |
| **test_cr2.13** ★ v2.0 新增 | scontrol clear route_exhausted | test_cr2.10 之后 root 执行 `scontrol update jobid=X CdRouteExhausted=0`, 验证下轮扫描重新命中 |
| test_cr2.14 (原 quota 满, v2.0 改在 broker 验证) | 见 broker 详设 §test_br2.X | ctld 端不再独立测试容量限流, 仅在端到端测试覆盖 |

### 14.3 端到端 (真 broker, 真集群对端)

见 `跨域调度-概要设计.md` §10.2 (5 款应用 LAMMPS/VASP/GROMACS/Gaussian/Fluent)。

### 14.4 故障注入

| 编号 | 注入 | 期望行为 |
|---|---|---|
| fault.1 | broker kill -9 | 已转发作业字段保持; broker 重启后 sync_ticker 继续推送; 主调度不受影响 |
| fault.2 | broker reject 100% | 所有作业被回滚, state_desc=CrossRegionRejected:5008, 等运维介入 |
| fault.3 | DBD 失联 | 终态写 sacct 失败由 jobcomp 自带重传机制; 作业 state 仍为终态 |
| fault.4 | ctld kill -9 | 重启后从 state save 恢复; broker 端 sync_ticker 在 < 30s 内重新推送状态 |
| fault.5 | 时钟漂移 > 24h | munge 校验失败, broker→ctld RPC 全部失败; ctld 日志可定位 |
| fault.6 | 100 用户并发 sbatch | tick 1s 内能消化 100 个作业的扫描决策, p99 调度延迟增量 < 10ms |

---

## 15. 部署与升级

### 15.1 升级路径 (v2.0)

#### 15.1.1 从官方 24.05.8 直接升级到 MVP-v2.0

```text
1. 准备阶段:
   - 把全部改动维护成一个 patch series (推荐 27 个文件分 ~11 个 commit)
   - patch series 在 24.05.8 干净源上 apply 通过, 全量 make 通过
   - 跑 testsuite/expect 全部 PASS (含 test_cr2.10/2.11/2.12/2.13)
   - broker 端同步打 v2.0 patch (详见 Broker详设 §3)

2. 滚动升级 (源端 A 集群 + 远端 B 集群):
   - 先升级 B 集群 (远端): 不打 cross-region 也能正常工作, 接收远端 sbatch 即可
   - 再升级 A 集群 (源端入口): 升级后 sbatch / squeue / scontrol / sacct / sacctmgr 均为改造版
   - 升级 SlurmDBD: 只需 ctld 重连 DBD, schema 自动 ALTER:
     * job_table 加 8 列 cd_remote_*
     * job_table 加 1 列 cd_route_exhausted (v2.0 新)
     * user_table 加 1 列 remote_allowed (v2.0 新)
     * <cluster>_assoc_table 加 1 列 remote_allowed (v2.0 新)
   - 升级 broker: 准备 routes.conf (替代 v1.5 的 partition.SendTo / AllowApp)

3. 启用跨域:
   - slurm.conf 加 CrossRegionEnabled=YES; 本地物理分区配置 `AllowRemote=yes`
   - broker routes.conf 配置 [Route] 段, 含 LocalCluster/LocalPartition/RemoteCluster/RemotePartitions/AllowApps/MaxInflight/Priority
   - sacctmgr 给试点用户 set remote_allowed=true
   - 灰度 5 用户 → 1 周 → 50 用户 → 全量
```

#### 15.1.2 从 MVP-1.5 增量升级到 MVP-v2.0 (★ v2.0 新增)

> 已运行 v1.5 的存量集群升级路径。

```text
1. 升级前数据准备 (重要):
   1.1 SQL 迁移 user/assoc.comment → remote_allowed 列:
       (详见 §9.6.5)
       UPDATE user_table SET remote_allowed=1 WHERE comment LIKE '%allow_remote%';
       UPDATE <cluster>_assoc_table SET remote_allowed=1 WHERE comment LIKE '%allow_remote%';
   1.2 v1.5 各本地 partition 的 SendTo / AllowApp 字段, 整理映射后写入新的 broker routes.conf
       (建议运维事先用 Python 脚本: scontrol -o show partition + 配置文件交叉, 生成 routes.conf)

2. 滚动升级:
   2.1 升级 broker: 加载 routes.conf, 此时 v1.5 ctld 仍能用 (broker 兼容 v1.5 forward_job_msg payload)
   2.2 升级 SlurmDBD (schema 自动 ALTER, 见 §15.1.1 步骤 2)
   2.3 升级 ctld + 客户端: 一起升级, 切换到 v2.0 协议

3. 切换配置:
   3.1 slurm.conf 删除 CrossRegionCommentTag= 行 (若有), 删 partition SendTo= AllowApp= 行
   3.2 slurm.conf 在 partition 上加 AllowRemote=yes (原有 SendTo 的分区都加)
   3.3 scontrol reconfigure
   3.4 验证: 提交一个跨域作业, 走完全流程

4. 清理 (可选):
   4.1 跑一段时间稳定后, 清掉 user/assoc.comment 里的 allow_remote 子串
   4.2 注意: 万勿删 job_table 的 cd_remote_* 列, 历史数据需要
```

### 15.2 回滚路径

```text
1. slurm.conf 改 CrossRegionEnabled=NO + scontrol reconfigure
   → 跨域线程 fini, 已转发作业字段保留, 不再有新转发
2. 等待已转发作业全部走完 (broker 仍能写终态)
3. 把 v2.0 二进制换回官方 24.05.8 二进制
   → DBD 端 schema 多出来的列不影响读写 (官方 ctld 不写就是 NULL)
4. 如需彻底清表: ALTER TABLE job_table DROP COLUMN cd_remote_*, cd_route_exhausted
   ALTER TABLE user_table DROP COLUMN remote_allowed
   (强烈建议保留, 历史数据值钱)
```

### 15.3 sacctmgr 一键迁移脚本 (v1.5 → v2.0)

```bash
#!/bin/bash
# migrate_v1_5_to_v2.sh
# 1. 把所有 comment LIKE '%allow_remote%' 的 user 设 remote_allowed=true
for u in $(sacctmgr -nP show user format=user,comment | awk -F'|' '$2 ~ /allow_remote/ {print $1}'); do
    sacctmgr -i modify user "$u" set remote_allowed=true
done

# 2. 对所有 cluster 库的 assoc 表也跑一遍
for c in $(sacctmgr -nP show cluster format=cluster); do
    for a in $(sacctmgr -nP show assoc cluster="$c" format=user,account,comment | awk -F'|' '$3 ~ /allow_remote/ {print $1":"$2}'); do
        user="${a%%:*}"; acct="${a##*:}"
        sacctmgr -i modify user "$user" account="$acct" cluster="$c" set remote_allowed=true
    done
done
echo "Migration done. Verify with: sacctmgr show user format=user,remote_allowed"
```

### 15.4 v2.0 → v1.5 紧急回滚 SOP (state 文件不兼容)

由于 v2.0 在 state 文件 pack 多了 1 字节 (`cd_route_exhausted`), v1.5 二进制直接读 v2.0 state 文件会因字段偏移导致 unpack 失败。回滚 SOP:

```text
1. 通知用户: 跨域作业可能丢失, 仅本地作业保留
2. scontrol shutdown ctld
3. 备份 /var/spool/slurmctld/  到 /var/spool/slurmctld.v2.bak/
4. rm /var/spool/slurmctld/job_state    (清空 state 文件)
5. 部署 v1.5 二进制
6. systemctl start slurmctld    (空 state 启动, 仅服务新作业)
7. 历史 sacct 数据仍在 MySQL job_table, 用户能查到; 但运行态作业 (含跨域) 全部丢失
```

### 15.5 patch series 分包建议 (v2.0)

| commit | 内容 | LoC |
|---|---|---|
| 1/11 | 协议层: 新 RPC 类型 + msg 结构 + pack/unpack (含 v2.0 瘦身的 forward_job_msg) | ~280 |
| 2/11 | slurm_conf 全局键解析 + slurm.h.in 字段 (删 CrossRegionCommentTag) | ~60 |
| 3/11 | partition 新关键字解析 + part_record cd_allow_remote 字段 (v2.0 极简) | ~50 |
| 4/11 | job_record + job_desc + job_info 字段 (含 cd_route_exhausted) + 持久化 | ~240 |
| 5/11 | slurm_opt + sbatch + 帮助文本 (--allow-remote 替代 --cross-domain) | ~80 |
| 6/11 | squeue --remote + %R* 占位符 (新增 RouteExhausted 列) | ~210 |
| 7/11 | scontrol show job 输出 + CdRouteExhausted=0\|1 update 接口 | ~80 |
| 8/11 | sacct + DBD job_table schema + 写入 | ~280 |
| **9/11** ★ v2.0 新增 | DBD user/assoc remote_allowed schema + sacctmgr 子命令 + assoc_mgr 缓存 | ~180 |
| 10/11 | ctld 跨域线程 cross_region.c/.h (v2.0 大幅简化, 删 AllowApp / capacity / SendTo 解析) | ~500 |
| 11/11 | ctld RPC handler (含首次回写 cluster/partition) + update 拦截 + controller hook | ~340 |

> 这种分包方式让评审人能"一口一口"读, 也便于针对单个 commit 做 reverse 调试。v2.0 总 LoC ~1860, 较 v1.5 的 ~2200 净减约 340 行。

---

## 16. 适配 24.05.8 的已知风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| 24.05.8 升级到 24.11 时 patch rebase 工作量 | 中 | 把改动收敛到新增文件 + 入口 hook 行; 维护 patch series 而非 fork; 每次 rebase 走 testsuite |
| `slurm_opt_t` 字段在 24.11 重命名 | 低 | 用 `slurm_opt_t.cross_region` `slurm_opt_t.app_name` 两个字段, 命名通用 |
| `assoc_mgr_fill_in_user()` 接口签名变更 | 中 | 抽一层私有 helper `_cd_get_user_comment(uid, char **)`, 升级时只改一处 |
| `jobcomp_g_record_job_end()` 调用语义变化 | 中 | 抽 helper `cd_finalize_job()` 集中 4 行调用, 配合 24.x 升级 |
| `update_job()` 函数签名变化 (24.05 vs 24.11 已经有差异) | 中 | 拦截放在最前面, 避免修改函数尾部; 加 helper `cd_intercept_update_job_msg()` |
| 24.05 protocol pack 函数命名 `_pack_*` 转 `pack_*` 全局化 | 低 | 静态函数命名按 24.05 现状, 24.11 升级时 sed 即可 |
| state_save 文件不兼容回滚 | 中 | 升级时 backup 一份原 state_save 目录; 回滚前用 `slurm-23.11` 工具读不行的话 fallback 到 backup |
| DBD schema ADD COLUMN 在大表 (亿行) 锁表 | 中 | 升级窗口选夜间; 用 pt-online-schema-change 或在 SlurmDBD 维护窗口执行 |
| jobcomp 插件 (es / kafka) schema 变化 | 中 | jobcomp 接收端按需扩字段; 短期不影响, 旧数据丢 cd_remote_* 字段 |

---

## 17. 附录

### 17.1 关键全局变量与函数索引 (v2.0)

| 名称 | 文件 | 类型 | 说明 |
|---|---|---|---|
| `slurm_conf.cross_region_enabled` | `slurm/slurm.h.in` | `uint16_t` | 全局开关 |
| `slurm_conf.cross_region_wait_time` | 同上 | `uint32_t` | 排队阈值 |
| `slurm_conf.broker_host` / `broker_port` | 同上 | `char *` / `uint16_t` | broker 地址 |
| ~~`slurm_conf.cross_region_comment_tag`~~ | — | — | **v2.0 删除** (改用 `user.remote_allowed` 显式列) |
| `slurm_conf.cross_region_scan_interval` | `slurm/slurm.h.in` | `uint16_t` | 扫描周期 (秒, 默认 5) |
| `slurm_conf.cross_region_max_handle_per_round` | 同上 | `uint16_t` | 单轮 Phase B 最大处理作业数 (默认 500) |
| `part_record.cd_allow_remote` ★ v2.0 新 | `slurmctld.h` | `uint8_t` | `AllowRemote=yes/no` (取代 v1.5 `cd_send_to`/`cd_allow_apps` 等) |
| `job_record.cd_route_exhausted` ★ v2.0 新 | `slurmctld.h` | `uint8_t` | broker 报告"硬不可达"后置 1, 跳出扫描热路径 |
| `slurmdb_user_rec.remote_allowed` ★ v2.0 新 | `slurmdb.h` | `uint16_t` | 替代 v1.5 `user.comment` 子串 |
| `slurmdb_assoc_rec.remote_allowed` ★ v2.0 新 | `slurmdb.h` | `uint16_t` | 替代 v1.5 `assoc.comment` 子串 |
| `cross_region_init` / `_fini` / `_reconfig` | `cross_region.c` | API | 生命周期 |
| `handle_broker_update_remote_state` | `cross_region_rpc.c` | RPC handler | broker 推字段; **v2.0**: 首次包写 `cd_remote_cluster_name` / `cd_remote_partition_name` |
| `handle_broker_terminal_state` | 同上 | RPC handler | broker 推终态 |
| `cross_region_check_update_block` | `cross_region.c` | hook | update 拦截 |
| `cross_region_check_update_reset` ★ v2.0 新 | 同上 | hook | 处理 `scontrol update CdRouteExhausted=0\|1` |
| `cd_user_allows_remote` | 同上 | helper | **v2.0**: 读 `assoc/user.remote_allowed` (替代 v1.5 `comment` 子串) |
| ~~`cd_partition_allows_app`~~ | — | — | **v2.0 删除** (下沉到 broker `routes.conf::AllowApps`) |
| ~~`cd_phys_cross_region_has_capacity`~~ | — | — | **v2.0 删除** (下沉到 broker `cap_check.c`) |
| `cd_mark_route_exhausted` ★ v2.0 新 | 同上 | API | 公开 helper, 把作业一次性钉死 |
| **`cd_tick_scan_pending`** | 同上 (static) | **★ 核心: 跨域线程主扫描** | **`CrossRegionScanInterval` 周期 (默认 5s) 全表扫描 `job_list`, Phase A `job=R/part=R` 收集候选 jobid (v2.0 增 `!cd_route_exhausted` + `partition.cd_allow_remote==1` 条件) → Phase B 逐个调 `cd_handle_pending_job_locked`; 分批上限 `CrossRegionMaxHandlePerRound`** |
| **`cd_handle_pending_job_locked`** | 同上 (static) | **★ 核心: 单 job 处理** | **v2.0 简化为 Step 1~3: AllowRemote 复检 / 用户 ACL / hold (不写远端字段); 释放锁后调 broker; 按返回码 5010/5011/5013 走 `cd_revert_forward_hard`, 5012/5020 走 `cd_revert_forward`** |
| `cd_revert_forward` | 同上 (static) | core | broker 临时性失败回滚 (priority 还原, **不**置 `cd_route_exhausted`) |
| `cd_revert_forward_hard` ★ v2.0 新 | 同上 (static) | core | broker 硬不可达回滚 (priority 还原 + **置 `cd_route_exhausted=1`** + SQL 落库) |
| `cd_revert_for_partition_disallow` ★ v2.0 新名 | 同上 (static) | core | partition AllowRemote=0 失败, 仅写 state_desc |
| `cd_revert_for_acl_failed` | 同上 (static) | core | 用户 ACL 失败, 仅写 state_desc |
| `_cd_revert_locked` (含 `mark_exhausted` 形参) | 同上 (static) | helper | 通用回滚实现, v2.0 增 `bool mark_exhausted` 参数 |
| `_cd_dbd_modify_route_exhausted` ★ v2.0 新 | `cross_region_rpc.c` (static) | helper | 单字段 SQL UPDATE, 让 sacct -X 可见 `cd_route_exhausted=1` |

> **v2.0 删除函数**: `cd_partition_allows_app` (~60 LoC) / `cd_phys_cross_region_has_capacity` (~80 LoC) / `cd_revert_for_route_lost` (~30 LoC) / partition 端 `_cd_partition_init` 中 `SendTo` 解析逻辑 (~50 LoC) / `clone_job_desc_from_record` 调用与 account 字段处理 (~70 LoC) — 全部下沉到 broker 或直接删除。
>
> **v2.0 新增函数**: `cd_revert_forward_hard` / `cd_mark_route_exhausted` / `cross_region_check_update_reset` / `_cd_dbd_modify_route_exhausted` / `cd_revert_for_partition_disallow` — 共 ~120 LoC。
>
> **v1.5 → v2.0 LoC 净变化**: -360 LoC (从 ~2200 降到 ~1840), 复杂度显著下降, 大部分逻辑迁到 broker 端。

### 17.2 快速验收命令包 (v2.0)

```bash
# === ctld 端基本可用 ===
scontrol show config | grep -i cross
# CrossRegionEnabled            = YES
# CrossRegionWaitTime           = 300
# CrossRegionScanInterval       = 5
# CrossRegionMaxHandlePerRound  = 500
# BrokerHost                    = 127.0.0.1
# BrokerPort                    = 8442
# (★ v2.0 已删: CrossRegionCommentTag)

scontrol show partition xahcnormal | grep -E '(AllowRemote)'
#    AllowRemote=YES
# (★ v2.0 已删: SendTo / AllowApp — 远端拓扑由 broker routes.conf 管理)

# === sbatch 透传 ===
sbatch --allow-remote --app=lammps-2Aug2023-intelmpi2018 -p xahcnormal sleep.sh
scontrol show job 12345 | grep -E '(CrossRegion|AppName|Remote|Route)'
#    CrossRegion=yes AppName=lammps-2Aug2023-intelmpi2018 RouteExhausted=NO
#    RemoteCluster=(null) RemoteJobId=0 RemoteState=N/A
# (尚未触发跨域时, Remote* 都是空; RouteExhausted=NO 表明候选有效)

# === ACL 检查 (★ v2.0: sacctmgr 显式列, 替代 comment 子串) ===
sacctmgr -i modify user test1 set remote_allowed=true
sacctmgr show user test1 format=user,remote_allowed
# User       Remote_Allowed
# test1      YES
sleep 5    # 触发跨域线程下一轮 tick

# === 触发跨域后, 首次 UPDATE_REMOTE_STATE 到达 ===
squeue --remote -j 12345
# JOBID PARTITION ... REMOTE_CLUSTER REMOTE_JOBID REMOTE_STATE REMOTE_TRES
# 12345 xahcnormal ... wz_cluster    8888         RUNNING      cpu=32,...
scontrol show job 12345 | grep -E '(RouteExhausted|RemoteCluster|RemotePartition)'
#    RouteExhausted=NO
#    RemoteCluster=wz_cluster RemotePartition=wznormal
#    (★ v2.0: 这两个字段由 broker 首次状态包写入, 不在 ctld 提前生成)

# === 终态后 ===
sacct -j 12345 -o JobID,State,Remote_Cluster,Remote_JobId,Remote_AllocTRES,Remote_ExitCode
# 12345  COMPLETED  wz_cluster  8888  cpu=32,mem=128G  0:0

# === 验证 hard 失败钉死 (★ v2.0 新场景) ===
# 1. 临时把 broker routes.conf 该路由的 AllowApps= 改成只允许 vasp
# 2. 提交 lammps 作业
sbatch --allow-remote --app=lammps -p xahcnormal sleep.sh   # 作业 12346
sleep 5    # 等 broker 探测一轮
scontrol show job 12346 | grep -E '(RouteExhausted|StateReason|Reason)'
# RouteExhausted=YES
# Reason=CrossRegionExhausted:5011

# 3. 运维恢复 routes.conf 后, 手动重置
scontrol update jobid=12346 CdRouteExhausted=0
sleep 5
scontrol show job 12346 | grep -E '(RouteExhausted|RemoteCluster)'
# RouteExhausted=NO    (扫描重新命中)
# RemoteCluster=wz_cluster   (broker 已重新接收并探测成功)
```

### 17.3 修订历史

| 版本 | 日期 | 变更 |
|---|---|---|
| MVP-1.0 | 2026-04 | 首次基于 Slurm 24.05.8 形成 ctld 端详细设计; 与整体方案 / broker MVP 文档对齐 |
| MVP-1.1 | 2026-04 | **职责重分配**: "是否要跨域"的触发判断从跨域线程移到主调度; 新增 `cross_region_main_sched_mark()` hook 由 `_schedule()` / `_attempt_backfill()` 调用; 跨域线程 1s tick 改为只消费决策队列 (`cd_eligible_for_remote==1`); 新增 `cd_eligible_for_remote` 字段贯通 job_record / 状态文件 / 协议 / 回滚; 失败回滚拆分为通用 `_cd_revert_locked` + 三个变体 (broker rejected / route lost / acl denied); §1.1 / §1.3 / §11.2 / §12 / §13 同步刷新 |
| **MVP-1.2** | **2026-04** | **打标与 hold 解耦, 不动 backfill**: ① `cross_region_main_sched_mark()` 简化为只写 1 字节字段 `cd_eligible_for_remote=1`, 不再改 `priority` / `state_desc` / 路由字段, 不再要求调用方 continue; ② hold 责任完全归跨域线程, 在新增的 `cd_handle_marked_job_locked()` Step 1 中 (`priority=0`); ③ 旧 `cd_trigger_forward_locked()` 合并为 `cd_handle_marked_job_locked()`, write_lock 内一气呵成 hold + ACL/限流/路由 + 切 Forwarded_*; ④ 决策队列扫描 `cd_tick_decide_marked()` 简化为 read_lock 内仅收集 jobid; ⑤ **撤销 backfill hook** (`_attempt_backfill()` 不再调 mark hook), 减小改动面; ⑥ §0.2 / §1.1 模块图 / §1.3 职责拆分表 / §11.2 状态机 / §12.1 锁矩阵 / §13.3 主调度集成 / §13.4 / §17.1 函数索引同步刷新; 实现"本地优先, 跨域兜底"的语义 — 主调度打标后 ≤ 1s 窗口期内仍可本地起作业 |
| **MVP-1.3** | **2026-04** | **取消虚拟分区，纯物理队列**：本地分区配置 `SendTo=<远端物理>@<集群>`（必选才可跨域）；可选 `AllowApp=` 与 `cd_app_name` 精确匹配；删除 `Remote`/`RemoteDestinations`/`cd_is_remote` 间接路由模型；限流改为 `cd_phys_cross_region_has_capacity()` 按本地分区 `MaxJobs`；partition pack/unpack 与 §6 / §17 示例同步改版 |
| **MVP-1.4** | **2026-05** | **撤销主调度 hook, 跨域线程全表扫描**: ① 删除 `cross_region_main_sched_mark()` 函数及其在 `_schedule()` 中的调用, `job_scheduler.c` / `backfill.c` 保持上游原样; ② 删除 `cd_eligible_for_remote` 字段 (不再需要"主调度已识别"中间态), 同步清理 `job_record_t` / pack/unpack / 状态保存 / 协议; ③ 原 `cd_tick_decide_marked()` 改为 `cd_tick_scan_pending()`, O(N) 全表扫描 `job_list` 一次性完成候选识别与筛选; ④ 原 `cd_handle_marked_job_locked()` 改为 `cd_handle_pending_job_locked()`, **hold (priority=0) 从 Step 1 挪到 Step 4** — Step 1~3 (路由/ACL/限流) 失败时不动 priority, 避免因 Phase A `priority>0` 过滤导致"永久卡死"; ⑤ `cd_revert_for_route_lost` / `_acl_failed` 简化为仅写 state_desc (priority 未被动过, 无需还原); ⑥ 新增 `CrossRegionScanInterval` (默认 5s) / `CrossRegionMaxHandlePerRound` (默认 500) 两个全局配置; ⑦ §0.2 / §0.3 / §1 / §4 / §6 / §11.2 / §12 / §13.3 / §13.4 / §17.1 全面刷新; ⑧ 动机: 原 MVP-1.2/1.3 设计的主调度 hook 在百万级堆积队列场景下, 因 `default_queue_depth` 限制覆盖不全, 尾部作业无法被识别; 全表扫描换来"单一数据源 + 百万作业全覆盖 + 无事件 hook 漏挂风险", O(N) 开销通过扫描周期与分批上限控制在可接受范围 |
| **MVP-1.5** | **2026-05** | **修复 scancel 抢跑场景下"远端字段永远写不进 sacct"的过度幂等漏洞**: ① 漏洞: 早期 §7.4 入口 `if (IS_JOB_FINISHED(job_ptr)) 短路返回 OK` 把"二次 transition 防护"和"首次远端字段补写"两件事一起挡掉, 导致用户 scancel 后 sacct 的 `cd_remote_alloc_tres` / `cd_remote_exit_code` / `cd_remote_end_time` 永远是空 / 0, 财务对账丢失; ② §4.1 新增 `uint8_t cd_terminal_received` 字段做真幂等判据; §4.5 状态文件 pack/unpack 同步追加; ③ §7.4 重写为分支 A (本地仍 PENDING(Held), 走完整 jobcomp + completion_logger) + 分支 B (本地已终态 scancel 抢跑, 仅补写远端字段), 真幂等改用 `cd_terminal_received==1` 短路; ④ §7.4.1 新增 `_cd_dbd_modify_remote_fields()` 走 `acct_storage_g_job_modify` 单线 SQL UPDATE, 不重入 jobcomp 插件 / 不二次解锁依赖作业; ⑤ §9.2.1 新增 `dbd_job_modify_msg_t` 字段扩展 + `as_mysql_modify_job()` SET 子句 (NULL/0 跳过原则), 与 `as_mysql_job_complete` 通道并行; ⑥ §6.13 / §8.3 补充 `cd_cancel_propagated` 与 `cd_terminal_received` 双标志独立性说明 + scancel 异步补写时序 + 与远端自然终态竞争的边界场景; ⑦ §11.1 时序图扩展分支 B; §11.2 关键观察追加两条; §11.3 失败回滚契约表追加一行; §12.1 锁矩阵拆分 §7.4 A/B 两行 + 新增 `_cd_dbd_modify_remote_fields` 行; §12.2 并发风险表追加 jobcomp 重入风险与 broker 重发风险两行; ⑧ 动机: 用户视角的最终一致性 — scancel 立即返回, 远端字段在 broker 异步回告后 (T0+30s 内) 自动补全, 异步补写流程与本地终态写入互不干扰 |
| **MVP-v2.0** | **2026-05** | **★ 路由能力完整下沉到 broker, ctld 端大幅瘦身**: ① **删除 ctld 端路由决策**: `partition.SendTo` / `cd_remote_cluster` / `cd_remote_partition` 全部删除, `partition_record_t` 跨域字段从 4 个缩减为 1 个 (`cd_allow_remote`); 远端 cluster / 物理队列由 broker 加载 `routes.conf` + test-only 探测后, 通过首次 `UPDATE_REMOTE_STATE` 包告知 ctld 回写 (§7.3); ② **删除 ctld 端 AllowApp ACL**: `partition.AllowApp` / `cd_partition_allows_app()` 删除 (~60 LoC), 改由 broker `routes.conf::AllowApps=` 在路由阶段拦截; ③ **删除 ctld 端在途容量限流**: `cd_phys_cross_region_has_capacity()` 删除 (~80 LoC), 改由 broker `cap_check.c` 接管; ④ **用户 ACL 改为显式列**: 删除 `slurm_conf.cross_region_comment_tag`, 新增 `slurmdb_user_rec.remote_allowed` / `slurmdb_assoc_rec.remote_allowed` (uint16) 列 + DBD `user_table` / `<cluster>_assoc_table` schema 升级 + `sacctmgr remote_allowed` 子命令; `cd_user_allows_remote()` 改读显式列, 不再走 `comment LIKE '%allow_remote%'` 子串; ⑤ **新增 `cd_route_exhausted` 字段**: `job_record_t.cd_route_exhausted` (uint8) + 状态文件 pack/unpack + scontrol show job RouteExhausted= 回显 + DBD 列; broker 返回 5010/5011/5013 硬不可达错误时 ctld 一次性置位, 扫描热路径短路, 避免百万次重试 RPC; 运维 `scontrol update jobid=X CdRouteExhausted=0` 可解钉; ⑥ **新增 `partition.AllowRemote=yes/no` 显式开关**: 替代 v1.5 "是否配置 `SendTo`" 的隐式判断; ⑦ **`forward_job_msg_t` 协议瘦身**: 从 v1.5 的 9 字段 + nested `job_desc_msg_t` (~150 字段) 缩为 6 个原子字段 (jobid / uid / gid / user / 本地 cluster_name / 本地 partition / work_dir / script_path / cd_app_name); broker 不再需要 ctld 灌完整 `job_desc`, 自己 GET_JOB_INFO 反查; ⑧ **新增错误码** `ESLURM_CR_NO_VIABLE_ROUTE=5010` / `ESLURM_CR_TEST_ONLY_REJECTED=5011` / `ESLURM_CR_TEST_ONLY_TIMEOUT=5012` / `ESLURM_CR_ALL_ROUTES_EXHAUSTED=5013` / `ESLURM_CR_CAP_FULL_SOFT_WAIT=5020`; **删除** `ESLURM_CR_CROSS_REGION_CAP_FULL=5005` (容量下沉到 broker); ⑨ **跨域线程主体逻辑简化** (§6): `cd_handle_pending_job_locked` 从 v1.5 的 Step 1~7 简化为 v2.0 的 Step 1~6 (合并 Step 4 不再写远端字段); 候选过滤新增 `!cd_route_exhausted` + `partition.cd_allow_remote==1` 条件; 回滚分类增加 hard / 临时性两类 (`cd_revert_forward_hard()` ★ 新增); ⑩ §10 协议字段表 / §11 状态机图 / §12 锁矩阵 / §14 测试用例 / §15 升级路径 (含 v1.5→v2.0 增量升级 SOP + SQL 数据迁移脚本) 全面刷新; ⑪ 数据上: ctld LoC 从 ~2200 降到 ~1840 (-360), 27 文件; broker 复杂度相应提升 (具体见 `Broker详细设计文档MVP_v2.md`); ⑫ 动机: 让 ctld 只做"识别 / hold / 转发请求 / 回写", 把 "远端拓扑感知 / 容量决策 / test-only 探测 / 应用白名单" 等业务复杂度集中到 broker, 形成清晰的"调度大脑 vs 转发桥"分工 |

