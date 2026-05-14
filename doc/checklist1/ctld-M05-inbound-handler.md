# ctld-M05 跨域入站 Handler Checklist (v2.0)

> 配套: [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §7
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §1.8
> 依赖: ctld-M01（msg_type / payload `broker_update_remote_state_msg_t` / `broker_terminal_state_msg_t`）/ ctld-M03（cd_remote_* 字段含 `cd_terminal_received`）/ ctld-M04（cross_region.{c,h} 已就位）/ ctld-M12（dbd_job_modify_msg_t cd_remote_* 字段，分支 B 走它落库）
> 下游: ctld-M11

> **v1.5 → v2.0 关键变化**:
> 1. **模块文件**：handler 移到 `cross_region_rpc.c`（与 `cross_region.c` 分离），头文件用 `cross_region_rpc.h`
> 2. **`handle_broker_update_remote_state` 必须实现"首次状态包写 cluster/partition"逻辑**：v1.5 ctld 在 hold 时已写好这两个字段，v2.0 完全由 broker 决策后通过本 handler 首次写入
> 3. **`handle_broker_terminal_state` A/B 双分支**（MVP-1.5 已修订，v2.0 保留并完善）：
>    - 分支 A：本地仍 PENDING(Held) → 走完整终态写入（同步 job_state / start_time / end_time / exit_code，调 `jobcomp_g_record_job_end` + `job_completion_logger`）
>    - 分支 B：本地已终态（scancel 抢跑）→ 仅补写 cd_remote_* 字段，**不**调 jobcomp（避免重复写入），改走 `_cd_dbd_modify_remote_fields` SQL UPDATE 通道
> 4. **真幂等标志改用 `cd_terminal_received`**（不再用 `IS_JOB_FINISHED` 短路），避免 scancel 抢跑场景下远端字段永远写不进
> 5. **proc_req.c 仍是 2 个 case**：UPDATE_REMOTE_STATE + TERMINAL_STATE（与 v1.5 一致，msg_type 改为 8003/8004）

---

## 1. 模块目标

ctld 接收 broker 主动推送的两类 RPC，把状态写回作业记录：

| msg_type | 触发时机 | ctld 动作 |
|---|---|---|
| 8003 `REQUEST_BROKER_UPDATE_REMOTE_STATE` | broker 决策完成 / 远端 SUBMITTED / 周期 query | ★ v2.0 首次包写 cluster/partition；后续覆盖 5 字段 |
| 8004 `REQUEST_BROKER_TERMINAL_STATE` | broker 检测到远端作业终态 | A/B 分流：本地仍 Held → 完整终态写入；本地已终态 → 仅补写 cd_remote_* |

## 2. 接口契约

### 2.1 ACL

入口必须 `_cd_check_caller(msg)`：`g_slurm_auth_get_uid(msg->auth_cred) == slurm_conf.slurm_user_id`，非 SlurmUser 一律返回 `ESLURM_USER_ID_MISSING`。broker 与 ctld 同机部署，auth_uid 必须是 SlurmUser。

### 2.2 找不到作业的处理

`find_job_record(req->src_job_id)` 返回 NULL 时回 `ESLURM_INVALID_JOB_ID`，不做任何写入；broker 收到此错误码后清掉本地 trace_id（M10 broker 端原生消化）。

### 2.3 ★ v2.0 首次状态包语义

```c
bool first_state_pack = (job_ptr->cd_remote_cluster_name == NULL);

if (req->remote_cluster_name) {
    xfree(job_ptr->cd_remote_cluster_name);
    job_ptr->cd_remote_cluster_name = xstrdup(req->remote_cluster_name);
}
if (req->remote_partition_name) {
    xfree(job_ptr->cd_remote_partition_name);
    job_ptr->cd_remote_partition_name = xstrdup(req->remote_partition_name);
}

if (first_state_pack) {
    /* 写 state_desc 让用户立即看到远端位置 */
    xfree(job_ptr->state_desc);
    job_ptr->state_desc = xstrdup_printf("Forwarded_%s_%s",
        job_ptr->cd_remote_cluster_name ?: "?",
        job_ptr->cd_remote_partition_name ?: "?");
    info("cross_region: job %u routed to %s@%s by broker", ...);
}
```

> **协议字段约束**: broker 端在第一次发起 8003 时**必须**带上 `remote_cluster_name` 和 `remote_partition_name`；后续状态更新可重复带（幂等），但**不允许**中途改变这两个字段的值（一旦绑定就不变，直到作业终态）。

### 2.4 真幂等：`cd_terminal_received` 标志（MVP-1.5 修订）

```c
if (job_ptr->cd_terminal_received) {
    unlock_slurmctld(job_write_lock);
    debug("cross_region: terminal_state for job %u already consumed");
    return SLURM_SUCCESS;
}
/* ... 处理后 ... */
job_ptr->cd_terminal_received = 1;
```

**不要用** `IS_JOB_FINISHED(job_ptr)` 做短路，那是过度幂等会把"首次补写远端字段"也挡掉。

### 2.5 终态同步语义（A/B 分支）

| 字段 | 分支 A（本地 PENDING(Held)） | 分支 B（本地已终态） |
|---|---|---|
| `cd_remote_*`（8 字段） | ✅ 写入 | ✅ 写入 |
| `job_state` | ✅ 由 remote_state 映射 | ❌ 保持本地终态（如 CANCELLED） |
| `start_time` / `end_time` / `exit_code` | ✅ 同步 remote_* | ❌ 保持 |
| `tres_alloc_str` | ✅ 同步 cd_remote_alloc_tres | ❌ 保持 |
| `priority` | 维持 0 | ❌ 保持 |
| 调 `jobcomp_g_record_job_end` | ✅ | ❌（避免重复写入 sacct）|
| 调 `job_completion_logger` | ✅ | ❌（避免依赖作业二次解锁）|
| 写 sacct 远端字段 | ✅ 走 `dbd_job_complete_msg_t`（含 cd_remote_* 8 字段） | ✅ 走 `dbd_job_modify_msg_t` 仅 SQL UPDATE 8 列（详见 ctld-M12 §9.2.1） |

---

## 3. 触及文件

| 文件 | 改动 |
|---|---|
| `src/slurmctld/cross_region_rpc.c` | **新增**（约 250 LoC，2 个 handler + helper + DBD 通道桥接） |
| `src/slurmctld/cross_region_rpc.h` | **新增**（声明 2 个 handler）|
| [src/slurmctld/Makefile.am](../../src/slurmctld/Makefile.am) | `slurmctld_SOURCES` 追加 |
| [src/slurmctld/proc_req.c](../../src/slurmctld/proc_req.c) | `slurmctld_req` 主 switch 加 2 case |

---

## 4. Checklist

### 4.1 头文件 + 骨架

- [ ] M5-1 新建 `src/slurmctld/cross_region_rpc.h`：声明 `handle_broker_update_remote_state` / `handle_broker_terminal_state` + 内部 helper 转发原型；ifdef 包裹
- [ ] M5-2 新建 `src/slurmctld/cross_region_rpc.c`：file-level header + ifdef 块；include `cross_region.h` / `slurmctld.h` / `acct_storage.h` / `state_save.h`

### 4.2 ACL helper

- [ ] M5-3 实现 `_cd_check_caller(slurm_msg_t *msg)`（v2 设计 §7.2）：
    ```c
    static int _cd_check_caller(slurm_msg_t *msg)
    {
        uid_t uid = g_slurm_auth_get_uid(msg->auth_cred);
        if (uid != slurm_conf.slurm_user_id) {
            error("cross_region RPC from non-SlurmUser uid=%u, rejected", uid);
            return ESLURM_USER_ID_MISSING;
        }
        return SLURM_SUCCESS;
    }
    ```

### 4.3 update handler `handle_broker_update_remote_state` (★ v2.0 首次包语义)

- [ ] M5-4 实现 `handle_broker_update_remote_state(msg)`（详见 v2 设计 §7.3）：
    - ACL 检查
    - 取 `broker_update_remote_state_msg_t *req = msg->data`
    - 校验 `req->src_job_id`，0 则返回 `ESLURM_INVALID_JOB_ID`
    - write_lock `{job=W}`
    - `find_job_record(req->src_job_id)`，NULL 返回 `ESLURM_INVALID_JOB_ID`
    - 校验 `job_ptr->cd_forwarded`，0 时返回 `ESLURM_INVALID_JOB_STATE` + warning
- [ ] M5-5 ★ **v2.0 首次包写入** cluster/partition：
    ```c
    bool first_state_pack = (job_ptr->cd_remote_cluster_name == NULL);

    if (req->remote_cluster_name) {
        xfree(job_ptr->cd_remote_cluster_name);
        job_ptr->cd_remote_cluster_name = xstrdup(req->remote_cluster_name);
    }
    if (req->remote_partition_name) {
        xfree(job_ptr->cd_remote_partition_name);
        job_ptr->cd_remote_partition_name = xstrdup(req->remote_partition_name);
    }
    ```
- [ ] M5-6 写其它 5 字段：`cd_remote_trace_id` / `cd_remote_job_id` / `cd_remote_state` / `cd_remote_alloc_tres` / `cd_remote_start_time`（注意 trace_id / alloc_tres 都是 xfree + xstrdup）
- [ ] M5-7 `first_state_pack` 时写 `state_desc = xstrdup_printf("Forwarded_%s_%s", cluster, partition)` + `info("cross_region: job %u routed to %s@%s by broker")`
- [ ] M5-8 释 write_lock + `schedule_job_save()` + return `SLURM_SUCCESS`
- [ ] M5-9 失败路径：`return ESLURM_INVALID_JOB_ID` 或 `ESLURM_USER_ID_MISSING` 或 `ESLURM_INVALID_JOB_STATE`；外层 `slurmctld_req` 用 `slurm_send_rc_msg` 回包

### 4.4 terminal handler `handle_broker_terminal_state` (★ A/B 双分支)

- [ ] M5-10 实现入口（详见 v2 设计 §7.4）：
    - ACL 检查；`broker_terminal_state_msg_t *req = msg->data`
    - write_lock `{job=W, part=R, user=R}`
    - `find_job_record` + 校验 `cd_forwarded`（同 update handler）
    - **真幂等**：`if (cd_terminal_received) { unlock; return SUCCESS; }`
    - 决定分支：`bool already_finished = IS_JOB_FINISHED(job_ptr);`
- [ ] M5-11 **Step 1**（A/B 共有）写远端字段：
    ```c
    job_ptr->cd_remote_start_time = req->remote_start_time;
    job_ptr->cd_remote_end_time   = req->remote_end_time;
    job_ptr->cd_remote_exit_code  = req->remote_exit_code;
    job_ptr->cd_remote_state      = req->remote_state;
    if (req->remote_alloc_tres) {
        xfree(job_ptr->cd_remote_alloc_tres);
        job_ptr->cd_remote_alloc_tres = xstrdup(req->remote_alloc_tres);
    }
    ```
- [ ] M5-12 **分支 A** (`!already_finished`)：
    - 同步本地账单：`job_ptr->start_time = req->remote_start_time`；`end_time = req->remote_end_time`；`exit_code = req->remote_exit_code`
    - `xfree(job_ptr->tres_alloc_str); tres_alloc_str = xstrdup(req->remote_alloc_tres ?: "");`
    - 决定 `job_state`：switch `req->remote_state` 映射到 `JOB_COMPLETE` / `JOB_FAILED` / `JOB_CANCELLED` / `JOB_TIMEOUT` / `JOB_NODE_FAIL`
    - 走 Slurm 内部完成路径（v2 设计 §7.4 (d)）：
        ```c
        job_ptr->job_state = job_state | JOB_COMPLETING;
        jobcomp_g_record_job_end(job_ptr);
        job_completion_logger(job_ptr, false);
        job_ptr->job_state = job_state;
        ```
    - 终态保持 `priority = 0`
    - `info("cross_region: job %u terminal=%s remote_jobid=%u exit=%u (branch A)")`
- [ ] M5-13 **分支 B** (`already_finished`)：
    - **不**调 `jobcomp_g_record_job_end` / `job_completion_logger`（避免 jobcomp 插件二次写入 + 依赖作业二次解锁）
    - 调 `_cd_dbd_modify_remote_fields(job_ptr)` 走 SQL UPDATE 通道补写远端字段（详见 §4.5）
    - `info("cross_region: job %u local-terminal arrived first; remote fields backfilled (branch B)")`
- [ ] M5-14 **Step 2**（A/B 共有）：`job_ptr->cd_terminal_received = 1`；释锁 + `schedule_job_save()` + return `SLURM_SUCCESS`

### 4.5 分支 B 字段补写专用通道 `_cd_dbd_modify_remote_fields`

- [ ] M5-15 实现 `_cd_dbd_modify_remote_fields(job_record_t *job_ptr)`（详见 v2 设计 §7.4.1）：
    ```c
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
        int rc = acct_storage_g_job_modify(acct_db_conn, &modify);
        if (rc != SLURM_SUCCESS)
            warning("cross_region: dbd modify remote fields failed for job %u: %s",
                    job_ptr->job_id, slurm_strerror(rc));
    }
    ```
- [ ] M5-16 该 helper 调用上下文：write_lock 内（与 §4.4 Step 1 在同一锁；`acct_storage_g_job_modify` 走异步通道，不会再申请锁）
- [ ] M5-17 `dbd_job_modify_msg_t` 8 字段扩展由 ctld-M12 §9.2.1 提供，本模块只调用接口

### 4.6 hard 回滚 DBD 落库桥接 `_cd_dbd_modify_route_exhausted`

- [ ] M5-18 实现 `_cd_dbd_modify_route_exhausted(uint32_t job_id)`（供 ctld-M04 `cd_revert_forward_hard` 调用）：
    - 与 §4.5 类似走 `acct_storage_g_job_modify`，仅设 `modify.cd_route_exhausted = 1`
    - `dbd_job_modify_msg_t` 需新增 `cd_route_exhausted` 字段（uint8_t）；由 ctld-M12 完成 schema 升级

### 4.7 proc_req 接入

- [ ] M5-19 [src/slurmctld/proc_req.c](../../src/slurmctld/proc_req.c) `#include "src/slurmctld/cross_region_rpc.h"`
- [ ] M5-20 `slurmctld_req` 主 switch 加 ifdef 块 + 2 case：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        case REQUEST_BROKER_UPDATE_REMOTE_STATE:
            rc = handle_broker_update_remote_state(msg);
            slurm_send_rc_msg(msg, rc);
            break;
        case REQUEST_BROKER_TERMINAL_STATE:
            rc = handle_broker_terminal_state(msg);
            slurm_send_rc_msg(msg, rc);
            break;
    #endif
    ```
- [ ] M5-21 [src/slurmctld/Makefile.am](../../src/slurmctld/Makefile.am) `slurmctld_SOURCES` 追加 `cross_region_rpc.c cross_region_rpc.h`

### 4.8 测试

- [ ] M5-22 单元测试：用 `inject_broker_update_remote_state` mock 工具（参考 broker `inject_broker_forward.c`）注入一个 8003 消息（含 `remote_cluster_name=wz_cluster` / `remote_partition_name=wzhcnormal`），验证 ctld 端 `cd_remote_cluster_name` / `cd_remote_partition_name` 写入正确，且 `state_desc=Forwarded_wz_cluster_wzhcnormal`
- [ ] M5-23 第二次 8003 注入（`remote_cluster_name` 字段相同），验证 `cd_remote_*` 覆盖且 `state_desc` 不重复改（`first_state_pack=false`）
- [ ] M5-24 注入 8004 TERMINAL_STATE 在 PENDING 作业上（**分支 A**）：验证 `job_state=COMPLETED`、`exit_code=0`、`tres_alloc_str` 同步、`cd_terminal_received=1`，sacct -j 看到 Remote_State=COMPLETED
- [ ] M5-25 先 scancel 同作业（本地 CANCELLED）后注入 8004（**分支 B**）：验证 `job_state` 保持 CANCELLED，`cd_remote_*` 字段补写，`cd_terminal_received=1`，sacct -j 看到 State=CANCELLED + Remote_State=COMPLETED
- [ ] M5-26 重复注入相同 8004（**真幂等**）：第二次 handler 直接 return SUCCESS，无副作用
- [ ] M5-27 用普通用户 munge 推送 8003：ctld 拒绝 + log `cross_region RPC from non-SlurmUser uid=N`

---

## 5. 验收标准

1. broker 推送 8003 → 100ms 内 `squeue --remote` 显示 `REMOTE_CLUSTER` / `REMOTE_PARTITION` 已填，`scontrol show job` 看到 `state_desc=Forwarded_<cluster>_<partition>`
2. broker 推送 8004 终态 RUNNING→COMPLETED（**分支 A**）→ `squeue` 看到本地 state 同步到 COMPLETED + end_time + exit_code，sacct 远端 8 字段齐全
3. scancel 抢跑后 broker 8004 到达（**分支 B**）→ 本地 State=CANCELLED 不变，远端字段通过 SQL UPDATE 补写到 sacct
4. 重复 8004 → 第二次直接幂等返回，sacct 不会重复行
5. broker 用普通用户 munge 推送 8003 → ctld 拒绝 + log `error("cross_region RPC from non-SlurmUser uid=N")`
6. 首次 8003 之前作业的 `cd_remote_cluster_name` / `cd_remote_partition_name` 始终为 NULL（M4 在 hold 时不写）

## 6. 风险

- **风险 1**: write_lock 持有时间过长（10+ 个 xfree + xstrdup + 调 `jobcomp_g_record_job_end` + `job_completion_logger`）。**降级**: 按 [locks.h](../../src/slurmctld/locks.h) 约定，job 字段写入 + jobcomp 是常态调用；持锁时间预估 < 5ms，可接受
- **风险 2**: 分支 B `_cd_dbd_modify_remote_fields` 内部又申请 DBD 锁 → 死锁。**降级**: `acct_storage_g_job_modify` 走异步队列，不持 ctld lock，安全
- **风险 3**: M10 broker 端未改名时仍走 BRKR 私有帧，8003/8004 入站不到 slurmctld_req。**降级**: M10 必须先于 M11 联调；M5 单元测试可临时让 broker 端 [src/slurmbrokerd/egress.c](../../src/slurmbrokerd/egress.c) 4 个原 LEGACY 路径都改成走 slurm-native（即提前做 M10-2 一项）
- **风险 4**: 分支 A 调 `jobcomp_g_record_job_end` 触发 jobcomp filetxt 二次写入。**降级**: 24.05 已有保护（`JOB_COMPLETING` 标志位），同一作业 jobcomp 只触发一次；通过 §4.4 M5-12 严格按"COMPLETING → record → logger → 清 COMPLETING"序列调用即可
- **风险 5**: broker 实现 bug：8003 第一次包**未**带 `remote_cluster_name`（只带状态），导致 ctld 永远看不到远端位置。**降级**: M5-7 在 `first_state_pack && cluster_name==NULL` 时打 warning（"first state pack without cluster_name, broker bug?"），让运维介入
- **风险 6**: `dbd_job_modify_msg_t` 字段 `cd_remote_*` 在 ctld-M12 完成前不可用。**降级**: ctld-M05 / ctld-M12 同 sprint 推进，M5-15 / M5-18 在 M12 完成前用 `#warning "ctld-M12 dbd_job_modify_msg_t fields pending"` 占位
