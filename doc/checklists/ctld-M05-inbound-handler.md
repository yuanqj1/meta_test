# ctld-M05 跨域入站 Handler Checklist

> 配套: [doc/Slurmctld跨域详细设计文档MVP.md](../Slurmctld跨域详细设计文档MVP.md) §6
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: ctld-M01（msg_type / payload）、ctld-M03（cd_remote_* 字段）、ctld-M04（cross_domain.{c,h} 已就位）
> 下游: ctld-M11

---

## 1. 模块目标

ctld 接收 broker 主动推送的两类 RPC，把状态写回作业记录：

| msg_type | 触发时机 | ctld 动作 |
|---|---|---|
| 9102 `REQUEST_BROKER_UPDATE_REMOTE_STATE` | broker 收到远端 SUBMITTED / 周期 query | 写 `cd_remote_*` 5 字段 |
| 9103 `REQUEST_BROKER_TERMINAL_STATE` | broker 检测到远端作业终态 | 写 `cd_remote_*` + 同步 `job_state`/`end_time`/`exit_code` |

## 2. 接口契约

### 2.1 ACL

入口必须 `validate_slurm_user(msg->auth_uid)`，非 root / SlurmUser 一律回 `ESLURM_USER_ID_MISSING`。broker 与 ctld 同机 munge，auth_uid = SlurmUser。

### 2.2 找不到作业的处理

`find_job_record(src_job_id)` 返回 NULL 时回 `ESLURM_INVALID_JOB_ID`，不做任何写入；broker 收到此错误码后清掉本地 trace_id（M10 broker 清理后由 broker 端原生消化）。

### 2.3 终态同步语义

```c
job_ptr->cd_remote_state    = m->base.remote_state;
job_ptr->cd_remote_exit_code= m->remote_exit_code;
/* 终态同步本地状态 */
job_ptr->job_state = m->base.remote_state | (job_ptr->job_state & JOB_STATE_FLAGS);
job_ptr->end_time  = m->remote_end_time ? m->remote_end_time : time(NULL);
job_ptr->exit_code = m->remote_exit_code;
job_ptr->state_desc = NULL;  /* 清掉 CrossDomainQueued */
```

> **不**调用 `job_completion_logger()` —— 那会触发本地 epilog；本期作业本地无资源占用，留给第二阶段处理。

---

## 3. 触及文件

| 文件 | 改动 |
|---|---|
| `src/slurmctld/cross_domain.c` | 实现 2 个 handler |
| [src/slurmctld/proc_req.c](../../src/slurmctld/proc_req.c) | `slurmctld_req` 主 switch（8462 行）加 2 case |

---

## 4. Checklist

### 4.1 update handler

- [ ] M5-1 `cross_domain.c` 实现 `cross_domain_handle_update_remote_state(slurm_msg_t *msg)`：
    - ACL 检查
    - `broker_remote_state_msg_t *m = msg->data`
    - write_lock 找 job_ptr
    - `xfree + xstrdup` 写回 5 个字段：`cd_remote_trace_id`、`cd_remote_cluster_name`、`cd_remote_partition_name`、`cd_remote_job_id`、`cd_remote_state`
    - 释锁
    - `slurm_send_rc_msg(msg, SLURM_SUCCESS)`
- [ ] M5-2 失败路径：`slurm_send_rc_msg(msg, ESLURM_INVALID_JOB_ID)` 或 `ESLURM_USER_ID_MISSING`

### 4.2 terminal handler

- [ ] M5-3 `cross_domain.c` 实现 `cross_domain_handle_terminal_state(slurm_msg_t *msg)`：
    - ACL 检查
    - `broker_terminal_state_msg_t *m = msg->data`
    - write_lock 找 job_ptr
    - 调一个 helper `_cd_apply_remote_state(job_ptr, &m->base)` 同 M5-1 的 5 字段
    - 再写 `cd_remote_exit_code`
    - 同步 `job_state` / `end_time` / `exit_code` / `state_desc=NULL`（见 §2.3）
    - 释锁
    - `slurm_send_rc_msg(msg, SLURM_SUCCESS)`

### 4.3 提取 helper

- [ ] M5-4 `_cd_apply_remote_state(job_record_t *jp, broker_remote_state_msg_t *src)` 私有 helper，被 M5-1 / M5-3 共用，避免拷贝粘贴

### 4.4 proc_req 接入

- [ ] M5-5 [src/slurmctld/proc_req.c](../../src/slurmctld/proc_req.c) `#include "src/slurmctld/cross_domain.h"`
- [ ] M5-6 `slurmctld_req` 主 switch（8462 行函数体内）加 ifdef 块 + 2 case：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        case REQUEST_BROKER_UPDATE_REMOTE_STATE:
            cross_domain_handle_update_remote_state(msg);
            break;
        case REQUEST_BROKER_TERMINAL_STATE:
            cross_domain_handle_terminal_state(msg);
            break;
    #endif
    ```

### 4.5 测试

- [ ] M5-7 单元: 临时给 cross_domain.c 加 `#if 0` 测试入口，伪造 broker_remote_state_msg_t 调 handler，看 job_ptr 字段写入正确
- [ ] M5-8 端到端: M11 联调时 broker 9102/9103 推送 → ctld 端 squeue 看到 RemoteCluster/RemoteJobId/state 同步

---

## 5. 验收标准

1. broker 推送 9102 → 100ms 内 `squeue --remote` 显示 RemoteJobId 已填
2. broker 推送 9103 终态 RUNNING→COMPLETED → `squeue` 看到本地 state 同步到 COMPLETED + end_time + exit_code
3. broker 用普通用户 munge 推送 9102 → ctld 拒绝 + log `error("cross_domain: ACL deny uid=N")`

## 6. 风险

- **风险 1**: write_lock 持有时间过长（5 个 xfree + 5 个 xstrdup）。**降级**: 按 [locks.h](../../src/slurmctld/locks.h) 约定，job 字段写入这种短临界区是常态，可接受
- **风险 2**: 终态同步 `job_state` 时和本地调度器 race（本地未真正分配资源，但 `job_state=PENDING`，强写为 COMPLETED 可能撞 `job_completion_logger`）。**降级**: 本期跨域作业本地始终 PENDING + priority=0，不会被 backfill 选中，可强写
- **风险 3**: M10 未完成时 broker 仍走 BRKR 私有帧，9102/9103 入站不到 slurmctld_req。**降级**: M10 必须先于 M11 联调；M5 单元测试可临时让 broker M5-broker 的 [src/slurmbrokerd/egress.c](../../src/slurmbrokerd/egress.c) 的 4 个原 LEGACY 路径都改成走 native（即提前做 M10-2 一项即可）
