# ctld-M01 跨域协议注册 Checklist

> 配套: [doc/Slurmctld跨域详细设计文档MVP.md](../Slurmctld跨域详细设计文档MVP.md) §3
> 设计: [doc/Broker详细设计文档MVP_new.md](../Broker详细设计文档MVP_new.md) §6
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: 无（关键路径起点）
> 下游: ctld-M03 / ctld-M04 / ctld-M05 / ctld-M06 / ctld-M10

> **关键勘误**: 原 plan 的 msg_type 数值 8001-8004 已被 [src/common/msg_type.h](../../src/common/msg_type.h) 中 `RESPONSE_SLURM_RC=8001`、`RESPONSE_SLURM_RC_MSG=8002`、`RESPONSE_SLURM_REROUTE_MSG=8003` 占用，本模块改用 **9100-9104** 一段干净空闲区。broker 内部 `BROKERD_REQUEST_BROKER_CANCEL=8016`（broker→broker 私有 BRKR 帧）保留不动。

---

## 1. 模块目标

把 5 个跨域 RPC 注册到 slurm-native pack/unpack/free 大 switch，使 ctld 与本地 broker 走标准 Slurm RPC 帧（munge 认证 + `slurm_msg_t`）通讯。这是"chicken-and-egg"问题的解开点：broker 端 4 个 `LEGACY_M04_TRANSITIONAL` 块在本模块完成后即可由 ctld-M10 删除。

## 2. 接口契约

### 2.1 msg_type 数值表

| 数值 | slurm-native 名 | broker 旧名（M10 删） | 方向 | payload |
|---|---|---|---|---|
| 9100 | `REQUEST_FORWARD_JOB` | `BROKERD_REQUEST_FORWARD_JOB` | ctld → broker | `forward_job_msg_t` |
| 9101 | `RESPONSE_FORWARD_JOB` | `BROKERD_RESPONSE_FORWARD_JOB` | broker → ctld | `forward_job_resp_msg_t` |
| 9102 | `REQUEST_BROKER_UPDATE_REMOTE_STATE` | `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` | broker → ctld | `broker_remote_state_msg_t` |
| 9103 | `REQUEST_BROKER_TERMINAL_STATE` | `BROKERD_REQUEST_BROKER_TERMINAL_STATE` | broker → ctld | `broker_terminal_state_msg_t` |
| 9104 | `REQUEST_BROKER_CANCEL` | `BROKERD_REQUEST_BROKER_CANCEL`（peer 仍 8016） | ctld → broker | `broker_cancel_msg_t` |

### 2.2 payload struct（字段顺序逐字节复制 [src/slurmbrokerd/proto.h](../../src/slurmbrokerd/proto.h)，去 `brokerd_` 前缀）

```c
typedef struct {
    uint32_t   src_job_id;
    uint32_t   src_uid;
    uint32_t   src_gid;
    char      *src_user_name;
    char      *target_cluster;
    char      *src_work_dir;
    char      *script_path;
    char      *account;
    char      *cd_app_name;     /* 与 job_desc_msg_t.cd_app_name 同名 */
    job_desc_msg_t *job_desc;
} forward_job_msg_t;

typedef struct {
    uint32_t   error_code;
    char      *trace_id;
} forward_job_resp_msg_t;

typedef struct {
    uint32_t   src_job_id;
    char      *trace_id;
    char      *remote_cluster_name;
    char      *remote_partition_name;
    uint32_t   remote_job_id;
    uint32_t   remote_state;
    char      *remote_alloc_tres;
    time_t     remote_start_time;
} broker_remote_state_msg_t;

typedef struct {
    broker_remote_state_msg_t base;
    time_t     remote_end_time;
    int32_t    remote_exit_code;
} broker_terminal_state_msg_t;

typedef struct {
    uint32_t   src_job_id;
    char      *trace_id;
} broker_cancel_msg_t;
```

### 2.3 ifdef 包裹

所有新增代码统一包 `#ifdef __METASTACK_NEW_CROSS_DOMAIN ... #endif`；宏定义放在 [slurm/slurm.h](../../slurm/slurm.h) 已有 `__METASTACK_NEW_*` 块附近。

---

## 3. 触及文件 + 行号锚点

| 文件 | 锚点 | 改动 |
|---|---|---|
| [src/common/msg_type.h](../../src/common/msg_type.h) | `RESPONSE_SLURM_REROUTE_MSG`（约 298 行）与 `RESPONSE_FORWARD_FAILED=9001`（300 行）之间 | 加 5 个枚举值 |
| [src/common/msg_type.c](../../src/common/msg_type.c) | `msg_types[]` 末尾（约 289 行） | 加 5 个 `ENTRY()` |
| [src/common/slurm_protocol_defs.h](../../src/common/slurm_protocol_defs.h) | `ctld_list_msg_t`（约 1308 行）之后 | 加 5 个 typedef struct |
| [src/common/slurm_protocol_defs.h](../../src/common/slurm_protocol_defs.h) | free 声明区（约 1742 行 `slurm_free_ctld_multi_msg` 附近） | 加 5 个 `extern void slurm_free_*_msg` |
| [src/common/slurm_protocol_defs.c](../../src/common/slurm_protocol_defs.c) | `slurm_free_msg_data` 大 switch（约 5117 行起） | 实现 5 个 free + 加 5 case |
| [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) | `pack_msg`（22793 行）/ `unpack_msg`（23507 行）大 switch | 加 5 对 static helper + 主 switch 各 5 case |

---

## 4. Checklist

### 4.1 msg_type 注册

- [ ] M1-1 [src/common/msg_type.h](../../src/common/msg_type.h) 在 `RESPONSE_SLURM_REROUTE_MSG` 与 `RESPONSE_FORWARD_FAILED=9001` 之间加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        REQUEST_FORWARD_JOB                = 9100,
        RESPONSE_FORWARD_JOB               = 9101,
        REQUEST_BROKER_UPDATE_REMOTE_STATE = 9102,
        REQUEST_BROKER_TERMINAL_STATE      = 9103,
        REQUEST_BROKER_CANCEL              = 9104,
    #endif
    ```
- [ ] M1-2 [src/common/msg_type.c](../../src/common/msg_type.c) `msg_types[]` 末尾追加 ifdef 块，5 个 `ENTRY()`

### 4.2 payload struct 与 free 声明

- [ ] M1-3 [src/common/slurm_protocol_defs.h](../../src/common/slurm_protocol_defs.h) `ctld_list_msg_t` 之后追加 ifdef 块，5 个 typedef struct（字段顺序与 §2.2 完全一致）
- [ ] M1-4 同文件 free 声明区追加 5 个 `extern void slurm_free_*_msg(<type>_t *msg)`

### 4.3 free 实现 + dispatcher

- [ ] M1-5 [src/common/slurm_protocol_defs.c](../../src/common/slurm_protocol_defs.c) 实现 5 个 `slurm_free_*_msg`：
    - `slurm_free_forward_job_msg`：xfree 字符串字段 + `slurm_free_job_desc_msg(m->job_desc)`
    - `slurm_free_forward_job_resp_msg`：xfree `trace_id`
    - `slurm_free_broker_remote_state_msg`：xfree 4 个 char*
    - `slurm_free_broker_terminal_state_msg`：调 `slurm_free_broker_remote_state_msg(&m->base)` 后整体 xfree
    - `slurm_free_broker_cancel_msg`：xfree `trace_id`
- [ ] M1-6 `slurm_free_msg_data()` 大 switch 加 5 case

### 4.4 pack/unpack

- [ ] M1-7 [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) 加 5 对 `_pack_*` / `_unpack_*` static helper（参考 [src/slurmbrokerd/proto_pack.c:168-235、485-572](../../src/slurmbrokerd/proto_pack.c)，**不写** `META_3_2_PROTOCOL_VERSION` 分支——这 5 个 msg_type 在更早版本不存在，对端是 broker 而非旧 slurm）
- [ ] M1-8 主 `pack_msg` 大 switch 加 5 case
- [ ] M1-9 主 `unpack_msg` 大 switch 加 5 case

### 4.5 编译检查

- [ ] M1-10 `./configure && make -j src/common/` 通过，无 warning
- [ ] M1-11 `grep -nE "REQUEST_FORWARD_JOB|REQUEST_BROKER_(UPDATE|TERMINAL|CANCEL)" src/common/` 必须出现在 4 个 .c/.h 文件
- [ ] M1-12 `nm libslurm_pack.* | grep slurm_free_forward_job_msg` 有符号

---

## 5. 验收标准

1. `make -j` 全树 0 error；ctld 与 broker 都能启动
2. 从 broker 端发一个 9100 + 默认 forward_job_msg_t，ctld 端能 unpack 并打印（M5 模块未完成前可临时在 `slurmctld_req` 加 `printk` 验证）
3. `scontrol ping` 仍正常（验证未破坏现有 RPC 路径）

## 6. 风险与降级

- **风险 1**: `_pack_forward_job_msg` 中 `_pack_job_desc_msg` 是 file-static。**降级**: 套 broker 同款 `pack_msg(REQUEST_SUBMIT_BATCH_JOB)` wrapper 走主 dispatcher
- **风险 2**: 新枚举 9100 撞到未来 slurm 升级。**降级**: 9100-9104 是当前 schedmd master 主线尚未占用的区间；若未来冲突，统一改为 `__META_PROTOCOL` 区段
