# ctld-M01 跨域协议注册 Checklist (v2.0)

> 配套: [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §7.5 / §7.6 / §10
> 配套: [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §6
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §1.8 / §1.11
> 依赖: 无（关键路径起点）
> 下游: ctld-M03 / ctld-M04 / ctld-M05 / ctld-M06 / ctld-M10

> **v1.5 → v2.0 关键变化**:
> 1. **协议号统一为 8001-8005**（与 broker 端 `src/slurmbrokerd/proto.h:56-79` 已使用的 LEGACY 占位值保持一致），不再使用 v1.5 的 9100-9104 临时段
> 2. **`forward_job_msg_t` 大幅瘦身**：删除 `target_cluster` / `target_partition` / `account` / `job_desc`；新增 `src_cluster_name` / `src_partition`；`app_name` 改名为 `cd_app_name`
> 3. **`broker_update_remote_state_msg_t` / `broker_terminal_state_msg_t` 新增 `remote_cluster_name` / `remote_partition_name`**：broker 决策完成后由首次状态包告知 ctld
> 4. **新增 8018/8019 占位**：broker→broker `TEST_ONLY` 探测，ctld 进程**不**注册 handler，仅占位防 ID 冲突

---

## 1. 模块目标

把 5 个跨域 RPC 注册到 slurm-native pack/unpack/free 大 switch，并预留 broker→broker 的 8018/8019 ID。完成本模块后，broker 侧 4 个 `LEGACY_M04_TRANSITIONAL` 块由 ctld-M10 统一改名 + 删除。

## 2. 接口契约

### 2.1 msg_type 数值表 (v2.0)

| 数值 | slurm-native 名 | broker 旧名（M10 改名/删） | 方向 | payload |
|---|---|---|---|---|
| 8001 | `REQUEST_FORWARD_JOB` | `BROKERD_REQUEST_FORWARD_JOB` | ctld → broker | `forward_job_msg_t` (v2 瘦身) |
| 8002 | `RESPONSE_FORWARD_JOB` | `BROKERD_RESPONSE_FORWARD_JOB` | broker → ctld | `forward_job_resp_msg_t` |
| 8003 | `REQUEST_BROKER_UPDATE_REMOTE_STATE` | `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` | broker → ctld | `broker_update_remote_state_msg_t` |
| 8004 | `REQUEST_BROKER_TERMINAL_STATE` | `BROKERD_REQUEST_BROKER_TERMINAL_STATE` | broker → ctld | `broker_terminal_state_msg_t` |
| 8005 | `REQUEST_BROKER_CANCEL` | `BROKERD_REQUEST_BROKER_CANCEL` (peer 仍 8016) | ctld → broker | `broker_cancel_msg_t` |
| 8018 | `REQUEST_BROKER_TEST_ONLY` (★ v2.0 新增占位) | `BROKERD_REQUEST_BROKER_TEST_ONLY` | broker → broker | broker 内部，ctld 不注册 |
| 8019 | `RESPONSE_BROKER_TEST_ONLY` (★ v2.0 新增占位) | `BROKERD_RESPONSE_BROKER_TEST_ONLY` | broker → broker | broker 内部，ctld 不注册 |

> **占用确认**: `src/common/msg_type.h` 中 `RESPONSE_SLURM_RC=8001/8002/8003` 等占用是 v1.5 checklist 的勘误，**v2.0 设计直接选 8001-8005 段**（broker 侧 `src/slurmbrokerd/proto.h:56-79` 已使用此段）。M01-1 实施前先用 `grep -n "= 80[01][0-9]" src/common/msg_type.h` 确认无冲突；若现版本占用，按规则改用 8050-8054 段并同步 broker 侧 `proto.h`。

### 2.2 payload struct（v2.0 字段顺序与 [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §7.5 完全一致）

```c
/* === ctld → broker 转发请求 (v2 大幅瘦身, 6 字段) === */
typedef struct {
    uint32_t  src_job_id;
    uint32_t  src_uid;
    uint32_t  src_gid;
    char     *src_user_name;
    char     *src_cluster_name;     /* ★ v2.0 新增: 本地 cluster 名, broker 查 routes.conf 用 */
    char     *src_partition;        /* ★ v2.0 改名: 本地 partition (不是 target!) */
    char     *src_work_dir;
    char     *script_path;
    char     *cd_app_name;          /* ★ v2.0 改名 (原 app_name), 与 job_desc_msg_t 字段同名 */
    /* === v2.0 已删字段 ===
     *   - char *target_cluster        (broker route_decide 决策)
     *   - char *target_partition      (broker route_decide 决策)
     *   - char *account               (远端 sacctmgr default account 接管)
     *   - job_desc_msg_t *job_desc    (broker 自己 GET_JOB_INFO 反查 / 读 dump 的 script)
     */
} forward_job_msg_t;

/* === broker → ctld 转发响应 === */
typedef struct {
    uint32_t  error_code;           /* 0 / 5010 / 5011 / 5013 / 5020 等 (见 §11.1) */
    char     *error_message;
    char     *trace_id;
    /* ★ v2.0: 不带 selected_cluster/selected_partition; 通过后续 UPDATE_REMOTE_STATE 异步告知 */
} forward_job_resp_msg_t;

/* === broker → ctld 周期状态推送 === */
typedef struct {
    uint32_t  src_job_id;
    char     *trace_id;
    char     *remote_cluster_name;  /* ★ v2.0 关键: 首次包必填, ctld 用此写入 cd_remote_cluster_name */
    char     *remote_partition_name;/* ★ v2.0 关键: 首次包必填 */
    uint32_t  remote_job_id;
    uint32_t  remote_state;         /* 复用 JOB_STATE_* */
    char     *remote_alloc_tres;
    time_t    remote_start_time;
} broker_update_remote_state_msg_t;

/* === broker → ctld 终态推送 === */
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

/* === ctld → broker 反向取消 === */
typedef struct {
    uint32_t  src_job_id;
    char     *trace_id;
    char     *reason;               /* "user_cancel" / "admin_cancel" 等 */
} broker_cancel_msg_t;
```

### 2.3 ifdef 包裹

所有新增代码统一包 `#ifdef __METASTACK_NEW_CROSS_DOMAIN ... #endif`；宏定义放在 [slurm/slurm.h](../../slurm/slurm.h) 已有 `__METASTACK_NEW_*` 块附近。

### 2.4 协议版本守恒

`_pack_*_msg` / `_unpack_*_msg` 全部走 `if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION)` 包装；由于这 5 个 msg_type 在更早版本不存在（对端是 broker 而非旧 slurm），不需要 `META_3_2_PROTOCOL_VERSION` 多分支。

---

## 3. 触及文件 + 行号锚点

| 文件 | 锚点 | 改动 |
|---|---|---|
| [src/common/msg_type.h](../../src/common/msg_type.h) | `RESPONSE_SLURM_REROUTE_MSG` 之后 / `RESPONSE_FORWARD_FAILED=9001` 之前；先 grep 8001-8019 段确认无冲突 | 加 7 个枚举值（5 个 ctld 用 + 2 个 broker→broker 占位） |
| [src/common/msg_type.c](../../src/common/msg_type.c) | `msg_types[]` 末尾 | 加 7 个 `ENTRY()` |
| [src/common/slurm_protocol_defs.h](../../src/common/slurm_protocol_defs.h) | `ctld_list_msg_t` 之后 | 加 5 个 typedef struct（v2 字段顺序见 §2.2） |
| [src/common/slurm_protocol_defs.h](../../src/common/slurm_protocol_defs.h) | free 声明区（`slurm_free_ctld_multi_msg` 附近） | 加 5 个 `extern void slurm_free_*_msg` |
| [src/common/slurm_protocol_defs.c](../../src/common/slurm_protocol_defs.c) | `slurm_free_msg_data` 大 switch | 实现 5 个 free + 加 5 case |
| [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) | `pack_msg` / `unpack_msg` 大 switch | 加 5 对 static helper + 主 switch 各 5 case |

---

## 4. Checklist

### 4.1 msg_type 注册

- [ ] M1-1 [src/common/msg_type.h](../../src/common/msg_type.h) 加 ifdef 块（先 grep 确认 8001-8019 段未被官方占用；若冲突按 §2.1 改 8050-8054 + 8068/8069）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        REQUEST_FORWARD_JOB                    = 8001,
        RESPONSE_FORWARD_JOB                   = 8002,
        REQUEST_BROKER_UPDATE_REMOTE_STATE     = 8003,
        REQUEST_BROKER_TERMINAL_STATE          = 8004,
        REQUEST_BROKER_CANCEL                  = 8005,
        /* 8006~8017 broker→broker 私有 BRKR 帧 (broker 侧定义, ctld 不注册) */
        REQUEST_BROKER_TEST_ONLY               = 8018,  /* 占位防冲突 */
        RESPONSE_BROKER_TEST_ONLY              = 8019,  /* 占位防冲突 */
    #endif
    ```
- [ ] M1-2 [src/common/msg_type.c](../../src/common/msg_type.c) `msg_types[]` 末尾追加 ifdef 块，7 个 `ENTRY()`（含 8018/8019 占位）

### 4.2 payload struct 与 free 声明

- [ ] M1-3 [src/common/slurm_protocol_defs.h](../../src/common/slurm_protocol_defs.h) 加 ifdef 块，**5 个 typedef struct**（字段顺序与 §2.2 完全一致；尤其注意 `forward_job_msg_t` 删除 4 字段、新增 2 字段，`broker_update_remote_state_msg_t` / `broker_terminal_state_msg_t` 必须包含 `remote_cluster_name` / `remote_partition_name`）
- [ ] M1-4 同文件 free 声明区追加 5 个 `extern void slurm_free_*_msg(<type>_t *msg)`

### 4.3 free 实现 + dispatcher

- [ ] M1-5 [src/common/slurm_protocol_defs.c](../../src/common/slurm_protocol_defs.c) 实现 5 个 `slurm_free_*_msg`：
    - `slurm_free_forward_job_msg`：xfree 8 个 char*（`src_user_name` / `src_cluster_name` / `src_partition` / `src_work_dir` / `script_path` / `cd_app_name`）；**v2 不再调** `slurm_free_job_desc_msg`
    - `slurm_free_forward_job_resp_msg`：xfree `error_message` / `trace_id`
    - `slurm_free_broker_update_remote_state_msg`：xfree 5 个 char*（含 `remote_cluster_name` / `remote_partition_name`）
    - `slurm_free_broker_terminal_state_msg`：xfree 5 个 char*（同上 + 复用即可）
    - `slurm_free_broker_cancel_msg`：xfree `trace_id` / `reason`
- [ ] M1-6 `slurm_free_msg_data()` 大 switch 加 5 case；8018/8019 占位**不需要** free（ctld 不收）

### 4.4 pack/unpack

- [ ] M1-7 [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) 加 5 对 `_pack_*` / `_unpack_*` static helper：
    - `_pack_forward_job_msg`：6 字段（v2 瘦身），**字段顺序严格对照** §2.2 `forward_job_msg_t`
    - `_pack_forward_job_resp_msg`：3 字段（pack32 + 2 packstr）
    - `_pack_broker_update_remote_state_msg`：8 字段（含 `remote_cluster_name` / `remote_partition_name` 两个 packstr）
    - `_pack_broker_terminal_state_msg`：10 字段（在 update 基础上 +2：`remote_end_time` / `remote_exit_code`）
    - `_pack_broker_cancel_msg`：3 字段
    - 全部包 `if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION)`
- [ ] M1-8 主 `pack_msg` 大 switch 加 5 case
- [ ] M1-9 主 `unpack_msg` 大 switch 加 5 case
- [ ] M1-10 8018/8019 在 `pack_msg` / `unpack_msg` **不加 case**（broker 进程内部用 BRKR 帧，不走 slurm-native）

### 4.5 编译检查

- [ ] M1-11 `./configure && make -j src/common/` 通过，无 warning
- [ ] M1-12 `rg -n "REQUEST_FORWARD_JOB|REQUEST_BROKER_(UPDATE|TERMINAL|CANCEL|TEST_ONLY)" src/common/` 至少出现在 4 个 .c/.h 文件
- [ ] M1-13 `nm libslurm_pack.* | grep slurm_free_forward_job_msg` 有符号
- [ ] M1-14 `nm libslurm_pack.* | grep slurm_free_broker_update_remote_state_msg` 有符号

---

## 5. 验收标准

1. `make -j` 全树 0 error；ctld 与 broker 都能启动
2. 从 broker 端 8001 入站一个 v2 字段顺序的 `forward_job_msg_t`，ctld 端能 unpack（`src_job_id` / `src_cluster_name` / `src_partition` / `cd_app_name` 6 字段对齐）
3. broker 端 8003 推送（含 `remote_cluster_name=wz_cluster`），ctld 端在 M5 完成后能写入 `cd_remote_cluster_name` 字段
4. `scontrol ping` 仍正常（验证未破坏现有 RPC 路径）

## 6. 风险与降级

- **风险 1**: 8001-8005 与现版本 schedmd 主线冲突。**降级**: 改为 8050-8054 + 8068/8069 段，broker 侧 `src/slurmbrokerd/proto.h:56-79` 同步刷新（注意 8016 `BROKERD_REQUEST_BROKER_CANCEL` peer→peer 私有保留）
- **风险 2**: `forward_job_msg_t` 字段顺序与 broker `proto.h::brokerd_forward_job_msg_t` 不一致 → unpack 错位。**降级**: 严格对照 [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §7.5 + [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §6.3 双侧字段顺序，写在 ctld-M10 改名前必须先做 broker 端 unpack 自测
- **风险 3**: v2 删除 `job_desc` 后 broker 端 `_pack_job_desc/_unpack_job_desc` 死代码。**降级**: ctld-M10 M10-11 中删除该 helper（无引用即删）
