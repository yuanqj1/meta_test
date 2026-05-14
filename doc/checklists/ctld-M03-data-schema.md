# ctld-M03 跨域数据结构扩展 Checklist

> 配套: [doc/Slurmctld跨域详细设计文档MVP.md](../Slurmctld跨域详细设计文档MVP.md) §4
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: ctld-M01（payload struct 已有 `forward_job_msg_t` 等）
> 下游: ctld-M04 / ctld-M05 / ctld-M07 / ctld-M08 / ctld-M09

---

## 1. 模块目标

把跨域元数据贯穿 3 层数据结构：

```mermaid
graph LR
    A["job_desc_msg_t (用户提交)"] --> B["job_record_t (ctld 内存)"]
    B --> C["slurm_job_info_t (squeue 客户端)"]
```

每一层加字段都要同步 init / free / pack / unpack 4 个步骤，否则会出现"包未配套"的内存泄漏或解码 buffer underflow。

## 2. 字段表（**单一源头**）

| 层 | 字段 | 类型 | 说明 |
|---|---|---|---|
| `job_desc_msg_t` | `cross_domain` | uint16_t | 用户标记 0/1 |
| `job_desc_msg_t` | `cd_app_name` | char* | --app=xxx 值（命名加 `cd_` 前缀避开已有 `app_name`） |
| `job_record_t` | `cd_cross_domain` | uint16_t | submit 时 = `desc.cross_domain` |
| `job_record_t` | `cd_app_name` | char* | submit 时 = `desc.cd_app_name` |
| `job_record_t` | `cd_forwarded` | uint8_t | bit0=已转发, bit1=cancel 已传播 |
| `job_record_t` | `cd_remote_trace_id` | char* | broker 9101 返回 |
| `job_record_t` | `cd_remote_cluster_name` | char* | broker 9102 写入 |
| `job_record_t` | `cd_remote_partition_name` | char* | broker 9102 写入 |
| `job_record_t` | `cd_remote_job_id` | uint32_t | broker 9102 写入 |
| `job_record_t` | `cd_remote_state` | uint32_t | JOB_PENDING/RUNNING/... |
| `job_record_t` | `cd_remote_exit_code` | int32_t | broker 9103 写入 |
| `slurm_job_info_t` | 同 job_record 前 7 字段（不要 `cd_forwarded` 和 `cd_remote_exit_code`） | | squeue --remote 显示 |

> **砍刀**: 原设计文档 §4.1 的 `cd_cancel_propagated` / `cd_terminal_received` / `cd_remote_alloc_tres` / `cd_remote_start_time` / `cd_remote_end_time` 一律推迟到第二阶段。`cd_forwarded` 用 bit1 兼任 cancel_propagated。

> **砍刀 2**: `_dump_job_state` / `_load_job_state` 本期不动，接受 ctld 重启丢跨域字段。在 `job_record` 字段定义处加 `// TODO(cd-stage2): persist via dump/load_job_state` 标注即可。

---

## 3. 触及文件 + 行号锚点

| 文件 | 锚点 |
|---|---|
| [slurm/slurm.h](../../slurm/slurm.h) | `} job_desc_msg_t;`（2374 行）/ `} slurm_job_info_t;`（2583 行） |
| [src/slurmctld/slurmctld.h](../../src/slurmctld/slurmctld.h) | `struct job_record { ... };` 末尾（2672 行 `};`） |
| [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) | `_pack_job_desc_msg`（12323 行）/ `_unpack_job_desc_msg`（13222 行）/ `_pack_job_info_members` / `_unpack_job_info_members`（5350 行）`META_3_2_PROTOCOL_VERSION` 分支末尾 |
| [src/api/job_info.c](../../src/api/job_info.c) | `slurm_free_job_info_members` |
| [src/slurmctld/job_mgr.c](../../src/slurmctld/job_mgr.c) | `_create_job_record` / `_list_delete_job` / `_pack_default_job_info` / `_pack_pending_job_details` |
| [src/common/slurm_protocol_defs.c](../../src/common/slurm_protocol_defs.c) | `slurm_init_job_desc_msg` / `slurm_free_job_desc_msg` |

---

## 4. Checklist

### 4.1 job_desc_msg_t（用户提交层）

- [ ] M3-1 [slurm/slurm.h](../../slurm/slurm.h) `job_desc_msg_t` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        uint16_t  cross_domain;
        char     *cd_app_name;
    #endif
    ```
- [ ] M3-2 [src/common/slurm_protocol_defs.c](../../src/common/slurm_protocol_defs.c) `slurm_init_job_desc_msg` 加 `job_desc->cross_domain = 0; job_desc->cd_app_name = NULL;`
- [ ] M3-3 同文件 `slurm_free_job_desc_msg` 加 `xfree(msg->cd_app_name);`
- [ ] M3-4 [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) `_pack_job_desc_msg` `META_3_2_PROTOCOL_VERSION` 分支末尾追加：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        pack16(job_desc_ptr->cross_domain, buffer);
        packstr(job_desc_ptr->cd_app_name, buffer);
    #endif
    ```
- [ ] M3-5 `_unpack_job_desc_msg` 同位置 `safe_unpack16` / `safe_unpackstr`

### 4.2 job_record_t（ctld 内存层）

- [ ] M3-6 [src/slurmctld/slurmctld.h](../../src/slurmctld/slurmctld.h) `struct job_record` 末尾追加 ifdef 块（9 字段，顺序见 §2）+ TODO 注释
- [ ] M3-7 [src/slurmctld/job_mgr.c](../../src/slurmctld/job_mgr.c) `_create_job_record` 末尾从 `job_desc` 拷贝：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        job_ptr->cd_cross_domain = job_desc->cross_domain;
        job_ptr->cd_app_name     = xstrdup(job_desc->cd_app_name);
    #endif
    ```
- [ ] M3-8 同文件 `_list_delete_job`（或 `_purge_job_record`）加 7 个 `xfree`：`cd_app_name`、`cd_remote_trace_id`、`cd_remote_cluster_name`、`cd_remote_partition_name`

### 4.3 slurm_job_info_t（squeue 客户端层）

- [ ] M3-9 [slurm/slurm.h](../../slurm/slurm.h) `slurm_job_info_t` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        uint16_t  cross_domain;
        char     *cd_app_name;
        char     *cd_remote_trace_id;
        char     *cd_remote_cluster_name;
        char     *cd_remote_partition_name;
        uint32_t  cd_remote_job_id;
        uint32_t  cd_remote_state;
    #endif
    ```
- [ ] M3-10 [src/api/job_info.c](../../src/api/job_info.c) `slurm_free_job_info_members` 加 4 个 `xfree`
- [ ] M3-11 [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) `_pack_job_info_members` `META_3_2_PROTOCOL_VERSION` 分支末尾追加：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        pack16(jp->cross_domain, buffer);
        packstr(jp->cd_app_name, buffer);
        packstr(jp->cd_remote_trace_id, buffer);
        packstr(jp->cd_remote_cluster_name, buffer);
        packstr(jp->cd_remote_partition_name, buffer);
        pack32(jp->cd_remote_job_id, buffer);
        pack32(jp->cd_remote_state, buffer);
    #endif
    ```
- [ ] M3-12 `_unpack_job_info_members` 同位置 unpack（顺序对应）
- [ ] M3-13 [src/slurmctld/job_mgr.c](../../src/slurmctld/job_mgr.c) `_pack_default_job_info` / `_pack_pending_job_details`（squeue 数据装配路径）把 `job_ptr->cd_*` 装填到 packed buffer

### 4.4 编译与单元验证

- [ ] M3-14 `make -j` 通过；ctld + squeue + sbatch 都能 link
- [ ] M3-15 普通 `sbatch /tmp/job.sh` 提交，`scontrol show job <id>` 不报错（跨域字段不打印）；`squeue --json` 中 `cross_domain=0`、`cd_app_name=null`

---

## 5. 验收标准

1. 普通作业链路完全不受影响（回归 squeue / scontrol / sbatch / scancel 各 1 次）
2. `sbatch --cross-domain --app=test /tmp/job.sh`（M7 完成后）→ `squeue --json` 中 `cross_domain=1`、`cd_app_name="test"`
3. valgrind 跑 1 次普通 sbatch + scontrol show job + 取消，无新泄漏

## 6. 风险

- **风险 1**: 字段命名 `cd_app_name` 与 `__METASTACK_OPT_APP` 块的 `app_name` 视觉接近。**降级**: 严格用 `cd_` 前缀，所有跨域字段同前缀好辨识
- **风险 2**: `_pack_job_desc_msg` 的 META 分支太长，新增 2 行容易栖到错误的 `if/else`。**降级**: 在 `pack16/packstr` 之前先写 `/* === Cross-Domain (cd-stage1) === */` 标记
