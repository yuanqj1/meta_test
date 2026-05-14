# ctld-M08 squeue 客户端 (--remote) Checklist (v2.0)

> 配套: [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §5.5
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §1.6
> 依赖: ctld-M03（`slurm_job_info_t.cd_remote_*` 11 字段）
> 下游: ctld-M11

> **v1.5 → v2.0 关键变化**:
> 1. **占位符从 2 个扩展到 8 个**：v1.5 只支持 `%RC` / `%RJ`，v2 支持完整 8 个字段（设计文档 §5.5.2）
> 2. **`squeue_params_t` 字段名** `cross_region_view`（v1.5 是 `cross_domain_view`）
> 3. **默认 format 加长**：`%.10i %.10P %.10j %.10u %.4T %.10M %.15RC %.12RJ %.12RS %.30RT`

---

## 1. 模块目标

支持 `squeue --remote` 用一套针对跨域作业的默认 format 显示远端集群、远端 jobid、远端状态、远端 TRES。

```
$ squeue --remote
JOBID    PARTITION  NAME      USER   ST   TIME   REMOTE_CLUSTER   REMOTE_JOBID   REMOTE_STATE   REMOTE_TRES
12345    xahcnormal hello.sh  alice  PD   0:00   wz_cluster        87654          RUNNING        cpu=32,mem=128G
```

## 2. 接口契约

### 2.1 默认 format（v2.0）

```
%.10i %.10P %.10j %.10u %.4T %.10M %.15RC %.12RJ %.12RS %.30RT
```

| 占位符 | 字段 |
|---|---|
| `%i` | jobid |
| `%P` | partition（本地） |
| `%j` | name |
| `%u` | user |
| `%T` | state（本地） |
| `%M` | time |
| **`%RC`** | remote_cluster |
| **`%RJ`** | remote_jobid |
| **`%RS`** | remote_state |
| **`%RT`** | remote_alloc_tres |

### 2.2 完整 8 个 `%R*` 占位符（设计文档 §5.5.2）

| 占位符 | 标题 | 字段 | 字段大小 |
|---|---|---|---|
| `%RC` | REMOTE_CLUSTER | cd_remote_cluster_name | 8 |
| `%RP` | REMOTE_PARTITION | cd_remote_partition_name | 8 |
| `%RJ` | REMOTE_JOBID | cd_remote_job_id | 8 |
| `%RS` | REMOTE_STATE | cd_remote_state（`job_state_string()`） | 8 |
| `%RT` | REMOTE_TRES | cd_remote_alloc_tres | 16 |
| `%Rs` | REMOTE_START | cd_remote_start_time | 16 |
| `%Re` | REMOTE_END | cd_remote_end_time | 16 |
| `%Rx` | REMOTE_EXIT | cd_remote_exit_code | 8 |

> **占位符规则**：
> - job==NULL 时 print 函数打表头
> - 字段为空时打 `-`
> - 现有 squeue 占位符 `%R`（reason）不冲突 — `%RC` / `%RJ` / `%RP` / `%RS` / `%RT` / `%Rs` / `%Re` / `%Rx` 是双字符占位符

---

## 3. 触及文件

| 文件 | 改动 |
|---|---|
| [src/squeue/squeue.h](../../src/squeue/squeue.h) | `squeue_params_t` 加 `bool cross_region_view` |
| [src/squeue/opt.c](../../src/squeue/opt.c) | `long_options` 加 `--remote`；case 设默认 format |
| [src/squeue/print.c](../../src/squeue/print.c) | `job_format_options[]` 加 8 项；实现 8 个 print 函数 |
| [src/squeue/print.h](../../src/squeue/print.h) | print 函数原型 |

---

## 4. Checklist

### 4.1 参数定义

- [ ] M8-1 [src/squeue/squeue.h](../../src/squeue/squeue.h) `squeue_params_t` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        bool cross_region_view;
    #endif
    ```

### 4.2 选项解析

- [ ] M8-2 [src/squeue/opt.c](../../src/squeue/opt.c) 加 `LONG_OPT_REMOTE`（参考已有 LONG_OPT_*）
- [ ] M8-3 `long_options` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        {"remote", no_argument, 0, LONG_OPT_REMOTE},
    #endif
    ```
- [ ] M8-4 case `LONG_OPT_REMOTE`（v2 设计 §5.5.1）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        case LONG_OPT_REMOTE:
            params.cross_region_view = true;
            if (!params.format)
                params.format = xstrdup(
                    "%.10i %.10P %.10j %.10u %.4T %.10M "
                    "%.15RC %.12RJ %.12RS %.30RT");
            break;
    #endif
    ```

### 4.3 print 函数原型 + 实现

- [ ] M8-5 [src/squeue/print.h](../../src/squeue/print.h) 加 8 个原型 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
    int _print_job_cd_remote_cluster(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_partition(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_job_id(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_state(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_alloc_tres(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_start(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_end(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_exit_code(job_info_t *job, int width, bool right, char *suffix);
    #endif
    ```
- [ ] M8-6 [src/squeue/print.c](../../src/squeue/print.c) 实现 `_print_job_cd_remote_cluster`（v2 设计 §5.5.2）：
    ```c
    int _print_job_cd_remote_cluster(job_info_t *job, int width, bool right_justify, char *suffix)
    {
        if (job == NULL)
            _print_str("REMOTE_CLUSTER", width, right_justify, true);
        else
            _print_str(job->cd_remote_cluster_name ?: "-",
                       width, right_justify, true);
        if (suffix) printf("%s", suffix);
        return SLURM_SUCCESS;
    }
    ```
- [ ] M8-7 类似实现 `_print_job_cd_remote_partition`：表头 `REMOTE_PARTITION`，字段 `cd_remote_partition_name`
- [ ] M8-8 实现 `_print_job_cd_remote_job_id`：表头 `REMOTE_JOBID`，`cd_remote_job_id == 0` 时打 `-`，否则用 `_print_int`
- [ ] M8-9 实现 `_print_job_cd_remote_state`：
    ```c
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
    ```
- [ ] M8-10 实现 `_print_job_cd_remote_alloc_tres`：表头 `REMOTE_TRES`，字段 `cd_remote_alloc_tres`，空时 `-`
- [ ] M8-11 实现 `_print_job_cd_remote_start` / `_print_job_cd_remote_end`：表头 `REMOTE_START` / `REMOTE_END`，用 `slurm_make_time_str` 格式化（0 时打 `Unknown`）
- [ ] M8-12 实现 `_print_job_cd_remote_exit_code`：表头 `REMOTE_EXIT`，用 `WEXITSTATUS` / `WTERMSIG` 双字段输出

### 4.4 注册占位符

- [ ] M8-13 [src/squeue/print.c](../../src/squeue/print.c) `job_format_options[]` 加 ifdef 块（**8 项**）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        {"RC",  "REMOTE_CLUSTER",   _print_job_cd_remote_cluster,      8 },
        {"RP",  "REMOTE_PARTITION", _print_job_cd_remote_partition,    8 },
        {"RJ",  "REMOTE_JOBID",     _print_job_cd_remote_job_id,       8 },
        {"RS",  "REMOTE_STATE",     _print_job_cd_remote_state,        8 },
        {"RT",  "REMOTE_TRES",      _print_job_cd_remote_alloc_tres,  16 },
        {"Rs",  "REMOTE_START",     _print_job_cd_remote_start,       16 },
        {"Re",  "REMOTE_END",       _print_job_cd_remote_end,         16 },
        {"Rx",  "REMOTE_EXIT",      _print_job_cd_remote_exit_code,    8 },
    #endif
    ```

### 4.5 双字符占位符 parser 验证

- [ ] M8-14 `_parse_format_token` / `_parse_long_token`（24.05 实现可能不同）支持双字符 token？grep `field_name_2` / `field_name_long` 确认；若不支持，按 v2 设计 §5.5.2 补一个 helper 来识别 `%RC` / `%RJ` 等（目前 24.05 的 `parse_format_token_long` 已支持多字符 long token）

### 4.6 单元测试

- [ ] M8-15 `squeue --remote` 跨域作业 + 普通作业混合，普通作业的 RC/RJ/RS/RT 列显示 `-`
- [ ] M8-16 `squeue --remote --format='%.5i %.10RC %.30RT'` 自定义 format 仍生效
- [ ] M8-17 `squeue` 不带 --remote，行为完全不变（4 列默认 format）
- [ ] M8-18 `squeue --remote -u alice` 过滤指定用户跨域作业
- [ ] M8-19 `squeue --json` 输出含 `cd_remote_cluster_name` / `cd_remote_partition_name` 等 8 字段（M3 已加入 pack）

---

## 5. 验收标准

1. `squeue --remote` 在跨域作业 + 普通作业混合环境下打 10 列，普通作业的 `REMOTE_*` 列显示 `-`
2. `squeue --json` 输出含完整 11 个跨域字段（M3 已加入 pack；含 `cd_route_exhausted`）
3. `squeue -h | grep remote` 看到帮助
4. forwarded 但 broker 还没回 8003 的作业 `squeue --remote` 显示 `REMOTE_CLUSTER=- REMOTE_PARTITION=-`（首次状态包未到，字段为空）
5. broker 8003 到达后 `squeue --remote` 立即显示 `REMOTE_CLUSTER=wz_cluster REMOTE_PARTITION=wzhcnormal`

## 6. 风险

- **风险 1**: 双字符占位符 `RC` / `RJ` 在 squeue 主 parser 是否支持。**降级**: 若 `_parse_format_token` 仅支持单字符，按 v2 设计 §5.5.2 改用 24.05 已有的 `parse_long_token` 入口；详见 v2 设计 §5.5.2 注释
- **风险 2**: 某些 print 函数原型微调（如最新 master 加了 char 而不是 char*）。**降级**: 先 grep 一个最近添加的 print 函数（如 `_print_job_state_compact`）对齐签名
- **风险 3**: `field_size` 默认值 8 太小。**降级**: 用户用 `%.15RC` 自定义即可；v2 设计文档默认 format 已用 `%.15RC %.12RJ %.12RS %.30RT` 加宽
- **风险 4**: `--remote` 与已有 `--cluster=remote` 等历史参数冲突。**降级**: 24.05 `--cluster=NAME` 是 federated cluster 概念，与 `--remote` 语义不同；保持选项名独立即可
