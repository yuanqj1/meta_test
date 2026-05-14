# ctld-M08 squeue 客户端 (--remote) Checklist

> 配套: [doc/Slurmctld跨域详细设计文档MVP.md](../Slurmctld跨域详细设计文档MVP.md) §8.2
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: ctld-M03（`slurm_job_info_t.cd_remote_*`）
> 下游: ctld-M11

---

## 1. 模块目标

支持 `squeue --remote` 用一套针对跨域作业的默认 format 显示远端集群与远端 jobid。

```
$ squeue --remote
JOBID    PARTITION  NAME       USER   ST   TIME  REMOTE_CLUSTER  REMOTE_JOBID
12345    normal     hello.sh   alice  PD   0:00  clusterB        87654
```

## 2. 接口契约

### 2.1 默认 format

```
%.10i %.10P %.10j %.10u %.4T %.10M %.12RC %.10RJ
```

| 占位符 | 字段 |
|---|---|
| `%i` | jobid |
| `%P` | partition |
| `%j` | name |
| `%u` | user |
| `%T` | state |
| `%M` | time |
| **`%RC`** | remote_cluster |
| **`%RJ`** | remote_jobid |

### 2.2 占位符规则

- job==NULL 时 print 函数打表头（`REMOTE_CLUSTER` / `REMOTE_JOBID`）
- 字段为空时打 `-`
- 现有 squeue 占位符 `%R`（reason）不冲突——M08 用 `%RC` / `%RJ` 双字符占位符

---

## 3. 触及文件

| 文件 | 改动 |
|---|---|
| [src/squeue/squeue.h](../../src/squeue/squeue.h) | `params_t` 加 `bool cross_domain_view` |
| [src/squeue/opt.c](../../src/squeue/opt.c) | `long_options` 加 `--remote`；case 设默认 format |
| [src/squeue/print.c](../../src/squeue/print.c) | `job_format_options[]` 加 2 项；实现 2 个 print 函数 |
| [src/squeue/print.h](../../src/squeue/print.h) | print 函数原型 |

---

## 4. Checklist

### 4.1 参数定义

- [ ] M8-1 [src/squeue/squeue.h](../../src/squeue/squeue.h) `params_t` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        bool cross_domain_view;
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
- [ ] M8-4 case `LONG_OPT_REMOTE`：
    ```c
    params.cross_domain_view = true;
    if (!params.format)
        params.format = xstrdup(
            "%.10i %.10P %.10j %.10u %.4T %.10M %.12RC %.10RJ");
    ```

### 4.3 print 函数

- [ ] M8-5 [src/squeue/print.h](../../src/squeue/print.h) 加 2 个原型 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
    int _print_job_cd_remote_cluster(job_info_t *job, int width, bool right, char *suffix);
    int _print_job_cd_remote_job_id(job_info_t *job, int width, bool right, char *suffix);
    #endif
    ```
- [ ] M8-6 [src/squeue/print.c](../../src/squeue/print.c) 实现 `_print_job_cd_remote_cluster`：
    ```c
    int _print_job_cd_remote_cluster(job_info_t *job, int width, bool right, char *suffix)
    {
        if (job == NULL) {
            _print_str("REMOTE_CLUSTER", width, right, true);
        } else {
            _print_str(job->cd_remote_cluster_name ?: "-",
                       width, right, true);
        }
        if (suffix)
            printf("%s", suffix);
        return SLURM_SUCCESS;
    }
    ```
- [ ] M8-7 类似地实现 `_print_job_cd_remote_job_id`（用 `_print_int` + `cd_remote_job_id`，0 时打 `-`）

### 4.4 注册占位符

- [ ] M8-8 [src/squeue/print.c](../../src/squeue/print.c) `job_format_options[]` 加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        {"RC", "REMOTE_CLUSTER", _print_job_cd_remote_cluster, 12 },
        {"RJ", "REMOTE_JOBID",   _print_job_cd_remote_job_id,   8 },
    #endif
    ```
    > **注意**: 占位符表里通常按 ASCII 顺序排，但本仓库未严格强制；末尾加 ifdef 块即可

### 4.5 单元测试

- [ ] M8-9 `squeue --remote` 看到 8 列，全空作业打 `-`
- [ ] M8-10 `squeue --remote --format='%.5i %.10RC'` 自定义 format 仍生效
- [ ] M8-11 `squeue` 不带 --remote，行为完全不变

---

## 5. 验收标准

1. `squeue --remote` 在跨域作业 + 普通作业混合环境下打 8 列，普通作业的 RC/RJ 列显示 `-`
2. `squeue --json` 输出含 `cd_remote_cluster_name` 字段（M3 已加入 pack）
3. `squeue -h | grep remote` 看到帮助

## 6. 风险

- **风险 1**: 双字符占位符 `RC` / `RJ` 在 squeue 主 parser 是否支持。**降级**: 检查 `_parse_format_token`/`_parse_long_token` 是否支持多字符；若不支持，改用 `%RC` 单字符 + 子模式或写文档说明只能配合 `--remote` 默认 format 使用，第二阶段补
- **风险 2**: 某些 print 函数原型微调（如最新 master 加了 char 而不是 char*）。**降级**: 先 grep 一个最近添加的 print 函数（如 `_print_job_state_compact`）对齐签名
