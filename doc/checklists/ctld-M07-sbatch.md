# ctld-M07 sbatch 客户端 Checklist

> 配套: [doc/Slurmctld跨域详细设计文档MVP.md](../Slurmctld跨域详细设计文档MVP.md) §8.1
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: ctld-M03（`job_desc_msg_t.cross_domain` / `cd_app_name`）
> 下游: ctld-M11

---

## 1. 模块目标

支持 `sbatch --cross-domain[=yes|no] --app=<name> /path/to/script.sh` 把跨域意图与软件标识透传给 ctld。`#SBATCH` 因复用 sbatch 的 long_options 数组自动支持。

```
$ cat /tmp/hello.slurm
#!/bin/bash
#SBATCH --cross-domain
#SBATCH --app=gromacs
echo hello

$ sbatch /tmp/hello.slurm
Submitted batch job 12345
```

## 2. 接口契约

### 2.1 选项语义

| 选项 | 参数性质 | 行为 |
|---|---|---|
| `--cross-domain` | optional `=yes/no/1/0/true/false` | 不带值默认 yes；写入 `job_desc.cross_domain=1` |
| `--app=<name>` | required argument | 写入 `job_desc.cd_app_name=<name>` |

### 2.2 与已有 `__METASTACK_OPT_APP` 的区分

仓库已有 `--app` 选项？查 [src/api/slurm_opt.c](../../src/api/slurm_opt.c) 中 `__METASTACK_OPT_APP` 块：

> 该块字段 `app/app_name/app_version/app_source` 是**特征参数**用于 app 识别（partition AllowApp ACL）。本模块的 `cd_app_name` 是**跨域用**专门字段，写入新字段 `job_desc->cd_app_name`，与已有 `app_name` 物理隔离。

> **决策**: 选项名也要避免冲突。已有 `--app` 是否被占用？需要在 [src/api/slurm_opt.c](../../src/api/slurm_opt.c) `slurm_options[]` grep 一遍。
> - 若 `--app` 已被 `__METASTACK_OPT_APP` 占用：本模块改用 `--cross-domain-app=<name>` 或 `--cd-app=<name>`，并相应更新 plan
> - 若未占用：直接复用 `--app`，但**新加 LONG_OPT 常量**避免和老 ifdef 块冲突

## 3. 触及文件

| 文件 | 改动 |
|---|---|
| [src/api/slurm_opt.c](../../src/api/slurm_opt.c) | 加 2 项 `LONG_OPT_*` + arg_set/get/reset |
| [src/api/slurm_opt.h](../../src/api/slurm_opt.h) | `slurm_opt_t` 加 2 字段 |
| [src/sbatch/sbatch.c](../../src/sbatch/sbatch.c) | `_usage()` 加 2 行帮助文本 |

---

## 4. Checklist

### 4.1 选项注册

- [ ] M7-1 [src/api/slurm_opt.c](../../src/api/slurm_opt.c) 内先 grep `--app` 看 `__METASTACK_OPT_APP` 是否已占用；占用则用 `--cd-app`，未占用则复用
- [ ] M7-2 加 2 个 `LONG_OPT_*` 常量（`LONG_OPT_CROSS_DOMAIN`、`LONG_OPT_CD_APP`）
- [ ] M7-3 `slurm_options[]` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        {"cross-domain", optional_argument, 0, LONG_OPT_CROSS_DOMAIN},
        {"cd-app",       required_argument, 0, LONG_OPT_CD_APP},
    #endif
    ```

### 4.2 arg_set / get / reset

- [ ] M7-4 实现 `arg_set_cross_domain(slurm_opt_t *opt, const char *arg)`：
    ```c
    if (!arg) opt->cross_domain = 1;
    else if (xstrcasestr(arg, "no") || xstrcasestr(arg, "0") || xstrcasestr(arg, "false"))
        opt->cross_domain = 0;
    else opt->cross_domain = 1;
    return SLURM_SUCCESS;
    ```
- [ ] M7-5 实现 `arg_set_cd_app(slurm_opt_t *opt, const char *arg)`：xfree + xstrdup
- [ ] M7-6 实现对应 `arg_get_*` / `arg_reset_*`
- [ ] M7-7 `slurm_options_cross_domain` / `slurm_options_cd_app` static `slurm_cli_opt_t` 表项绑定 set/get/reset/option 名

### 4.3 数据结构

- [ ] M7-8 [src/api/slurm_opt.h](../../src/api/slurm_opt.h) `slurm_opt_t` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        uint16_t  cross_domain;
        char     *cd_app_name;
    #endif
    ```
- [ ] M7-9 `slurm_opt_destroy` 同步 `xfree(cd_app_name)`
- [ ] M7-10 `slurm_opt_to_job_desc` 末尾追加：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        job_desc->cross_domain = opt->cross_domain;
        job_desc->cd_app_name  = xstrdup(opt->cd_app_name);
    #endif
    ```

### 4.4 帮助文本

- [ ] M7-11 [src/sbatch/sbatch.c](../../src/sbatch/sbatch.c) `_usage()` 在 `--`-options 列表合适位置追加：
    ```
        --cross-domain[=yes|no]   submit job to a remote cluster via brokerd
        --cd-app=<name>           software identifier used by remote lookup
    ```

### 4.5 单元测试

- [ ] M7-12 `sbatch --cross-domain --cd-app=test /tmp/hello.sh`：`scontrol show job <id>`（M9 完成后）看到 `CrossDomain=yes AppName=test`
- [ ] M7-13 不带 `--cross-domain`：`cross_domain=0`，cd_thread 不会扫到
- [ ] M7-14 `#SBATCH --cross-domain` 写在脚本里也能识别

---

## 5. 验收标准

1. `sbatch --cross-domain --cd-app=foo job.sh` 提交成功
2. `sbatch --cross-domain=no job.sh` 显式禁用，作业按普通作业走
3. `sbatch --help | grep cross-domain` 看到帮助行
4. 旧脚本（不写新选项）行为完全不变

## 6. 风险

- **风险 1**: `__METASTACK_OPT_APP` 的 `--app` 已占用导致命名冲突。**降级**: 用 `--cd-app`，相应改 plan 和 M3 字段名（`cd_app_name` 已是这个名字，对齐）
- **风险 2**: optional_argument 在 zsh/bash 行为差异（必须 `--cross-domain=yes` 不能 `--cross-domain yes`）。**降级**: 文档化提示，参考 sbatch 现有 `--exclusive` 同款写法
