# ctld-M07 sbatch 客户端 Checklist (v2.0)

> 配套: [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §5.1 / §5.2 / §5.3 / §5.4
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §1.6
> 依赖: ctld-M03（`job_desc_msg_t.cross_region` / `app_name`）
> 下游: ctld-M11

> **v1.5 → v2.0 关键变化**:
> 1. **选项名统一为 `--allow-remote`**（v1.5 是 `--cross-domain`），与 v2 设计文档 §5.1 一致
> 2. **应用名选项统一为 `--app=<name>`**（不再用 `--cd-app`）；`slurm_opt_t` / `job_desc_msg_t` 的字段名也用 `app_name`（v1.5 是 `cd_app_name`）；`job_record_t` 内部仍叫 `cd_app_name`（防 Slurm 已有字段冲突，详见 ctld-M03）
> 3. **`--allow-remote=yes/no/true/false/0/1` 完整支持**：未带值默认 yes
> 4. **`#SBATCH --allow-remote` / `#SBATCH --app=<name>` 自动支持**（24.05 `_get_next_opt()` 复用 `slurm_options[]`）

---

## 1. 模块目标

支持 `sbatch --allow-remote[=yes|no] --app=<name> /path/to/script.sh` 把跨域意图与软件标识透传给 ctld。`#SBATCH` 因复用 sbatch 的 long_options 数组自动支持。

```
$ cat /tmp/hello.slurm
#!/bin/bash
#SBATCH --allow-remote
#SBATCH --app=lammps-2Aug2023-intelmpi2018
echo hello

$ sbatch /tmp/hello.slurm
Submitted batch job 12345
```

## 2. 接口契约

### 2.1 选项语义

| 选项 | 参数性质 | 行为 |
|---|---|---|
| `--allow-remote` | optional `=yes/no/1/0/true/false` | 不带值默认 yes；写入 `job_desc.cross_region=1`；语义=用户意图（实际是否被跨域线程候选还要看 `partition.AllowRemote` + `user/assoc.remote_allowed`） |
| `--app=<name>` | required argument | 写入 `job_desc.app_name=<name>`，broker `routes.conf` 段 `AllowApps=` 用此匹配 |

### 2.2 与已有 `__METASTACK_OPT_APP` 的区分

仓库可能已有 `--app` 选项（旧版 OPT_APP 块）：

> v2 设计文档 §5.1 明确使用 `--app`，与 v1.5 的 `--cd-app` 命名不同。需在 [src/api/slurm_opt.c](../../src/api/slurm_opt.c) 内 grep 确认：
>
> - 若 `--app` 已被 `__METASTACK_OPT_APP` 占用：评审决策选 (a) 复用同一选项（v2 跨域字段共享 `app_name`），或 (b) 用 `--cross-region-app=<name>` 别名
> - 若未占用：直接复用 `--app`，新加 LONG_OPT 常量避免和老 ifdef 块冲突

> **本 checklist 默认决策**: `--app` 未被占用 → 直接复用，新增 `LONG_OPT_APP` 与 `__METASTACK_NEW_CROSS_DOMAIN` 块独立挂载

### 2.3 `slurm_opt_t` 新字段

```c
#ifdef __METASTACK_NEW_CROSS_DOMAIN
    uint16_t  cross_region;
    char     *app_name;
#endif
```

## 3. 触及文件

| 文件 | 改动 |
|---|---|
| [src/api/slurm_opt.c](../../src/api/slurm_opt.c) | 加 2 项 `LONG_OPT_*` + arg_set/get/reset + slurm_options[] 表项 + slurm_opt_to_job_desc 映射 |
| [src/api/slurm_opt.h](../../src/api/slurm_opt.h) 或 [src/common/slurm_opt.h](../../src/common/slurm_opt.h) | `slurm_opt_t` 加 2 字段 |
| [src/sbatch/sbatch.c](../../src/sbatch/sbatch.c) | `_usage()` 加 2 行帮助文本 |

---

## 4. Checklist

### 4.1 选项注册

- [ ] M7-1 [src/api/slurm_opt.c](../../src/api/slurm_opt.c) 内先 `rg "^\\s*\\{[^,]*\"app\"" src/api/slurm_opt.c` 确认 `--app` 是否被 `__METASTACK_OPT_APP` 占用
- [ ] M7-2 加 2 个 `LONG_OPT_*` 常量：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
    #define LONG_OPT_CROSS_REGION   <空闲值, 复用 LONG_OPT_* 段>
    #define LONG_OPT_APP            <空闲值>
    #endif
    ```
- [ ] M7-3 `slurm_options[]` 末尾追加 ifdef 块（详见 v2 设计 §5.1）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
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
    #endif
    ```

### 4.2 arg_set / get / reset

- [ ] M7-4 实现 `arg_set_cross_region(slurm_opt_t *opt, const char *arg)`（v2 设计 §5.1）：
    ```c
    static int arg_set_cross_region(slurm_opt_t *opt, const char *arg)
    {
        if (!arg || !xstrcasecmp(arg, "yes") || !xstrcasecmp(arg, "true")
            || !xstrcasecmp(arg, "1"))
            opt->cross_region = 1;
        else if (!xstrcasecmp(arg, "no") || !xstrcasecmp(arg, "false")
                 || !xstrcasecmp(arg, "0"))
            opt->cross_region = 0;
        else {
            error("Invalid --allow-remote value: %s (yes|no)", arg);
            return SLURM_ERROR;
        }
        return SLURM_SUCCESS;
    }
    ```
- [ ] M7-5 实现 `arg_set_app_name(slurm_opt_t *opt, const char *arg)`：
    ```c
    static int arg_set_app_name(slurm_opt_t *opt, const char *arg)
    {
        xfree(opt->app_name);
        opt->app_name = xstrdup(arg);
        return SLURM_SUCCESS;
    }
    ```
- [ ] M7-6 实现对应 `arg_get_cross_region` / `arg_get_app_name`（返回字符串表示，便于 `--export-options`）
- [ ] M7-7 实现 `arg_reset_cross_region` / `arg_reset_app_name`：
    ```c
    static void arg_reset_cross_region(slurm_opt_t *opt) { opt->cross_region = 0; }
    static void arg_reset_app_name(slurm_opt_t *opt)
    {
        xfree(opt->app_name);
        opt->app_name = NULL;
    }
    ```

### 4.3 数据结构

- [ ] M7-8 `slurm_opt_t` 头文件末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        uint16_t  cross_region;
        char     *app_name;
    #endif
    ```
- [ ] M7-9 `slurm_opt_destroy`（或 `slurm_reset_all_options` 中的 free 路径）同步 `xfree(opt->app_name)`
- [ ] M7-10 `slurm_opt_to_job_desc` 末尾追加映射（v2 设计 §5.2）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        desc->cross_region = opt_local->cross_region;
        desc->app_name     = xstrdup(opt_local->app_name);
    #endif
    ```

### 4.4 帮助文本

- [ ] M7-11 [src/sbatch/sbatch.c](../../src/sbatch/sbatch.c) `_usage()` 在 `--`-options 列表合适位置追加（v2 设计 §5.4）：
    ```
        --allow-remote[=yes|no]   Allow cross-region forwarding of this job to broker
        --app=<name>              Application full name (with version) for cross-region routing
    ```
- [ ] M7-12 同样在 [src/salloc/opt.c](../../src/salloc/opt.c) / [src/srun/opt.c](../../src/srun/opt.c) 的 `_usage()` 中追加（24.05 共享 `slurm_options[]` 的二进制都需要展示帮助；如果某些客户端不支持跨域，至少 sbatch + salloc 要展示）

### 4.5 单元测试

- [ ] M7-13 `sbatch --allow-remote --app=lammps-test /tmp/hello.sh`：`scontrol show job <id>`（M9 完成后）看到 `CrossRegion=yes AppName=lammps-test`
- [ ] M7-14 不带 `--allow-remote`：`cross_region=0`，cd_thread 不会扫到（条件 `cd_cross_region != 1` 短路）
- [ ] M7-15 `#SBATCH --allow-remote` 写在脚本里也能识别（24.05 `_get_next_opt()` 自动复用）
- [ ] M7-16 `sbatch --allow-remote=no`：`cross_region=0`
- [ ] M7-17 `sbatch --allow-remote=invalid`：报错 `Invalid --allow-remote value: invalid`
- [ ] M7-18 `sbatch --help | grep -E "allow-remote|^\s+--app="` 看到 2 行帮助
- [ ] M7-19 `sbatch --allow-remote --app=test /tmp/hello.sh; squeue --json -j <id> | jq '.jobs[].cross_region'` 输出 `1`（验证 ctld-M03 pack 路径）

---

## 5. 验收标准

1. `sbatch --allow-remote --app=foo job.sh` 提交成功，`squeue --json` 中 `cross_region=1` / `app_name="foo"`
2. `sbatch --allow-remote=no job.sh` 显式禁用，作业按普通作业走
3. `sbatch --help | grep allow-remote` 看到帮助行
4. 旧脚本（不写新选项）行为完全不变
5. `#SBATCH --allow-remote` 在脚本头部能识别

## 6. 风险

- **风险 1**: `__METASTACK_OPT_APP` 的 `--app` 已占用导致命名冲突。**降级**: 评审后选 (a) 复用同一选项 + 字段共享 `app_name`，或 (b) 用 `--cross-region-app=<name>` 别名 + ctld-M03 字段名 `cd_app_name` 不变
- **风险 2**: optional_argument 在 zsh/bash 行为差异（`--allow-remote=yes` 必须用 `=`，不能空格）。**降级**: 文档化提示，参考 sbatch 现有 `--exclusive` 同款写法
- **风险 3**: `app_name` 字段值在 `slurm_opt_to_job_desc` xstrdup(NULL) 安全性。**降级**: `xstrdup(NULL)` 在 Slurm xmalloc 系列已经是 NULL-safe，回 NULL 即可
- **风险 4**: 旧 v1.5 用户脚本写 `#SBATCH --cross-domain` 升级后失效。**降级**: 在 `arg_set_*` 路径不需要兼容旧选项名（用户主动迁移），文档 + 升级指南显式说明
