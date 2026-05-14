# ctld-M09 scontrol show job/partition 跨域行 Checklist (v2.0)

> 配套: [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §3.3 / §5.6
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §1.4 / §1.6
> 依赖: ctld-M03（`slurm_job_info_t.cd_remote_*` + `cd_route_exhausted`；partition `cd_allow_remote`）
> 下游: ctld-M11

> **v1.5 → v2.0 关键变化**:
> 1. **`scontrol show job` 字段从 5 个扩展到完整 11 个**（含 `RouteExhausted=YES/NO`、`RemoteAllocTRES`、`RemoteStartTime` / `RemoteEndTime`、`RemoteExitCode`、`RemoteTraceId`）
> 2. **`scontrol show job` 头行加 `RouteExhausted=YES/NO`**（设计文档 §5.6）
> 3. **`scontrol show partition` 输出 `AllowRemote=YES/NO`**（替代 v1.5 显示 `SendTo=` / `AllowApp=` 两行）
> 4. **`scontrol update jobid=<JID> CdRouteExhausted=0|1`**（重置/钉死）：scontrol 客户端解析在 ctld-M06 已处理

---

## 1. 模块目标

### 1.1 `scontrol show job <id>` 跨域作业显示完整信息

```
JobId=12345 ...
   ...
   CrossRegion=yes AppName=lammps-2Aug2023-intelmpi2018 RouteExhausted=NO
   RemoteCluster=wz_cluster RemotePartition=wzhcnormal
   RemoteJobId=87654 RemoteState=RUNNING
   RemoteAllocTRES=cpu=32,mem=128G,node=1
   RemoteStartTime=2026-05-14T10:00:00 RemoteEndTime=Unknown
   RemoteExitCode=N/A
   RemoteTraceId=xian_cluster-12345
   ...
```

普通作业完全不显示这些行，避免噪声。

### 1.2 `scontrol show partition <name>` 显示 `AllowRemote`

```
PartitionName=xahcnormal ...
   AllowRemote=YES
   ...
```

## 2. 接口契约

### 2.1 job 显示格式（设计文档 §5.6）

```c
xstrcat(out, "   ");
xstrfmtcat(out, "CrossRegion=%s ", job_ptr->cross_region ? "yes" : "no");
if (job_ptr->app_name)
    xstrfmtcat(out, "AppName=%s ", job_ptr->app_name);
xstrfmtcat(out, "RouteExhausted=%s",            /* ★ v2.0 新增 */
           job_ptr->cd_route_exhausted ? "YES" : "NO");
xstrcat(out, "\n");

if (job_ptr->cd_remote_cluster_name) {
    /* RemoteCluster + RemotePartition 一行 */
    /* RemoteJobId + RemoteState 一行 */
    /* RemoteAllocTRES 一行 (非空时) */
    /* RemoteStartTime + RemoteEndTime 一行 */
    /* RemoteExitCode 一行 */
    /* RemoteTraceId 一行 (非空时) */
}
```

### 2.2 partition 显示格式（设计文档 §3.3）

```c
fprintf(out, "   AllowRemote=%s\n",
        part->cd_allow_remote ? "YES" : "NO");
/* v2.0 已删: SendTo / AllowApp 回显 */
```

### 2.3 与已有 `__METASTACK_OPT_*` 的关系

scontrol 走 `slurm_print_job_info` / `slurm_print_partition_info`；本模块在末尾追加输出逻辑，包在 `__METASTACK_NEW_CROSS_DOMAIN` 内，与 cache_query 等已有 ifdef 块互不影响。

---

## 3. 触及文件

| 文件 | 锚点 |
|---|---|
| [src/api/job_info.c](../../src/api/job_info.c) | `slurm_print_job_info` 输出装配末尾、`xfree` 之前 |
| [src/api/partition_info.c](../../src/api/partition_info.c) | `slurm_print_partition_info` 输出装配末尾 |

---

## 4. Checklist

### 4.1 job show 跨域字段输出

- [ ] M9-1 [src/api/job_info.c](../../src/api/job_info.c) `slurm_print_job_info`（实际可能在 `_print_job_struct`）找到 `xstrfmtcat(out, "   Comment=...` 之后、`return out` / `xfree(out)` 之前的位置
- [ ] M9-2 在末尾追加 ifdef 块（v2 设计 §5.6 完整版）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        xstrcat(out, "   ");
        xstrfmtcat(out, "CrossRegion=%s ", job_ptr->cross_region ? "yes" : "no");
        if (job_ptr->app_name)
            xstrfmtcat(out, "AppName=%s ", job_ptr->app_name);
        xstrfmtcat(out, "RouteExhausted=%s",
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
    #endif
    ```
- [ ] M9-3 `Comment=` 行保持原样，**不要**插入跨域内容（v2 设计 §5.6 重要约束）

### 4.2 partition show `AllowRemote` 输出

- [ ] M9-4 [src/api/partition_info.c](../../src/api/partition_info.c) `slurm_print_partition_info` 末尾追加（v2 设计 §3.3）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        xstrfmtcat(out, "   AllowRemote=%s\n",
                   part->cd_allow_remote ? "YES" : "NO");
        /* v2.0 已删: SendTo / AllowApp 回显 */
    #endif
    ```

### 4.3 编译与验证

- [ ] M9-5 `make -j` 通过；`scontrol show job <跨域 id>` 看到完整 6~7 行附加信息
- [ ] M9-6 `scontrol show job <普通 id>` 不显示跨域字段（仅 1 行 `CrossRegion=no AppName=... RouteExhausted=NO`，但因 `cross_region=0` 默认输出"no"，可考虑只在 `cross_region==1` 时输出整段；按 v2 设计 §5.6 实际是无条件输出 `CrossRegion=no/yes` 一行 — 评审决策保持设计一致）
- [ ] M9-7 跨域作业刚提交（broker 还没回 8003）时只显示第一行 `CrossRegion=yes AppName=... RouteExhausted=NO`；第二行（RemoteCluster）字段为 NULL 自动跳过
- [ ] M9-8 broker 8003 后 `scontrol show job` 显示完整远端字段
- [ ] M9-9 broker 返回 5010 NO_VIABLE_ROUTE 后 `scontrol show job` 显示 `RouteExhausted=YES`
- [ ] M9-10 `scontrol show partition xahcnormal` 看到 `AllowRemote=YES`
- [ ] M9-11 `scontrol show job <id> --json | jq .jobs[].cross_region` 已经能拿到 `cross_region=1`（ctld-M3 的 pack 已支持），本模块只是文本展示
- [ ] M9-12 `scontrol show job <id> --json | jq .jobs[].cd_route_exhausted` 输出 `0`/`1`

---

## 5. 验收标准

1. 普通作业 `scontrol show job` 输出与改动前**完全一致**（diff 测试，除 1 行 CrossRegion=no 行）
2. 跨域作业 8003 推送前 `scontrol show job` 多 1 行；8003 推送后多 6~7 行（含 RemoteAllocTRES 等可选行）
3. broker 报 NO_VIABLE_ROUTE 后 `scontrol show job` 看到 `RouteExhausted=YES`
4. 8004 终态后第二行 `RemoteState=COMPLETED`/`CANCELLED`/`FAILED` 同步；`RemoteEndTime` 显示真实时间；`RemoteExitCode` 显示真实退出码
5. `scontrol show partition` 看到 `AllowRemote=YES/NO`，**不**显示 v1.5 的 `SendTo=` / `AllowApp=`
6. 升级老用户脚本 `scontrol show partition | grep SendTo` 无输出，但 `grep AllowRemote` 有输出

## 6. 风险

- **风险 1**: `slurm_print_job_info` 函数过长，多个 if 分支末尾。**降级**: 严格选 "最后一个 xstrfmtcat 紧邻 return" 那个位置；不要往中间插，避免 if 嵌套乱
- **风险 2**: `_print_job_struct` 在某些 24.05.x 版本拆出多个 helper（如 `_print_job_basic_info` / `_print_job_resources`）。**降级**: 找到入口函数（用户调 `scontrol show job` 实际触发的函数），把跨域块挂在末尾即可；定位方法 `rg "Reason=" src/api/job_info.c`
- **风险 3**: `RemoteExitCode` 用 `WEXITSTATUS` / `WTERMSIG` 在 32-bit 平台溢出。**降级**: 24.05 已统一用 `uint32_t`，按 `WEXITSTATUS(code)` / `WTERMSIG(code)` 标准宏使用即可
- **风险 4**: `slurm_make_time_str` 签名变化。**降级**: 24.05.8 签名 `slurm_make_time_str(time_t *time, char *string, int size)`，与 v2 设计 §5.6 一致；若版本差异以 `src/common/slurm_time.h` 为准
- **风险 5**: 老 v1.5 用户 `scontrol show partition` 脚本 grep `SendTo=`。**降级**: 升级指南显式说明该字段已删，改用 broker `routes.conf` 查询路由
