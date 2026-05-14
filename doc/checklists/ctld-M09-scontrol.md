# ctld-M09 scontrol show job 跨域行 Checklist

> 配套: [doc/Slurmctld跨域详细设计文档MVP.md](../Slurmctld跨域详细设计文档MVP.md) §8.3
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: ctld-M03（`slurm_job_info_t.cd_remote_*`）
> 下游: ctld-M11

---

## 1. 模块目标

`scontrol show job <id>` 在跨域作业上显示 2 行附加信息：

```
JobId=12345 ...
   ...
   CrossDomain=yes AppName=gromacs
   RemoteCluster=clusterB RemoteJobId=87654 RemoteState=RUNNING
   ...
```

普通作业完全不显示这 2 行，避免噪声。

## 2. 接口契约

### 2.1 显示格式

```c
if (job_ptr->cross_domain) {
    xstrfmtcat(out, "   CrossDomain=yes AppName=%s\n",
               job_ptr->cd_app_name ? job_ptr->cd_app_name : "");
}
if (job_ptr->cd_remote_cluster_name) {
    xstrfmtcat(out, "   RemoteCluster=%s RemoteJobId=%u RemoteState=%s\n",
               job_ptr->cd_remote_cluster_name,
               job_ptr->cd_remote_job_id,
               job_state_string(job_ptr->cd_remote_state));
}
```

### 2.2 与 [`__METASTACK_OPT_CACHE_QUERY`](../../src/api/job_info.c) 等已有改动的关系

scontrol 的 show job 走 `slurm_print_job_info`，本模块在它末尾追加 2 行打印逻辑，包在 `__METASTACK_NEW_CROSS_DOMAIN` 内，与 cache_query 等已有 ifdef 块互不影响。

---

## 3. 触及文件

| 文件 | 锚点 |
|---|---|
| [src/api/job_info.c](../../src/api/job_info.c) | `slurm_print_job_info` 输出装配末尾、`xfree` 之前 |

---

## 4. Checklist

- [ ] M9-1 [src/api/job_info.c](../../src/api/job_info.c) `slurm_print_job_info` 中找到 `xstrfmtcat(out, "   Comment=...` 这一行附近的输出段（绝大多数版本是 ~600-800 行附近的 `out` 累积过程）
- [ ] M9-2 在末尾、最后一个 `xstrfmtcat` 之后、`return out`/`xfree(out)` 之前追加 ifdef 块约 12 行（见 §2.1）
- [ ] M9-3 `make -j` 通过；`make -C src/api install` 后 `scontrol show job <跨域 id>` 看到 2 行；`scontrol show job <普通 id>` 不显示
- [ ] M9-4 跨域作业刚提交（broker 还没回 9102）时只显示第一行 `CrossDomain=yes AppName=...`，第二行（RemoteCluster）字段为 NULL 自动跳过
- [ ] M9-5 `scontrol show job <id> --json | jq .jobs[].cross_domain` 已经能拿到 `cross_domain=1`（ctld-M3 的 pack 已支持），本模块只是文本展示

---

## 5. 验收标准

1. 普通作业 `scontrol show job` 输出与改动前**完全一致**（diff 测试）
2. 跨域作业 9102 推送前 `scontrol show job` 多 1 行；9102 推送后多 2 行
3. 9103 终态后第二行 `RemoteState=COMPLETED`/`CANCELLED`/`FAILED` 同步

## 6. 风险

- **风险 1**: `slurm_print_job_info` 函数过长，多个 if 分支末尾。**降级**: 严格选 "最后一个 xstrfmtcat 紧邻 return" 那个位置；不要往中间插，避免 if 嵌套乱
- **风险 2**: 第二阶段会扩展更多 cd_* 字段。**降级**: 本期只显示 5 个字段；新增字段在第二阶段独立 PR 增加
