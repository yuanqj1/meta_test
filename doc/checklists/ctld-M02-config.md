# ctld-M02 slurm.conf 跨域配置项 Checklist

> 配套: [doc/Slurmctld跨域详细设计文档MVP.md](../Slurmctld跨域详细设计文档MVP.md) §2
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: 无（与 ctld-M01 / ctld-M03 并行）
> 下游: ctld-M04（cd_thread 读 broker 地址）/ ctld-M06（scancel 反向用 broker_host）

---

## 1. 模块目标

让 ctld 启动时能读到：
1. `CrossDomainEnabled`：跨域开关，关闭则 cd_thread 不启动
2. `BrokerHost` / `BrokerPort`：本机 broker 的 munge 端口（默认 8442）
3. `BrokerForwardCluster` / `BrokerForwardPartition`：默认目标集群与分区（用户 `--cross-domain` 不指定时兜底）

**不进 slurm.conf** 的常量（直接写在 [src/slurmctld/cross_domain.c](../../src/slurmctld/cross_domain.c) 内）：
- `CD_WAIT_TIME_SEC=0`
- `CD_SCAN_INTERVAL_SEC=5`
- `CD_MAX_PER_ROUND=100`

## 2. 接口契约

### 2.1 conf 键

```ini
CrossDomainEnabled=YES
BrokerHost=127.0.0.1
BrokerPort=8442
BrokerForwardCluster=clusterB
BrokerForwardPartition=normal
```

### 2.2 `slurm_conf_t` 新字段

挂在 [slurm/slurm.h:4067](../../slurm/slurm.h)（`} slurm_conf_t;` 之前）已有 `__METASTACK_OPT_CACHE_QUERY` 块之后：

```c
#ifdef __METASTACK_NEW_CROSS_DOMAIN
    uint16_t  cross_domain_enabled;
    char     *broker_host;
    uint16_t  broker_port;
    char     *broker_forward_cluster;
    char     *broker_forward_partition;
#endif
```

### 2.3 默认值

| 键 | 默认值（未配置时） |
|---|---|
| `cross_domain_enabled` | 0 |
| `broker_host` | NULL |
| `broker_port` | 8442 |
| `broker_forward_cluster` | NULL |
| `broker_forward_partition` | NULL |

ctld 启动时若 `cross_domain_enabled=1` 但 `broker_host=NULL`，cd_thread fatal 退出，避免 silent 失败。

---

## 3. 触及文件 + 行号锚点

| 文件 | 锚点 |
|---|---|
| [src/common/read_config.c](../../src/common/read_config.c) | `slurm_conf_options[]`（279 行）、`_init_slurm_conf`（4268 行）、`init_slurm_conf`（4014 行）、`free_slurm_conf`（3854 行） |
| [slurm/slurm.h](../../slurm/slurm.h) | `slurm_conf_t` 末尾（4068 行 `}` 之前） |
| [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) | `_pack_slurm_ctl_conf_msg`（6903 行）/ `_unpack_slurm_ctl_conf_msg`（9263 行）`META_3_2_PROTOCOL_VERSION` 分支末尾 |

---

## 4. Checklist

### 4.1 解析层

- [ ] M2-1 [src/common/read_config.c](../../src/common/read_config.c) `slurm_conf_options[]` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        {"CrossDomainEnabled",     S_P_BOOLEAN},
        {"BrokerHost",             S_P_STRING},
        {"BrokerPort",             S_P_UINT16},
        {"BrokerForwardCluster",   S_P_STRING},
        {"BrokerForwardPartition", S_P_STRING},
    #endif
    ```
- [ ] M2-2 `_init_slurm_conf` 内追加 5 个 `s_p_get_*` 抽取并写入 `conf_ptr`：
    ```c
    s_p_get_boolean((bool *)&conf->cross_domain_enabled, "CrossDomainEnabled", hashtbl);
    s_p_get_string(&conf->broker_host, "BrokerHost", hashtbl);
    if (!s_p_get_uint16(&conf->broker_port, "BrokerPort", hashtbl))
        conf->broker_port = 8442;
    s_p_get_string(&conf->broker_forward_cluster, "BrokerForwardCluster", hashtbl);
    s_p_get_string(&conf->broker_forward_partition, "BrokerForwardPartition", hashtbl);
    ```

### 4.2 init/free

- [ ] M2-3 `init_slurm_conf` 设默认：`cross_domain_enabled=0`、`broker_port=8442`、3 个字符串 `NULL`
- [ ] M2-4 `free_slurm_conf` 释放：`xfree(ctl_conf_ptr->broker_host)`、`xfree(broker_forward_cluster)`、`xfree(broker_forward_partition)`

### 4.3 字段挂载

- [ ] M2-5 [slurm/slurm.h](../../slurm/slurm.h) `slurm_conf_t` 末尾加 ifdef 块（5 字段，顺序见 §2.2）

### 4.4 RPC 协议同步

- [ ] M2-6 [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) `_pack_slurm_ctl_conf_msg` 在 `META_3_2_PROTOCOL_VERSION` 分支末尾追加：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        pack16(build_ptr->cross_domain_enabled, buffer);
        packstr(build_ptr->broker_host, buffer);
        pack16(build_ptr->broker_port, buffer);
        packstr(build_ptr->broker_forward_cluster, buffer);
        packstr(build_ptr->broker_forward_partition, buffer);
    #endif
    ```
- [ ] M2-7 `_unpack_slurm_ctl_conf_msg` 同位置 `safe_unpack16` / `safe_unpackstr`

### 4.5 编译与运行检查

- [ ] M2-8 `make -j` 通过
- [ ] M2-9 在 `slurm.conf` 写完 5 行启动 ctld，`scontrol show config | grep -iE "BrokerHost|BrokerPort|CrossDomainEnabled|BrokerForward"` 看到 5 行
- [ ] M2-10 不写跨域字段时 `scontrol show config | grep -i CrossDomainEnabled` 显示 `CrossDomainEnabled = 0`，cd_thread 不启动

---

## 5. 验收标准

1. `slurm.conf` 5 行配置写入后，ctld 启动 + `scontrol show config` 看见 5 行；ctld 重启后值持久
2. 删除 `BrokerHost` 一行重启 ctld + 设 `CrossDomainEnabled=YES`，ctld 启动失败 + log 提示 `BrokerHost not set`
3. `scontrol reconfigure` 后字段刷新（**注**：cd_thread 是否实时识别留给第二阶段，本期允许 reconfigure 后需重启 ctld 才生效）

## 6. 风险

- **风险 1**: `slurm_conf_t` 字段位置错位 → ABI 灾难。**降级**: 严格挂在 `}` 之前末尾，永远 append-only
- **风险 2**: 旧版 sacctmgr/squeue 客户端连新 ctld → 多 unpack 5 字段 → 报 `Bad Magic`。**降级**: pack/unpack 包在 `META_3_2_PROTOCOL_VERSION >=` 分支末尾，旧版本路径自动跳过
