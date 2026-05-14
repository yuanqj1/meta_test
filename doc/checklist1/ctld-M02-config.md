# ctld-M02 slurm.conf 跨域配置项 Checklist (v2.0)

> 配套: [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §2 / §3
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §1.3 / §1.4
> 依赖: 无（与 ctld-M01 / ctld-M03 并行）
> 下游: ctld-M04（cd_thread 读 broker 地址 / scan_interval / max_handle）/ ctld-M06（scancel 反向用 broker_host）

> **v1.5 → v2.0 关键变化**:
> 1. **键名统一为 `CrossRegion*`**（替换 v1.5 的 `CrossDomainEnabled`），与设计文档 §2.1 一致
> 2. **新增 3 个全局键**：`CrossRegionWaitTime` / `CrossRegionScanInterval` / `CrossRegionMaxHandlePerRound`（v1.5 这些是写死的常量）
> 3. **删除 `CrossRegionCommentTag`**（v1.5 用于 comment 子串模糊匹配，v2 改用 `user/assoc.remote_allowed` 显式列）
> 4. **删除 `BrokerForwardCluster` / `BrokerForwardPartition`**（路由决策下沉到 broker `routes.conf`，ctld 不再需要默认目标）
> 5. **partition 子键大幅瘦身**：删除 `SendTo` / `AllowApp`；新增 `AllowRemote=yes|no` 一个布尔开关

---

## 1. 模块目标

让 ctld 启动时能读到：

1. **跨域线程开关 + 调度参数**：
    - `CrossRegionEnabled` 总开关，关闭则 cd_thread 不启动
    - `CrossRegionWaitTime` 作业排队多久才纳入跨域候选（默认 300s）
    - `CrossRegionScanInterval` 跨域线程主扫描周期（默认 5s，百万级队列建议 10~30）
    - `CrossRegionMaxHandlePerRound` 单轮扫描最多处理候选数（默认 500，防瞬时拥塞 write_lock）
2. **broker 入口地址**：`BrokerHost` / `BrokerPort`（默认 `127.0.0.1:8442`）
3. **partition 跨域开关**：`AllowRemote=yes|no`（默认 no），决定本地物理分区的作业是否允许被跨域转发

**v2.0 删除的常量/键**：
- ~~`CrossRegionCommentTag`~~（→ `user/assoc.remote_allowed` 显式布尔列，见 ctld-M13）
- ~~`BrokerForwardCluster`~~ / ~~`BrokerForwardPartition`~~（→ broker `routes.conf` 路由表）
- ~~partition `SendTo`~~（→ broker `routes.conf` `LocalPartition→RemoteCluster/RemotePartitions`）
- ~~partition `AllowApp`~~（→ broker `routes.conf` `AllowApps=`）

## 2. 接口契约

### 2.1 `slurm.conf` 全局键（v2.0）

```ini
# === 跨域总开关与调度参数 ===
CrossRegionEnabled=YES
CrossRegionWaitTime=300              # 作业排队 300s 后才考虑跨域
CrossRegionScanInterval=5            # 5s 一轮全表扫描
CrossRegionMaxHandlePerRound=500     # 单轮最多处理 500 个候选

# === broker 入口地址 ===
BrokerHost=127.0.0.1
BrokerPort=8442

# === v2.0 已删 ===
# CrossRegionCommentTag=allow_remote   # 改用 user/assoc.remote_allowed
# BrokerForwardCluster=clusterB        # 路由下沉到 broker routes.conf
# BrokerForwardPartition=normal        # 同上
```

### 2.2 `slurm.conf` partition 子键（v2.0 大幅瘦身）

```ini
# v2.0: 仅一个布尔开关
PartitionName=xahcnormal Nodes=node[001-100] Default=YES State=UP \
    AllowRemote=yes
```

| 子键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `AllowRemote` | bool | `no` | 该本地分区作业排队超时后是否允许被跨域线程转发到 broker；替代 v1.5 "是否配置 SendTo" 的隐式判断 |

**v2.0 已删**：
- ~~`SendTo`~~：远端拓扑由 broker `/etc/slurmbroker/routes.conf` 段 `[Route] LocalPartition=` → `RemoteCluster=` / `RemotePartitions=` 给出
- ~~`AllowApp`~~：应用白名单下沉到 broker `routes.conf` 段 `AllowApps=`

### 2.3 `slurm_conf_t` 新字段

挂在 [slurm/slurm.h](../../slurm/slurm.h) `} slurm_conf_t;` 之前，已有 `__METASTACK_OPT_CACHE_QUERY` 块之后：

```c
#ifdef __METASTACK_NEW_CROSS_DOMAIN
    uint16_t  cross_region_enabled;             /* 0/1 */
    uint32_t  cross_region_wait_time;           /* 默认 300s */
    uint16_t  cross_region_scan_interval;       /* 默认 5s, 范围 1~300 */
    uint16_t  cross_region_max_handle_per_round;/* 默认 500 */
    char     *broker_host;
    uint16_t  broker_port;
    /* v2.0 已删:
     *   char *cross_region_comment_tag;
     *   char *broker_forward_cluster;
     *   char *broker_forward_partition;
     */
#endif
```

### 2.4 默认值

| 键 | 默认值（未配置时） |
|---|---|
| `cross_region_enabled` | 0 |
| `cross_region_wait_time` | 300 |
| `cross_region_scan_interval` | 5 |
| `cross_region_max_handle_per_round` | 500 |
| `broker_host` | NULL |
| `broker_port` | 8442 |

ctld 启动时若 `cross_region_enabled=1` 但 `broker_host=NULL`，cross_region_init() 调 `error()` 并返回 `SLURM_ERROR`（设计文档 §6.3）。

### 2.5 partition_record_t 新字段（与 §2.2 配套）

挂在 [src/slurmctld/slurmctld.h](../../src/slurmctld/slurmctld.h) `struct part_record` 末尾：

```c
#ifdef __METASTACK_NEW_CROSS_DOMAIN
    uint8_t   cd_allow_remote;     /* 0=no(默认) / 1=yes; AllowRemote= 解析结果 */
#endif
```

> 详细的 `partition_record_t` 字段管理（init/free/pack/unpack）在 ctld-M03 §4.x 处理，本模块只负责 `read_config.c` 解析路径。

---

## 3. 触及文件 + 行号锚点

| 文件 | 锚点 |
|---|---|
| [src/common/read_config.c](../../src/common/read_config.c) | `slurm_conf_options[]` / `_init_slurm_conf` / `init_slurm_conf` / `free_slurm_conf` / `_partition_options[]` / `_create_partition_record_with_default` |
| [slurm/slurm.h](../../slurm/slurm.h) | `slurm_conf_t` 末尾（`} slurm_conf_t;` 之前） |
| [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) | `_pack_slurm_ctl_conf_msg` / `_unpack_slurm_ctl_conf_msg` `META_3_2_PROTOCOL_VERSION` 分支末尾 |
| [src/slurmctld/partition_mgr.c](../../src/slurmctld/partition_mgr.c) | `_list_find_part` / `_build_part_internal` 末尾（v2 `_cd_partition_init` 简化版） |

---

## 4. Checklist

### 4.1 全局键解析

- [ ] M2-1 [src/common/read_config.c](../../src/common/read_config.c) `slurm_conf_options[]` 末尾追加 ifdef 块（**6 项，比 v1.5 删 3 加 3**）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        {"CrossRegionEnabled",             S_P_BOOLEAN},
        {"CrossRegionWaitTime",            S_P_UINT32},
        {"CrossRegionScanInterval",        S_P_UINT16},
        {"CrossRegionMaxHandlePerRound",   S_P_UINT16},
        {"BrokerHost",                     S_P_STRING},
        {"BrokerPort",                     S_P_UINT16},
        /* v2.0 已删:
         *   {"CrossRegionCommentTag",     S_P_STRING},
         *   {"BrokerForwardCluster",      S_P_STRING},
         *   {"BrokerForwardPartition",    S_P_STRING},
         */
    #endif
    ```
- [ ] M2-2 `_init_slurm_conf` 内追加 6 个 `s_p_get_*` 抽取并写入 `conf_ptr`：
    ```c
    s_p_get_boolean((bool *)&conf->cross_region_enabled, "CrossRegionEnabled", hashtbl);
    if (!s_p_get_uint32(&conf->cross_region_wait_time, "CrossRegionWaitTime", hashtbl))
        conf->cross_region_wait_time = 300;
    if (!s_p_get_uint16(&conf->cross_region_scan_interval, "CrossRegionScanInterval", hashtbl))
        conf->cross_region_scan_interval = 5;
    if (!s_p_get_uint16(&conf->cross_region_max_handle_per_round, "CrossRegionMaxHandlePerRound", hashtbl))
        conf->cross_region_max_handle_per_round = 500;
    s_p_get_string(&conf->broker_host, "BrokerHost", hashtbl);
    if (!s_p_get_uint16(&conf->broker_port, "BrokerPort", hashtbl))
        conf->broker_port = 8442;
    ```
- [ ] M2-3 边界校验：`cross_region_scan_interval` 限制在 1~300 之间（超出范围警告并截断），`cross_region_max_handle_per_round` 至少 1（0 / NO_VAL 当作默认 500）

### 4.2 init/free

- [ ] M2-4 `init_slurm_conf` 设默认值（与 §2.4 表一致）
- [ ] M2-5 `free_slurm_conf` 释放 `xfree(ctl_conf_ptr->broker_host)`；**v2.0 已无** `broker_forward_cluster` / `broker_forward_partition` / `cross_region_comment_tag` 三个 char* 需要 free
- [ ] M2-6 [slurm/slurm.h](../../slurm/slurm.h) `slurm_conf_t` 末尾加 ifdef 块（6 字段，顺序见 §2.3）

### 4.3 partition 子键解析

- [ ] M2-7 [src/common/read_config.c](../../src/common/read_config.c) `_partition_options[]` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        {"AllowRemote",         S_P_BOOLEAN},
        /* v2.0 已删: {"SendTo", S_P_STRING}, {"AllowApp", S_P_STRING} */
    #endif
    ```
- [ ] M2-8 [src/slurmctld/partition_mgr.c](../../src/slurmctld/partition_mgr.c) 在 `_list_find_part` / `_build_part_internal` 末尾增加 `_cd_partition_init()` helper：
    ```c
    static void _cd_partition_init(part_record_t *p, s_p_hashtbl_t *tbl)
    {
        bool allow_remote = false;
        s_p_get_boolean(&allow_remote, "AllowRemote", tbl);
        p->cd_allow_remote = allow_remote ? 1 : 0;
        /* v2.0 已删: SendTo / AllowApp 解析与 "<remote>@<cluster>" 切分 */
    }
    ```
- [ ] M2-9 reconfig 路径同样调用 `_cd_partition_init()`，切换 `AllowRemote=` 实时生效，不需要重启

### 4.4 RPC 协议同步（slurm_ctl_conf 透传给 scontrol show config）

- [ ] M2-10 [src/common/slurm_protocol_pack.c](../../src/common/slurm_protocol_pack.c) `_pack_slurm_ctl_conf_msg` 在 `META_3_2_PROTOCOL_VERSION` 分支末尾追加：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        pack16(build_ptr->cross_region_enabled, buffer);
        pack32(build_ptr->cross_region_wait_time, buffer);
        pack16(build_ptr->cross_region_scan_interval, buffer);
        pack16(build_ptr->cross_region_max_handle_per_round, buffer);
        packstr(build_ptr->broker_host, buffer);
        pack16(build_ptr->broker_port, buffer);
    #endif
    ```
- [ ] M2-11 `_unpack_slurm_ctl_conf_msg` 同位置 `safe_unpack16` / `safe_unpack32` / `safe_unpackstr`（顺序对应）

### 4.5 reconfigure 行为

- [ ] M2-12 `scontrol reconfigure` 走 `_handle_reconfig_req()` → `read_slurm_conf()` 自动重读上述键
- [ ] M2-13 全局键运行期生效（cd_thread 下个 tick 自动用新值；`CrossRegionEnabled` NO→YES 触发 `cross_region_init()`，YES→NO 触发 `cross_region_fini()`，由 ctld-M04 实现钩子）
- [ ] M2-14 partition `AllowRemote=` 在 reconfig 时由 `_build_part_bitmap()` 一并刷新 `cd_allow_remote`

### 4.6 编译与运行检查

- [ ] M2-15 `make -j` 通过
- [ ] M2-16 写完 6 行全局键 + 1 行 partition 启动 ctld，`scontrol show config | grep -iE "BrokerHost|BrokerPort|CrossRegion"` 看到 6 行
- [ ] M2-17 `scontrol show partition xahcnormal | grep AllowRemote` 看到 `AllowRemote=YES`
- [ ] M2-18 不写跨域字段时 `scontrol show config | grep -i CrossRegionEnabled` 显示 `CrossRegionEnabled = 0`，cd_thread 不启动
- [ ] M2-19 `CrossRegionScanInterval=10000`（超范围）启动，看到 warning 日志且实际值被截断到 300

---

## 5. 验收标准

1. 全部 6 行全局键 + partition `AllowRemote=yes` 写入后，ctld 启动 + `scontrol show config` / `scontrol show partition` 看见全部字段；ctld 重启后值持久
2. 删除 `BrokerHost` 一行重启 ctld + 设 `CrossRegionEnabled=YES`，ctld 启动失败 + log 提示 `BrokerHost not set`（由 ctld-M04 `cross_region_init` 实现）
3. `scontrol reconfigure` 后 `CrossRegionScanInterval` 改值，下个 tick 周期实时生效（无须重启 ctld）；partition `AllowRemote=no→yes` reconfig 后 cd_thread 下轮扫描即可识别
4. 升级安装时旧 v1.5 `slurm.conf` 还存有 `CrossRegionCommentTag` / `SendTo` / `AllowApp` 行，启动**不报 fatal**（`s_p_options_t` 不识别的键自动忽略 + warning），文档化提示运维清理

## 6. 风险

- **风险 1**: `slurm_conf_t` 字段位置错位 → ABI 灾难。**降级**: 严格挂在 `}` 之前末尾，永远 append-only；24.05.8 已有 ~200 字段，新加 6 个仍在末尾
- **风险 2**: 旧版 sacctmgr/squeue 客户端连新 ctld → 多 unpack 6 字段 → `Bad Magic`。**降级**: pack/unpack 包在 `META_3_2_PROTOCOL_VERSION >=` 分支末尾，旧版本路径自动跳过
- **风险 3**: v1.5 部署的 `CrossDomainEnabled` 旧键升级后失效，用户无感知。**降级**: 在 `_init_slurm_conf` 加 fallback 探测：若 `CrossRegionEnabled` 未设但 `CrossDomainEnabled` 存在，复制旧值并 warning（一次性兼容，下个大版本删除）
- **风险 4**: partition 删除 `SendTo` / `AllowApp` 后旧配置文件解析报错。**降级**: `s_p_options_t` 不识别的键只记 warning 不报错，slurm 24.05 行为天然兼容
