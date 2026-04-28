# Slurm 跨域 Broker (slurmbrokerd) 开发任务清单 — MVP

> 版本: DEV-1.0 (基于 `Broker详细设计文档MVP.md` 拆解)
> 配套文档: `跨域调度MVP整体方案.md`、`Broker详细设计文档MVP.md`、`Broker详细设计文档.md`(v0.1)
> 适用范围: 8 周 broker MVP 开发实施期 (Sprint 1 ~ Sprint 4)
> 受众: broker 模块开发工程师、跨域功能 ctld 端开发同学 (协同接口部分)、运维 / 测试

---

## 0. 如何使用本文档

### 0.1 文档定位

本文档是 **施工图**，把 `Broker详细设计文档MVP.md` 第 12 章 (Sprint 计划) 展开为可被单人独立认领、独立测试的"闭环开发任务"。每个任务尽量满足:

1. **可独立完成**: 不依赖其他未完成任务即可写出代码或脚本。
2. **可独立验证**: 文档给出本任务"完成"的客观判定方式 (单测 / 命令 / 日志 grep)。
3. **粒度合适**: 0.5 ~ 2 人天为目标; 超出 2 天的强制再拆。

> 设计文档回答 *"做成什么样"*; 本任务清单回答 *"按什么顺序、怎么一条一条把它做出来"*。代码细节请回去查 `Broker详细设计文档MVP.md` 对应小节, 本文档不重复贴大段代码。

### 0.2 任务编号规则

```
M{模块号}-T{任务号}        例如 M03-T2
M{模块号}-T{任务号}.{步骤} 例如 M03-T2.1, M03-T2.2
```

- 模块号 `M01 ~ M16` 一一对应 §1.2 的模块清单。
- 同一模块内任务按建议开发顺序编号; 显式依赖关系在每个任务的 `依赖` 字段标注。
- 跨模块依赖一律写全编号 (例如 `依赖: M02-T3, M04-T1`)。

### 0.3 任务条目模板

```
#### Mxx-Ty 任务标题
- 模块: Mxx <模块中文名>
- Sprint: S{1|2|3|4}
- 预估: 0.5d / 1d / 2d
- 依赖: Mxx-Ty / 无
- 涉及文件: src/slurmbrokerd/xxx.c, src/slurmbrokerd/xxx.h, ...
- 目标: 一句话说明完成后能干什么
- 实现步骤:
  1. ...
  2. ...
- 接口/数据 (新增或修改时填):
  - 函数签名 / 结构字段 / 配置项 / 命令行
- 完成标准 (Definition of Done):
  - [ ] 可观测的判定 1
  - [ ] 可观测的判定 2
- 风险/备注: 已知坑、与设计文档差异、与其他模块的协调点
```

### 0.4 与 ctld / 客户端的协作边界

> 本文档只列 **broker 工程师要写的代码**。下面这些归整体方案 / ctld 端工程师, 在依赖列里以 `[external]` 标识:
>
> - 新增 `REQUEST_FORWARD_JOB` / `REQUEST_BROKER_UPDATE_REMOTE_STATE` / `REQUEST_BROKER_TERMINAL_STATE` / `REQUEST_BROKER_CANCEL` 的 ctld handler、`job_record_t` 字段、`squeue --remote` / `scontrol show job` 字段输出 → ctld + 客户端工程师。
> - `slurm.conf` 增加 `BrokerAddr` / `BrokerPort` 等配置 → ctld 工程师。
> - sacct 字段写入由 ctld 注入终态时用 `jobcomp_g_record_job_end` 走标准链路。
>
> broker 端只负责: **遵守对方约定的 RPC payload 字段顺序**, 按 §6 协议 pack/unpack。

---

## 1. 模块清单与依赖图

### 1.1 模块总览

| 模块号 | 模块名 | 主要文件 | 一句话职责 | Sprint |
|---|---|---|---|---|
| **M01** | 进程骨架与生命周期 | `slurmbrokerd.c`, systemd unit | 进程启动 / 信号 / 日志 / shutdown / restore | S1 |
| **M02** | 配置加载 | `broker_conf.c/.h`, `user_mapping.c/.h` | 解析 `broker.conf` + `user_mapping.conf` | S1 |
| **M03** | 数据结构与持久化 | `broker_job.c/.h`, `persist.c/.h` | `broker_job_t` + 全局表 + JSONL checkpoint | S1 |
| **M04** | RPC 协议 pack/unpack | `proto.c/.h` (+ `slurm_protocol_pack.c` 共用段) | 自定义 broker 消息序列化 | S1 |
| **M05** | 网络监听与分发 | `listener.c/.h` | select 双端口 + dispatch | S1 → S2 |
| **M06** | ctld 入站处理 | `handler_ctld.c/.h` | `FORWARD_JOB` / `CANCEL` 入口 | S2 |
| **M07** | 远端 broker 入站处理 | `handler_remote.c/.h` | `BROKER_FORWARD_JOB` / `STAGED_IN` / `QUERY_STATUS` / `CANCEL` / `TERMINAL_STATE` | S2 → S3 |
| **M08** | egress 出站调用 | `egress.c/.h` | broker → broker / broker → ctld 主动发起的 RPC | S2 → S3 |
| **M09** | 状态机 | `state_machine.c/.h` | tick + transition + 超时 + 重试 | S2 |
| **M10** | 数据传输 (rsync) | `stage.c/.h` | stage-in / stage-out worker pool | S2 |
| **M11** | 软件路径解析 | `software.c/.h`, `lookup_software.sh` | fork+exec 外部脚本 + 超时 | S3 |
| **M12** | 脚本改写 | `rewrite.c/.h` | partition 替换 + drop SBATCH 行 + 前缀替换 + 清空 account | S3 |
| **M13** | 状态轮询 | `sync_ticker.c/.h` | 10s 批量 QUERY_STATUS + apply | S3 |
| **M14** | 远端清理与保留 | (融入 `state_machine.c`, `cleanup.c`) | RemoteWorkDir 24h / 失败 7d 清理 | S3 |
| **M15** | 部署运维工件 | systemd / sudoers / ssh key / munge / `software_routes.conf` | 一次性运维基线 | S1, S4 |
| **M16** | 集成测试与验收 | `tests/`, `mvp_smoke_test.sh`, 故障注入 | smoke / 端到端 / 故障 / 长稳 | S4 |

### 1.2 模块依赖图 (建议先读后开工)

```text
                              +-----------+
                              |  M15 部署 |
                              | (SSH/munge|
                              |  /sudoers)|
                              +-----+-----+
                                    | (运行时前置)
                                    v
+------+    +------+    +------+    +------+
| M02  |--->| M03  |--->| M04  |--->| M05  |---+
| conf |    | job+ |    | proto|    | listen|  |
|      |    |  pers|    |      |    |       |  |
+------+    +------+    +------+    +------+   |
   ^                                           |
   |                                           v
   |                         +-------+    +-------+    +-------+
   +---------- M01 ----------|  M06  |    |  M07  |    |  M08  |
              (skeleton)     | ctld  |    | remote|    | egress|
                             | hand  |    | hand  |    |       |
                             +---+---+    +---+---+    +---+---+
                                 |            |            |
                                 v            v            v
                             +---------------------------------+
                             |              M09                |
                             |          state_machine          |
                             +-------+--------+--------+-------+
                                     |        |        |
                                     v        v        v
                                  +-----+  +-----+  +-----+
                                  | M10 |  | M11 |  | M13 |
                                  | stag|  | sw  |  | sync|
                                  +--+--+  +--+--+  +-----+
                                     \      /
                                      \    /
                                       v  v
                                     +-----+
                                     | M12 |
                                     |rewri|
                                     +-----+

                                     +-----+    +-----+
                                     | M14 |    | M16 |
                                     | clnu|    | test|
                                     +-----+    +-----+
```

> **关键路径**: `M01 → M02 → M03 → M04 → M05 → (M06 || M07) → M08 → M09 → M10 → M11 → M12 → M13`。M14 / M16 在大盘合龙后做; M15 与代码并行, S1 内交付一版基线。

### 1.3 全局完成判定 (MVP)

> 全部以下条目 `[ ]` 打勾即视为 broker MVP 完成 (与设计文档 §13 验收清单一致):

- [ ] 单 broker 进程启动, `systemctl status slurmbrokerd` Running, restore 0 jobs。
- [ ] ctld 通过 `REQUEST_FORWARD_JOB` 投递 100 个作业, broker `state.jsonl` 出现对应条目, `state=INIT` → `STAGING_IN` → ... → `COMPLETED` 全部跑通。
- [ ] 在源端 `squeue --remote` 看到 `RemoteJobId` / `RemoteState=RUNNING` 字段; `comment` 字段保持用户提交时原样。
- [ ] `scancel <src_job_id>` 30s 内远端作业被 kill。
- [ ] 重启 broker, 在途作业全部 restore, 状态机继续推进, 不丢、不重投。
- [ ] sacct 写入了 `Remote_*` 字段。
- [ ] `lookup_software.sh` 失败时, 作业 FAILED 且 `state_reason` 含 `lookup_software`。

---

# 第一部分: 基础模块 (M01 ~ M04)

> 这部分是骨架, 不依赖任何业务逻辑, 必须在 Sprint 1 全部完成。完成后任意人都可以跑起来一个"什么都不做但活着"的 slurmbrokerd 进程, 是后续开发的"地基"。

---

## M01 进程骨架与生命周期

### 模块概述
打通 `slurmbrokerd` 进程从命令行解析到 systemd 托管、信号优雅关闭、日志输出的最小闭环。完成后代码 `int main()` 能跑、能 `kill -TERM` 优雅退出, 但还不接受任何 RPC。

### 接口契约
- 命令行: `slurmbrokerd [-D] [-f /etc/slurm/broker.conf] [-v[v[v]]]`
- 信号: `SIGTERM` / `SIGINT` 触发 `g_shutdown_requested = true`; `SIGHUP` 重载 user_mapping (M02-T4 提供回调, MVP 可先 no-op)。
- 日志: 复用 `src/common/log.c`, 输出到 stderr (前台 `-D`) 或 syslog (默认)。
- 退出码: 0 正常; 1 配置错误; 2 状态文件损坏不可恢复。

### 涉及文件
```
src/slurmbrokerd/slurmbrokerd.c
src/slurmbrokerd/slurmbrokerd.h          # g_shutdown_requested 等全局
etc/systemd/slurmbrokerd.service
src/slurmbrokerd/Makefile.am             # 仅占位, 详见 M01-T5
configure.ac                              # AC_CONFIG_FILES 增加
```

### 任务列表

#### M01-T1 创建 `src/slurmbrokerd/` 目录骨架与 Makefile.am
- 模块: M01
- Sprint: S1
- 预估: 0.5d
- 依赖: 无
- 目标: 拥有可被 autoreconf + make 识别的源码目录, 编译出空壳 `slurmbrokerd` 可执行文件。
- 实现步骤:
  1. 新建 `src/slurmbrokerd/` 目录; 参考 `src/slurmrestd/Makefile.am` 写 `Makefile.am`, `slurmbrokerd_SOURCES` 暂只列 `slurmbrokerd.c slurmbrokerd.h`。
  2. `configure.ac` 加 `AC_CONFIG_FILES([src/slurmbrokerd/Makefile])`。
  3. 顶层 `src/Makefile.am` 的 `SUBDIRS` 加 `slurmbrokerd`。
  4. `slurmbrokerd.c` 写一个 `int main(int argc, char **argv) { return 0; }` 占位。
  5. `autoreconf -fi && ./configure --prefix=... && make -C src/slurmbrokerd`。
- 完成标准:
  - [ ] `make -C src/slurmbrokerd` 成功, 产物 `src/slurmbrokerd/slurmbrokerd`。
  - [ ] `./slurmbrokerd` 立即返回 0, 无段错误。
- 风险: configure.ac 改动需要全工程 `autoreconf -fi`; 与 slurmrestd 的可选编译 `--with-slurmrestd` 类似, 暂不加 `--with-slurmbrokerd` (MVP 默认编进去)。

#### M01-T2 命令行参数解析
- 模块: M01
- Sprint: S1
- 预估: 0.5d
- 依赖: M01-T1
- 涉及文件: `slurmbrokerd.c`
- 目标: 支持 `-D` (前台不 fork) / `-f path` (指定配置文件) / `-v` (递增日志等级) / `-h`。
- 实现步骤:
  1. 用 `getopt_long`, 选项表参考 `src/slurmrestd/slurmrestd.c`。
  2. 解析后写入 `g_argv_opts` 全局 (`bool foreground; char *conf_path; int verbose;`)。
  3. `-h` 打印 usage 后 `exit(0)`。
- 完成标准:
  - [ ] `slurmbrokerd -h` 打印用法。
  - [ ] `slurmbrokerd -f /tmp/no.conf` 若文件不存在, 打到 M01-T3 时退出码 1。

#### M01-T3 日志初始化与 verbose 等级映射
- 模块: M01
- Sprint: S1
- 预估: 0.5d
- 依赖: M01-T2
- 目标: 复用 `log.c`, 前台 / syslog 模式自动切换, `-v -v -v` 控制等级。
- 实现步骤:
  1. `log_init("slurmbrokerd", logopt, syslog_facility, NULL);` 参考 slurmctld/`controller.c::_init_logging`。
  2. 前台: `logopt.stderr_level = LOG_LEVEL_INFO + verbose;`
  3. 后台: `LOG_DAEMON` syslog facility。
- 完成标准:
  - [ ] `slurmbrokerd -D -v` 打 INFO+; `-vv` DEBUG; 启动行 `info("slurmbrokerd %s starting", SLURM_VERSION_STRING);` 可见。

#### M01-T4 信号处理与优雅退出
- 模块: M01
- Sprint: S1
- 预估: 0.5d
- 依赖: M01-T3
- 目标: 收到 `SIGTERM`/`SIGINT` 后, 主循环退出前先 flush 一次 persist + close listener fd, 不丢数据。
- 实现步骤:
  1. 安装 `sigaction` for `SIGTERM/SIGINT/SIGHUP/SIGPIPE` (PIPE 设为 SIG_IGN)。
  2. handler 内只置位 `g_shutdown_requested = true; g_sighup_requested = true;`, 不打日志不分配内存。
  3. 主循环退出后调 `broker_state_save()` (M03-T6 提供), 再 `_exit(0)`。
- 完成标准:
  - [ ] 前台运行下 Ctrl-C 看到 "graceful shutdown initiated" 日志, 进程在 ≤ 5s 内退出。

#### M01-T5 主循环骨架 (尚未启动子线程)
- 模块: M01
- Sprint: S1
- 预估: 0.5d
- 依赖: M01-T4
- 目标: 写 `int main()` 主循环空跑 1s sleep, 只检测 `g_shutdown_requested`, 用作后续模块 hook 点。
- 实现步骤:
  1. `while (!g_shutdown_requested) sleep(1);`
  2. 在循环前后预留注释 `/* TODO: M01-T6 spawn threads */` `/* TODO: M03-T6 final save */`。
- 完成标准:
  - [ ] 进程能正常起、停、不 CPU 占满。

#### M01-T6 启动顺序编排 (各模块线程拉起占位)
- 模块: M01
- Sprint: S1 (随各模块就位逐步打开)
- 预估: 0.5d (本任务自身) + 后续若干模块完成时来更新本函数
- 依赖: M01-T5
- 涉及函数 (新增):
  - `int broker_init(void);`
  - `void broker_fini(void);`
- 目标: 集中"启动顺序", 后续每个模块只在这里加一行调用。
- 实现步骤 (按完成时间逐步填):
  1. `broker_conf_init()` (M02-T3)
  2. `user_mapping_load()` (M02-T4)
  3. `broker_state_restore()` (M03-T6)
  4. `proto_init()` (M04-T2)
  5. `egress_init()` (M08-T1)
  6. `state_machine_start()` (M09-T1)
  7. `stage_pool_start()` (M10-T1)
  8. `sync_ticker_start()` (M13-T1)
  9. `listener_start()` (M05-T1)
- 完成标准:
  - [ ] 当所有依赖模块都到位后, 单元启动 `broker_init()` 成功; `broker_fini()` 反向卸载不漏资源 (valgrind clean)。

#### M01-T7 systemd unit + 安装路径
- 模块: M01
- Sprint: S1
- 预估: 0.5d
- 依赖: M01-T1
- 涉及文件: `etc/systemd/slurmbrokerd.service`, `src/slurmbrokerd/Makefile.am` (install hook)
- 目标: 给运维一份生产可用的 systemd 单元。
- 实现步骤:
  1. 参考 `etc/slurmctld.service`, 复制改 `ExecStart=/usr/sbin/slurmbrokerd`。
  2. `User=slurm Group=slurm Restart=on-failure RestartSec=10`。
  3. `LimitNOFILE=65536`。
  4. 在 Makefile.am 写 `unitsdir = $(prefix)/lib/systemd/system; units_DATA = slurmbrokerd.service`。
- 完成标准:
  - [ ] `make install`; `systemctl daemon-reload`; `systemctl enable --now slurmbrokerd`; `systemctl status` Running。

---

## M02 配置加载

### 模块概述
解析 `broker.conf` + `user_mapping.conf` 两份文件, 校验、生成全局只读结构 `g_broker_conf` 和 `g_user_mappings`。MVP 不支持热加载 user_mapping, 重启生效。

### 接口契约
- 配置文件路径默认 `/etc/slurm/broker.conf`, 由 M01-T2 命令行 `-f` 覆盖。
- `g_broker_conf.cluster_name` 等字段后续模块只读访问。
- 失败时 `error()` + `exit(1)`, 启动阶段不容忍任何错误。

### 涉及文件
```
src/slurmbrokerd/broker_conf.c/.h
src/slurmbrokerd/user_mapping.c/.h
```

### 任务列表

#### M02-T1 定义 `broker_conf_t` 结构与字段映射
- 模块: M02 / Sprint: S1 / 预估: 0.5d / 依赖: M01-T1
- 目标: 与设计文档 §10.1 保持一致, 字段与配置 key 一一对应。
- 实现步骤:
  1. `broker_conf.h` 定义结构: `cluster_name`, `broker_node_name`, `ctld_port`, `peer_port`, `remote_cluster_name`, `remote_broker_host`, `remote_broker_port`, `remote_munge_key_path`, `default_remote_partition`, `auth_type`, `state_save_location`, `state_file_name`, `checkpoint_interval`, `max_inflight`, `max_stage_bytes`, `poll_interval`, `poll_max_retries`, `stage_rsync_bin`, `stage_ssh_key`, `stage_ssh_user`, `stage_worker_count`, `stage_timeout_per_gb`, `lookup_software_script`, `lookup_timeout_sec`, `remote_work_dir_retention_hours`, `remote_work_dir_failure_retention_days`。
  2. 留 `void *reserved[8]` 给后续扩展不破坏 ABI。
- 完成标准:
  - [ ] 结构字段与设计文档对照齐全; 头文件 include 安全 (有 guard)。

#### M02-T2 复用 Slurm `s_p_hashtbl_t` 写解析器
- 模块: M02 / Sprint: S1 / 预估: 1d / 依赖: M02-T1
- 目标: 用 `s_p_parse_file()` 一次解析所有键, 走 Slurm 现成基础设施, 不自己写词法器。
- 实现步骤:
  1. 定义 `static s_p_options_t broker_options[] = { {"ClusterName", S_P_STRING}, ... };`
  2. `int broker_conf_init(const char *path)`:
     - `s_p_hashtbl_t *tbl = s_p_hashtbl_create(broker_options);`
     - `s_p_parse_file(tbl, NULL, path, false);`
     - 逐字段 `s_p_get_string(...)` / `s_p_get_uint16(...)` 拷贝到 `g_broker_conf`。
     - 必填字段缺失 → `fatal("broker.conf: missing %s")`。
  3. 处理 `Include` 指令 (M02-T4 用)。
- 完成标准:
  - [ ] 写一份合法 conf 与一份缺字段 conf, 前者解析后字段值正确, 后者 fatal 退出码 1。

#### M02-T3 字段语义校验
- 模块: M02 / Sprint: S1 / 预估: 0.5d / 依赖: M02-T2
- 目标: 端口范围、目录可读、SSH key 文件存在等非低级语法校验。
- 实现步骤:
  1. `ctld_port`/`peer_port` ∈ [1024, 65535] 且互不相等。
  2. `state_save_location` 必须存在且 broker 进程可写; 不存在则 `mkdir -p` 一次。
  3. `stage_ssh_key` 必须存在, `stat` 后权限 ≤ 0600。
  4. `lookup_software_script` 必须 `X_OK`, 否则 fatal。
- 完成标准:
  - [ ] 故意把 ssh key 改成 0644, 启动时 fatal 提示。

#### M02-T4 user_mapping.conf 解析与全局表
- 模块: M02 / Sprint: S1 / 预估: 1d / 依赖: M02-T2
- 目标: 解析 `UserMapping LocalUser=... RemoteCluster=... RemoteUser=... RemoteUid=... RemoteGid=...`, 构建可 O(1) 查询的 hash 表。
- 实现步骤:
  1. `user_mapping.h` 定义 `typedef struct user_mapping { char *local_user; char *remote_cluster; char *remote_user; uint32_t remote_uid; uint32_t remote_gid; } user_mapping_t;`
  2. 全局: `xhash_t *g_user_mappings;` key = `"local_user|remote_cluster"`。
  3. `int user_mapping_load(const char *path);` 用 `s_p_options_t` 中 `S_P_LINE` 处理多行。
  4. 提供 `user_mapping_t *user_mapping_lookup(const char *local_user, const char *remote_cluster);`。
  5. 提供 `void user_mapping_destroy_all(void);` 给 fini 用。
- 完成标准:
  - [ ] 写 3 行映射, lookup `(test1, wz_cluster)` 返回正确 remote_uid。
  - [ ] lookup 不存在的 user 返回 NULL。

#### M02-T5 启动日志摘要打印
- 模块: M02 / Sprint: S1 / 预估: 0.25d / 依赖: M02-T4
- 目标: 启动后 INFO 级别打印关键配置一行式摘要, 方便运维 / 排错。
- 实现步骤:
  1. `info("broker_conf: cluster=%s peer=%s:%u default_partition=%s lookup=%s mappings=%u", ...)`。
- 完成标准:
  - [ ] `journalctl -u slurmbrokerd | head` 能看到该行。

---

## M03 数据结构与持久化

### 模块概述
内存全局表 `g_broker_jobs` + `g_broker_jobs_list` + 锁; JSONL 三文件原子 rename checkpoint; 启动 restore。是 broker 的"内存数据库"。

### 接口契约
- `broker_job_t` 字段顺序与设计文档 §4.1 一致。
- 持久化文件: `<state_save_location>/<state_file_name>`, 后缀 `.tmp` / `.old`。
- API:
  - `broker_job_t *broker_job_create(void);`
  - `void broker_job_destroy(broker_job_t *);`
  - `int broker_job_table_add(broker_job_t *);`
  - `broker_job_t *broker_job_table_get(const char *trace_id);`
  - `int broker_job_table_remove(const char *trace_id);`
  - `int broker_state_save(void);` / `int broker_state_restore(void);`
  - `void persist_async_request(void);` (打异步标志位, 由 ticker 线程下次 tick flush)

### 涉及文件
```
src/slurmbrokerd/broker_job.c/.h
src/slurmbrokerd/persist.c/.h
```

### 任务列表

#### M03-T1 `broker_job_t` 定义与构造/析构
- 模块: M03 / Sprint: S1 / 预估: 0.5d / 依赖: M02-T1
- 目标: 严格按设计文档 §4.1, 字段一字不漏, 加 mutex。
- 实现步骤:
  1. `broker_job.h` 写 `enum broker_job_state_t` (8 个值) 与 `enum broker_role_t` (2 值) 与 `struct broker_job`。
  2. `broker_job_create()`: xmalloc + 初始化 mutex + state=INIT + state_enter_time=now。
  3. `broker_job_destroy()`: xfree 所有 char*, free job_desc (用 `slurm_free_job_desc_msg()`), destroy mutex。
- 完成标准:
  - [ ] valgrind: create+destroy 1000 次 0 byte still reachable。

#### M03-T2 全局表初始化与基本 CRUD
- 模块: M03 / Sprint: S1 / 预估: 0.5d / 依赖: M03-T1
- 目标: 一次性写好加锁的 add / get / remove / count / iterate。
- 实现步骤:
  1. `broker_job_table_init()`: `xhash_init`(...), `list_create(...)`, `pthread_mutex_init(...)`。
  2. add: 先持表锁, `xhash_add` + `list_append`。
  3. get: 持表锁, `xhash_get`。
  4. remove: 持表锁, `xhash_delete` + `list_remove`。
  5. 提供 `void broker_job_table_foreach(int (*fn)(broker_job_t *, void *), void *arg);` 内部加锁迭代。
- 完成标准:
  - [ ] 多线程 100 ms 内并发 1000 次 add/get/remove 不死锁不脏读。

#### M03-T3 `broker_job_to_json()` 序列化
- 模块: M03 / Sprint: S1 / 预估: 1d / 依赖: M03-T1
- 目标: 把 `broker_job_t` 转成 JSON 行, 复杂的 `job_desc` 走 Slurm pack + base64。
- 实现步骤:
  1. 引入 `src/common/data.c`/`data_t` 或自己用 `xstrdup_printf` 拼接 (MVP 后者更轻)。
  2. 简单字段: `trace_id`, `src_job_id`, `state`, `src_user_name`, `remote_user_name`, `remote_job_id`, `dst_cluster`, `target_partition`, `state_reason`, `retry_count`, `state_enter_time`, `submit_time`, `last_poll_time`, `remote_start_time`, `remote_end_time`, `remote_alloc_tres`, `remote_exit_code`, `cancel_requested`, `cancel_propagated`, `hop_count`, `role`。
  3. 复杂字段: 用 `pack_job_desc_msg(job->job_desc, buffer, protocol_version)` → `base64_encode()` → `"job_desc_b64": "..."`。
  4. 字符串中 `"` `\` 必须 escape, 写一个 `_json_escape()` helper。
- 完成标准:
  - [ ] 一个含中文 reason / 含特殊字符的 job 序列化后, `python3 -c 'import json,sys;json.loads(sys.stdin.read())'` 能解析。

#### M03-T4 `broker_job_from_json()` 反序列化
- 模块: M03 / Sprint: S1 / 预估: 1d / 依赖: M03-T3
- 目标: 反向, 兼容字段缺失 (老版本文件 → 给 0/NULL 默认值)。
- 实现步骤:
  1. 用 `data_g_deserialize("json", line, ...)` 拿 `data_t`, 或自写极简扫描 (键值对到 `:`、`,`、`}` 边界)。MVP 推荐用 Slurm 已经 link 的 json-c 或 jansson。
  2. base64 decode `job_desc_b64` → buffer → `unpack_job_desc_msg(buffer, protocol_version)`。
  3. 字段拷贝到新 `broker_job_t`, mutex 初始化, 返回。
  4. base64 解析失败或必填字段缺失 → 返回 NULL, 上层跳过此行 + warn。
- 完成标准:
  - [ ] save 100 jobs → restart → restore 后表内 100 条, 关键字段全部一致 (写一个 diff 工具脚本)。

#### M03-T5 `broker_state_save()` 原子写
- 模块: M03 / Sprint: S1 / 预估: 0.5d / 依赖: M03-T3
- 目标: 三文件 (`current` / `tmp` / `old`) + fsync + rename, 与设计文档 §4.4 完全一致。
- 实现步骤:
  1. 打开 `tmp`, foreach 全局表写 JSONL。
  2. `fflush + fsync(fd) + fclose`。
  3. `rename(STATE_FILE, STATE_FILE_OLD)` (允许 ENOENT)。
  4. `rename(STATE_FILE_TMP, STATE_FILE)`。
  5. 全程持表锁 (M03-T2 的 foreach 已加锁就好), 失败回滚 tmp 文件 (unlink)。
- 完成标准:
  - [ ] 故意在 fclose 后 kill -9, 重启后能用 `.old` 恢复 (M03-T6 涵盖)。

#### M03-T6 `broker_state_restore()` 启动加载
- 模块: M03 / Sprint: S1 / 预估: 0.5d / 依赖: M03-T4, M03-T5
- 目标: 启动时优先读 `current`, 不存在或 0 字节就读 `.old`, 都没有就 0 jobs 启动。
- 实现步骤:
  1. `if (stat(STATE_FILE) == 0 && size > 0) restore_from(STATE_FILE);`
  2. `else if (stat(STATE_FILE_OLD) == 0) restore_from(STATE_FILE_OLD); warn("primary state missing, fell back to .old")`
  3. 行级 try/skip-with-warn, 不让一行损坏阻断整体 restore。
- 完成标准:
  - [ ] 删 current 留 old, 启动看到 fallback 日志且 jobs 数正确。

#### M03-T7 30s 周期 checkpoint + 异步触发
- 模块: M03 / Sprint: S1 / 预估: 0.5d / 依赖: M03-T5
- 目标: 后台一个低频线程, 每 `CheckpointInterval` 秒 + `persist_async_request()` 被调用时, 触发一次 save。
- 实现步骤:
  1. `pthread_create(&persist_tid, NULL, persist_thread_main, NULL);`
  2. 内部用 `pthread_cond_timedwait()`, timeout = checkpoint_interval; 被 `persist_async_request()` 调 `pthread_cond_signal` 后立即跑一次。
  3. shutdown 时 join 并最后强制 save 一次。
- 完成标准:
  - [ ] handle_forward_job 接到一个 job 后 ≤ 1s state.jsonl 被刷新 (因为 `persist_async_request` 即时触发)。

---

## M04 RPC 协议 pack/unpack

### 模块概述
broker 自定义消息的 wire format。**ctld 端必须能解出**, 因此 pack/unpack 代码放 `src/common/slurm_protocol_pack.c` 段中, broker 与 ctld 链接同一份目标文件。本模块在 broker 这边只做"调用方", 但需要主导评审 ctld 工程师写的 pack 代码。

### 接口契约
- 消息类型 ID: 沿用设计文档 v0.1 §6 已申请的偏移 (与 ctld 协调, 文档化在 `src/common/slurm_protocol_defs.h`)。
- protocol_version: `SLURM_25_05_PROTOCOL_VERSION` (与当前主线一致, 后续 master 升级再改)。
- 消息列表 (MVP):
  | 消息 | 字段顺序 (pack 顺序) |
  |---|---|
  | REQUEST_FORWARD_JOB | src_job_id (uint32), src_uid, src_gid, src_user_name, target_cluster, src_work_dir, script_path, account, app_name, job_desc (pack_job_desc_msg) |
  | REQUEST_BROKER_FORWARD_JOB | trace_id, hop_count, src_cluster, src_job_id, src_user_name, remote_user_name, target_partition, app_name, job_desc |
  | RESPONSE_BROKER_ACK | error_code (uint32), trace_id |
  | REQUEST_BROKER_STAGED_IN | trace_id |
  | RESPONSE_BROKER_SUBMITTED | error_code, trace_id, remote_job_id |
  | REQUEST_BROKER_QUERY_STATUS | trace_id_count (uint32), [trace_ids] |
  | RESPONSE_BROKER_STATUS | entry_count, [entries: trace_id, remote_state, remote_start_time, remote_end_time, remote_alloc_tres, remote_exit_code] |
  | REQUEST_BROKER_CANCEL | src_job_id (源端发) 或 trace_id (broker→broker 发) |
  | REQUEST_BROKER_CLEANUP | trace_id |
  | REQUEST_BROKER_UPDATE_REMOTE_STATE | src_job_id, trace_id, remote_cluster_name, remote_partition_name, remote_job_id, remote_state, remote_alloc_tres, remote_start_time |
  | REQUEST_BROKER_TERMINAL_STATE | (上者) + remote_end_time, remote_exit_code |

### 涉及文件
```
src/slurmbrokerd/proto.c/.h            # broker 内部 wrapper / 类型定义
src/common/slurm_protocol_defs.h       # 加 msg type 枚举 + 错误码 ESLURM_BROKER_*
src/common/slurm_protocol_pack.c       # pack/unpack 实现 (与 ctld 共享)
src/common/slurm_protocol_pack.h
```

### 任务列表

#### M04-T1 申请并固化 msg_type 枚举与错误码
- 模块: M04 / Sprint: S1 / 预估: 0.5d / 依赖: 无 / 协调: ctld
- 目标: 在 `slurm_protocol_defs.h` 给 broker 消息划一段连续 ID, 确定 `ESLURM_BROKER_*` 错误码。
- 实现步骤:
  1. 找一段未占用的 msg_type 区间 (建议 8000~8099 给跨域 broker, 与 slurmrestd 区分)。
  2. 加 `REQUEST_FORWARD_JOB`, `REQUEST_BROKER_FORWARD_JOB`, ... 等 11 个 enum。
  3. 加 `ESLURM_BROKER_OVERLOAD`, `ESLURM_BROKER_NO_USER_MAPPING`, `ESLURM_BROKER_USER_MAPPING_MISMATCH`, `ESLURM_BROKER_HOP_EXCEEDED`, `ESLURM_BROKER_LOOKUP_FAILED`, `ESLURM_BROKER_LOOKUP_TIMEOUT`, `ESLURM_BROKER_STAGE_FAILED`, `ESLURM_BROKER_REMOTE_SUBMIT_FAILED`, `ESLURM_BROKER_NOT_FOUND`。
- 完成标准:
  - [ ] enum 在 `slurm_strerror()` 有对应中英文; ctld 工程师确认本 PR。

#### M04-T2 定义 broker payload C 结构 + 提供 init/free
- 模块: M04 / Sprint: S1 / 预估: 1d / 依赖: M04-T1
- 涉及文件: `src/common/slurm_protocol_defs.h` (struct 声明)、`src/common/slurm_protocol_defs.c` (init/free)
- 目标: 11 种消息每种一个 `*_msg_t`, 与 §6.1 字段顺序一致。
- 实现步骤:
  1. 例: `typedef struct broker_forward_job_msg { char *trace_id; uint8_t hop_count; ... job_desc_msg_t *job_desc; } broker_forward_job_msg_t;`
  2. 提供 `slurm_free_broker_forward_job_msg(broker_forward_job_msg_t *)` 释放所有 char* + job_desc。
  3. 在 `slurm_msg_t_init`/free 路径增加分支。
- 完成标准:
  - [ ] valgrind: 构造 → free 不漏。

#### M04-T3 pack/unpack 实现 (broker → broker 5 个)
- 模块: M04 / Sprint: S1 / 预估: 1.5d / 依赖: M04-T2
- 目标: `pack_*_msg` / `unpack_*_msg` 5 套, 加入 `slurm_protocol_pack.c` 大 switch 分支。
- 实现步骤:
  1. 严格按字段顺序: `pack32(msg->trace_id_len, buffer); packstr(msg->trace_id, buffer); ...`
  2. 复杂的 `job_desc` 调 `pack_job_desc_msg(msg->job_desc, buffer, protocol_version)`。
  3. unpack 镜像。
  4. 在 `pack_msg()` / `unpack_msg()` 大 switch 中插入 case。
- 完成标准:
  - [ ] 单测 round-trip: 构造 → pack → unpack → 字段值与原结构 deep equal (写在 `src/common/test/test_broker_proto.c`)。

#### M04-T4 pack/unpack 实现 (ctld ↔ broker 4 个)
- 模块: M04 / Sprint: S1 / 预估: 1d / 依赖: M04-T3
- 目标: `REQUEST_FORWARD_JOB`, `REQUEST_BROKER_UPDATE_REMOTE_STATE`, `REQUEST_BROKER_TERMINAL_STATE`, `REQUEST_BROKER_CANCEL`。
- 实现步骤: 同 M04-T3。
- 完成标准:
  - [ ] 与 ctld 工程师跨进程对齐: 用 mock client 发一条到 mock server, 字段一致。

#### M04-T5 broker 内部 wrapper (`proto.c/.h`)
- 模块: M04 / Sprint: S1 / 预估: 0.5d / 依赖: M04-T3, M04-T4
- 目标: 在 broker 进程里给业务代码提供更简单的封装 (例如 `proto_send_to_peer(slurm_msg_t *)`), 隐藏 working_cluster_rec 的细节。
- 实现步骤:
  1. `proto_init()` 注册各种 msg 的 init/free 回调到全局 (M01-T6 启动时调用)。
  2. `proto_send_recv_to_peer(int msg_type, void *req, int *rc, void **resp);` 内部用 `slurm_send_recv_node_msg` (已经支持 munge), 目标地址从 `g_broker_conf.remote_broker_host:port` 取。
- 完成标准:
  - [ ] 单元: 空启动 + proto_init + proto_fini 无错。

---

# 第二部分: 网络与处理模块 (M05 ~ M08)

> 这部分把"骨架"接通"通信线"。完成后, broker 已经具备接收 ctld 投递、向远端 broker 转发、向 ctld 回写状态的能力, 但还没有调度逻辑驱动状态向前推进。

---

## M05 网络监听与分发

### 模块概述
单线程 select 双端口监听 ctld 入站 (`BrokerCtldPort`) 与远端 broker 入站 (`BrokerPeerPort`), 同步阻塞处理。设计文档 §8.3 已给完整代码骨架。

### 涉及文件
```
src/slurmbrokerd/listener.c/.h
```

### 任务列表

#### M05-T1 监听 socket 创建 + select 循环
- 模块: M05 / Sprint: S1 / 预估: 1d / 依赖: M02-T3
- 涉及函数: `int listener_start(void)`, `static void *listener_thread(void *)`, `static int listen_socket(const char *bind_addr, uint16_t port)`
- 目标: 起一个线程, listen 0.0.0.0 两个端口, 1s tick 检查 shutdown。
- 实现步骤:
  1. `listen_socket()`: socket + setsockopt(SO_REUSEADDR) + bind + listen(128)。
  2. select 双 fd, timeout 1s, 收到事件后 `accept()`。
  3. 优雅退出: shutdown 触发后 close fd 退线程。
- 完成标准:
  - [ ] `nc -zv localhost <port>` 通; `kill -TERM` 后端口立刻释放。

#### M05-T2 单请求处理 `handle_one_request()`
- 模块: M05 / Sprint: S1 / 预估: 0.5d / 依赖: M04-T2, M05-T1
- 目标: 读一个 RPC, 走 munge_decode (Slurm 自带), 调 dispatch。
- 实现步骤:
  1. `slurm_msg_t_init(&msg);`
  2. `slurm_recv_msg_blocking(fd, &msg);` (Slurm 自带, 内部包含 munge_decode)
  3. `if (from_ctld) dispatch_ctld_msg(&msg); else dispatch_remote_msg(&msg);`
  4. 处理完毕 close(fd)。
- 完成标准:
  - [ ] 用现成 Slurm 客户端发一条任意类型 RPC, broker 不 crash, dispatch 路径打 debug 日志。

#### M05-T3 dispatch_ctld_msg / dispatch_remote_msg 路由表
- 模块: M05 / Sprint: S1 / 预估: 0.5d / 依赖: M05-T2
- 目标: 大 switch 路由, 匹配后调对应 handler (M06/M07 实现); 不认识的 msg_type → 回 ESLURM_INVALID_RPC。
- 实现步骤:
  1. `dispatch_ctld_msg`: case REQUEST_FORWARD_JOB → handle_forward_job(); case REQUEST_BROKER_CANCEL → handle_cancel_from_ctld(); default → 回错误。
  2. `dispatch_remote_msg`: case REQUEST_BROKER_FORWARD_JOB → handle_broker_forward_job(); ... 共 5 case。
  3. 路由前打 `debug2("dispatch ctld msg_type=%u from %s", msg->msg_type, addr_str)`。
- 完成标准:
  - [ ] 未知 msg_type 客户端收到错误 ESLURM_INVALID_RPC; 已知 msg_type 调用了对应 handler (mock handler 打日志)。

#### M05-T4 ACL 与来源 IP 校验
- 模块: M05 / Sprint: S2 / 预估: 0.5d / 依赖: M05-T3
- 目标: ctld 端口只允许本机连入 (slurmctld 在同机); peer 端口只允许 `RemoteBrokerHost` 解析后的 IP。
- 实现步骤:
  1. accept 后 getpeername; 与 `127.0.0.1` / 解析 `RemoteBrokerHost` 比对。
  2. 不符 → close + warn。
- 完成标准:
  - [ ] 异机 telnet ctld 端口立即被拒; 同机正常。

---

## M06 ctld 入站处理 (handler_ctld)

### 模块概述
处理来自本机 slurmctld 的两类请求: `REQUEST_FORWARD_JOB` (创建 broker_job) 与 `REQUEST_BROKER_CANCEL` (用户 scancel)。设计文档 §7.1 ~ §7.2 已给完整骨架。

### 涉及文件
```
src/slurmbrokerd/handler_ctld.c/.h
```

### 任务列表

#### M06-T1 `handle_forward_job()` 主流程
- 模块: M06 / Sprint: S2 / 预估: 1.5d / 依赖: M02-T4, M03-T2, M04-T2, M05-T3
- 目标: 与设计文档 §7.1 完全一致, 7 步骨架可跑。
- 实现步骤:
  1. 溢出保护: `count_inflight() >= max_inflight` → 回 `ESLURM_BROKER_OVERLOAD`。
  2. 用户映射 lookup, 缺失 → `ESLURM_BROKER_NO_USER_MAPPING`。
  3. 创建 `broker_job_t`, 填字段 (trace_id 用 `<src_cluster>-<src_job_id>`)。
  4. `broker_job_table_add()`。
  5. `persist_async_request()`。
  6. 构造 `RESPONSE_FORWARD_JOB`: error_code=0 + trace_id, `slurm_send_response()`。
  7. 立即返回; INIT → STAGING_IN 由状态机推。
- 接口:
  - 函数: `int handle_forward_job(slurm_msg_t *msg);`
  - 内部 helper: `static uint32_t count_inflight(void);` (遍历表统计 state ∈ {INIT,STAGING_IN,STAGED_IN,SUBMITTED,RUNNING,STAGING_OUT})
- 完成标准:
  - [ ] 用 mock ctld client 投 1 个作业, broker 表内出现一条 INIT, ACK 返回 OK trace_id。
  - [ ] 投 600 个时, 第 501 个起返回 ESLURM_BROKER_OVERLOAD。

#### M06-T2 `handle_cancel_from_ctld()` 主流程
- 模块: M06 / Sprint: S2 / 预估: 0.5d / 依赖: M03-T2, M06-T1
- 目标: 用户 scancel, ctld 通知 broker 反向传播。
- 实现步骤:
  1. 按 src_job_id 查表; 找不到回 ESLURM_BROKER_NOT_FOUND (ctld 端做幂等)。
  2. `pthread_mutex_lock(&job->lock); job->cancel_requested = true; pthread_mutex_unlock(...)`。
  3. `persist_async_request()`。
  4. 回 SLURM_SUCCESS。状态机 tick 会调 `egress_cancel_async()`。
- 完成标准:
  - [ ] 投 1 个作业到 RUNNING, scancel 后 ≤ 5s broker_job state 变 CANCELLED, 远端 (mock) 收到 CANCEL。

#### M06-T3 ACL: src_uid 必须等于 broker_job.src_uid
- 模块: M06 / Sprint: S2 / 预估: 0.25d / 依赖: M06-T2
- 目标: scancel 反向到 broker 时, 校验请求者 uid (msg->auth_uid) 是 owner 或 root/SlurmUser, 防止越权。
- 实现步骤:
  1. 取 `slurm_get_uid_from_msg(msg)`; 与 `job->src_uid` 比对; 否则 `ESLURM_USER_ID_MISSING`。
- 完成标准:
  - [ ] 用其他用户的 uid 模拟 cancel, 被拒。

---

## M07 远端 broker 入站处理 (handler_remote)

### 模块概述
处理远端 broker 发来的 5 类 RPC, broker 在此承担 RECEIVER 角色。设计文档 §7.1 (后半段) 与 §7.x 给了骨架。

### 涉及文件
```
src/slurmbrokerd/handler_remote.c/.h
```

### 任务列表

#### M07-T1 `handle_broker_forward_job()` (RECEIVER 入单)
- 模块: M07 / Sprint: S2 / 预估: 1.5d / 依赖: M02-T4, M03-T2, M04-T2
- 目标: 远端创建 RECEIVER 角色 broker_job, 创建 dst_work_dir, 回 ACK。
- 实现步骤:
  1. hop_count > 0 → `ESLURM_BROKER_HOP_EXCEEDED`。
  2. user_mapping_lookup 反向匹配, mismatch → `ESLURM_BROKER_USER_MAPPING_MISMATCH`。
  3. 创建 broker_job (role=RECEIVER, hop_count = req+1)。
  4. dst_work_dir 模板: `/work/home/<remote_user>/.burst/<src_cluster>/<src_job_id>`。
  5. fork+exec `sudo -u <remote_user> mkdir -p <dst_work_dir> && chmod 700 <dst_work_dir>`, 失败回 ESLURM_BROKER_STAGE_FAILED。
  6. add 表 + persist。
  7. 回 RESPONSE_BROKER_ACK { 0, trace_id }。
- 完成标准:
  - [ ] mock 源端发 BROKER_FORWARD_JOB, 远端 broker 表出现 RECEIVER, dst dir 实际创建 (ls 确认)。

#### M07-T2 `handle_broker_staged_in()` (远端代投)
- 模块: M07 / Sprint: S2 / 预估: 1d / 依赖: M07-T1, M12-T1 (rewrite 也要先有 stub)
- 目标: 收到源端"数据已传完", broker 调 rewrite + slurm_submit_batch_job 给本地 ctld。
- 实现步骤:
  1. 查表拿 RECEIVER job; 状态机加锁。
  2. `rewrite_job_script(job, &modified_path)` (M12)。
  3. `submit_response_msg_t *resp = NULL; slurm_submit_batch_job(job->job_desc, &resp);` (注意此前 M12-T2 已经 `xfree(job->job_desc->account); = NULL`)。
  4. 成功: `job->remote_job_id = resp->job_id;` `transition(job, BROKER_STATE_SUBMITTED);` 回 RESPONSE_BROKER_SUBMITTED { 0, trace_id, remote_job_id }。
  5. 失败: 回 RESPONSE_BROKER_SUBMITTED { errno, trace_id, 0 }; 标记 job FAILED。
- 完成标准:
  - [ ] 端到端 mock: 远端真的有 sbatch 提交成功, `squeue` 看到 remote_job_id。

#### M07-T3 `handle_broker_query_status()` 批量回报
- 模块: M07 / Sprint: S3 / 预估: 1d / 依赖: M03-T2
- 目标: 收一批 trace_id, 查每个 RECEIVER 对应 remote_job_id 的实时状态, 拼 RESPONSE_BROKER_STATUS。
- 实现步骤:
  1. 解析 trace_ids 数组。
  2. 对每个 trace_id 查 RECEIVER job; 若 job 不存在或 remote_job_id=0 → 占位 entry remote_state=JOB_PENDING。
  3. 对存在的, 调 `slurm_load_job(&job_info_msg, remote_job_id, SHOW_DETAIL)` → 取 state, start_time, end_time, exit_code, alloc_tres。
  4. 组装 `broker_status_msg_t resp` 回。
- 完成标准:
  - [ ] 单 broker 双角色测试: 源端调 query, 远端 (同机伪装) 返回正确 state。

#### M07-T4 `handle_broker_cancel()` (远端 broker 收到 cancel)
- 模块: M07 / Sprint: S2 / 预估: 0.5d / 依赖: M07-T1
- 目标: 远端调 `slurm_kill_job(remote_job_id, SIGTERM, KILL_FULL_JOB)`。
- 实现步骤:
  1. 查表 RECEIVER job by trace_id。
  2. `slurm_kill_job(job->remote_job_id, SIGTERM, KILL_FULL_JOB);` 失败 warn, 不阻塞。
  3. `job->cancel_propagated = true;` `transition(job, BROKER_STATE_CANCELLED);`
  4. 回 SLURM_SUCCESS。
- 完成标准:
  - [ ] 远端 squeue 看到作业被 kill。

#### M07-T5 `handle_broker_cleanup()` 远端清单
- 模块: M07 / Sprint: S3 / 预估: 0.5d / 依赖: M07-T1
- 目标: RECEIVER 收到清理指令, fork+exec `sudo -u <remote_user> rm -rf <dst_work_dir>`, 删表条目。
- 实现步骤:
  1. 查表 RECEIVER job; 不存在视为已清理, 回 OK。
  2. fork+exec `sudo -u <remote_user> /bin/rm -rf <dst_work_dir>`; 等 30s 超时。
  3. table_remove + persist_async_request。
- 完成标准:
  - [ ] dst 目录被实际删除; broker 表条目消失。

---

## M08 egress 出站调用

### 模块概述
所有 broker 主动发起的 RPC: 给远端 broker 发 (FORWARD_JOB / STAGED_IN / QUERY_STATUS / CANCEL / CLEANUP), 给本地 ctld 推 (UPDATE_REMOTE_STATE / TERMINAL_STATE)。封装 `slurm_send_recv_*_msg`, 加重试 + 超时 + 日志。

### 涉及文件
```
src/slurmbrokerd/egress.c/.h
```

### 任务列表

#### M08-T1 `egress_init/fini` + 通用 send_recv 包装
- 模块: M08 / Sprint: S2 / 预估: 1d / 依赖: M04-T5
- 目标: `static int _send_to_peer(int msg_type, void *req, int *rc, void **resp_out, int timeout_sec)`, 内部:
  - 构 working_cluster_rec (host=remote_broker_host, port=remote_broker_port)
  - `slurm_send_recv_node_msg(...)`
  - 超时 → ESLURM_PROTOCOL_TIMEOUT。
- 实现步骤:
  1. 准备 working_cluster_rec_t 单例, init 时填好。
  2. 写一个 retry helper: `int retry_n_times(fn, n, backoff)` (用于网络抖动)。
- 完成标准:
  - [ ] mock peer 关闭端口, 客户端在 timeout 内返回错误码, 不阻塞主线程。

#### M08-T2 `egress_forward_async()`
- 模块: M08 / Sprint: S2 / 预估: 0.5d / 依赖: M08-T1
- 目标: 把 broker_job_t 打成 BROKER_FORWARD_JOB, 发给远端, 收到 ACK 后推进 INIT → STAGING_IN。
- 实现步骤:
  1. 构 `broker_forward_job_msg_t`, 复制字段, job_desc 浅拷贝指针 (引用计数由 broker_job 持有)。
  2. `_send_to_peer(REQUEST_BROKER_FORWARD_JOB, &req, &rc, NULL, 30)`。
  3. 成功: `transition(job, BROKER_STATE_STAGING_IN); stage_submit_in(job);`
  4. 失败: 重试 3 次后 transition FAILED, state_reason="forward rejected: <code>"。
- 完成标准:
  - [ ] 单 job 端到端 forward 成功; ACK 失败时 FAILED 写入。

#### M08-T3 `egress_staged_in_async()`
- 模块: M08 / Sprint: S2 / 预估: 0.5d / 依赖: M08-T1
- 目标: stage worker 完成 stage-in 后调用; 发 STAGED_IN, 拿 RESPONSE_BROKER_SUBMITTED。
- 实现步骤:
  1. 构 `broker_staged_in_msg_t = { trace_id }`。
  2. `_send_to_peer(REQUEST_BROKER_STAGED_IN, &req, &rc, &resp, 60)`。
  3. resp 有 remote_job_id → `job->remote_job_id = ...; transition SUBMITTED; ctld_update_remote_state(job);`
  4. 失败: state FAILED + reason。
- 完成标准:
  - [ ] 端到端跑通 SUBMITTED 状态。

#### M08-T4 `egress_query_status_sync()`
- 模块: M08 / Sprint: S3 / 预估: 0.5d / 依赖: M08-T1
- 目标: sync_ticker 用, 一次问一批 trace_ids, 拿 entries。
- 实现步骤:
  1. 构 `broker_query_status_msg_t = { trace_ids[], n }`。
  2. `_send_to_peer(REQUEST_BROKER_QUERY_STATUS, ..., &resp, 30)`。
  3. 把 resp 的 entries 透传给调用者 (sync_ticker 自己 free)。
- 完成标准:
  - [ ] 给 100 个 trace_id 拿到 100 entries (mock peer)。

#### M08-T5 `egress_cancel_async()`
- 模块: M08 / Sprint: S2 / 预估: 0.5d / 依赖: M08-T1
- 目标: 状态机检测 cancel_requested 后调用; 一次性发 REQUEST_BROKER_CANCEL { trace_id }。
- 实现步骤:
  1. 调 _send_to_peer(REQUEST_BROKER_CANCEL, ...), 不等 resp body, 只看 rc。
  2. 失败重试 3 次 (cancel 必须最大努力)。
- 完成标准:
  - [ ] 远端 receive 到 CANCEL 并 kill。

#### M08-T6 `ctld_update_remote_state(job)` (broker → ctld 周期推)
- 模块: M08 / Sprint: S3 / 预估: 0.5d / 依赖: M08-T1, M04-T4
- 目标: 用 REQUEST_BROKER_UPDATE_REMOTE_STATE 推送, 字段顺序与设计文档 §6.3.1 一致。
- 实现步骤:
  1. 构 `broker_remote_state_msg_t`, 字段映射见 §6.3.2。
  2. `slurm_send_recv_controller_rc_msg(&req_msg, &rc, working_cluster_rec)`。
  3. 失败 warn, 不阻塞 (下一轮 sync 会重推)。
- 完成标准:
  - [ ] ctld 端 mock handler 收到对应字段; `comment` 字段保持空。

#### M08-T7 `ctld_inject_terminal_state(job)`
- 模块: M08 / Sprint: S3 / 预估: 0.5d / 依赖: M08-T6
- 目标: 终态时一次性推完整字段。失败 error 不阻塞 (ctld 跨域线程会重试)。
- 实现步骤:
  1. 构 `broker_terminal_state_msg_t`, 比 update 多 end_time, exit_code。
  2. `slurm_send_recv_controller_rc_msg(...)`。
  3. 成功 info; 失败 error。
- 完成标准:
  - [ ] sacct 写入 Remote_End_Time / Remote_ExitCode; 影子作业由 PENDING(Held) 跳变 COMPLETED/FAILED。

---

# 第三部分: 业务模块 (M09 ~ M13)

> 这部分是 broker 的"心脏": 状态机驱动 + 数据搬运 + 脚本改写 + 远端轮询。完成后 broker 真正能把一个跨域作业从 INIT 一路推到 COMPLETED。

---

## M09 状态机

### 模块概述
全局 1s tick 线程, 遍历表, 按设计文档 §5.1/§5.2 处理超时、推进、终态。

### 涉及文件
```
src/slurmbrokerd/state_machine.c/.h
```

### 任务列表

#### M09-T1 状态机线程骨架 + transition()
- 模块: M09 / Sprint: S2 / 预估: 0.5d / 依赖: M03-T2
- 实现步骤:
  1. `pthread_create(&sm_tid, NULL, state_machine_thread, NULL);`
  2. 线程: while(!shutdown) { state_machine_tick(); sleep(1); }
  3. `void transition(broker_job_t *job, broker_job_state_t to, const char *reason);`: 写新 state + state_enter_time = now + state_reason; info 日志; persist_async_request()。
- 完成标准:
  - [ ] 线程能跑能停; transition 调用日志可见。

#### M09-T2 INIT 分支
- 模块: M09 / Sprint: S2 / 预估: 0.5d / 依赖: M08-T2, M09-T1
- 实现步骤:
  1. role==ORIGINATOR → `egress_forward_async(job)` (M08-T2 内部包含状态推进)。
  2. now - state_enter_time > 60 → transition FAILED reason="INIT timeout"。
- 完成标准:
  - [ ] mock peer 不响应, 60s 后 job FAILED。

#### M09-T3 STAGING_IN / STAGING_OUT 超时与重试
- 模块: M09 / Sprint: S2 / 预估: 1d / 依赖: M10-T1, M09-T1
- 实现步骤:
  1. `stage_timeout(job)` = `data_size_GB * stage_timeout_per_gb + 600` (data_size_GB 暂从 src_work_dir 估计或固定 1)。
  2. 超时 + retry < 3 → transition INIT (重发) 或 直接重发 stage_submit_out。
  3. retry == 3 → FAILED。
- 完成标准:
  - [ ] 故意杀掉 rsync, 看到 retry, 第 3 次 FAILED。

#### M09-T4 STAGED_IN 等响应超时 (30s)
- 模块: M09 / Sprint: S2 / 预估: 0.25d / 依赖: M08-T3
- 实现步骤: 30s 重发 STAGED_IN, 3 次 FAILED。
- 完成标准: mock peer 不响应, 90s 后 FAILED。

#### M09-T5 SUBMITTED 24h pending 看门狗
- 模块: M09 / Sprint: S3 / 预估: 0.25d / 依赖: M09-T1
- 实现步骤: 状态 SUBMITTED 且 now - state_enter_time > 86400 → FAILED reason="remote pending too long"。
- 完成标准: 单测构造一个旧 state_enter_time, tick 后 FAILED。

#### M09-T6 cancel 优先级处理
- 模块: M09 / Sprint: S2 / 预估: 0.5d / 依赖: M08-T5
- 实现步骤: 在 switch 前先判 cancel_requested && !cancel_propagated && !终态 → egress_cancel_async + transition CANCELLED。
- 完成标准: scancel 后 ≤ 5s 状态 CANCELLED。

#### M09-T7 终态出表 + ctld_inject_terminal_state
- 模块: M09 / Sprint: S3 / 预估: 0.5d / 依赖: M08-T7, M14-T1
- 实现步骤:
  1. 终态 (COMPLETED/FAILED/CANCELLED) 时调 ctld_inject_terminal_state。
  2. schedule_remote_cleanup(job) (M14)。
  3. 加入 to_remove list, tick 末尾从全局表 remove。
- 完成标准: 端到端 1 个 job 跑完后表内消失 (除非 cleanup 还在 schedule); sacct 字段写入。

---

## M10 数据传输 (rsync)

### 模块概述
设计文档 §9, 用 sudo + rsync + ssh key 跨主机拉/推数据。MVP 用固定 worker pool (默认 4) 异步执行。

### 涉及文件
```
src/slurmbrokerd/stage.c/.h
```

### 任务列表

#### M10-T1 stage worker pool 启动 + 任务队列
- 模块: M10 / Sprint: S2 / 预估: 1d / 依赖: M02-T3
- 实现步骤:
  1. `pthread_t stage_workers[stage_worker_count];`
  2. `list_t *stage_in_queue, *stage_out_queue;` + cond + mutex
  3. worker 循环: pop 任务 → 执行 rsync (M10-T2/T3) → 回调状态机 (transition)。
- 完成标准: 启动 4 worker; submit 一个 noop 任务能被消费。

#### M10-T2 stage-in 子进程实现
- 模块: M10 / Sprint: S2 / 预估: 1.5d / 依赖: M10-T1
- 实现步骤 (与设计文档 §9.1 完全一致):
  1. 拼 ssh / rsync_path / endpoint 参数。
  2. fork+execv `/usr/bin/sudo`, 父进程 waitpid 阻塞 (worker 线程内自然阻塞)。
  3. 退出码 0 → 调 transition(STAGED_IN) + egress_staged_in_async。
  4. 非 0 → 调状态机 transition INIT (让 M09-T3 retry) 或直接 FAILED (retry 3 次)。
  5. stdout/stderr 重定向到 `/var/log/slurm/broker_stage/<trace_id>.log`。
- 完成标准:
  - [ ] 真实 1 GB 目录跨机 rsync, 完成态正确; 故意把 ssh key 删掉跑出 FAILED + 日志可定位。

#### M10-T3 stage-out 子进程实现
- 模块: M10 / Sprint: S3 / 预估: 1d / 依赖: M10-T2
- 实现步骤: 与 stage-in 镜像, 方向反, 触发点是 sync_ticker apply_remote_status 检测 COMPLETE/FAILED 时。完成回调 transition COMPLETED。
- 完成标准:
  - [ ] 远端跑完作业, 源端 src_work_dir 出现回写文件。

#### M10-T4 字节限额 MaxStageBytes
- 模块: M10 / Sprint: S3 / 预估: 0.5d / 依赖: M10-T2
- 实现步骤: 提交 stage-in 任务前 `du -sb` (sudo -u src_user) 估算总字节, 超过 max_stage_bytes 拒绝 + transition FAILED reason="stage size exceeded"。
- 完成标准: 用 60GB 目录测试, 拒绝。

---

## M11 软件路径解析

### 模块概述
fork+exec 外部 `lookup_software.sh`, 输入 `<cluster> <app>`, 输出绝对路径, 3s 超时。`software_routes.conf` 由运维管理, broker 不感知格式。

### 涉及文件
```
src/slurmbrokerd/software.c/.h
scripts/lookup_software.sh                           # 模板, 部署到 /opt/slurm-broker/scripts/
etc/slurm-broker/software_routes.conf.example        # 运维填写
```

### 任务列表

#### M11-T1 `read_with_timeout()` / `waitpid_timeout()` 两个工具
- 模块: M11 / Sprint: S3 / 预估: 0.5d / 依赖: 无
- 实现步骤: select fd 超时读; waitpid + 100 ms 轮询直到超时 → kill -9。
- 完成标准: 单测 sleep 5 配 1s 超时, 进程被 kill。

#### M11-T2 `lookup_software_path()` 主函数
- 模块: M11 / Sprint: S3 / 预估: 1d / 依赖: M11-T1
- 实现步骤 (设计文档 §8.2.1):
  1. pipe + fork + execl 脚本。
  2. 父读 stdout 一行, waitpid 3s 超时 → ESLURM_BROKER_LOOKUP_TIMEOUT。
  3. exit != 0 → ESLURM_BROKER_LOOKUP_FAILED。
  4. 输出非 `/` 开头 → 视为非法 → ESLURM_BROKER_LOOKUP_FAILED。
  5. 成功 *out_path = xstrdup(buf)。
- 完成标准:
  - [ ] mock 脚本 echo 路径; mock 脚本 sleep 10 → 超时; mock exit 1 → FAILED。

#### M11-T3 提供 `lookup_software.sh` 模板与示例 routes
- 模块: M11 / Sprint: S3 / 预估: 0.5d / 依赖: 无
- 实现步骤:
  1. 简单 shell: 读 conf 文件 (key=`<cluster>:<app>`, value=`<path>`), grep 输出。
  2. 找不到 → exit 2 + stderr 提示。
  3. 加 `set -euo pipefail`。
- 完成标准:
  - [ ] `./lookup_software.sh xian_cluster gromacs` 输出路径正确; 不存在 cluster 退出码 2。

---

## M12 脚本改写 (rewrite)

### 模块概述
RECEIVER 端在 STAGED_IN → SUBMITTED 之前调用, 修改 job 脚本: partition / 删除 SBATCH 行 / 前缀替换 / 清空 account。设计文档 §8.2.2 给完整规则。

### 涉及文件
```
src/slurmbrokerd/rewrite.c/.h
```

### 任务列表

#### M12-T1 `rewrite_job_script()` 框架
- 模块: M12 / Sprint: S3 / 预估: 0.5d / 依赖: M11-T2
- 实现步骤:
  1. 函数签名 `int rewrite_job_script(broker_job_t *job, char **out_modified_path);`
  2. 双向 lookup_software_path; 任一失败 → 整体返回 LOOKUP_FAILED 不重试。
- 完成标准:
  - [ ] 单测两端路径都能拿到。

#### M12-T2 行级替换 + drop 规则
- 模块: M12 / Sprint: S3 / 预估: 1.5d / 依赖: M12-T1
- 实现步骤:
  1. 读 `<dst_work_dir>/<basename(script)>` 全文。
  2. 逐行处理:
     - 正则 `^[[:space:]]*#SBATCH[[:space:]]+(--reservation|--cross-domain|--app|--account=|-A[[:space:]]+)` → drop。
     - `#SBATCH -p <X>` / `#SBATCH --partition=X` → 替换为 `target_partition`。
     - 行内子串替换 `src_prefix → dst_prefix` (用 strstr 多次替换)。
  3. 写到 `<dst_work_dir>/<basename>.cd_modified.sh`, mode 0700。
  4. `xfree(job->job_desc->script); job->job_desc->script = xstrdup_printf("%s/%s.cd_modified.sh", dst_work_dir, basename);` (具体看 job_desc 字段, 实际可能是 `job_desc->script` 内联文本)。
  5. **关键**: `xfree(job->job_desc->account); job->job_desc->account = NULL;`
  6. *out_modified_path = xstrdup(...)
- 完成标准:
  - [ ] 给 1 份示例脚本 (含 -p / --reservation / --account / source xxx/env.sh), 输出脚本: partition 已换 / reservation 已删 / account 已删 / source 路径已替换。

#### M12-T3 错误处理与回滚
- 模块: M12 / Sprint: S3 / 预估: 0.25d / 依赖: M12-T2
- 实现步骤:
  1. 任一步失败: log + 返回错误 + 删除半成品 modified.sh。
  2. 调用方 (handler_remote.c::handle_broker_staged_in) 收到非 0 → state FAILED, reason="rewrite failed: ...".
- 完成标准:
  - [ ] 脚本不存在 / 没有写权限 → 失败被正确捕获。

---

## M13 状态轮询 (sync_ticker)

### 模块概述
ORIGINATOR 角色独有, 10s 周期收集 SUBMITTED/RUNNING 的 trace_ids, 一次性 QUERY_STATUS 拿全量, 应用更新到本地 state + 推 ctld。设计文档 §6.2 / §6.3。

### 涉及文件
```
src/slurmbrokerd/sync_ticker.c/.h
```

### 任务列表

#### M13-T1 ticker 线程骨架
- 模块: M13 / Sprint: S3 / 预估: 0.5d / 依赖: M03-T2
- 实现步骤: pthread, while !shutdown { sync_ticker_run(); sleep(poll_interval); }
- 完成标准: 起停干净。

#### M13-T2 收集与发起批量查询
- 模块: M13 / Sprint: S3 / 预估: 1d / 依赖: M08-T4, M13-T1
- 实现步骤:
  1. 表 foreach 收集 ORIGINATOR && state ∈ {SUBMITTED, RUNNING}, push trace_id 到数组。
  2. 调 `egress_query_status_sync(trace_ids, n, &resp)`。
  3. 对每个 entry → `apply_remote_status(entry)`。
  4. free trace_ids, free resp。
- 完成标准:
  - [ ] 100 个 ORIGINATOR job 时, ticker 每 10s 发一次, peer 收到一条带 100 个 id 的请求。

#### M13-T3 `apply_remote_status()` 状态分发
- 模块: M13 / Sprint: S3 / 预估: 1d / 依赖: M08-T6, M10-T3
- 实现步骤 (与设计文档 §6.2 一致):
  - PENDING → ctld_update_remote_state (推送一次 PENDING)
  - RUNNING → 若 state==SUBMITTED, 写 start_time, alloc_tres, transition RUNNING, ctld_update_remote_state
  - COMPLETE/FAILED/TIMEOUT → 写 end_time, exit_code, alloc_tres, stage_submit_out, transition STAGING_OUT
  - CANCELLED → transition CANCELLED
- 完成标准:
  - [ ] 远端 job 走完一个生命周期, 源端 state.jsonl 看到与远端同步的 state 字段; ctld 收到 update RPC。

#### M13-T4 重试与降级
- 模块: M13 / Sprint: S3 / 预估: 0.25d / 依赖: M13-T2
- 实现步骤: query_status 失败 → warn + 跳过本轮 (下轮再试); 连续 `poll_max_retries` 轮失败 → error + 不影响其他逻辑。
- 完成标准: peer 临时不可达, 几轮内无 crash, 恢复后继续。

---

# 第四部分: 运维与验收 (M14 ~ M16)

> 这部分把"代码"变"产品": 把脏数据清干净, 给运维一份"开机就用"的部署清单, 用一套自动化测试守住质量基线。

---

## M14 远端清理与保留

### 模块概述
COMPLETED 24h / FAILED 7d 后用 schedule_remote_cleanup → 发送 REQUEST_BROKER_CLEANUP 给远端, 删除 dst_work_dir。

### 涉及文件
```
src/slurmbrokerd/state_machine.c (新增函数)
src/slurmbrokerd/cleanup.c/.h (可选独立)
```

### 任务列表

#### M14-T1 `schedule_remote_cleanup(job)` 数据结构
- 模块: M14 / Sprint: S3 / 预估: 0.5d / 依赖: M03-T2
- 实现步骤:
  1. 全局延迟队列: `list_t *g_cleanup_queue;`, 元素 `{ trace_id, due_time, dst_cluster }`。
  2. 入队时 `due_time = now + (state==COMPLETED ? retention_h * 3600 : failure_retention_d * 86400)`。
- 完成标准: 入队后 list 长度 +1。

#### M14-T2 cleanup ticker (可与 state_machine 同线程)
- 模块: M14 / Sprint: S3 / 预估: 0.5d / 依赖: M14-T1, M08-T1
- 实现步骤:
  1. 每分钟检查 g_cleanup_queue, due_time <= now 的 pop 出来。
  2. 构 `REQUEST_BROKER_CLEANUP { trace_id }` 发送; 失败重试 3 次后丢警告 (远端目录最终会被运维 cron 清)。
- 完成标准:
  - [ ] 写一个 retention=60 测试构造, 60s 后远端 dst dir 被删 (M07-T5 落地)。

---

## M15 部署运维工件

### 模块概述
设计文档 §10 全部"非代码"产物: systemd, sudoers, ssh key, munge.key 同步, lookup_software.sh, software_routes.conf, broker.conf 模板。

### 任务列表

#### M15-T1 broker.conf 与 user_mapping.conf 模板与文档
- 模块: M15 / Sprint: S1 / 预估: 0.5d / 依赖: M02-T1
- 涉及文件: `etc/slurm-broker/broker.conf.example`, `etc/slurm-broker/user_mapping.conf.example`, README 段落
- 完成标准: 运维拿模板改三五项就能跑。

#### M15-T2 SSH key 生成与分发脚本 (`scripts/setup_ssh.sh`)
- 模块: M15 / Sprint: S1 / 预估: 0.5d / 依赖: 无
- 实现步骤: 设计文档 §9.2 四步打包成 idempotent shell。
- 完成标准: 单机连测, 二次跑不会破坏现有 key。

#### M15-T3 sudoers 片段 + lint
- 模块: M15 / Sprint: S1 / 预估: 0.25d / 依赖: 无
- 涉及文件: `etc/sudoers.d/slurm-broker.example` (远端) / `etc/sudoers.d/slurm-rsync.example` (源端)
- 实现步骤: 严格匹配 `/usr/bin/rsync`, `/bin/mkdir`, `/bin/chmod`, `/bin/rm`; visudo -c 通过。
- 完成标准: visudo -cf 该文件成功。

#### M15-T4 munge.key 同步 + TTL 调整脚本
- 模块: M15 / Sprint: S1 / 预估: 0.5d / 依赖: 无
- 实现步骤: 设计文档 §10.3, §10.4。提供 `scripts/sync_munge.sh` (rsync munge.key 到对端 broker 主机, 备份本地, 重启 munged); 提供 `etc/sysconfig/munge.example` 设置 `--ttl 86400`。
- 完成标准: 两端 munge cred 互认证 24h 内不过期。

#### M15-T5 lookup_software.sh + software_routes.conf 部署位置规约
- 模块: M15 / Sprint: S3 / 预估: 0.25d / 依赖: M11-T3
- 实现步骤: 路径固化 `/opt/slurm-broker/scripts/lookup_software.sh`, `/etc/slurm-broker/software_routes.conf`; broker.conf 默认值与之一致。
- 完成标准: 安装后默认配置即可工作。

#### M15-T6 systemd unit 与 logrotate
- 模块: M15 / Sprint: S1 / 预估: 0.5d / 依赖: M01-T7
- 实现步骤:
  1. logrotate 片段: `/var/log/slurm/slurmbrokerd.log` weekly 4 keep。
  2. systemd `LimitNOFILE=65536`, `Restart=on-failure`。
- 完成标准: logrotate -d 不报错。

---

## M16 集成测试与验收

### 模块概述
端到端 smoke / 故障注入 / 长稳压测。把设计文档 §13 验收清单逐条变成可重复脚本。

### 涉及文件
```
tests/broker/mvp_smoke_test.sh
tests/broker/fault_*.sh
tests/broker/load_burst.sh
```

### 任务列表

#### M16-T1 `mvp_smoke_test.sh` 端到端冒烟
- 模块: M16 / Sprint: S4 / 预估: 1d / 依赖: 全部
- 实现步骤 (设计文档 §15.3 已给草稿):
  1. submit cross-domain job (sbatch --cross-domain --app=...)。
  2. 轮询 squeue --remote 等 RemoteJobId 出现。
  3. 等 RUNNING; 等 COMPLETED。
  4. 校验 sacct 里 Remote_*; 校验 comment 字段为空 (无污染)。
  5. 校验源端工作目录有回传文件。
- 完成标准: bash -e 跑通, exit 0。

#### M16-T2 故障注入: lookup_software 失败
- 模块: M16 / Sprint: S4 / 预估: 0.5d / 依赖: M16-T1
- 实现步骤: 替换 lookup_software.sh 为 `exit 1`, 提交跨域作业, 期望 broker_job 状态 FAILED, state_reason 含 "lookup_software", 源端 squeue --remote 看到 RemoteState=FAILED。
- 完成标准: 自动校验。

#### M16-T3 故障注入: peer broker 重启
- 模块: M16 / Sprint: S4 / 预估: 0.5d / 依赖: M16-T1
- 实现步骤: 一作业 SUBMITTED 时 kill -9 远端 broker; 启动后 systemd 拉起; 校验 sync_ticker 在 < 60s 内恢复轮询, 状态继续推进。
- 完成标准: 作业最终 COMPLETED。

#### M16-T4 故障注入: 源端 broker restore 一致性
- 模块: M16 / Sprint: S4 / 预估: 0.5d / 依赖: M03-T6
- 实现步骤: 投 100 jobs, kill -9 broker, 启动后比对 jobs 数 / 状态分布。
- 完成标准: 0 丢失, 0 重投。

#### M16-T5 故障注入: scancel 各阶段
- 模块: M16 / Sprint: S4 / 预估: 0.5d / 依赖: M16-T1
- 实现步骤: 在 INIT / STAGING_IN / RUNNING / STAGING_OUT 各阶段 scancel, 期望全部能在 30s 内 CANCELLED。
- 完成标准: 4 个子用例 100% 通过。

#### M16-T6 长稳: 100 个并发跨域作业 24h
- 模块: M16 / Sprint: S4 / 预估: 1d (设置) + 1d (运行) / 依赖: M16-T1
- 实现步骤: 循环投递, 平均 RPS=1, 24h 累计 ~80k 作业, 监控 broker RSS / FD / state.jsonl 大小, 不漏不堆积。
- 完成标准: 0 OOM, 0 fd 泄漏, 24h 后 state.jsonl 仍可被一次 checkpoint 在 < 1s 内写完。

---

# 第五部分: Sprint 排期映射与跟踪

## 5.1 Sprint 排期总表

> 与 `Broker详细设计文档MVP.md` §12 完全对齐, 每 Sprint 2 周。

### Sprint 1 (W1~W2) — 骨架贯通

**目标**: 跑起来一个 "什么 RPC 都不接收, 但能起停 / 配置 / 持久化 / 协议握手" 的 broker。

| 任务 | 模块 | 工时 |
|---|---|---|
| M01-T1 ~ M01-T7 | 进程骨架 | 3.5d |
| M02-T1 ~ M02-T5 | 配置加载 | 3.25d |
| M03-T1 ~ M03-T7 | 数据结构 + 持久化 | 4.5d |
| M04-T1 ~ M04-T5 | RPC pack/unpack | 4.5d |
| M15-T1 ~ M15-T4 | 部署基线 | 1.75d |

合计 ~17.5 人天 (单人略紧, 建议 2 人并行)。

### Sprint 2 (W3~W4) — 核心通信打通

**目标**: 端到端把 ORIGINATOR → RECEIVER 的 BROKER_FORWARD_JOB / STAGED_IN / CANCEL 跑通; 状态机能驱动 INIT → STAGING_IN → STAGED_IN → SUBMITTED; stage-in 真实跑通。

| 任务 | 模块 | 工时 |
|---|---|---|
| M05-T1 ~ M05-T4 | listener | 2.5d |
| M06-T1 ~ M06-T3 | handler_ctld | 2.25d |
| M07-T1, M07-T2, M07-T4 | handler_remote (核心) | 3d |
| M08-T1 ~ M08-T3, T5 | egress 主路径 + cancel | 2.5d |
| M09-T1 ~ M09-T4, T6 | 状态机主路径 + cancel | 2.5d |
| M10-T1, M10-T2 | stage worker + stage-in | 2.5d |

合计 ~15.25 人天。

### Sprint 3 (W5~W6) — 远端轮询 + 终态闭环 + 改写

**目标**: SUBMITTED → RUNNING → STAGING_OUT → COMPLETED 全链路打通; rewrite + lookup_software 接入; 终态字段写回 ctld + sacct。

| 任务 | 模块 | 工时 |
|---|---|---|
| M07-T3, M07-T5 | handler_remote 查询 + cleanup | 1.5d |
| M08-T4, M08-T6, M08-T7 | egress 查询 + ctld 推送 | 1.5d |
| M09-T5, M09-T7 | 状态机看门狗 + 终态出表 | 0.75d |
| M10-T3, M10-T4 | stage-out + 限额 | 1.5d |
| M11-T1 ~ M11-T3 | software 解析 | 2d |
| M12-T1 ~ M12-T3 | rewrite | 2.25d |
| M13-T1 ~ M13-T4 | sync_ticker | 2.75d |
| M14-T1, M14-T2 | 远端清理 | 1d |
| M15-T5 | software 部署 | 0.25d |

合计 ~13.5 人天。

### Sprint 4 (W7~W8) — 集成测试 + 加固 + 试点

**目标**: 把所有验收清单跑成自动化, 解决长稳问题, 进入 1 用户 1 队列的灰度。

| 任务 | 模块 | 工时 |
|---|---|---|
| M16-T1 ~ M16-T6 | 集成测试套 | 4d (并行) |
| M15-T6 | logrotate / systemd 完善 | 0.5d |
| Bug 修复 + 文档完善 | 全模块 | 4d |
| 试点联调 | 全模块 + 运维 | 1.5d |

合计 ~10 人天 (留 buffer 处理 bug)。

---

## 5.2 关键路径 (建议优先排单)

```
M01-T1 → M02-T2 → M03-T1 → M03-T2 → M04-T1 → M04-T2
                                              ↓
                                   M04-T3 → M05-T1 → M05-T3
                                              ↓
                                          M06-T1 → M07-T1
                                              ↓
                                          M08-T2 → M09-T2 → M10-T2
                                              ↓
                                          M07-T2 ← M12-T2 ← M11-T2
                                              ↓
                                          M08-T3 → M13-T2 → M13-T3
                                              ↓
                                          M08-T7 → M16-T1
```

> 任意节点延期会推后整体上线, 建议每天站会 review 这条线。

---

## 5.3 跨模块协调点 (Owner 协议)

| 协调点 | broker 工程师 | ctld 工程师 / 运维 | 触发 Sprint |
|---|---|---|---|
| 消息类型 ID + payload 字段顺序 | 提议方 (M04-T1, T2) | 评审 + 一起改 `src/common/slurm_protocol_pack.c` | S1 |
| `slurm.conf` 里 BrokerAddr/Port | 提供 client 调用样例 | 实施 ctld 端配置加载 | S1 |
| `partition.MaxJobs` 限流 | 提供建议值 | ctld 配置 + 跨域线程实现 | S2 |
| `squeue --remote` / `scontrol show job` 字段 | 推送字段值 | 实现客户端展示 | S3 |
| `sacct` 字段 | 推 TERMINAL_STATE | 走 jobcomp_g_record_job_end | S3 |
| `lookup_software.sh` + routes 部署 | 提供模板 | 运维填表 | S3 |
| 远端 sacctmgr default account | — | 运维预先配 association (设计文档 §13.4) | 试点前 |
| munge.key 互信 + TTL 86400 | 给 setup 脚本 | 运维双向同步 + 重启 munged | S1 / 试点前 |

---

## 5.4 任务进度跟踪模板

> 建议复制到团队的 issue tracker / Excel, 每天站会更新。

| TaskID | 模块 | 标题 | 预估 | Owner | Sprint | 状态 (TODO/WIP/REVIEW/DONE/BLOCKED) | 阻塞原因 | 实际工时 |
|---|---|---|---|---|---|---|---|---|
| M01-T1 | M01 | 创建 src/slurmbrokerd/ 骨架 | 0.5d | — | S1 | TODO | — | — |
| M01-T2 | M01 | 命令行解析 | 0.5d | — | S1 | TODO | — | — |
| ... | ... | ... | ... | ... | ... | ... | ... | ... |

> **进度门禁**:
> - Sprint N 末必须把对应 Sprint 任务全部 DONE; 未完任务挪到下一 Sprint 时连带评估关键路径影响。
> - 任何 BLOCKED 任务 ≥ 2 天必须升级到模块 owner + 跨域调度负责人。

---

## 5.5 验收里程碑 (M0 ~ M3)

| 里程碑 | 触发时间 | 出口标准 | 涉及模块 |
|---|---|---|---|
| **M0 骨架就绪** | Sprint 1 末 | broker 进程能起停, restore 0 jobs 干净, conf/user_mapping 解析正确, 通过 mock 客户端发任意 RPC dispatch 不 crash | M01 ~ M05 |
| **M1 单跳投递闭环** | Sprint 2 末 | 1 个 job 跑到 SUBMITTED 状态; scancel 能 CANCELLED | M01 ~ M10 (主路径), 不含 sync/rewrite/sw |
| **M2 端到端跑通** | Sprint 3 末 | 1 个 job 全链路 INIT → COMPLETED, sacct 写入 Remote_*, comment 不污染 | 全模块除 M16 |
| **M3 MVP 可上线** | Sprint 4 末 | M16 全部用例通过; 24h 长稳 0 故障; 1 试点用户 1 队列灰度 ≥ 50 个跨域作业全部成功 | 全模块 |

---

## 附录 A: 文件创建清单 (一目了然版)

> 开发前可一次性 `git add -A` 所有空文件框架, 然后按任务逐个填充。

```
src/slurmbrokerd/
├── Makefile.am              [M01-T1]
├── slurmbrokerd.c/.h        [M01]
├── broker_conf.c/.h         [M02]
├── user_mapping.c/.h        [M02]
├── broker_job.c/.h          [M03]
├── persist.c/.h             [M03]
├── proto.c/.h               [M04]
├── listener.c/.h            [M05]
├── handler_ctld.c/.h        [M06]
├── handler_remote.c/.h      [M07]
├── egress.c/.h              [M08]
├── state_machine.c/.h       [M09]
├── stage.c/.h               [M10]
├── software.c/.h            [M11]
├── rewrite.c/.h             [M12]
├── sync_ticker.c/.h         [M13]
└── cleanup.c/.h (可选)      [M14]

src/common/
├── slurm_protocol_defs.h    [M04-T1: 加 msg_type 与错误码]
├── slurm_protocol_defs.c    [M04-T2: msg init/free]
├── slurm_protocol_pack.c    [M04-T3, T4: pack/unpack 实现]
└── slurm_protocol_pack.h    [M04-T3, T4: 声明]

etc/
├── systemd/slurmbrokerd.service        [M01-T7]
├── slurm-broker/broker.conf.example    [M15-T1]
├── slurm-broker/user_mapping.conf.example  [M15-T1]
├── sudoers.d/slurm-broker.example      [M15-T3]
├── sudoers.d/slurm-rsync.example       [M15-T3]
├── sysconfig/munge.example              [M15-T4]
└── slurm-broker/software_routes.conf.example [M15-T5]

scripts/
├── setup_ssh.sh                         [M15-T2]
├── sync_munge.sh                        [M15-T4]
└── lookup_software.sh                   [M11-T3]

tests/broker/
├── mvp_smoke_test.sh                    [M16-T1]
├── fault_lookup_software.sh             [M16-T2]
├── fault_peer_restart.sh                [M16-T3]
├── fault_self_restart.sh                [M16-T4]
├── fault_scancel_phases.sh              [M16-T5]
└── load_burst.sh                        [M16-T6]
```

---

## 附录 B: 修订历史

| 版本 | 日期 | 修改人 | 修改说明 |
|---|---|---|---|
| DEV-1.0 | 2026-04 | — | 基于 `Broker详细设计文档MVP.md` 首次拆分, 16 模块 / ~80 任务, 4 Sprint 排期 |

