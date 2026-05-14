# M02 配置加载 Checklist (broker · v2.0)

> 配套: [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §10
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §2.10
> Sprint: S1
> 依赖: M01-T1（slurmbrokerd.c 信号/启停骨架）
> 下游: M03 / M04 / M06 / M07 / M08 / M09 / M10 / M11 / M13 / M16 全部读 `g_broker_conf` / `g_user_mappings`

> **v1.5 → v2.0 增量**:
> 1. 新增 `RouteSource=` / `RoutesConfPath=` / `RoutesReloadMode=` / `RoutesMtimePollSec=` 四字段 (路由配置外置)
> 2. 新增 `SubmitMode=mapped_user|root_uid` 远端投递身份策略
> 3. 新增 `RemoteAllowedCheck=strict|none` 用户授权二次校验开关
> 4. 新增 `TestOnlyTimeoutSec=` / `TestOnlyMaxCandidates=` 探测调参
> 5. `RemoteClusterName=` / `RemoteBrokerHost=` / `DefaultRemotePartition=` 在 `RouteSource=file` 时 **被忽略**, 仅 `static_legacy` 兜底模式下保留
> 6. `LocalUser=` 多行内联仍走 `S_P_LINE`（不变）

---

## 1. 模块概述与目标

### 1.1 一句话定位

读 `/etc/slurm/broker.conf`，构造**全局只读**的 `g_broker_conf` 与 `g_user_mappings`，并按 `RouteSource=` 决定后续是否需要由 M16 加载 `routes.conf`。失败立即 `fatal()` 退出码 1，不允许"半就绪"的进程存在。

### 1.2 v2.0 MVP 范围

- **解析**：复用 `s_p_options_t` / `s_p_parse_file()`，**不**自己写词法器。
- **校验**：端口范围、目录可写、SSH key 权限 ≤ 0600、lookup 脚本 `X_OK`；★ v2.0 新增 `RouteSource=file` 时 `RoutesConfPath` 必须可读。
- **user_mapping**：构造 `xhash_t`，key = `"local_user|remote_cluster"`（保持 v1.5 行为）。
- **★ v2.0**：`SubmitMode=root_uid` 时校验 broker 进程当前 EUID = 0，否则 fatal。
- **启动摘要**：INFO 级别一行打印关键配置（含 `RouteSource` / `SubmitMode`）。

### 1.3 不在 MVP 范围

- ~~SIGHUP 热加载 `broker.conf` 主体~~（仅 `routes.conf` 支持热加载，由 M16 负责）
- ~~`Include` 二级配置~~（强约束 §1.2，所有字段单文件内联）
- ~~配置回滚~~（解析失败直接 fatal，不留半成品）

### 1.4 与 v1.5 的差异

| 维度 | v1.5 | v2.0 |
|---|---|---|
| 路由配置 | broker.conf 内联 `RemoteClusterName=` 单字段 | 外置 `routes.conf` (M16) + `RouteSource=file` 开关 |
| 远端投递身份 | 默认 sudo -u remote_user | `SubmitMode=mapped_user|root_uid` 二选一 |
| ACL 校验 | broker 不做 | `RemoteAllowedCheck=strict` 时调用 SlurmDBD `assoc->remote_allowed` 二次校验 |
| 启动失败语义 | fatal | 同 (但新增 routes.conf 不可读 / SubmitMode=root_uid 但 EUID≠0 两种 fatal 触发) |

---

## 2. 接口契约

### 2.1 公共函数签名（v2.0）

```c
/* src/slurmbrokerd/broker_conf.h */

typedef enum {
	BROKER_ROUTE_SOURCE_FILE          = 0,   /* 默认: 读 routes.conf */
	BROKER_ROUTE_SOURCE_STATIC_LEGACY = 1,   /* v1.5 单对端兜底 */
} broker_route_source_t;

typedef enum {
	BROKER_ROUTES_RELOAD_NONE              = 0,
	BROKER_ROUTES_RELOAD_SIGHUP            = 1,
	BROKER_ROUTES_RELOAD_MTIME             = 2,
	BROKER_ROUTES_RELOAD_SIGHUP_OR_MTIME   = 3,
} broker_routes_reload_mode_t;

typedef enum {
	BROKER_SUBMIT_MAPPED_USER = 0,   /* sudo -u <remote_user> sbatch */
	BROKER_SUBMIT_ROOT_UID    = 1,   /* sbatch --uid=<remote_uid> (broker 必须 EUID=0) */
} broker_submit_mode_t;

typedef enum {
	BROKER_REMOTE_ALLOWED_NONE   = 0,
	BROKER_REMOTE_ALLOWED_STRICT = 1,
} broker_remote_allowed_check_t;

typedef struct {
	/* === 集群标识 === */
	char *cluster_name;
	char *broker_node_name;

	/* === 服务监听 === */
	uint16_t ctld_port;                  /* default 8442 */
	uint16_t peer_port;                  /* default 8443 */

	/* === ★ v2.0 路由源 === */
	broker_route_source_t routes_source;
	char     *routes_conf_path;          /* 仅 routes_source==FILE 时使用 */
	broker_routes_reload_mode_t routes_reload_mode;
	uint32_t  routes_mtime_poll_sec;     /* default 30 */

	/* === ★ v2.0 远端投递身份 === */
	broker_submit_mode_t submit_mode;

	/* === ★ v2.0 ACL 二次校验 === */
	broker_remote_allowed_check_t remote_allowed_check;

	/* === ★ v2.0 test-only 调参 === */
	uint32_t test_only_timeout_sec;      /* default 5 */
	uint32_t test_only_max_candidates;   /* default 8 */

	/* === v1.5 兼容字段 (仅 routes_source==STATIC_LEGACY 生效) === */
	char *remote_cluster_name;
	char *remote_broker_host;
	uint16_t remote_broker_port;
	char *remote_munge_key_path;
	char *default_remote_partition;

	/* === 鉴权 === */
	char *auth_type;                     /* default auth/munge */

	/* === 持久化 === */
	char *state_save_location;
	char *state_file_name;               /* default broker_state.jsonl */
	uint32_t checkpoint_interval;        /* default 30 */

	/* === 限流 (broker 兜底, 配合 routes.conf RemoteMaxInflight 二维) === */
	uint32_t max_inflight;               /* default 500 */
	uint64_t max_stage_bytes;            /* default 50 GB */

	/* === 状态轮询 === */
	uint32_t poll_interval;              /* default 10s */
	uint32_t poll_max_retries;           /* default 5 */

	/* === 数据传输 === */
	char *stage_rsync_bin;               /* default /usr/bin/rsync */
	char *stage_ssh_key;
	char *stage_ssh_user;
	uint16_t stage_worker_count;         /* default 4 */
	uint32_t stage_timeout_per_gb;       /* default 120 */

	/* === 软件路径解析 === */
	char *lookup_software_script;
	uint32_t lookup_timeout_sec;         /* default 10 */

	/* === 数据保留 (M14 废弃, 字段保留兼容) === */
	uint32_t remote_work_dir_retention_hours;
	uint32_t remote_work_dir_failure_retention_days;

	/* === ABI 预留 === */
	void *reserved[8];
} broker_conf_t;

extern broker_conf_t g_broker_conf;

extern int broker_conf_init(const char *path);   /* fatal on error */
extern void broker_conf_fini(void);
extern void broker_conf_log_summary(void);
```

`user_mapping.h` 不变（沿用 v1.5）。

### 2.2 全局变量

| 名称 | 类型 | 生命周期 | 谁写 / 谁读 |
|---|---|---|---|
| `g_broker_conf` | `broker_conf_t` | 进程级 | `broker_conf_init` 写一次 → 所有模块读 |
| `g_user_mappings` | `xhash_t *` | 进程级 | `user_mapping_load_from_hashtbl` 写一次 → 所有 handler 读 |
| `DEFAULT_BROKER_CONF_PATH` | `"/etc/slurm/broker.conf"` | 编译时 | `slurmbrokerd.c` `_parse_commandline` 已用 |

### 2.3 文件路径

| 路径 | 由谁创建 | 用途 |
|---|---|---|
| `/etc/slurm/broker.conf` | 运维（M15 模板） | 主配置 |
| `/etc/slurm/routes.conf` ★ v2.0 | 运维（M15 模板） | M16 routes_loader 加载 |
| `g_broker_conf.state_save_location` | broker 自身（不存在则 mkdir） | M03 持久化 |

---

## 3. 参考代码

| 用途 | 文件 | 关键行 |
|---|---|---|
| `s_p_options_t` 用法 | [src/common/parse_config.h](../../src/common/parse_config.h) | L73-L200 |
| `S_P_LINE` 嵌套结构（用于 `LocalUser`） | [src/common/parse_config.h](../../src/common/parse_config.h) | L132-L170 |
| `s_p_parse_file` 调用范式 | [src/common/read_config.c](../../src/common/read_config.c) | grep `s_p_parse_file` |
| `xhash_init` / `xhash_add` / `xhash_get_str` | [src/common/xhash.h](../../src/common/xhash.h) | L70 / L91 / L82 |
| `getuid()` / `geteuid()` | `<unistd.h>` | SubmitMode=root_uid 校验 |
| `stat()` 权限校验范式 | [src/common/read_config.c](../../src/common/read_config.c) | grep `stat_buf.st_mode & 077` |

---

## 4. 文件清单

| 文件 | 类型 | 用途 |
|---|---|---|
| [src/slurmbrokerd/broker_conf.h](../../src/slurmbrokerd/broker_conf.h) | 修改 | 新增 4 个 enum + 7 个字段; 旧字段保留 |
| [src/slurmbrokerd/broker_conf.c](../../src/slurmbrokerd/broker_conf.c) | 修改 | `broker_options[]` 增 7 行; `_parse_route_source` / `_parse_submit_mode` / `_validate_or_die` 扩展 |
| [src/slurmbrokerd/user_mapping.c](../../src/slurmbrokerd/user_mapping.c) | 不变 | v1.5 已落地 |
| [src/slurmbrokerd/Makefile.am](../../src/slurmbrokerd/Makefile.am) | 不变 | M16 加 routes_loader.c 时再改 |
| [src/slurmbrokerd/slurmbrokerd.c](../../src/slurmbrokerd/slurmbrokerd.c) | 修改 | `broker_init` 在 `broker_conf_init` 后**根据 routes_source** 决定是否调 `routes_loader_init` (M16 PR 同步) |

---

## 5. 数据结构草图

```text
g_broker_conf (broker_conf_t, v2.0)
  ├── cluster_name         "xian_cluster"
  ├── ctld_port            8442
  ├── peer_port            8443
  ├── routes_source        FILE          ★ v2.0
  ├── routes_conf_path     "/etc/slurm/routes.conf"   ★ v2.0
  ├── routes_reload_mode   SIGHUP_OR_MTIME            ★ v2.0
  ├── submit_mode          MAPPED_USER                ★ v2.0
  ├── remote_allowed_check STRICT                     ★ v2.0
  ├── test_only_timeout_sec 5                         ★ v2.0
  ├── test_only_max_candidates 8                      ★ v2.0
  ├── max_inflight         500
  ├── max_stage_bytes      53687091200 (50 GB)
  └── ... (28 + 7 字段, reserved)

g_user_mappings (xhash_t *, key = "local_user|remote_cluster")
  ├── ("test1|wz_cluster")  -> { remote_user="wz_test1", uid=20001, gid=20001 }
  ├── ("test1|hf_cluster")  -> { remote_user="hf_test1", uid=30001, gid=30001 }   ★ v2.0 多对端时同一 local_user 可有多条
  └── ...
```

---

## 6. 任务展开

### M02-T1 扩展 `broker_conf_t` 结构（★ v2.0 新增 7 字段）

- **依赖**: M01-T1（v1.5 已落地）
- **预估**: 0.25d
- **关键决策**:
  1. 新字段统一**追加在 v1.5 字段后面**，不重排（保持 ABI 与 diff 可读）。
  2. 4 个 enum 全在 `broker_conf.h` 顶端定义，便于 grep。
  3. `routes_source` 默认 `FILE`，`submit_mode` 默认 `MAPPED_USER`，`remote_allowed_check` 默认 `STRICT`，`test_only_timeout_sec=5`，`test_only_max_candidates=8`。
- **DoD**:
  - [ ] 头文件 include guard 存在
  - [ ] `gcc -fsyntax-only -I$(top_srcdir)` 通过
  - [ ] 与 §2.1 字段顺序一致

### M02-T2 `broker_options[]` 注册新字段

- **依赖**: M02-T1
- **预估**: 0.5d
- **关键决策**:
  1. `RouteSource` / `RoutesReloadMode` / `SubmitMode` / `RemoteAllowedCheck` 用 `S_P_STRING`，由 `_parse_*_enum` 转 enum。
  2. `RoutesConfPath` 用 `S_P_STRING`，但 `routes_source != FILE` 时不必填。
  3. `RoutesMtimePollSec` / `TestOnlyTimeoutSec` / `TestOnlyMaxCandidates` 用 `S_P_UINT32`。
  4. `s_p_parse_file(tbl, NULL, path, false)` 第 3 参数 false → typo 直接 fatal。
- **代码草图**:

```c
/* broker_conf.c */
static const s_p_options_t broker_options[] = {
	/* === v1.5 字段 (略, 保持原样) === */
	{ "ClusterName",                         S_P_STRING },
	/* ... 见 v1.5 checklist ... */

	/* === ★ v2.0 新增 === */
	{ "RouteSource",                         S_P_STRING },
	{ "RoutesConfPath",                      S_P_STRING },
	{ "RoutesReloadMode",                    S_P_STRING },
	{ "RoutesMtimePollSec",                  S_P_UINT32 },
	{ "SubmitMode",                          S_P_STRING },
	{ "RemoteAllowedCheck",                  S_P_STRING },
	{ "TestOnlyTimeoutSec",                  S_P_UINT32 },
	{ "TestOnlyMaxCandidates",               S_P_UINT32 },

	/* === v1.5 兼容字段 (RouteSource=static_legacy 时仍生效) === */
	{ "RemoteClusterName",                   S_P_STRING },
	{ "RemoteBrokerHost",                    S_P_STRING },
	{ "RemoteBrokerPort",                    S_P_UINT16 },
	{ "RemoteMungeKeyPath",                  S_P_STRING },
	{ "DefaultRemotePartition",              S_P_STRING },

	/* === LocalUser= 多行 (S_P_LINE, 沿用 v1.5) === */
	{ "LocalUser", S_P_LINE, NULL, NULL, local_user_line_opts },

	{ NULL, 0 }
};

static broker_route_source_t _parse_route_source(const char *s)
{
	if (!s || !xstrcasecmp(s, "file"))           return BROKER_ROUTE_SOURCE_FILE;
	if (!xstrcasecmp(s, "static_legacy"))        return BROKER_ROUTE_SOURCE_STATIC_LEGACY;
	fatal("broker.conf: invalid RouteSource '%s'", s);
	return -1;   /* unreachable */
}

static broker_submit_mode_t _parse_submit_mode(const char *s)
{
	if (!s || !xstrcasecmp(s, "mapped_user"))    return BROKER_SUBMIT_MAPPED_USER;
	if (!xstrcasecmp(s, "root_uid"))             return BROKER_SUBMIT_ROOT_UID;
	fatal("broker.conf: invalid SubmitMode '%s'", s);
	return -1;
}
```

- **DoD**:
  - [ ] typo `RoutSource=` 触发 fatal
  - [ ] `RouteSource=` 缺失时取默认 `file`
  - [ ] `SubmitMode=invalid` 触发 fatal

### M02-T3 v2.0 新增校验（`_validate_or_die` 扩展）

- **依赖**: M02-T2
- **预估**: 0.5d
- **关键决策**:
  1. 端口校验保持 v1.5。
  2. **★ v2.0**: `routes_source == FILE` → `routes_conf_path` 必须非空 + `access(p, R_OK) == 0`。
  3. **★ v2.0**: `submit_mode == ROOT_UID` → `geteuid() == 0`（broker 必须 root 启动），否则 fatal。
  4. **★ v2.0**: `remote_allowed_check == STRICT` → 仅 info 级别提示 "依赖 SlurmDBD assoc->remote_allowed 字段 (ctld 端 ctld-M13 已落地)"，不做硬校验。
  5. **★ v2.0**: `test_only_max_candidates ∈ [1, 32]` 且 `test_only_timeout_sec ∈ [1, 60]`。
- **代码草图**:

```c
static void _validate_or_die(void)
{
	struct stat st;

	/* v1.5 端口/目录/SSH key/LookupSoftwareScript 校验 (略) */

	/* === ★ v2.0 新增 === */
	if (g_broker_conf.routes_source == BROKER_ROUTE_SOURCE_FILE) {
		if (!g_broker_conf.routes_conf_path)
			fatal("broker.conf: RouteSource=file requires RoutesConfPath=");
		if (access(g_broker_conf.routes_conf_path, R_OK) < 0)
			fatal("broker.conf: RoutesConfPath %s not readable: %m",
			      g_broker_conf.routes_conf_path);
	} else {
		/* STATIC_LEGACY 兼容: 检查 v1.5 单对端字段是否齐全 */
		if (!g_broker_conf.remote_cluster_name ||
		    !g_broker_conf.remote_broker_host ||
		    !g_broker_conf.default_remote_partition)
			fatal("broker.conf: RouteSource=static_legacy requires "
			      "RemoteClusterName / RemoteBrokerHost / "
			      "DefaultRemotePartition");
	}

	if (g_broker_conf.submit_mode == BROKER_SUBMIT_ROOT_UID) {
		if (geteuid() != 0)
			fatal("broker.conf: SubmitMode=root_uid requires broker "
			      "to run as EUID=0 (current EUID=%u)", geteuid());
		info("broker.conf: SubmitMode=root_uid; broker must NOT spawn "
		     "untrusted child processes");
	}

	if (g_broker_conf.test_only_timeout_sec < 1 ||
	    g_broker_conf.test_only_timeout_sec > 60)
		fatal("broker.conf: TestOnlyTimeoutSec %u out of range [1,60]",
		      g_broker_conf.test_only_timeout_sec);
	if (g_broker_conf.test_only_max_candidates < 1 ||
	    g_broker_conf.test_only_max_candidates > 32)
		fatal("broker.conf: TestOnlyMaxCandidates %u out of range [1,32]",
		      g_broker_conf.test_only_max_candidates);
}
```

- **风险与坑**:
  - root_uid 模式下 broker 拥有 `--uid` 提交权限，安全审计要求 sudoers / sbatch wrapper 双因素；运维文档（M15）必须明示。
  - macOS 上 `geteuid()` 与 Linux 一致；CI 跑 Linux 即可。
- **DoD**:
  - [ ] `RouteSource=file` + `RoutesConfPath` 缺失 → fatal
  - [ ] `RouteSource=file` + `RoutesConfPath=/nonexistent` → fatal
  - [ ] `SubmitMode=root_uid` 非 root 启动 → fatal
  - [ ] `TestOnlyTimeoutSec=999` → fatal

### M02-T4 启动日志摘要（v2.0 增 4 字段）

- **依赖**: M02-T3
- **预估**: 0.1d
- **代码草图**:

```c
void broker_conf_log_summary(void)
{
	uint32_t mapping_count = g_user_mappings
	                         ? xhash_count(g_user_mappings) : 0;
	const char *src_str = (g_broker_conf.routes_source ==
	                       BROKER_ROUTE_SOURCE_FILE) ? "file" : "static_legacy";
	const char *sub_str = (g_broker_conf.submit_mode ==
	                       BROKER_SUBMIT_ROOT_UID) ? "root_uid" : "mapped_user";

	info("broker_conf: cluster=%s ctld=%u peer=%u "
	     "route_source=%s submit_mode=%s "
	     "test_only_timeout=%us test_only_max=%u "
	     "mappings=%u",
	     g_broker_conf.cluster_name,
	     g_broker_conf.ctld_port,
	     g_broker_conf.peer_port,
	     src_str, sub_str,
	     g_broker_conf.test_only_timeout_sec,
	     g_broker_conf.test_only_max_candidates,
	     mapping_count);
}
```

- **DoD**:
  - [ ] `journalctl -u slurmbrokerd -n 20` 能看到该行
  - [ ] 各字段正确反映 conf 内容

### M02-T5 v1.5 → v2.0 升级路径文档化

- **依赖**: M02-T3
- **预估**: 0.25d
- **关键决策**:
  1. v1.5 conf 文件**直接启动 v2.0 broker** → fatal "RouteSource=file requires RoutesConfPath="（明确错误）。
  2. 提供 `etc/broker.conf.v15-to-v20.md` 升级指引（与 M15 一并交付）。
  3. SOP：先停 broker → 备份 conf → 加 4 行 v2.0 必填 → 启动。
- **DoD**:
  - [ ] 升级文档链接到 [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §11.6
  - [ ] CI 测试矩阵包含"v1.5 conf 直接启动 v2.0 broker fail-stop"用例

---

## 7. 整体 DoD（汇总）

- [ ] 5 个子任务全部勾选
- [ ] `broker_conf_init` + `user_mapping_load_from_hashtbl` 在 `broker_init()` 中已 wire-up
- [ ] valgrind: 启动 → fini，0 byte still reachable（v2.0 新增字段全部 xfree）
- [ ] **★ v2.0**: `journalctl` 能看到 `route_source=file submit_mode=mapped_user` 一行
- [ ] **★ v2.0**: 三种 fatal 用例（缺 RoutesConfPath / SubmitMode=root_uid 非 root / TestOnly 越界）全部通过

## 8. 验证脚本

```bash
# === Linux 服务器上 ===
cd <repo-root>
autoreconf -fi && ./configure --prefix=/usr/local && make -C src/slurmbrokerd

sudo install -d /etc/slurm

# 1) v2.0 默认配置 (file 模式)
sudo cp etc/broker.conf.example /etc/slurm/broker.conf
sudo cp etc/routes.conf.example /etc/slurm/routes.conf
sudo chmod 0600 /etc/slurm/broker_id_ed25519
./src/slurmbrokerd/slurmbrokerd -D -v -f /etc/slurm/broker.conf
# 期望: broker_conf: cluster=... route_source=file submit_mode=mapped_user ...

# 2) v1.5 conf 直接启动 v2.0 broker (期望 fatal)
sudo sed -i '/^RouteSource=/d; /^RoutesConfPath=/d' /etc/slurm/broker.conf
./src/slurmbrokerd/slurmbrokerd -D -f /etc/slurm/broker.conf
# 期望: fatal "RouteSource=file requires RoutesConfPath="

# 3) SubmitMode=root_uid 非 root 启动 (期望 fatal)
sudo sed -i 's/^SubmitMode=mapped_user/SubmitMode=root_uid/' /etc/slurm/broker.conf
./src/slurmbrokerd/slurmbrokerd -D -f /etc/slurm/broker.conf
# 期望: fatal "SubmitMode=root_uid requires broker to run as EUID=0"

# 4) TestOnlyTimeoutSec 越界
sudo sed -i 's/^TestOnlyTimeoutSec=5/TestOnlyTimeoutSec=999/' /etc/slurm/broker.conf
./src/slurmbrokerd/slurmbrokerd -D -f /etc/slurm/broker.conf
# 期望: fatal "TestOnlyTimeoutSec 999 out of range [1,60]"
```

---

## 9. 风险与回滚

### 9.1 已知风险

| 风险 | 触发条件 | 缓解 |
|---|---|---|
| `RouteSource=file` + `routes.conf` 临时不可读（NFS 抖动）| 升级窗口 | M02-T3 fatal；运维必须保证 `routes.conf` 落本地 fs |
| `SubmitMode=root_uid` 误开启于普通 broker | 配置疏忽 | EUID 校验；启动失败明确提示 |
| `TestOnlyTimeoutSec` 设小（1s）导致跨域抖动 | 运维试错 | M02-T3 范围校验；v1.5 默认值 5s 起步 |
| v1.5 conf 直接启动 v2.0 broker 静默退化 | 未读 release notes | 故意 fatal-stop，避免"假成功" |

### 9.2 回滚方案

- 改动局限于 `broker_conf.h` / `broker_conf.c`，未上线时 `git revert` 两个文件即可。
- 已上线后回滚：`systemctl stop slurmbrokerd` → 用 `broker.conf.v1.5.bak` 覆盖 → 部署 v1.5 二进制 → start。
