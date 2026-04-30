# M02 配置加载 Checklist

> 配套: [doc/Broker开发任务清单.md](../Broker开发任务清单.md) §M02
> 设计: [doc/Broker详细设计文档MVP.md](../Broker详细设计文档MVP.md) §10
> Sprint: S1
> 依赖: M01-T1
> 下游: M03 / M04 / M05 / M06 / M07 / M08 / M09 / M10 / M11 / M13 全部读 `g_broker_conf` / `g_user_mappings`

---

## 1. 模块概述与目标

### 1.1 一句话定位

读 `/etc/slurm/broker.conf` 与 `/etc/slurm/user_mapping.conf`，构造**全局只读**的 `g_broker_conf` 与 `g_user_mappings`，供其它模块通过指针访问。失败立即 `fatal()` 退出码 1，不允许"半就绪"的进程存在。

### 1.2 MVP 范围

- **解析**：复用 `s_p_options_t` / `s_p_parse_file()`，**不**自己写词法器。
- **校验**：端口范围、目录可写、SSH key 权限 ≤ 0600、lookup 脚本 `X_OK`。
- **user_mapping**：构造 `xhash_t`，key = `"local_user|remote_cluster"`。
- **启动摘要**：INFO 级别一行打印关键配置。

### 1.3 不在 MVP 范围（推迟）

- ~~SIGHUP 热加载~~（M01-T4 已留 hook，但本模块仅做"重启生效"）
- ~~多 RemoteCluster~~（设计文档 §14 演进列表中说明：单字段 → list 仅改 `broker_conf.c`）
- ~~配置回滚~~（解析失败直接 fatal，不留半成品）

### 1.4 与 v0.1 设计的差异

设计文档 §10 给出完整字段列表；MVP 阶段直接照单全收，所有字段都进 `broker_conf_t`，不做缩减。

---

## 2. 接口契约

### 2.1 公共函数签名

```c
/* broker_conf.h */

typedef struct {
	/* === 集群标识 === */
	char *cluster_name;                  /* ClusterName */
	char *broker_node_name;              /* BrokerNodeName */

	/* === 服务监听 === */
	uint16_t ctld_port;                  /* BrokerCtldPort   default 8442 */
	uint16_t peer_port;                  /* BrokerPeerPort   default 8443 */

	/* === 单对端 (MVP) === */
	char *remote_cluster_name;           /* RemoteClusterName */
	char *remote_broker_host;            /* RemoteBrokerHost */
	uint16_t remote_broker_port;         /* RemoteBrokerPort */
	char *remote_munge_key_path;         /* RemoteMungeKeyPath (optional) */

	/* === 单一目标 partition === */
	char *default_remote_partition;      /* DefaultRemotePartition */

	/* === 鉴权 === */
	char *auth_type;                     /* AuthType  default auth/munge */

	/* === 持久化 === */
	char *state_save_location;           /* StateSaveLocation */
	char *state_file_name;               /* StateFileName */
	uint32_t checkpoint_interval;        /* CheckpointInterval (s) */

	/* === 限流 === */
	uint32_t max_inflight;               /* MaxInFlightJobs */
	uint64_t max_stage_bytes;            /* MaxStageBytes */

	/* === 状态轮询 === */
	uint32_t poll_interval;              /* PollInterval     default 10s */
	uint32_t poll_max_retries;           /* PollMaxRetries   default 5 */

	/* === 数据传输 === */
	char *stage_rsync_bin;               /* StageRsyncBin */
	char *stage_ssh_key;                 /* StageSshKey */
	char *stage_ssh_user;                /* StageSshUser */
	uint16_t stage_worker_count;         /* StageWorkerCount default 4 */
	uint32_t stage_timeout_per_gb;       /* StageTimeoutPerGB default 120 */

	/* === 软件路径解析 === */
	char *lookup_software_script;        /* LookupSoftwareScript */
	uint32_t lookup_timeout_sec;         /* LookupTimeoutSec default 3 */

	/* === 数据保留 === */
	uint32_t remote_work_dir_retention_hours;       /* default 24 */
	uint32_t remote_work_dir_failure_retention_days;/* default 7 */

	/* === ABI 预留 === */
	void *reserved[8];
} broker_conf_t;

extern broker_conf_t g_broker_conf;

/* Lifecycle */
extern int broker_conf_init(const char *path);  /* fatal on error */
extern void broker_conf_fini(void);
```

```c
/* user_mapping.h */

typedef struct {
	char *local_user;       /* slurmctld 提交用户 */
	char *remote_cluster;   /* 目标集群名 */
	char *remote_user;      /* 远端登录用户 */
	uint32_t remote_uid;    /* 远端 uid (作业提交 uid) */
	uint32_t remote_gid;    /* 远端 gid */
} user_mapping_t;

extern xhash_t *g_user_mappings;        /* key = "local_user|remote_cluster" */

extern int user_mapping_load(const char *path);
extern user_mapping_t *user_mapping_lookup(const char *local_user,
                                           const char *remote_cluster);
extern void user_mapping_destroy_all(void);
```

### 2.2 全局变量与常量

| 名称 | 类型 | 生命周期 | 谁写 / 谁读 |
|---|---|---|---|
| `g_broker_conf` | `broker_conf_t` (struct, not pointer) | 进程级 | `broker_conf_init` 写一次 → 所有模块读 |
| `g_user_mappings` | `xhash_t *` | 进程级 | `user_mapping_load` 写一次 → 所有 handler 读 |
| `DEFAULT_BROKER_CONF_PATH` | `"/etc/slurm/broker.conf"` | 编译时 | `slurmbrokerd.c` `_parse_commandline` 已用 |

### 2.3 文件路径

| 路径 | 由谁创建 | 用途 |
|---|---|---|
| `/etc/slurm/broker.conf` | 运维（M15-T1 提供模板） | 主配置 |
| `/etc/slurm/user_mapping.conf` | 运维（M15-T1 提供模板） | 通过 `Include` 拼入 |
| `g_broker_conf.state_save_location` | broker 自身（不存在则 mkdir） | M03 持久化 |

---

## 3. 参考代码

### 3.1 Slurm 主线现成基础设施

| 用途 | 文件 | 关键行 |
|---|---|---|
| `s_p_options_t` 用法举例 | [src/common/parse_config.h](../../src/common/parse_config.h) | L73-L200 头部注释 |
| `S_P_LINE` 嵌套结构（用于 `UserMapping` 多行） | [src/common/parse_config.h](../../src/common/parse_config.h) | L132-L170 |
| `s_p_parse_file` 真实调用样例 | [src/slurmd/common/setproctitle.c](../../src/slurmd/common/setproctitle.c) 同类 daemon | grep `s_p_parse_file` |
| `xhash_init` / `xhash_add` / `xhash_get_str` | [src/common/xhash.h](../../src/common/xhash.h) | L70 / L91 / L82 |
| `xhash_freefunc_t` 释放回调签名 | [src/common/xhash.h](../../src/common/xhash.h) | L59 |
| 端口范围检查参考 | [src/common/proc_args.c](../../src/common/proc_args.c) | grep `parse_port` |
| `stat()` 权限校验参考 | [src/common/read_config.c](../../src/common/read_config.c) | grep `stat_buf.st_mode & 077` |

### 3.2 与 `slurm.conf` 解析的对比

`slurmctld` 用同一套 `s_p_*` API 解析 `slurm.conf`（[src/common/read_config.c](../../src/common/read_config.c)），broker 完全照搬，只换字段表。

---

## 4. 文件清单

| 文件 | 类型 | 用途 |
|---|---|---|
| [src/slurmbrokerd/broker_conf.h](../../src/slurmbrokerd/broker_conf.h) | 新增 | `broker_conf_t` + API |
| [src/slurmbrokerd/broker_conf.c](../../src/slurmbrokerd/broker_conf.c) | 新增 | `s_p_*` 解析、字段校验、`g_broker_conf` 定义 |
| [src/slurmbrokerd/user_mapping.h](../../src/slurmbrokerd/user_mapping.h) | 新增 | `user_mapping_t` + API |
| [src/slurmbrokerd/user_mapping.c](../../src/slurmbrokerd/user_mapping.c) | 新增 | `xhash_t` + lookup |
| [src/slurmbrokerd/Makefile.am](../../src/slurmbrokerd/Makefile.am) | 修改 | `slurmbrokerd_SOURCES` 加 4 个文件 |
| [src/slurmbrokerd/slurmbrokerd.c](../../src/slurmbrokerd/slurmbrokerd.c) | 修改 | `broker_init` 顶端激活 `broker_conf_init` / `user_mapping_load` 调用 |
| [src/slurmbrokerd/slurmbrokerd.h](../../src/slurmbrokerd/slurmbrokerd.h) | 修改（可选） | 把 `g_broker_conf` extern 暴露给主程序（也可保留各自 header） |

---

## 5. 数据结构草图

```text
g_broker_conf (broker_conf_t)
  ├── cluster_name "xian_cluster"
  ├── ctld_port  8442
  ├── peer_port  8443
  ├── remote_cluster_name "wz_cluster"
  ├── remote_broker_host "broker.wz.example.com"
  ├── default_remote_partition "wzhcnormal"
  ├── stage_ssh_key "/etc/slurm/broker_id_ed25519" (mode 0600)
  ├── lookup_software_script "/opt/slurm-broker/scripts/lookup_software.sh"
  └── ... 28 个字段

g_user_mappings (xhash_t *, key = "local_user|remote_cluster")
  ├── ("test1|wz_cluster")  -> { remote_user="wz_test1", uid=20001, gid=20001 }
  ├── ("test2|wz_cluster")  -> { remote_user="wz_test2", uid=20002, gid=20002 }
  └── ...
```

---

## 6. 任务展开

### M02-T1 定义 `broker_conf_t` 结构与字段映射

- **依赖**：M01-T1
- **预估**：0.5d
- **关键决策**：
  1. **字段顺序**与设计文档 §10.1 一一对应，不重排。这样未来任何字段增删都只在尾部追加，diff 可读。
  2. 留 `void *reserved[8]` 给后续扩展不破坏 ABI（相同二进制可加载更新版的 broker_conf）。
  3. **类型选择**：端口用 `uint16_t`、字节大小用 `uint64_t`、超时用 `uint32_t`、其余 size_t/int 不用（slurm 主线惯例）。
- **代码草图**：见 §2.1 的 `broker_conf_t`。
- **风险**：
  - 字段名 `S_P_*` 选错（如把 `MaxInFlightJobs` 写成 `S_P_UINT16` 但实际可能 > 65535）→ 用 `S_P_UINT32`。
  - 含路径的字段必须 `xstrdup`，否则 `s_p_hashtbl_destroy` 会重复 free。
- **DoD**：
  - [ ] 头文件 include guard 存在 (`#ifndef _BROKER_CONF_H`)
  - [ ] `gcc -fsyntax-only -I$(top_srcdir)` 通过
  - [ ] 字段数量与设计文档 §10.1 完全一致（28 个 + reserved）

### M02-T2 复用 `s_p_hashtbl_t` 写解析器

- **依赖**：M02-T1
- **预估**：1d
- **关键决策**：
  1. **不**自己写词法器，全用 `s_p_*`。
  2. `s_p_parse_file(tbl, NULL, path, false /*ignore_new*/)`，最后参数 false 表示遇到未知 key 报错（防 typo）。
  3. **必填字段缺失** → `fatal()` 立即退出，不给 NULL 默认值。
  4. **可选字段**（如 `RemoteMungeKeyPath`）允许缺失，但 `g_broker_conf` 字段保持 NULL。
  5. **数值默认值**通过常量宏定义：`#define DEFAULT_CTLD_PORT 8442` 等。
- **代码草图**：

```c
/* broker_conf.c */
static const s_p_options_t broker_options[] = {
	{ "ClusterName",         S_P_STRING },
	{ "BrokerNodeName",      S_P_STRING },
	{ "BrokerCtldPort",      S_P_UINT16 },
	{ "BrokerPeerPort",      S_P_UINT16 },
	{ "RemoteClusterName",   S_P_STRING },
	{ "RemoteBrokerHost",    S_P_STRING },
	{ "RemoteBrokerPort",    S_P_UINT16 },
	{ "RemoteMungeKeyPath",  S_P_STRING },
	{ "DefaultRemotePartition", S_P_STRING },
	{ "AuthType",            S_P_STRING },
	{ "StateSaveLocation",   S_P_STRING },
	{ "StateFileName",       S_P_STRING },
	{ "CheckpointInterval",  S_P_UINT32 },
	{ "MaxInFlightJobs",     S_P_UINT32 },
	{ "MaxStageBytes",       S_P_UINT64 },
	{ "PollInterval",        S_P_UINT32 },
	{ "PollMaxRetries",      S_P_UINT32 },
	{ "StageRsyncBin",       S_P_STRING },
	{ "StageSshKey",         S_P_STRING },
	{ "StageSshUser",        S_P_STRING },
	{ "StageWorkerCount",    S_P_UINT16 },
	{ "StageTimeoutPerGB",   S_P_UINT32 },
	{ "LookupSoftwareScript",S_P_STRING },
	{ "LookupTimeoutSec",    S_P_UINT32 },
	{ "RemoteWorkDirRetentionHours",       S_P_UINT32 },
	{ "RemoteWorkDirFailureRetentionDays", S_P_UINT32 },
	/* UserMapping 行交给 user_mapping_load 用 S_P_LINE 单独解析 */
	{ NULL, 0 }
};

int broker_conf_init(const char *path)
{
	s_p_hashtbl_t *tbl = s_p_hashtbl_create(broker_options);
	char *str;

	if (s_p_parse_file(tbl, NULL, path, false))
		fatal("broker.conf: parse failed");

	memset(&g_broker_conf, 0, sizeof(g_broker_conf));

#define GET_STR(K, F)  do { \
	if (s_p_get_string(&g_broker_conf.F, K, tbl)) {} \
} while (0)
#define GET_STR_REQ(K, F) do { \
	if (!s_p_get_string(&g_broker_conf.F, K, tbl)) \
		fatal("broker.conf: missing required '%s'", K); \
} while (0)
#define GET_U16(K, F, DEF) do { \
	if (!s_p_get_uint16(&g_broker_conf.F, K, tbl)) \
		g_broker_conf.F = (DEF); \
} while (0)
#define GET_U32(K, F, DEF) do { \
	if (!s_p_get_uint32(&g_broker_conf.F, K, tbl)) \
		g_broker_conf.F = (DEF); \
} while (0)

	GET_STR_REQ("ClusterName",            cluster_name);
	GET_STR_REQ("BrokerNodeName",         broker_node_name);
	GET_U16("BrokerCtldPort",             ctld_port,                  8442);
	GET_U16("BrokerPeerPort",             peer_port,                  8443);
	GET_STR_REQ("RemoteClusterName",      remote_cluster_name);
	GET_STR_REQ("RemoteBrokerHost",       remote_broker_host);
	GET_U16("RemoteBrokerPort",           remote_broker_port,         8443);
	GET_STR("RemoteMungeKeyPath",         remote_munge_key_path);
	GET_STR_REQ("DefaultRemotePartition", default_remote_partition);
	GET_STR("AuthType",                   auth_type);
	if (!g_broker_conf.auth_type)
		g_broker_conf.auth_type = xstrdup("auth/munge");
	GET_STR_REQ("StateSaveLocation",      state_save_location);
	GET_STR("StateFileName",              state_file_name);
	if (!g_broker_conf.state_file_name)
		g_broker_conf.state_file_name = xstrdup("broker_state.jsonl");
	GET_U32("CheckpointInterval",         checkpoint_interval,         30);
	GET_U32("MaxInFlightJobs",            max_inflight,               500);
	/* MaxStageBytes uint64 helper not in s_p; parse via string */
	if (s_p_get_string(&str, "MaxStageBytes", tbl)) {
		g_broker_conf.max_stage_bytes = strtoull(str, NULL, 10);
		xfree(str);
	} else {
		g_broker_conf.max_stage_bytes = 53687091200ULL; /* 50 GB */
	}
	GET_U32("PollInterval",               poll_interval,                10);
	GET_U32("PollMaxRetries",             poll_max_retries,              5);
	GET_STR("StageRsyncBin",              stage_rsync_bin);
	if (!g_broker_conf.stage_rsync_bin)
		g_broker_conf.stage_rsync_bin = xstrdup("/usr/bin/rsync");
	GET_STR_REQ("StageSshKey",            stage_ssh_key);
	GET_STR_REQ("StageSshUser",           stage_ssh_user);
	GET_U16("StageWorkerCount",           stage_worker_count,            4);
	GET_U32("StageTimeoutPerGB",          stage_timeout_per_gb,        120);
	GET_STR_REQ("LookupSoftwareScript",   lookup_software_script);
	GET_U32("LookupTimeoutSec",           lookup_timeout_sec,            3);
	GET_U32("RemoteWorkDirRetentionHours",       remote_work_dir_retention_hours,        24);
	GET_U32("RemoteWorkDirFailureRetentionDays", remote_work_dir_failure_retention_days,  7);

#undef GET_STR
#undef GET_STR_REQ
#undef GET_U16
#undef GET_U32

	s_p_hashtbl_destroy(tbl);
	return SLURM_SUCCESS;
}
```

- **风险**：
  - `s_p_parse_file` 第 3 个参数 `ignore_new` 设错 → 拼写错误的 key 不报错，运维改 typo 永远没生效。务必传 `false`。
  - `s_p_get_string` 返回 `xstrdup` 的副本（详见头文件 L120），所以 `xfree` 责任在 `g_broker_conf`。
  - `MaxStageBytes` 没有 `S_P_UINT64`？检查：`parse_config.h` 有 `S_P_UINT64`（L114），可直接用。上面草图为安全用 string 解析，可改成 `S_P_UINT64` 简化。
- **DoD**：
  - [ ] 一份合法 conf 解析后字段值正确（写一份 examples/broker.conf 验证）
  - [ ] 一份缺 `ClusterName` 的 conf → fatal，退出码 1
  - [ ] 一份带 typo (`ClsuterName=`) 的 conf → fatal（验证 ignore_new=false）

### M02-T3 字段语义校验

- **依赖**：M02-T2
- **预估**：0.5d
- **关键决策**：
  1. 端口校验：`[1024, 65535]` 且 `ctld_port != peer_port`。
  2. `state_save_location` 不存在 → `mkdir -p` 一次（`mkdir(path, 0755)`，递归用 `S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH`）。
  3. `stage_ssh_key`：必须存在 + 模式权限 `(st_mode & 077) == 0`（owner-only）。
  4. `lookup_software_script`：必须 `access(p, X_OK) == 0`。
- **代码草图**：

```c
static void _validate_or_die(void)
{
	struct stat st;

	if (g_broker_conf.ctld_port < 1024 || g_broker_conf.ctld_port > 65535)
		fatal("broker.conf: BrokerCtldPort %u out of range",
		      g_broker_conf.ctld_port);
	if (g_broker_conf.peer_port < 1024 || g_broker_conf.peer_port > 65535)
		fatal("broker.conf: BrokerPeerPort %u out of range",
		      g_broker_conf.peer_port);
	if (g_broker_conf.ctld_port == g_broker_conf.peer_port)
		fatal("broker.conf: BrokerCtldPort and BrokerPeerPort must differ");

	if (stat(g_broker_conf.state_save_location, &st) < 0) {
		if (mkdir(g_broker_conf.state_save_location, 0755) < 0)
			fatal("broker.conf: cannot create StateSaveLocation %s: %m",
			      g_broker_conf.state_save_location);
	} else if (!S_ISDIR(st.st_mode)) {
		fatal("broker.conf: StateSaveLocation %s is not a directory",
		      g_broker_conf.state_save_location);
	}

	if (stat(g_broker_conf.stage_ssh_key, &st) < 0)
		fatal("broker.conf: StageSshKey %s missing: %m",
		      g_broker_conf.stage_ssh_key);
	if (st.st_mode & 077)
		fatal("broker.conf: StageSshKey %s permissions %#o too open (need 0600)",
		      g_broker_conf.stage_ssh_key, st.st_mode & 0777);

	if (access(g_broker_conf.lookup_software_script, X_OK) < 0)
		fatal("broker.conf: LookupSoftwareScript %s not executable: %m",
		      g_broker_conf.lookup_software_script);
}
```

- **风险**：
  - macOS 上 `chmod 0600` 后 `(st_mode & 077) == 0`，但 NFS 挂载可能因 root squash 导致 mode 看起来错。本验证只在 Linux 服务器执行。
- **DoD**：
  - [ ] 故意 `chmod 0644` SSH key → fatal
  - [ ] 故意把 ports 写成相同 → fatal
  - [ ] `state_save_location` 路径不存在时自动 mkdir 成功

### M02-T4 user_mapping.conf 解析与全局表

- **依赖**：M02-T2
- **预估**：1d
- **关键决策**：
  1. 用 `S_P_LINE` 把 `UserMapping` 当多行整体解析。
  2. key 格式 = `"local_user|remote_cluster"`（用管道分隔，避免冲突）。
  3. lookup 不命中返回 NULL，调用方处理 `ESLURM_BROKER_NO_USER_MAPPING`。
  4. **重启生效**：MVP 不做热加载；M01-T4 SIGHUP handler 留 hook，但 body 仅 info 日志。
- **代码草图**：

```c
/* user_mapping.c */
static const s_p_options_t mapping_line_opts[] = {
	{ "LocalUser",     S_P_STRING },
	{ "RemoteCluster", S_P_STRING },
	{ "RemoteUser",    S_P_STRING },
	{ "RemoteUid",     S_P_UINT32 },
	{ "RemoteGid",     S_P_UINT32 },
	{ NULL, 0 }
};

static const s_p_options_t mapping_options[] = {
	{ "UserMapping", S_P_LINE, NULL, NULL, mapping_line_opts },
	{ NULL, 0 }
};

static void _mapping_idfunc(void *item, const char **key, uint32_t *len)
{
	user_mapping_t *m = item;
	static __thread char buf[256];
	int n = snprintf(buf, sizeof(buf), "%s|%s",
	                 m->local_user, m->remote_cluster);
	*key = buf;
	*len = n;
}

static void _mapping_freefunc(void *item)
{
	user_mapping_t *m = item;
	if (!m) return;
	xfree(m->local_user);
	xfree(m->remote_cluster);
	xfree(m->remote_user);
	xfree(m);
}

int user_mapping_load(const char *path)
{
	s_p_hashtbl_t *tbl = s_p_hashtbl_create(mapping_options);
	s_p_hashtbl_t **lines = NULL;
	int line_count = 0;

	if (s_p_parse_file(tbl, NULL, path, false))
		fatal("user_mapping.conf: parse failed");

	g_user_mappings = xhash_init(_mapping_idfunc, _mapping_freefunc);

	if (s_p_get_array((void ***)&lines, &line_count, "UserMapping", tbl)) {
		for (int i = 0; i < line_count; i++) {
			user_mapping_t *m = xmalloc(sizeof(*m));
			s_p_get_string(&m->local_user,     "LocalUser",     lines[i]);
			s_p_get_string(&m->remote_cluster, "RemoteCluster", lines[i]);
			s_p_get_string(&m->remote_user,    "RemoteUser",    lines[i]);
			s_p_get_uint32(&m->remote_uid,     "RemoteUid",     lines[i]);
			s_p_get_uint32(&m->remote_gid,     "RemoteGid",     lines[i]);
			if (!m->local_user || !m->remote_cluster ||
			    !m->remote_user) {
				_mapping_freefunc(m);
				fatal("user_mapping.conf line %d incomplete", i);
			}
			xhash_add(g_user_mappings, m);
		}
	}

	s_p_hashtbl_destroy(tbl);
	return SLURM_SUCCESS;
}

user_mapping_t *user_mapping_lookup(const char *local_user,
                                    const char *remote_cluster)
{
	char key[256];
	int n = snprintf(key, sizeof(key), "%s|%s", local_user, remote_cluster);
	return xhash_get(g_user_mappings, key, n);
}

void user_mapping_destroy_all(void)
{
	xhash_free(g_user_mappings);
	g_user_mappings = NULL;
}
```

- **风险**：
  - `_mapping_idfunc` 用 `__thread char buf[256]` 是 hack（xhash 要求 idfunc 返回的指针生命周期由调用方保证，看 [src/common/xhash.c](../../src/common/xhash.c) 实现确认是否拷贝；如不拷贝，必须改成在 `xhash_add` 之前先把 key 算好存到 `m->_cache_key`）。**实际该函数会立即拷贝**（slurm 现有 xhash 用法都这么用），但写新代码时务必读一遍 xhash.c 确认。
  - 同一 `(local_user, remote_cluster)` 在 conf 中出现两次 → 后者覆盖前者？xhash_add 默认行为？需要测一下。建议解析时显式检查重复 key 并 fatal。
- **DoD**：
  - [ ] 写 3 行映射，lookup `(test1, wz_cluster)` 返回 `remote_uid=20001`
  - [ ] lookup 不存在的 user 返回 NULL
  - [ ] 缺字段（如 LocalUser=）的行 → fatal
  - [ ] 重复 key 两次出现 → fatal（不允许覆盖）

### M02-T5 启动日志摘要打印

- **依赖**：M02-T4
- **预估**：0.25d
- **关键决策**：一行式 INFO，方便 `journalctl -u slurmbrokerd | head` 一眼看出关键参数。
- **代码草图**：

```c
void broker_conf_log_summary(void)
{
	uint32_t mapping_count = g_user_mappings
	                         ? xhash_count(g_user_mappings) : 0;

	info("broker_conf: cluster=%s peer=%s:%u default_partition=%s "
	     "lookup=%s mappings=%u",
	     g_broker_conf.cluster_name,
	     g_broker_conf.remote_broker_host,
	     g_broker_conf.remote_broker_port,
	     g_broker_conf.default_remote_partition,
	     g_broker_conf.lookup_software_script,
	     mapping_count);
}
```

调用点：在 `broker_init()` 内 `user_mapping_load` 之后、`proto_init` 之前调用一次。

- **DoD**：
  - [ ] `journalctl -u slurmbrokerd -n 20` 能看到该行
  - [ ] 各字段正确反映 conf 内容

---

## 7. 整体 DoD（汇总）

- [ ] 5 个子任务全部勾选
- [ ] `broker_conf_init` + `user_mapping_load` 在 `broker_init()` 中已 wire-up
- [ ] valgrind: 启动 → fini，0 byte still reachable（需 `xfree` 所有 `g_broker_conf.*` 字符串字段，`broker_conf_fini` 实现完整）
- [ ] 一份完整 conf 启动 + 一份缺字段 conf fatal + 一份 typo conf fatal，三种用例都符合预期
- [ ] 与 [src/slurmbrokerd/slurmbrokerd.c](../../src/slurmbrokerd/slurmbrokerd.c) 的 SIGHUP handler 无功能性回归（仍只置位标志位）

## 8. 验证脚本

```bash
# Linux 服务器上：
cd <repo-root>
autoreconf -fi
./configure --prefix=/usr/local
make -C src/slurmbrokerd

# 1) 合法 conf
sudo install -d /etc/slurm
sudo cp etc/slurm-broker/broker.conf.example /etc/slurm/broker.conf
sudo cp etc/slurm-broker/user_mapping.conf.example /etc/slurm/user_mapping.conf
sudo chmod 0600 /etc/slurm/broker_id_ed25519   # 满足 stage_ssh_key 校验
./src/slurmbrokerd/slurmbrokerd -D -v -f /etc/slurm/broker.conf
# 期望：看到 "broker_conf: cluster=xian_cluster peer=... mappings=N" 一行

# 2) 缺必填字段
sudo sed -i '/^ClusterName=/d' /etc/slurm/broker.conf
./src/slurmbrokerd/slurmbrokerd -D -f /etc/slurm/broker.conf
# 期望：fatal "missing required 'ClusterName'", exit 1

# 3) typo
sudo sed -i 's/^ClusterName=/ClsuterName=/' /etc/slurm/broker.conf
./src/slurmbrokerd/slurmbrokerd -D -f /etc/slurm/broker.conf
# 期望：fatal "parse failed", exit 1

# 4) ssh key 权限错
sudo chmod 0644 /etc/slurm/broker_id_ed25519
./src/slurmbrokerd/slurmbrokerd -D -f /etc/slurm/broker.conf
# 期望：fatal "StageSshKey ... permissions 0644 too open (need 0600)"
```

---

## 9. 风险与回滚

### 9.1 已知风险

| 风险 | 触发条件 | 缓解 |
|---|---|---|
| `MaxStageBytes` 数值过大溢出 `uint64_t` | 写 `MaxStageBytes=99999999999999999999` | 解析时 `errno=ERANGE` 检查 |
| `s_p_get_array` 在 `S_P_LINE` 为空时返回 0 | 没写任何 `UserMapping` 行 | OK，`g_user_mappings` 仍是空 hash；T4 lookup 永远 NULL |
| `Include /etc/slurm/user_mapping.conf` 路径不存在 | 运维忘了拷贝 | `s_p_parse_file` 会 fatal；提示明确 |
| `auth_type` 默认 `auth/munge`，但实际系统没装 munge | 某些 minimum image | M04-T5 `proto_init` 时再 fail-fast |

### 9.2 回滚方案

- **本模块独立**：未上线时 `git revert` 4 个文件 + 撤销 [src/slurmbrokerd/Makefile.am](../../src/slurmbrokerd/Makefile.am) 修改即可，对 M01 无影响。
- **已上线后回滚**：service 已起的话先 `systemctl stop slurmbrokerd`，回滚二进制，再 start。状态文件由 M03 维护，与本模块无关。
