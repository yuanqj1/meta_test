# M16 路由决策模块 (routes_loader + route + cap_check) Checklist (broker · v2.0) ★ NEW

> 配套: [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §8.A / §8.B / §8.C / §14.4.4
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §2.5（路由决策权下沉到 broker）
> Sprint: S4 W7-W8
> 依赖:
>   - **M02 v2.0**（`broker_conf::RouteSource` / `RoutesConfPath` / `RoutesReloadMode` / `MaxInFlightJobs` 字段已落地）
>   - **M03 v2.0**（`broker_job_t::route_candidates[]` / `route_attempted_mask` / `route_current_idx` / `target_broker_addr`）
>   - **M04 v2.0**（错误码 9010 `BROKERD_ERR_NO_VIABLE_ROUTE` / 9013 `BROKERD_ERR_ALL_ROUTES_EXHAUSTED` / 9020 `BROKERD_ERR_CAP_FULL_SOFT_WAIT`）
>   - **M11 v2.0**（user_mapping_lookup 接口）
> 下游:
>   - **M06 v2.0**（handler_ctld 调 `route_decide` 填充 candidates）
>   - **M09 v2.0**（state_machine `_on_init_deciding` / `_on_init_probing` / `_on_init_selected` / `_on_init_exhausted` 四子状态）
>   - **M08 v2.0**（egress 用 `target_broker_addr` 路由）
>   - **M15 v2.0**（`sbroker route check` dry-run 工具复用 routes_loader_init）

> **核心定位**：本模块是 v2.0 路由能力下沉的**核心引擎**，把 v1.5 ctld 的 `partition.SendTo` + `AllowApp` 决策权全部下沉到 broker。
>
> ★ **3 个新 .c/.h 文件** + 约 600 LoC + 单元覆盖率 ≥ 80%

---

## 1. 模块概述与目标

### 1.1 一句话定位

把 v1.5 的「ctld 单一目的地映射」升级为「broker 多远端候选 + 优先级 + test-only 探测 + 软容量限流」的完整路由决策子系统：
- `routes_loader.c/h`: 解析 `routes.conf`（INI），SIGHUP/mtime 热加载，rwlock 读多写少
- `route.c/h`: 给定 `(src_cluster, src_partition, src_user, app_name)` 输出按 priority 升序的 `route_candidate_t[]`
- `cap_check.c/h`: 路由级 + 全局二维计数；过滤 + 占用 + 释放

### 1.2 v2.0 MVP 范围

- **routes_loader**: 启动时同步加载、运行期 SIGHUP/mtime 热加载、rwlock 读路径无阻塞
- **route**: `route_decide()` 返回候选数组（priority 升序），`route_candidates_free()` 释放
- **cap_check**: `cap_check_filter()` 过滤满载候选；`cap_inc()` / `cap_dec()` 占用与释放；启动时从 `broker_state.jsonl` 重建占用计数
- 单元测试覆盖率 ≥ 80%（含异常路径：syntax error、user_mapping 缺失、cap 边界、reload 中读路径）

### 1.3 不在 MVP 范围

- ~~资源画像（GPU 数 / 内存）维度匹配~~：v2.0 仅 `(src_cluster, src_partition, app_name)` 三元组（设计文档 §1904 明示）
- ~~平台 API 路由（`RouteSource=platform` / `auto`）~~：v2/v3 演进
- ~~路由维度的硬性限流（拒绝入队）~~：v2.0 仅软限流，cap 满 → `BROKERD_ERR_CAP_FULL_SOFT_WAIT`，ctld 下轮重试

### 1.4 与 v1.5 的差异

| 维度 | v1.5 | v2.0 |
|---|---|---|
| 路由表载体 | `partition.SendTo` + `partition.AllowApp` (slurm.conf) | **★ `routes.conf` 独立文件 + broker 加载** |
| 路由决策位置 | ctld `forward_thread` | **★ broker `state_machine._on_init_deciding`** |
| 候选数 | 1 (单一目的地) | **★ N (多 [Route] 段同 (Src, AllowApps))** |
| 决策算法 | 直接读字段 | **★ priority 升序 + AllowApps 过滤 + user_mapping 过滤 + cap 过滤** |
| 热加载 | 重启 ctld | **★ SIGHUP / mtime poll，rwlock writer 等读者退出** |
| 容量限流 | `partition.MaxJobs` (硬限) | **★ `routes.conf::RemoteMaxInflight` + `broker.conf::MaxInFlightJobs` 二维软限** |

---

## 2. 接口契约

### 2.1 公共 API

#### 2.1.1 `routes_loader.h`

```c
/* src/slurmbrokerd/routes_loader.h */
#ifndef _BROKERD_ROUTES_LOADER_H
#define _BROKERD_ROUTES_LOADER_H

#include "src/common/macros.h"
#include "src/common/list.h"
#include <pthread.h>

typedef struct route_entry {
    char     *route_id;                 /* [Route <id>] section name */
    char     *src_cluster_name;         /* Src= 前半 */
    char     *src_partition;            /* Src= 后半 ("*" 为通配) */
    char    **allow_apps;               /* AllowApps= 拆分 ("*" => 单元素 "*") */
    uint16_t  n_allow_apps;
    char     *target_broker_addr;       /* "host:port" */
    char     *target_cluster_name;      /* TargetCluster= */
    char     *target_partition;         /* TargetPartition= */
    uint16_t  priority;                 /* Priority=（数值越小越优先） */
    uint32_t  remote_max_inflight;      /* RemoteMaxInflight= */
    uint16_t  test_only_timeout_s;      /* TestOnlyTimeout=, 缺省 5 */
    struct route_entry *next;           /* 单链表，按 priority 升序 */
} route_entry_t;

typedef struct {
    pthread_rwlock_t  lock;
    route_entry_t    *head;             /* priority 升序单链表 */
    uint32_t          version;          /* 每次 reload +1 */
    time_t            mtime;
    char             *path;
    uint32_t          count;
} routes_table_t;

extern routes_table_t *g_routes;

/*
 * 启动时同步加载; 解析失败则 fail-stop (避免半配置上线)。
 * STATIC_LEGACY 模式不调用本函数; FILE 模式必调。
 */
extern int  routes_loader_init(const char *path);

/*
 * SIGHUP / mtime poll 触发; 解析失败则保留旧表 + warn。
 * 内部加 wrlock; 读者用 rdlock 不阻塞 (rwlock 公平模式)。
 */
extern int  routes_loader_reload(void);

/*
 * 按 (src_cluster, src_partition, app_name) 过滤；
 * Src 与 AllowApps 通配支持 ("*")；
 * 输出按 priority 升序。
 *
 * 返回值:
 *   SLURM_SUCCESS                       *out_matched 非空, *out_n > 0
 *   ESLURM_BROKER_NO_VIABLE_ROUTE       无匹配 (9010)
 *
 * 调用方需用 xfree() 释放 *out_matched (浅拷贝指针数组)。
 * route_entry_t* 指向 g_routes->head 链表内节点, 不可独立 free。
 */
extern int  routes_loader_lookup_by_src(const char *src_cluster,
                                         const char *src_partition,
                                         const char *app_name,
                                         route_entry_t ***out_matched,
                                         uint16_t *out_n);

extern void routes_loader_fini(void);
extern uint32_t routes_loader_count(void);
extern uint32_t routes_loader_version(void);

/*
 * ★ M05 v2.0 listener::_peer_ip_allowed 调用:
 * 返回当前所有 [Route].TargetBroker 解析后的 ip set; 调用方需 xfree。
 */
extern int routes_loader_get_known_peers_ipset(char ***out_ips, uint32_t *out_n);

#endif /* _BROKERD_ROUTES_LOADER_H */
```

#### 2.1.2 `route.h`

```c
/* src/slurmbrokerd/route.h */
#ifndef _BROKERD_ROUTE_H
#define _BROKERD_ROUTE_H

typedef struct route_candidate {
    char     *route_id;
    char     *target_broker_addr;       /* "host:port" 字符串 */
    slurm_addr_t target_broker_sockaddr;/* 解析后的 sockaddr (M08 直接用) */
    char     *remote_cluster_name;
    char     *remote_partition;
    uint16_t  priority;
    uint32_t  remote_max_inflight;
    uint16_t  test_only_timeout_s;
    /* user_mapping_lookup 解析结果 */
    uint32_t  remote_uid;
    char     *remote_user_name;
} route_candidate_t;

/*
 * 决策入口 (handler_ctld v2.0 / state_machine v2.0 调).
 * 输出 priority 升序数组; 已通过 user_mapping 过滤 (找不到 mapping 的路由跳过).
 *
 * 返回:
 *   SLURM_SUCCESS                         *out 非空, *n_out > 0
 *   ESLURM_BROKER_NO_VIABLE_ROUTE          无匹配 (9010)
 *   ESLURM_BROKER_NO_USER_MAPPING          全部候选都因 user_mapping 缺失被过滤 (9011)
 */
extern int  route_decide(const char *src_cluster_name,
                          const char *src_partition,
                          const char *src_user_name,
                          const char *cd_app_name,
                          route_candidate_t **out, uint16_t *n_out);

extern void route_candidates_free(route_candidate_t *arr, uint16_t n);

#endif /* _BROKERD_ROUTE_H */
```

#### 2.1.3 `cap_check.h`

```c
/* src/slurmbrokerd/cap_check.h */
#ifndef _BROKERD_CAP_CHECK_H
#define _BROKERD_CAP_CHECK_H

typedef struct cap_slot {
    char     *route_id;
    uint32_t  remote_max_inflight;
    uint32_t  current_inflight;         /* 已 SELECTED 但未到终态的作业数 */
} cap_slot_t;

typedef struct {
    pthread_mutex_t  lock;
    cap_slot_t      *slots;
    uint32_t         n_slots;
    uint32_t         routes_version;    /* 与 g_routes->version 对齐, 不一致 → rebuild */
    uint32_t         global_max_inflight;
    uint32_t         global_current_inflight;
} cap_state_t;

extern cap_state_t g_cap_state;

extern int  cap_check_init(uint32_t global_max_inflight);
extern void cap_check_fini(void);

/*
 * 在 INIT.DECIDING 阶段调用; 过滤掉容量满的候选; 不实际占用名额。
 *
 * 返回:
 *   SLURM_SUCCESS                          有 ≥1 候选可用; in-place 压缩 cands[]
 *   ESLURM_BROKER_OVERLOAD                 全局 MaxInFlightJobs 已满
 *   ESLURM_BROKER_CAP_FULL_SOFT_WAIT       全部候选 RemoteMaxInflight 满 (9020, 临时)
 */
extern int  cap_check_filter(route_candidate_t *cands, uint16_t *n);

/* SELECTED 后正式占住名额 (state_machine._on_init_selected 调用) */
extern int  cap_inc(const char *route_id);
/* 任意失败回滚 / 终态时释放 */
extern int  cap_dec(const char *route_id);

/*
 * 启动时 broker_state_restore 内调用: 把每条已 SELECTED 的作业加回 cap_slot.current_inflight。
 */
extern int  cap_check_replay_one(const char *route_id);

#endif /* _BROKERD_CAP_CHECK_H */
```

### 2.2 错误码（M04 v2.0 已注册）

| 码 | 名 | 触发 |
|---|---|---|
| 9010 | `ESLURM_BROKER_NO_VIABLE_ROUTE` | routes.conf 无匹配 (Src/AllowApps) |
| 9011 | `ESLURM_BROKER_NO_USER_MAPPING` | 候选有但全部因 user_mapping 缺失被过滤 |
| 9013 | `ESLURM_BROKER_ALL_ROUTES_EXHAUSTED` | test-only 全部候选探测失败 (state_machine 而非本模块抛出) |
| 9020 | `ESLURM_BROKER_CAP_FULL_SOFT_WAIT` | 全部候选 RemoteMaxInflight 满（软限流，ctld 下轮重试） |
| `ESLURM_BROKER_OVERLOAD`（沿用 v1.5） | 全局 MaxInFlightJobs 满 |

---

## 3. 参考代码

| 用途 | 文件 | 说明 |
|---|---|---|
| `s_p_options_t` / `s_p_parse_file` | [src/common/parse_config.h](../../src/common/parse_config.h) | INI 解析复用 |
| `s_p_get_array_xx` | [src/common/parse_config.c](../../src/common/parse_config.c) | section 数组取值 |
| `pthread_rwlock_*` | 系统 | 读多写少锁 |
| `slurm_addr_t` 解析 | [src/common/slurm_protocol_socket.c](../../src/common/slurm_protocol_socket.c) | `slurm_set_addr()` |
| List + ListIterator | [src/common/list.h](../../src/common/list.h) | 候选管理 |
| `xstrcmp` / `xstrcat` | [src/common/xstring.h](../../src/common/xstring.h) | 字符串工具 |
| user_mapping_lookup | [src/slurmbrokerd/broker_conf.c](../../src/slurmbrokerd/broker_conf.c) | M02 v1.5 已有 |

---

## 4. 文件清单

| 文件 | 类型 | 用途 |
|---|---|---|
| `src/slurmbrokerd/routes_loader.h` | ★ 新增 | API |
| `src/slurmbrokerd/routes_loader.c` | ★ 新增 | INI 解析 + rwlock 持有 + atomic swap |
| `src/slurmbrokerd/route.h` | ★ 新增 | API |
| `src/slurmbrokerd/route.c` | ★ 新增 | 决策算法 + user_mapping 集成 |
| `src/slurmbrokerd/cap_check.h` | ★ 新增 | API |
| `src/slurmbrokerd/cap_check.c` | ★ 新增 | 二维计数 + replay |
| `src/slurmbrokerd/Makefile.am` | 修改 | SOURCES += `routes_loader.c route.c cap_check.c` |
| `src/slurmbrokerd/broker_main.c` | 修改 | 启动调 `routes_loader_init` + `cap_check_init`，安装 SIGHUP handler |
| `src/slurmbrokerd/sig_handlers.c` | 修改 | SIGHUP → `routes_loader_reload()` |
| `tests/broker/test_routes_loader.c` | ★ 新增 | 单元测试（含异常路径）|
| `tests/broker/test_route_decide.c` | ★ 新增 | 单元测试 |
| `tests/broker/test_cap_check.c` | ★ 新增 | 单元测试（含 replay）|

---

## 5. 流程

### 5.1 路由决策流程（INIT.DECIDING）

```mermaid
sequenceDiagram
    participant sm as state_machine._on_init_deciding
    participant r as route_decide
    participant rl as routes_loader_lookup_by_src
    participant um as user_mapping_lookup
    participant cc as cap_check_filter

    sm->>r: route_decide(src_cluster, src_part, src_user, app)
    r->>rl: lookup_by_src(...)
    rl->>rl: rwlock_rdlock
    rl->>rl: filter Src + AllowApps + 通配
    rl-->>r: matched[N] (priority 升序)
    rl->>rl: rwlock_unlock
    loop foreach matched
        r->>um: lookup(src_user, target_cluster, &remote_uid, &remote_user)
        alt mapping 存在
            r->>r: append to candidates[]
        else mapping 缺失
            r->>r: skip; warn 该用户该路由不可用
        end
    end
    alt candidates 空
        r-->>sm: ESLURM_BROKER_NO_USER_MAPPING (9011)
    end
    r-->>sm: candidates[K], K ≤ N

    sm->>cc: cap_check_filter(candidates, &K)
    cc->>cc: mutex_lock
    cc->>cc: check global cap; 满 → OVERLOAD
    cc->>cc: foreach c: 满 → 标 skip; 否则保留
    cc->>cc: in-place 压缩
    cc->>cc: mutex_unlock
    alt 全部满
        cc-->>sm: ESLURM_BROKER_CAP_FULL_SOFT_WAIT (9020)
    end
    cc-->>sm: K' ≤ K, 至少 1 候选
    sm->>sm: 进入 INIT.PROBING
```

### 5.2 SIGHUP 热加载流程

```mermaid
sequenceDiagram
    participant op as 运维 / kill -HUP
    participant sh as sig_handlers
    participant rl as routes_loader_reload
    participant rd as route_decide reader

    op->>sh: SIGHUP
    sh->>rl: routes_loader_reload()
    rl->>rl: parse new file → tmp table
    alt parse 失败
        rl->>rl: warn + 保留旧表
        rl-->>sh: SLURM_ERROR
    end
    rl->>rl: rwlock_wrlock (等所有 reader 退出)
    rl->>rl: free old; head = new; version++
    rl->>rl: rwlock_unlock
    rl-->>sh: SLURM_SUCCESS

    par 期间的并发读
        rd->>rl: lookup_by_src
        rd->>rd: rwlock_rdlock (等 wrlock 释放; pthread 公平模式 < 100ms)
        rd-->>sm: candidates from 新表
    end
```

---

## 6. 任务展开

### M16-T1 ★ NEW `routes_loader.c/h` 解析与持久缓存

- **依赖**: M02 v2.0
- **预估**: 2d
- **关键决策**:
  1. 用 Slurm 自带 `s_p_parse_file()` + `S_P_LINE` for `[Route ...]` section 数组
  2. `Src=` 字段 split `:`，前半为 `src_cluster_name`，后半为 `src_partition`（"*" 通配）
  3. `AllowApps=` 按 `,` 拆分，结果 `xmalloc` 数组
  4. `TargetBroker=` 用 `slurm_set_addr()` 解析为 `slurm_addr_t`，字符串原样保存为 `target_broker_addr`
  5. 字段缺失或解析失败 → fail-stop（启动）/ 保留旧表 + warn（reload）
  6. 解析后单链表按 `priority` 升序插入
- **代码草图**（核心解析）:

```c
static s_p_options_t route_options[] = {
	{"Src",                S_P_STRING},
	{"AllowApps",          S_P_STRING},
	{"TargetBroker",       S_P_STRING},
	{"TargetCluster",      S_P_STRING},
	{"TargetPartition",    S_P_STRING},
	{"Priority",           S_P_UINT16},
	{"RemoteMaxInflight",  S_P_UINT32},
	{"TestOnlyTimeout",    S_P_UINT16},
	{NULL}
};

static s_p_options_t routes_top_options[] = {
	{"Route", S_P_LINE, NULL, NULL, route_options},
	{NULL}
};

int routes_loader_init(const char *path)
{
	if (!path || !*path) {
		error("routes_loader: path empty");
		return SLURM_ERROR;
	}
	g_routes = xmalloc(sizeof(*g_routes));
	pthread_rwlock_init(&g_routes->lock, NULL);
	g_routes->path = xstrdup(path);
	g_routes->head = NULL;
	g_routes->version = 0;
	g_routes->count = 0;

	int rc = _do_load_locked(false);
	if (rc != SLURM_SUCCESS) {
		error("routes_loader_init: failed to load %s, fail-stop", path);
		routes_loader_fini();
		return SLURM_ERROR;
	}
	info("routes_loader: loaded %u routes from %s",
	     g_routes->count, path);
	return SLURM_SUCCESS;
}

static int _do_load_locked(bool reload)
{
	s_p_hashtbl_t *hashtbl = s_p_hashtbl_create(routes_top_options);
	if (s_p_parse_file(hashtbl, NULL, g_routes->path, false) != SLURM_SUCCESS) {
		error("routes_loader: parse %s failed", g_routes->path);
		s_p_hashtbl_destroy(hashtbl);
		return SLURM_ERROR;
	}

	struct stat st;
	if (stat(g_routes->path, &st) == 0)
		g_routes->mtime = st.st_mtime;

	void **route_ptr_array = NULL;
	int n = 0;
	s_p_get_array(&route_ptr_array, &n, "Route", hashtbl);

	route_entry_t *new_head = NULL;
	uint32_t new_count = 0;

	for (int i = 0; i < n; i++) {
		s_p_hashtbl_t *t = route_ptr_array[i];
		const char *route_id = s_p_get_section_name(t);
		char *src = NULL, *apps = NULL, *tbroker = NULL;
		char *tcluster = NULL, *tpart = NULL;
		uint16_t prio = 100, t_to = 5;
		uint32_t rmax = 0;

		s_p_get_string(&src,      "Src",               t);
		s_p_get_string(&apps,     "AllowApps",         t);
		s_p_get_string(&tbroker,  "TargetBroker",      t);
		s_p_get_string(&tcluster, "TargetCluster",     t);
		s_p_get_string(&tpart,    "TargetPartition",   t);
		s_p_get_uint16(&prio,     "Priority",          t);
		s_p_get_uint32(&rmax,     "RemoteMaxInflight", t);
		s_p_get_uint16(&t_to,     "TestOnlyTimeout",   t);

		if (!src || !apps || !tbroker || !tcluster || !tpart) {
			error("routes_loader: route_id=%s missing required field",
			      route_id);
			xfree(src); xfree(apps); xfree(tbroker);
			xfree(tcluster); xfree(tpart);
			_free_list(new_head);
			s_p_hashtbl_destroy(hashtbl);
			return SLURM_ERROR;
		}

		route_entry_t *e = xmalloc(sizeof(*e));
		e->route_id          = xstrdup(route_id);
		e->priority          = prio;
		e->remote_max_inflight = rmax;
		e->test_only_timeout_s = t_to;
		e->target_broker_addr = tbroker;     /* 已 xstrdup */
		e->target_cluster_name = tcluster;
		e->target_partition  = tpart;

		/* split Src "<cluster>:<partition>" */
		char *colon = strchr(src, ':');
		if (!colon) {
			error("routes_loader: route_id=%s Src '%s' must be cluster:partition",
			      route_id, src);
			xfree(src); xfree(e); _free_list(new_head);
			s_p_hashtbl_destroy(hashtbl);
			return SLURM_ERROR;
		}
		*colon = 0;
		e->src_cluster_name = xstrdup(src);
		e->src_partition    = xstrdup(colon + 1);
		xfree(src);

		/* split AllowApps */
		_split_csv(apps, &e->allow_apps, &e->n_allow_apps);
		xfree(apps);

		/* priority 升序插入 */
		_insert_sorted(&new_head, e);
		new_count++;
	}

	s_p_hashtbl_destroy(hashtbl);

	/* swap */
	route_entry_t *old_head;
	if (reload) pthread_rwlock_wrlock(&g_routes->lock);
	old_head = g_routes->head;
	g_routes->head = new_head;
	g_routes->count = new_count;
	g_routes->version++;
	if (reload) pthread_rwlock_unlock(&g_routes->lock);
	_free_list(old_head);
	return SLURM_SUCCESS;
}
```

- **DoD**:
  - [ ] 4 个 [Route] 段的 `etc/routes.conf.example` 加载后 count=4，priority 升序
  - [ ] 字段缺失（如缺 `TargetCluster`）→ init 失败 + fail-stop
  - [ ] reload 时新文件 syntax error → 旧表保留 + warn
  - [ ] 1000 个 [Route] 段加载耗时 < 200ms
  - [ ] valgrind clean

### M16-T2 ★ NEW `routes_loader_lookup_by_src` + 通配匹配

- **依赖**: T1
- **预估**: 0.75d
- **关键决策**:
  1. 进入加 `rwlock_rdlock`，扫完链表后 unlock
  2. 匹配 `src_cluster_name` 必须精确（cluster name 不允许通配）
  3. `src_partition` 支持 `*` 通配
  4. `allow_apps` 任一元素为 `*` 即放通；否则 `app_name` 必须在数组内（精确）
  5. `app_name == NULL` 视为不匹配 AllowApps（防御）
  6. 输出 `route_entry_t*` 浅拷贝指针数组（`xmalloc(N * sizeof(*))`）
- **代码草图**:

```c
int routes_loader_lookup_by_src(const char *src_cluster,
                                  const char *src_partition,
                                  const char *app_name,
                                  route_entry_t ***out_matched,
                                  uint16_t *out_n)
{
	*out_matched = NULL;
	*out_n = 0;

	if (!src_cluster || !src_partition) return SLURM_ERROR;

	pthread_rwlock_rdlock(&g_routes->lock);

	/* 第一遍计数 */
	uint16_t n = 0;
	for (route_entry_t *e = g_routes->head; e; e = e->next) {
		if (xstrcmp(e->src_cluster_name, src_cluster)) continue;
		if (xstrcmp(e->src_partition, "*") &&
		    xstrcmp(e->src_partition, src_partition)) continue;
		if (!_app_match(e, app_name)) continue;
		n++;
	}

	if (n == 0) {
		pthread_rwlock_unlock(&g_routes->lock);
		return ESLURM_BROKER_NO_VIABLE_ROUTE;
	}

	route_entry_t **arr = xmalloc(n * sizeof(*arr));
	uint16_t k = 0;
	for (route_entry_t *e = g_routes->head; e && k < n; e = e->next) {
		if (xstrcmp(e->src_cluster_name, src_cluster)) continue;
		if (xstrcmp(e->src_partition, "*") &&
		    xstrcmp(e->src_partition, src_partition)) continue;
		if (!_app_match(e, app_name)) continue;
		arr[k++] = e;
	}

	pthread_rwlock_unlock(&g_routes->lock);
	*out_matched = arr;
	*out_n = n;
	return SLURM_SUCCESS;
}

static bool _app_match(const route_entry_t *e, const char *app_name)
{
	if (!app_name) return false;
	for (uint16_t i = 0; i < e->n_allow_apps; i++) {
		if (xstrcmp(e->allow_apps[i], "*") == 0) return true;
		if (xstrcmp(e->allow_apps[i], app_name) == 0) return true;
	}
	return false;
}
```

- **DoD**:
  - [ ] 通配 `Src=cluA:*` + `AllowApps=*` 命中所有 (cluA, *, *) 请求
  - [ ] 精确 `Src=cluA:p1` + `AllowApps=gromacs` 仅命中 (cluA, p1, gromacs)
  - [ ] `app_name=NULL` 永远 NO_VIABLE_ROUTE
  - [ ] 100 候选表 + 1000 次 lookup 总耗时 < 100ms
  - [ ] reload 期间并发读 1000 次无 crash

### M16-T3 ★ NEW SIGHUP / mtime 热加载

- **依赖**: T1, T2
- **预估**: 0.75d
- **关键决策**:
  1. SIGHUP handler 仅设置一个 `volatile bool g_routes_reload_requested`，由主循环 / 单独 worker 调 `routes_loader_reload()`（避免 signal handler 内做重活）
  2. `RoutesReloadMode = sighup_or_mtime` 时，启动一个 mtime poll 线程，按 `RoutesMtimePollSec` 周期 `stat(path)` 比 `g_routes->mtime`，变化则 trigger reload
  3. reload 内部：先在临时栈上 parse 新表 → 校验通过 → wrlock 期间 swap + version++
  4. wrlock 等所有 rdlock 退出（pthread 默认偏写者，高负载下 reader 不会饿死 writer）
- **代码草图**:

```c
/* sig_handlers.c */
volatile sig_atomic_t g_routes_reload_requested = 0;

void sig_handler_sighup(int signo)
{
	g_routes_reload_requested = 1;
}

/* broker_main.c (主循环) */
while (running) {
	if (g_routes_reload_requested) {
		g_routes_reload_requested = 0;
		uint64_t t0 = _now_ms();
		int rc = routes_loader_reload();
		info("routes_loader_reload: rc=%d took=%lums",
		     rc, _now_ms() - t0);
	}
	sleep(1);
}

/* routes_loader.c */
int routes_loader_reload(void)
{
	return _do_load_locked(true);   /* reload=true → 内部 wrlock swap */
}

/* mtime poll worker */
static void *_mtime_poll_main(void *arg)
{
	while (g_routes_running) {
		struct stat st;
		if (stat(g_routes->path, &st) == 0) {
			pthread_rwlock_rdlock(&g_routes->lock);
			time_t old_mtime = g_routes->mtime;
			pthread_rwlock_unlock(&g_routes->lock);
			if (st.st_mtime != old_mtime) {
				info("routes_loader: mtime changed, reload");
				routes_loader_reload();
			}
		}
		sleep(g_broker_conf.routes_mtime_poll_sec);
	}
	return NULL;
}
```

- **DoD**:
  - [ ] `kill -HUP $(pgrep slurmbrokerd)` 后 < 100ms 内 reload 完成（log 可见）
  - [ ] reload 中 1000 个并发 `route_decide` 无 crash，最终结果一致
  - [ ] 新文件 syntax error → reload 失败 + 旧表继续工作
  - [ ] 修改 routes.conf 后 < `RoutesMtimePollSec + 1` 内自动 reload (sighup_or_mtime 模式)

### M16-T4 ★ NEW `route.c::route_decide()`

- **依赖**: T2, M11 v1.5（user_mapping_lookup）
- **预估**: 1d
- **关键决策**:
  1. 调 `routes_loader_lookup_by_src()` 拿 matched[]
  2. 对每个 matched，调 `user_mapping_lookup(src_user, target_cluster_name, &remote_uid, &remote_user)`
     - 失败 → skip + debug log（"route_id=X 对用户 Y 不可用"）
     - 成功 → 填 `route_candidate_t` append 到结果
  3. 已经按 priority 升序（`routes_loader_lookup_by_src` 已排序）
  4. 全部跳过 → 返回 `ESLURM_BROKER_NO_USER_MAPPING`
  5. 同时把 `target_broker_addr` 字符串解析为 `slurm_addr_t`（M08 直接用）
- **代码草图**:

```c
int route_decide(const char *src_cluster_name,
                 const char *src_partition,
                 const char *src_user_name,
                 const char *cd_app_name,
                 route_candidate_t **out, uint16_t *n_out)
{
	*out = NULL;
	*n_out = 0;

	route_entry_t **matched = NULL;
	uint16_t n = 0;
	int rc = routes_loader_lookup_by_src(src_cluster_name, src_partition,
	                                      cd_app_name, &matched, &n);
	if (rc != SLURM_SUCCESS) return rc;

	route_candidate_t *arr = xmalloc(n * sizeof(*arr));
	uint16_t k = 0;
	for (uint16_t i = 0; i < n; i++) {
		uint32_t remote_uid = 0;
		char *remote_user = NULL;

		if (user_mapping_lookup(src_user_name,
		                        matched[i]->target_cluster_name,
		                        &remote_uid, &remote_user) != SLURM_SUCCESS) {
			debug("route_decide: route_id=%s skipped — no user_mapping for "
			      "src_user=%s target_cluster=%s",
			      matched[i]->route_id, src_user_name,
			      matched[i]->target_cluster_name);
			continue;
		}

		arr[k].route_id            = xstrdup(matched[i]->route_id);
		arr[k].target_broker_addr  = xstrdup(matched[i]->target_broker_addr);
		arr[k].remote_cluster_name = xstrdup(matched[i]->target_cluster_name);
		arr[k].remote_partition    = xstrdup(matched[i]->target_partition);
		arr[k].priority            = matched[i]->priority;
		arr[k].remote_max_inflight = matched[i]->remote_max_inflight;
		arr[k].test_only_timeout_s = matched[i]->test_only_timeout_s;
		arr[k].remote_uid          = remote_uid;
		arr[k].remote_user_name    = remote_user;   /* xstrdup'd by user_mapping_lookup */
		_parse_addr(matched[i]->target_broker_addr, &arr[k].target_broker_sockaddr);
		k++;
	}

	xfree(matched);

	if (k == 0) {
		xfree(arr);
		return ESLURM_BROKER_NO_USER_MAPPING;
	}

	*out = arr;
	*n_out = k;
	return SLURM_SUCCESS;
}

void route_candidates_free(route_candidate_t *arr, uint16_t n)
{
	for (uint16_t i = 0; i < n; i++) {
		xfree(arr[i].route_id);
		xfree(arr[i].target_broker_addr);
		xfree(arr[i].remote_cluster_name);
		xfree(arr[i].remote_partition);
		xfree(arr[i].remote_user_name);
	}
	xfree(arr);
}
```

- **DoD**:
  - [ ] 多 [Route] + user_mapping 全有 → 候选数 = 路由数，按 priority 升序
  - [ ] user_mapping 缺失 → 该路由 skip，不影响其它
  - [ ] 全部缺失 → ESLURM_BROKER_NO_USER_MAPPING
  - [ ] valgrind clean（含 candidates 释放）

### M16-T5 ★ NEW `cap_check.c` 二维计数

- **依赖**: T1（routes_table_t::version）, T4
- **预估**: 1.5d
- **关键决策**:
  1. `cap_check_init(global_max)` 读 broker.conf::MaxInFlightJobs；初始化 slots[] 与 g_routes 同步
  2. `cap_check_filter`：
     - mutex_lock
     - 检查 routes_version 是否变化 → 变化则 `_rebuild_locked()`（重新分配 slots[]，已占用的按 route_id 平移；已不存在的 route_id 占用清零）
     - 检查 global_current_inflight ≥ global_max → 返回 OVERLOAD
     - 遍历 cands[]：找对应 slot，若 current_inflight >= remote_max_inflight 标 skip
     - in-place 压缩 cands[]
     - mutex_unlock
  3. `cap_inc(route_id)`：mutex 内 +1（global + per-route），找不到 slot 视为 NEW（route_id 不在 routes.conf 中，可能是热加载丢失，warn）
  4. `cap_dec(route_id)`：mutex 内 -1，下溢防御 = 0
  5. `cap_check_replay_one(route_id)`：M03-T1 v2.0 的 broker_state_restore 调，重建启动时 inflight 计数（不影响 global，因为已通过 broker_job_table 计数）
- **代码草图**:

```c
int cap_check_init(uint32_t global_max_inflight)
{
	slurm_mutex_init(&g_cap_state.lock);
	g_cap_state.global_max_inflight = global_max_inflight;
	g_cap_state.global_current_inflight = 0;
	g_cap_state.slots = NULL;
	g_cap_state.n_slots = 0;
	g_cap_state.routes_version = 0;
	_rebuild_locked();   /* 启动时 build 一次 */
	return SLURM_SUCCESS;
}

static void _rebuild_locked(void)
{
	uint32_t new_ver = routes_loader_version();
	if (new_ver == g_cap_state.routes_version) return;

	pthread_rwlock_rdlock(&g_routes->lock);
	uint32_t n = g_routes->count;
	cap_slot_t *new_slots = xmalloc(n * sizeof(*new_slots));
	uint32_t i = 0;
	for (route_entry_t *e = g_routes->head; e && i < n; e = e->next) {
		new_slots[i].route_id            = xstrdup(e->route_id);
		new_slots[i].remote_max_inflight = e->remote_max_inflight;
		/* 平移已占用 */
		new_slots[i].current_inflight = 0;
		for (uint32_t j = 0; j < g_cap_state.n_slots; j++) {
			if (xstrcmp(g_cap_state.slots[j].route_id, e->route_id) == 0) {
				new_slots[i].current_inflight =
				    g_cap_state.slots[j].current_inflight;
				break;
			}
		}
		i++;
	}
	pthread_rwlock_unlock(&g_routes->lock);

	for (uint32_t j = 0; j < g_cap_state.n_slots; j++)
		xfree(g_cap_state.slots[j].route_id);
	xfree(g_cap_state.slots);

	g_cap_state.slots = new_slots;
	g_cap_state.n_slots = n;
	g_cap_state.routes_version = new_ver;
	debug("cap_check: rebuilt %u slots, version=%u", n, new_ver);
}

int cap_check_filter(route_candidate_t *cands, uint16_t *n)
{
	int rc = SLURM_SUCCESS;
	slurm_mutex_lock(&g_cap_state.lock);
	_rebuild_locked();

	if (g_cap_state.global_current_inflight >= g_cap_state.global_max_inflight) {
		rc = ESLURM_BROKER_OVERLOAD;
		goto out;
	}

	uint16_t k = 0;
	for (uint16_t i = 0; i < *n; i++) {
		bool ok = true;
		for (uint32_t j = 0; j < g_cap_state.n_slots; j++) {
			if (xstrcmp(g_cap_state.slots[j].route_id,
			            cands[i].route_id) == 0) {
				if (g_cap_state.slots[j].current_inflight >=
				    g_cap_state.slots[j].remote_max_inflight) {
					ok = false;
				}
				break;
			}
		}
		if (ok) {
			if (k != i) cands[k] = cands[i];
			k++;
		} else {
			route_candidate_t tmp = cands[i];
			route_candidates_free(&tmp, 1);   /* 清掉被过滤的 */
		}
	}
	*n = k;
	if (k == 0) rc = ESLURM_BROKER_CAP_FULL_SOFT_WAIT;

out:
	slurm_mutex_unlock(&g_cap_state.lock);
	return rc;
}

int cap_inc(const char *route_id)
{
	slurm_mutex_lock(&g_cap_state.lock);
	g_cap_state.global_current_inflight++;
	for (uint32_t j = 0; j < g_cap_state.n_slots; j++) {
		if (xstrcmp(g_cap_state.slots[j].route_id, route_id) == 0) {
			g_cap_state.slots[j].current_inflight++;
			slurm_mutex_unlock(&g_cap_state.lock);
			return SLURM_SUCCESS;
		}
	}
	warning("cap_inc: route_id=%s not in slots (reload race?)", route_id);
	slurm_mutex_unlock(&g_cap_state.lock);
	return SLURM_SUCCESS;
}

int cap_dec(const char *route_id)
{
	slurm_mutex_lock(&g_cap_state.lock);
	if (g_cap_state.global_current_inflight)
		g_cap_state.global_current_inflight--;
	for (uint32_t j = 0; j < g_cap_state.n_slots; j++) {
		if (xstrcmp(g_cap_state.slots[j].route_id, route_id) == 0) {
			if (g_cap_state.slots[j].current_inflight)
				g_cap_state.slots[j].current_inflight--;
			break;
		}
	}
	slurm_mutex_unlock(&g_cap_state.lock);
	return SLURM_SUCCESS;
}

int cap_check_replay_one(const char *route_id)
{
	return cap_inc(route_id);   /* 复用 inc 语义 */
}
```

- **DoD**:
  - [ ] 单 route 的 RemoteMaxInflight=4，连续 5 个候选过滤后剩 4 个；inc 4 次后 filter 返回 CAP_FULL_SOFT_WAIT
  - [ ] global_max=10，11 个不同 route 的候选累计 inc 11 次第 11 个返回 OVERLOAD
  - [ ] reload 后 routes_version 变化，filter 触发 _rebuild，已占用平移正确
  - [ ] cap_check_replay_one 100 次后 current_inflight=100
  - [ ] valgrind clean

### M16-T6 ★ NEW broker_main 集成 + SIGHUP wire-up

- **依赖**: T1, T3, T5
- **预估**: 0.5d
- **关键决策**:
  1. `broker_init()` 中：
     - 当 `g_broker_conf.route_source == ROUTE_SOURCE_FILE` 时调 `routes_loader_init(g_broker_conf.routes_conf_path)`
     - 紧接着调 `cap_check_init(g_broker_conf.max_in_flight_jobs)`
     - 再调 `broker_state_restore()`（在 cap_check_init 之后，这样 replay 能写入 cap_state）
     - 安装 SIGHUP handler
     - 当 `g_broker_conf.routes_reload_mode` 包含 mtime 时，启动 mtime poll worker
  2. `broker_fini()` 反向：stop mtime poll → cap_check_fini → routes_loader_fini
- **DoD**:
  - [ ] 启动日志依次出现 `routes_loader: loaded X routes` → `cap_check: init global_max=Y` → `broker_state: replayed Z jobs` → `cap_check: replayed Z increments`
  - [ ] kill -HUP 触发 reload 日志
  - [ ] kill -TERM 干净退出，valgrind clean

### M16-T7 ★ NEW 单元测试套件（覆盖率 ≥ 80%）

- **依赖**: T1-T6
- **预估**: 1.5d
- **关键决策**:
  1. routes_loader: 4 案例 × 2 路径 = 8 测试
     - happy: 4 routes 加载、版本号、count
     - exception: 字段缺失、Src 格式错、AllowApps 空
     - reload: SIGHUP 后并发读 1k 次
     - mtime poll: 修改文件后自动 reload
  2. route_decide: 5 案例
     - 通配 Src + AllowApps
     - 精确 Src + 多 AllowApps
     - user_mapping 缺失 skip
     - 全部缺 mapping → NO_USER_MAPPING
     - cd_app_name=NULL → NO_VIABLE_ROUTE
  3. cap_check: 6 案例
     - filter happy
     - filter 单 route 满 → soft_wait
     - filter global 满 → overload
     - inc/dec 边界（下溢、不存在的 route_id）
     - reload 后 _rebuild 平移
     - replay 1000 次
- **DoD**:
  - [ ] gcov 报告 3 文件覆盖率 ≥ 80%
  - [ ] CI 集成 `make check-broker-routing`
  - [ ] valgrind clean

---

## 7. 整体 DoD（汇总）

- [ ] 7 个子任务全部勾选
- [ ] **★ 端到端**：mock routes.conf 含 3 个候选 + 1 个候选 user_mapping 缺失 → state_machine `_on_init_deciding` 收到 2 个候选，priority 升序
- [ ] **★ 端到端**：第 1 候选 cap 满 → `_on_init_probing` 直接试第 2 候选；2 个全满 → `_on_init_deciding` 返回 CAP_FULL_SOFT_WAIT，ctld 下轮重试
- [ ] **★ 性能**：100 候选 routes.conf 下 `route_decide()` < 1 ms（rwlock 读路径）
- [ ] **★ 性能**：SIGHUP routes.conf 热加载 < 100 ms，期间无 forward_job 阻塞
- [ ] **★ 容量**：global_max=1000 + 单路由 max=64 边界正确
- [ ] valgrind clean（init/reload/lookup/inc/dec/fini 全路径）
- [ ] gcov 覆盖率 ≥ 80%

## 8. 验证脚本

```bash
# T1-T2: routes_loader
cat > /tmp/routes_t.conf <<'EOF'
[Route r-cpu]
Src=cluA:cpu_p1
AllowApps=*
TargetBroker=10.0.2.50:7001
TargetCluster=cluB
TargetPartition=cpu_q
Priority=100
RemoteMaxInflight=64

[Route r-gpu]
Src=cluA:*
AllowApps=lammps,vasp
TargetBroker=10.0.3.50:7001
TargetCluster=cluC
TargetPartition=gpu_r
Priority=50
RemoteMaxInflight=16
EOF

./tests/broker/test_routes_loader /tmp/routes_t.conf
# 期望: count=2, version=1, head->priority=50 (gpu 优先)

# T3: SIGHUP reload
./tests/broker/test_routes_loader_reload /tmp/routes_t.conf
# (内部修改 conf + raise SIGHUP)
# 期望: 看到 "reload: loaded N routes"

# T4: route_decide
./tests/broker/test_route_decide /tmp/routes_t.conf cluA cpu_p1 user1 lammps
# 期望: 2 candidates (gpu 优先), user_mapping 全 OK

./tests/broker/test_route_decide /tmp/routes_t.conf cluA cpu_p1 ghost lammps
# 期望: ESLURM_BROKER_NO_USER_MAPPING (ghost 用户无映射)

./tests/broker/test_route_decide /tmp/routes_t.conf cluA xpu_p9 user1 lammps
# 期望: 1 candidate (gpu 通配 cluA:* 命中, cpu r-cpu 不命中 xpu_p9)

# T5: cap_check
./tests/broker/test_cap_check /tmp/routes_t.conf
# 期望: 64 inc 后 r-cpu 满, 第 65 次 filter 排除 r-cpu

# T7: gcov
make check-broker-routing GCOV=1
gcov src/slurmbrokerd/{routes_loader,route,cap_check}.c | tail
# 期望: ≥ 80%
```

---

## 9. 风险与回滚

| 风险 | 触发 | 缓解 |
|---|---|---|
| reload 中并发读结果不一致 | rwlock 写在 swap 期间 | wrlock 期间 reader 全部阻塞，最多 100ms；性能 DoD 验证 |
| `target_broker_addr` 域名解析慢 | DNS 抖动 | `slurm_set_addr` 缓存；reload 后失败的 entry log warn 但保留（运行时 egress 调用时再 retry） |
| user_mapping_lookup 高 QPS 慢 | 候选 × 用户基数大 | M02 user_mapping 已用 hash O(1) |
| cap_check 平移漏掉新增 route | reload 加 route 但未 _rebuild | filter/inc 入口都调 `_rebuild_locked`，version 触发 |
| cap_dec 下溢 | 重复 dec | `if (count) count--;` 防御 + warn |
| routes.conf 字段拼写错误难以排查 | 运维手抖 | M15 `sbroker route check` dry-run + CI 集成 |
| SIGHUP 风暴 | 运维循环 send | 主循环只在 `g_routes_reload_requested == 1` 时跑一次（后续 SIGHUP 合并） |
| pthread rwlock 写者饥饿 | 高并发 reader | pthread 默认 PREFER_WRITER, 已防御 |
| target_broker_sockaddr 与 reload 错峰 | reload 时 candidate 已分发 | candidate 是 deep copy, 不依赖 g_routes 节点生命周期 |

回滚（M16 整体回滚）：

> M16 是 v2.0 的核心新增模块，回滚意味着退回 v1.5 的 ctld-side routing。

1. `git revert` 所有 M16 commit（routes_loader / route / cap_check / Makefile / broker_main / sig_handlers）
2. broker.conf 设 `RouteSource=static_legacy`（M02 v2.0 仍兼容此分支）
3. M06 v2.0 handler_ctld 走 STATIC_LEGACY 分支（M06 v2.0 已设计为兼容两种模式）
4. M09 v2.0 state_machine `_on_init_deciding` 走 STATIC_LEGACY 分支（直接填 1 个候选 = 旧字段）
5. M08 v2.0 egress 走 `_send_to_peer_v1` 分支
6. 单元测试 `test_routes_loader / test_route_decide / test_cap_check` 跳过

整模块回滚后，所有跨 M06/M08/M09 的多 peer 路由逻辑自动降级为 v1.5 单 peer 行为；现网作业不丢失。
