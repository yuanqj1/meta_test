# M03 数据结构与持久化 Checklist (broker · v2.0)

> 配套: [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §4 / §11.6.1
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §2.4
> Sprint: S1
> 依赖: M02-T1（broker_conf_t）
> 下游: M06 / M07 / M08 / M09 / M13 / M16 全部读写 `g_broker_jobs`
> **跨模块单一源头**: 本文档定义 `broker_job_state_t` / `broker_role_t` / **★ v2.0 `broker_init_phase_t`** 枚举，**其它文档只引用不重复**。

> **v1.5 → v2.0 增量**:
> 1. 新增 `broker_init_phase_t` 枚举（DECIDING / PROBING / SELECTED / EXHAUSTED）
> 2. `broker_job_t` 新增 6 字段：`src_partition` / `selected_route_id` / `init_phase` / `route_candidates[]` / `route_attempted_mask` / `route_current_idx`
> 3. `broker_job_t` 删除 `account` 字段（forward_job_msg_t 已不传 account）
> 4. JSON schema 末尾追加上述新字段（向前兼容；v1.5 jsonl 缺这些字段时 fallback 到 SELECTED）
> 5. `state_machine_resume_inflight()` 新增 PROBING 续探测逻辑

---

## 1. 模块概述与目标

### 1.1 一句话定位

提供 `broker_job_t` 内存数据结构、O(1) 查询的 `g_broker_jobs` 全局表、JSONL 三文件原子写 checkpoint、启动恢复、★ v2.0 PROBING 续探测。

### 1.2 v2.0 MVP 范围

- `broker_job_t` 严格按 [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §4.1。
- 全局表 = `xhash_t` (key=`trace_id`) + `list_t` (有序遍历) + 全局锁。
- 持久化采用 JSONL：每行一个 job，**v2.0 删除 base64 嵌入的 `job_desc` 字段**（v1.5 `app_name + script_path` 平铺已生效，v2.0 沿用）。
- 三文件原子 rename：`<state_file>` / `.tmp` / `.old`。
- 30s 周期 checkpoint + `persist_async_request()` 立即 flush。
- ★ v2.0 `state_machine_resume_inflight()`：恢复 INIT/PROBING 作业从 `route_attempted_mask` 续探测下一个候选。

### 1.3 不在 MVP 范围

- ~~增量 WAL~~（每次全量 dump 即可）
- ~~多版本 schema migration~~（基础字段只增不改；v1.5→v2.0 走"缺字段 fallback"机制）
- ~~崩溃后自动 fsck~~（行级 try/skip-with-warn 已足够）

### 1.4 与 v1.5 的差异（关键变更）

| 维度 | v1.5 | v2.0 |
|---|---|---|
| INIT 阶段语义 | 单步：等 `_on_init` 调 `egress_forward_async` | 4 子状态：DECIDING → PROBING → SELECTED → EXHAUSTED |
| 路由结果字段 | `dst_cluster` / `target_partition` 由 ctld 在 forward_job 内传入 | `dst_cluster` / `target_partition` 由 broker 路由决策结果填，新增 `selected_route_id` |
| `account` | 内联在 `broker_job_t.account` | **删除**（远端 sbatch 不带 --account） |
| 持久化字段 | 23 字段 | 28 字段（+5 v2.0 路由相关） |
| restore 后初始 init_phase | n/a (v1.5 INIT 单步) | 缺字段时默认 `SELECTED`（兼容 v1.5 jsonl）；存量 PROBING 走续探测 |

---

## 2. 接口契约

### 2.1 `broker_job_state_t` / `broker_role_t` 枚举（**单一源头**, 沿用 v1.5）

```c
/* src/slurmbrokerd/broker_job.h */
typedef enum {
	BROKER_STATE_INIT          = 0,
	BROKER_STATE_STAGING_IN    = 1,
	BROKER_STATE_STAGED_IN     = 2,
	BROKER_STATE_SUBMITTED     = 3,
	BROKER_STATE_RUNNING       = 4,
	BROKER_STATE_STAGING_OUT   = 5,
	BROKER_STATE_COMPLETED     = 6,
	BROKER_STATE_FAILED        = 7,
	BROKER_STATE_CANCELLED     = 8,
} broker_job_state_t;

typedef enum {
	BROKER_ROLE_ORIGINATOR = 0,
	BROKER_ROLE_RECEIVER   = 1,
} broker_role_t;
```

### 2.2 ★ v2.0 新增 `broker_init_phase_t` 枚举（**单一源头**）

```c
typedef enum {
	BROKER_INIT_PHASE_DECIDING    = 0,   /* 还没调 route_decide */
	BROKER_INIT_PHASE_PROBING     = 1,   /* 正在逐个 test-only */
	BROKER_INIT_PHASE_SELECTED    = 2,   /* 已选定 selected_route_id, 可以进 stage_in */
	BROKER_INIT_PHASE_EXHAUSTED   = 3,   /* 全部候选均失败, 转 FAILED */
} broker_init_phase_t;

#define BROKER_TRACE_ID_LEN 48
#define BROKER_MAX_ROUTE_CANDIDATES 8   /* ★ v2.0 与 broker.conf TestOnlyMaxCandidates 默认一致 */
```

> 任何其它模块（M06/M07/M08/M09/M13/M16）若引用，**仅 include `broker_job.h`**，不复制定义。

### 2.3 `broker_job_t` 结构（v2.0 字段顺序按 [Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §4.1）

字段顺序 wire-significant，**禁止重排**。变更概览：

| 区段 | v1.5 字段 | v2.0 增量 |
|---|---|---|
| identity | `trace_id / src_job_id / remote_job_id / role / hop_count` | 不变 |
| user identity | `src_user_name / src_uid / remote_user_name / remote_uid / remote_gid` | 不变 |
| cluster routing | `src_cluster / dst_cluster / target_partition` | **+ `src_partition`** ★ v2.0; `+ selected_route_id` ★ v2.0 |
| ★ v2.0 路由探测状态 | — | `+ init_phase`（broker_init_phase_t）<br>`+ route_candidates[BROKER_MAX_ROUTE_CANDIDATES]`（char\*）<br>`+ route_candidates_count`（uint8_t）<br>`+ route_attempted_mask`（uint8_t bitmask）<br>`+ route_current_idx`（uint8_t） |
| working dirs | `src_work_dir / dst_work_dir / script_path` | 不变 |
| application | `app_name`（v1.5 已用 `cd_app_name`） | 不变 |
| state machine | `state / state_reason / retry_count / state_enter_time / submit_time / last_poll_time` | 不变 |
| terminal | `remote_start_time / remote_end_time / remote_alloc_tres / remote_exit_code` | 不变 |
| cancel | `cancel_requested / cancel_propagated` | 不变 |
| lock | `pthread_mutex_t lock` | 不变 |
| **删除** | `account` | **删除** ★ v2.0 |

xfree 责任与 v1.5 一致：`broker_job_destroy` 内对每个 `char *` xfree；★ v2.0 `route_candidates[]` 数组内的 strdup 字符串需逐个 xfree（详见 §6 M03-T1）。

### 2.4 公共 API

```c
/* broker_job.h */
extern int  broker_job_table_init(void);
extern void broker_job_table_fini(void);

extern broker_job_t *broker_job_create(void);
extern void broker_job_destroy(broker_job_t *job);

extern int  broker_job_table_add(broker_job_t *job);
extern broker_job_t *broker_job_table_get(const char *trace_id);
extern int  broker_job_table_remove(const char *trace_id);
extern uint32_t broker_job_table_count(void);

extern void broker_job_table_foreach(int (*fn)(broker_job_t *, void *),
                                     void *arg);

extern char *broker_job_to_json(broker_job_t *job);
extern broker_job_t *broker_job_from_json(const char *line);

/* persist.h */
extern int  broker_state_save(void);
extern int  broker_state_restore(void);
extern void persist_async_request(void);
extern int  persist_thread_start(void);
extern void persist_thread_stop(void);

/* state_machine.h, ★ v2.0 PROBING 续探测在 M09 实现, 此处声明只为 cross-ref */
extern void state_machine_resume_inflight(void);
```

### 2.5 全局变量

```c
xhash_t        *g_broker_jobs;
list_t         *g_broker_jobs_list;
pthread_mutex_t g_broker_jobs_lock = PTHREAD_MUTEX_INITIALIZER;
```

### 2.6 文件路径（不变）

| 文件 | 用途 |
|---|---|
| `<state_save_location>/<state_file_name>` | 当前 checkpoint（默认 `/var/spool/slurm/broker/broker_state.jsonl`） |
| 同上 + `.tmp` | 写入中 |
| 同上 + `.old` | 上次 checkpoint，崩溃 fallback |

---

## 3. 参考代码

| 用途 | 文件 | 关键行 |
|---|---|---|
| `xhash_t` API | [src/common/xhash.h](../../src/common/xhash.h) | L70 / L91 / L82 / L110 |
| `list_t` / `list_iterator_t` | [src/common/list.h](../../src/common/list.h) | grep `list_create` |
| `data_g_deserialize` JSON | [src/common/data.c](../../src/common/data.c) | grep `data_g_deserialize` |
| `slurm_mutex_lock` / `unlock` | [src/common/macros.h](../../src/common/macros.h) | grep `slurm_mutex_` |
| 状态文件原子 rename 范式 | [src/slurmctld/state_save.c](../../src/slurmctld/state_save.c) | 整体可参考 |
| `pthread_cond_timedwait` 范式 | [src/slurmctld/agent.c](../../src/slurmctld/agent.c) | grep `pthread_cond_timedwait` |

---

## 4. 文件清单

| 文件 | 类型 | 用途 |
|---|---|---|
| [src/slurmbrokerd/broker_job.h](../../src/slurmbrokerd/broker_job.h) | 修改 | 新增 `broker_init_phase_t` enum + 6 字段 |
| [src/slurmbrokerd/broker_job.c](../../src/slurmbrokerd/broker_job.c) | 修改 | `broker_job_create` 初始化 `init_phase=DECIDING`；`broker_job_destroy` xfree `route_candidates[]`；`to_json` / `from_json` 增 5 字段 |
| [src/slurmbrokerd/persist.h](../../src/slurmbrokerd/persist.h) | 不变 | v1.5 已落地 |
| [src/slurmbrokerd/persist.c](../../src/slurmbrokerd/persist.c) | 不变 | v1.5 已落地 |
| [src/slurmbrokerd/state_machine.c](../../src/slurmbrokerd/state_machine.c) | 修改 | `state_machine_resume_inflight()` 新增 PROBING 续探测分支（详见 M09） |
| [src/slurmbrokerd/Makefile.am](../../src/slurmbrokerd/Makefile.am) | 不变 | M16 加 routes_loader.c 时再改 |

---

## 5. JSON schema (★ v2.0 新增字段示例)

```json
{"trace_id":"xian-12345","src_job_id":12345,"remote_job_id":8888,
 "role":0,"hop_count":0,
 "src_user_name":"test1","src_uid":20001,
 "remote_user_name":"wz_test1","remote_uid":20001,"remote_gid":20001,
 "src_cluster":"xian_cluster","src_partition":"xahcnormal",                ⭐ v2.0
 "dst_cluster":"wz_cluster","target_partition":"wzhcnormal",
 "selected_route_id":"route_xahc_to_wz",                                    ⭐ v2.0
 "init_phase":2,                                                            ⭐ v2.0
 "route_candidates":["route_xahc_to_wz","route_xahc_to_bj"],               ⭐ v2.0
 "route_attempted_mask":1,                                                  ⭐ v2.0
 "route_current_idx":0,                                                     ⭐ v2.0
 "src_work_dir":"/work/home/test1/case1",
 "dst_work_dir":"/work/home/wz_test1/.burst/xian_cluster/12345",
 "script_path":"/work/home/test1/case1/run.sh",
 "app_name":"lammps-2Aug2023-intelmpi2018",
 "state":4,"state_reason":"","retry_count":0,
 "state_enter_time":1715000000,"submit_time":1715000005,"last_poll_time":1715000060,
 "remote_start_time":1715000010,"remote_end_time":0,
 "remote_alloc_tres":"cpu=32,mem=128G,node=1","remote_exit_code":0,
 "cancel_requested":false,"cancel_propagated":false}
```

> **v1.5 → v2.0 兼容性**：v1.5 jsonl 缺少 6 个新字段；`broker_job_from_json()` 给默认值（空数组 / 0 / NULL），并在末尾把 `init_phase` 设为 `SELECTED`（v1.5 假设已通过 ctld 提前决策路由）。

---

## 6. 任务展开

### M03-T1 `broker_job_t` v2.0 字段扩展

- **依赖**: M02-T1
- **预估**: 0.5d
- **关键决策**:
  1. 字段顺序与设计文档 §4.1 完全一致，**禁止重排**（影响 JSON schema 字段顺序）。
  2. `route_candidates[]` 是定长数组（避免 v1.5 → v2.0 ABI 变化时动态分配的额外考量）。
  3. `BROKER_MAX_ROUTE_CANDIDATES = 8`；与 `broker.conf::TestOnlyMaxCandidates` 上限一致。
  4. **删除** `account` 字段；同时同步删除 `broker_job_destroy` 中对 `account` 的 xfree。
  5. `init_phase` 默认值 `DECIDING`；ORIGINATOR 入表后由 M09 推进，RECEIVER 入表后保持 `SELECTED`（v2.0 receiver 没有路由探测）。
- **代码草图**（差异部分）:

```c
broker_job_t *broker_job_create(void)
{
	broker_job_t *job = xmalloc(sizeof(*job));
	slurm_mutex_init(&job->lock);
	job->state = BROKER_STATE_INIT;
	job->state_enter_time = time(NULL);
	job->role = BROKER_ROLE_ORIGINATOR;
	job->init_phase = BROKER_INIT_PHASE_DECIDING;        /* ★ v2.0 */
	job->route_candidates_count = 0;
	job->route_attempted_mask = 0;
	job->route_current_idx = 0;
	memset(job->route_candidates, 0,
	       sizeof(job->route_candidates));               /* ★ v2.0 */
	return job;
}

void broker_job_destroy(broker_job_t *job)
{
	if (!job) return;

	xfree(job->src_user_name);
	xfree(job->remote_user_name);
	xfree(job->src_cluster);
	xfree(job->src_partition);                            /* ★ v2.0 */
	xfree(job->dst_cluster);
	xfree(job->target_partition);
	xfree(job->selected_route_id);                        /* ★ v2.0 */
	xfree(job->src_work_dir);
	xfree(job->dst_work_dir);
	xfree(job->script_path);
	xfree(job->app_name);
	xfree(job->state_reason);
	xfree(job->remote_alloc_tres);

	for (uint8_t i = 0; i < job->route_candidates_count; i++)
		xfree(job->route_candidates[i]);                  /* ★ v2.0 */

	/* v1.5 删除: xfree(job->account); */

	slurm_mutex_destroy(&job->lock);
	xfree(job);
}
```

- **风险与坑**:
  - 漏掉某个 `char *` 字段不 xfree → valgrind still reachable
  - `route_candidates[]` 数组未先 xfree 内部字符串再 destroy → 泄漏
- **DoD**:
  - [ ] valgrind: 1000 次 destroy(create()) 0 still reachable
  - [ ] `sizeof(broker_job_t)` < 768B（v2.0 增 6 字段后保持紧凑）
  - [ ] grep `account` in broker_job.c → 仅出现在注释（"v1.5 已删"）

### M03-T2 全局表初始化与 CRUD（不变）

- **依赖**: M03-T1
- **预估**: 0d（v1.5 已落地）
- **DoD**:
  - [ ] 100 次 add/get/remove 仍正常
  - [ ] valgrind clean

### M03-T3 `broker_job_to_json` v2.0 新增 5 字段

- **依赖**: M03-T1
- **预估**: 0.5d
- **关键决策**:
  1. 复用 v1.5 `xstrfmtcat` + `_json_escape` helper，按 §5 schema 顺序新增 5 个字段。
  2. `route_candidates[]` 用 JSON 数组语法 `["a","b",...]`；空数组写 `[]`。
  3. `route_attempted_mask` 用十进制整数即可（uint8_t 最大 255）。
  4. **不再** 序列化 `account`（v2.0 已删字段）。
- **代码草图**（差异部分）:

```c
char *broker_job_to_json(broker_job_t *job)
{
	char *out = NULL;
	char *esc;

	xstrcat(out, "{");
	/* === v1.5 字段 (略) === */

	/* === ★ v2.0 新增字段 === */
	esc = _json_escape(job->src_partition);
	xstrfmtcat(out, "\"src_partition\":%s,", esc); xfree(esc);

	esc = _json_escape(job->selected_route_id);
	xstrfmtcat(out, "\"selected_route_id\":%s,", esc); xfree(esc);

	xstrfmtcat(out, "\"init_phase\":%d,", job->init_phase);

	xstrcat(out, "\"route_candidates\":[");
	for (uint8_t i = 0; i < job->route_candidates_count; i++) {
		esc = _json_escape(job->route_candidates[i]);
		xstrfmtcat(out, "%s%s", (i == 0) ? "" : ",", esc);
		xfree(esc);
	}
	xstrcat(out, "],");

	xstrfmtcat(out, "\"route_attempted_mask\":%u,",
	           (unsigned) job->route_attempted_mask);
	xstrfmtcat(out, "\"route_current_idx\":%u,",
	           (unsigned) job->route_current_idx);

	/* === 终态字段 (略, 沿用 v1.5) === */

	xstrcat(out, "}");
	return out;
}
```

- **DoD**:
  - [ ] 含中文 / `"` / `\\` 的 reason 序列化后 `python3 -c 'import json,sys; json.loads(sys.stdin.read())'` 能解析
  - [ ] `route_candidates_count = 0` 时输出 `[]` 不破语法
  - [ ] `route_candidates_count = 8` 时全部正确序列化

### M03-T4 `broker_job_from_json` v2.0 字段恢复 + v1.5 兼容

- **依赖**: M03-T3
- **预估**: 1d
- **关键决策**:
  1. 优先用 Slurm `data_t` + `data_g_deserialize("json", ...)`。
  2. **★ v2.0 新增字段缺失时的兼容**：
     - `src_partition` / `selected_route_id` 缺失 → NULL（v1.5 jsonl 没有，OK）
     - `init_phase` 缺失 → `SELECTED`（v1.5 默认走 stage_in，等价于 v2.0 SELECTED）
     - `route_candidates` 缺失或空数组 → `count = 0`
     - `route_attempted_mask` / `route_current_idx` 缺失 → 0
  3. **必填字段缺失** → 返回 NULL，调用方 warn 跳过：`trace_id` / `src_job_id` / `state`
- **代码草图**（差异部分）:

```c
broker_job_t *broker_job_from_json(const char *line)
{
	data_t *root = NULL;
	broker_job_t *job = NULL;

	if (data_g_deserialize(&root, MIME_TYPE_JSON, line, strlen(line))) {
		warning("broker_job_from_json: invalid JSON, skipping line");
		return NULL;
	}

	job = broker_job_create();   /* init_phase 默认 DECIDING */

	/* === v1.5 字段反序列化 (略) === */

	/* === ★ v2.0 新增字段, 缺失走 fallback === */
	GET_STR("src_partition",      job->src_partition);
	GET_STR("selected_route_id",  job->selected_route_id);

	int64_t v;
	data_t *d = data_key_get(root, "init_phase");
	if (d && !data_get_int_converted(d, &v))
		job->init_phase = (broker_init_phase_t) v;
	else
		job->init_phase = BROKER_INIT_PHASE_SELECTED;   /* v1.5 fallback */

	GET_INT("route_attempted_mask", job->route_attempted_mask);
	GET_INT("route_current_idx",    job->route_current_idx);

	d = data_key_get(root, "route_candidates");
	if (d && data_get_type(d) == DATA_TYPE_LIST) {
		uint32_t n = data_get_list_length(d);
		if (n > BROKER_MAX_ROUTE_CANDIDATES) n = BROKER_MAX_ROUTE_CANDIDATES;
		for (uint32_t i = 0; i < n; i++) {
			data_t *e = data_list_get_index(d, i);
			if (e && data_get_type(e) == DATA_TYPE_STRING)
				job->route_candidates[i] = xstrdup(data_get_string(e));
		}
		job->route_candidates_count = n;
	}

	FREE_NULL_DATA(root);
	return job;
}
```

- **风险与坑**:
  - `data_g_deserialize` 需 serializer/json plugin 已 init（M01 主进程内 slurm 库已 init plugin rack）。
  - `init_phase` 字段缺失 fallback 到 SELECTED；如果实际是 v1.5 PROBING（不会发生，因 v1.5 无此概念）则可能跳过续探测。
- **DoD**:
  - [ ] save 100 jobs（含 PROBING 状态）→ restart → restore 后字段一致
  - [ ] 旧 v1.5 jsonl 启动 v2.0 broker → init_phase 自动 = SELECTED, broker 行为与 v1.5 等价
  - [ ] 故意篡改某行 base64 → restore 时 warn 并跳过该行

### M03-T5 `broker_state_save` 原子写（不变）

- **依赖**: M03-T3
- **预估**: 0d（v1.5 已落地）
- **DoD**:
  - [ ] kill -9 后 .old fallback 仍可用

### M03-T6 `broker_state_restore` 启动加载（不变）

- **依赖**: M03-T4 / M03-T5
- **预估**: 0d（v1.5 已落地）
- **DoD**:
  - [ ] 删 current 留 old → 启动看到 fallback 日志

### M03-T7 30s checkpoint 后台线程（不变）

- **依赖**: M03-T5
- **预估**: 0d（v1.5 已落地）

### M03-T8 ★ v2.0 `state_machine_resume_inflight()` 新增 PROBING 续探测

- **依赖**: M03-T4, M09-T7（PROBING 子状态实现）
- **预估**: 0.5d
- **关键决策**:
  1. resume 时，对每条 `state == INIT && init_phase == PROBING` 的作业：
     - 若 `route_attempted_mask` 已覆盖所有 `route_candidates_count` → 直接转 EXHAUSTED；
     - 否则把 `route_current_idx` 推到 `(current_idx + 1) % count` 中第一个未 mask 的位置；
     - `state_enter_time = time(NULL) - (test_only_timeout_sec - 1)`，让下一次 tick 立刻继续探测。
  2. 对 `INIT && init_phase == SELECTED` 的作业（broker 重启时尚未发 8010）：触发 `_on_init_selected`（M09 阶段做）。
  3. STAGING_*/STAGED_IN 时间戳拨快（v1.5 已实现，沿用）。
- **代码草图**（在 M09 内）:

```c
static int _resume_one(broker_job_t *job, void *arg)
{
	if (job->state != BROKER_STATE_INIT) {
		/* v1.5 旧分支 (STAGING_IN / STAGED_IN / STAGING_OUT 拨快) */
		return 0;
	}

	switch (job->init_phase) {
	case BROKER_INIT_PHASE_DECIDING:
		/* 重新走 DECIDING (route_decide 是无副作用的) */
		break;

	case BROKER_INIT_PHASE_PROBING:
		/* ★ v2.0 续探测 */
		if (job->route_attempted_mask >=
		    (1 << job->route_candidates_count) - 1) {
			job->init_phase = BROKER_INIT_PHASE_EXHAUSTED;
			info("resume: trace_id=%s all candidates already "
			     "attempted, → EXHAUSTED", job->trace_id);
		} else {
			/* 找到下一个未 mask 的 idx */
			for (uint8_t i = 0; i < job->route_candidates_count; i++) {
				uint8_t idx = (job->route_current_idx + 1 + i)
				              % job->route_candidates_count;
				if (!(job->route_attempted_mask & (1 << idx))) {
					job->route_current_idx = idx;
					break;
				}
			}
			info("resume: trace_id=%s PROBING resumed at idx=%u",
			     job->trace_id, job->route_current_idx);
		}
		break;

	case BROKER_INIT_PHASE_SELECTED:
	case BROKER_INIT_PHASE_EXHAUSTED:
		/* 让下一次 tick 自然推进 */
		break;
	}
	job->state_enter_time = time(NULL) - 1;   /* 立即触发 tick */
	return 0;
}
```

- **风险与坑**:
  - 若 broker 重启时 `routes.conf` 已变化，部分 `route_candidates[]` 中的 id 已不存在 → M16 `route_decide_by_id()` 返回 NULL → 该 candidate 视为失败，mask 置位继续。
  - mask 用 uint8_t 限定 `count <= 8`；若 v3 引入更多候选要换 uint16_t/uint32_t。
- **DoD**:
  - [ ] kill -9 broker 在 PROBING 第 3 个候选 → 重启后从第 4 个续起
  - [ ] route_candidates 全部 mask 后重启 → 直接转 EXHAUSTED

---

## 7. 整体 DoD（汇总）

- [ ] 8 个子任务全部勾选
- [ ] valgrind: 启动 → restore 100 jobs → save → fini，0 byte still reachable
- [ ] 多线程压测：500 jobs 并发 add/get/remove + 后台 30s checkpoint，无死锁
- [ ] **★ v2.0**: kill -9 PROBING 阶段 → 重启续探测 idx 正确
- [ ] **★ v2.0**: v1.5 jsonl 启动 v2.0 broker → init_phase 默认 SELECTED, behavior 等价
- [ ] kill -9 后 .old fallback 仍生效
- [ ] `broker_init` 已 wire-up `broker_state_restore` + `state_machine_resume_inflight`

## 8. 验证脚本

```bash
# === v2.0 序列化/反序列化 round-trip ===
gcc -I$(top_srcdir) -o /tmp/test_jsonrt_v2 \
    src/slurmbrokerd/broker_job.c \
    tests/broker/test_json_roundtrip_v2.c \
    -lslurm
/tmp/test_jsonrt_v2

# 期望: 100 jobs 含 PROBING / SELECTED / EXHAUSTED 各种 init_phase, round-trip 全 pass

# === v1.5 jsonl 启动 v2.0 broker ===
sudo cp tests/broker/data/broker_state.jsonl.v1_5_sample \
        /var/spool/slurm/broker/broker_state.jsonl
sudo ./src/slurmbrokerd/slurmbrokerd -D -v 2>&1 | grep -E "(restore|init_phase)"
# 期望:
#   restore: ... loaded=N skipped=0
#   resume: trace_id=... PROBING resumed at idx=...  (★ v2.0 新分支)
#   或    : (无该日志, 因 v1.5 jsonl 全部回退到 SELECTED)

# === kill -9 在 PROBING 阶段 ===
./tests/broker/sm_kill_at_probing.sh xian-200
# 期望: 重启后 grep "PROBING resumed at idx="
```

---

## 9. 风险与回滚

| 风险 | 触发 | 缓解 |
|---|---|---|
| `route_candidates_count > 8` 越界 | routes.conf 配 9+ 候选规则匹配同 src | M02 校验 `TestOnlyMaxCandidates ≤ 32`；M16 路由匹配截断到 8 |
| 单个 `broker_job_t` 内存膨胀 | 8 个 candidate 每个 128B = 1KB | 整体 < 768B + 1KB ≈ 1.7KB；500 jobs ≈ 850KB，OK |
| v1.5 jsonl `init_phase` fallback 错误 | broker.conf 切到 v2.0 但 jsonl 仍含 v1.5 INIT 作业 | fallback SELECTED 等价于"已决策"，与 v1.5 行为一致；除非 v1.5 INIT 卡在 forward_async 长时间未完成才有误差 |
| PROBING 续探测无限循环 | mask 全 0 但 count = 0 | `_resume_one` 先检查 `count > 0` |
| `data_g_deserialize` 未 init | broker_init 顺序错乱 | M01 已固化 `slurm_init(NULL) → serializer_g_init(JSON) → broker_state_restore` |

回滚：本模块独立。未上线 → `git revert broker_job.h/.c + state_machine.c::_resume_one`。已上线 → 停 systemd，`mv broker_state.jsonl{,.bak}` 后回滚二进制（v1.5 broker 启动会忽略 v2.0 字段，但 PROBING 状态会被当 SELECTED → 可能重复转发，运维需 manual scancel）。
