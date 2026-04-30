# M12 脚本改写 (rewrite) Checklist

> 配套: [doc/Broker开发任务清单.md](../Broker开发任务清单.md) §M12
> 设计: [doc/Broker详细设计文档MVP.md](../Broker详细设计文档MVP.md) §8.2.2
> Sprint: S3
> 依赖: M11-T2（lookup_software_path）
> 下游: M07-T2 `handle_broker_staged_in` 调用前

---

## 1. 模块概述与目标

### 1.1 一句话定位

RECEIVER 端在 STAGED_IN → SUBMITTED 之前调用，修改用户原始 sbatch 脚本：替换 partition、删除特殊 SBATCH 行、做软件路径前缀替换、清空 account。这是跨域作业能在远端正确运行的关键改写步骤。

### 1.2 MVP 范围

- 一个主入口 `rewrite_job_script(job, &out_modified_path)`
- 行级处理：drop / replace / prefix-substitute
- 失败回滚（删半成品 `.cd_modified.sh`）
- **关键副作用**：`xfree(job->job_desc->account); = NULL;`

### 1.3 不在 MVP 范围

- ~~多轮迭代改写（多次 lookup 不同应用）~~：MVP 单次 lookup
- ~~复杂正则匹配 / DSL 化~~：硬编码规则即可

### 1.4 与设计文档差异

设计文档 §8.2.2 给完整规则；本文档保持一致。

---

## 2. 接口契约

### 2.1 公共 API

```c
/* src/slurmbrokerd/rewrite.h */

/*
 * 在 RECEIVER 端 dst_work_dir 内对 job 脚本做改写，写入
 * <basename>.cd_modified.sh，并把 job->job_desc->script 切到改写后版本。
 *
 * 同时执行：
 *   xfree(job->job_desc->account);
 *   job->job_desc->account = NULL;
 *
 * 成功时 *out_modified_path = xstrdup(<modified script path>)，调用方 xfree。
 *
 * 返回 SLURM_SUCCESS / ESLURM_BROKER_LOOKUP_FAILED / SLURM_ERROR。
 */
extern int rewrite_job_script(broker_job_t *job, char **out_modified_path);
```

### 2.2 改写规则（`#SBATCH` 行）

| 原始行 | 处理 |
|---|---|
| `#SBATCH --reservation=<X>` | drop |
| `#SBATCH --cross-domain` | drop |
| `#SBATCH --app=<X>` | drop |
| `#SBATCH --account=<X>` / `#SBATCH -A <X>` | drop |
| `#SBATCH -p <X>` / `#SBATCH --partition=<X>` | 替换为 `target_partition` |
| 其它 SBATCH 行 | 保留 |

### 2.3 改写规则（脚本正文）

- **前缀替换**：把 `src_software_path` 的所有出现替换为 `dst_software_path`（来自 lookup_software 双向查询）
- 例：`source /xian/opt/apps/gromacs-2024.1/setup.sh` → `source /wz/opt/apps/gromacs-2024.1/setup.sh`
- 实现：strstr 多次替换；字符串足够短（< 1MB），O(N) OK

### 2.4 输出

- 文件：`<dst_work_dir>/<basename(orig_script)>.cd_modified.sh`，`mode 0700`
- 替换 job_desc：`xfree(job_desc->script); job_desc->script = xstrdup_printf(...)`（具体看 `job_desc->script` 是路径还是内联文本，需要在落地时确认 Slurm `submit_batch_job` 的语义）

---

## 3. 参考代码

| 用途 | 文件 | 说明 |
|---|---|---|
| `xstrcatchar` / `xstrfmtcat` | [src/common/xstring.h](../../src/common/xstring.h) | 拼字符串 |
| `regex.h` POSIX | 系统 | 正则匹配 SBATCH 行 |
| `strstr` 多次替换 | 标准 C | 前缀替换 |
| `mkstemp` 安全临时文件 | 标准 C | 不需要：直接写到 cd_modified.sh |
| `job_desc_msg_t.script` | [slurm/slurm.h](../../slurm/slurm.h) | grep `script;` 字段说明 |

---

## 4. 文件清单

| 文件 | 类型 | 用途 |
|---|---|---|
| [src/slurmbrokerd/rewrite.h](../../src/slurmbrokerd/rewrite.h) | 新增 | API |
| [src/slurmbrokerd/rewrite.c](../../src/slurmbrokerd/rewrite.c) | 新增 | rewrite 主流程 + 行处理 |
| [src/slurmbrokerd/Makefile.am](../../src/slurmbrokerd/Makefile.am) | 修改 | 加 rewrite.c |

---

## 5. 数据流

```mermaid
sequenceDiagram
    participant h as M07-T2 staged_in
    participant rw as rewrite_job_script
    participant lk as M11 lookup_software
    participant fs as filesystem

    h->>rw: rewrite_job_script(job, &out)
    rw->>lk: lookup_software_path(src_cluster, app)
    lk-->>rw: src_software_path
    rw->>lk: lookup_software_path(dst_cluster, app)
    lk-->>rw: dst_software_path
    rw->>fs: read <dst_work_dir>/<basename>
    rw->>rw: per-line drop/replace
    rw->>rw: full-text strstr substitute
    rw->>fs: write <dst_work_dir>/<basename>.cd_modified.sh (mode 0700)
    rw->>rw: xfree(job_desc->account); = NULL
    rw->>rw: xfree(job_desc->script); = new path
    rw-->>h: SLURM_SUCCESS, *out = path
```

---

## 6. 任务展开

### M12-T1 `rewrite_job_script` 框架

- **依赖**: M11-T2
- **预估**: 0.5d
- **关键决策**:
  1. 双向 lookup_software_path：源端集群和目标集群都查
  2. 任一失败 → 整体返回 LOOKUP_FAILED 不重试（lookup 内部已重试）
  3. job->job_desc 必须非 NULL
- **代码草图**:

```c
int rewrite_job_script(broker_job_t *job, char **out_modified_path)
{
	char *src_path = NULL, *dst_path = NULL;
	char *app_name = NULL;
	int rc;

	*out_modified_path = NULL;

	/* MVP: app_name 暂时从 job->job_desc->name 取（用户提交时 -J <app>）
	 * 未来在 M07-T1 把 app_name 透传过来，更可靠 */
	app_name = job->job_desc ? job->job_desc->name : NULL;
	if (!app_name) {
		error("rewrite: trace_id=%s app_name missing", job->trace_id);
		return ESLURM_BROKER_LOOKUP_FAILED;
	}

	rc = lookup_software_path(job->src_cluster, app_name, &src_path);
	if (rc != SLURM_SUCCESS) goto fail;

	rc = lookup_software_path(job->dst_cluster, app_name, &dst_path);
	if (rc != SLURM_SUCCESS) goto fail;

	rc = _do_rewrite(job, src_path, dst_path, out_modified_path);

fail:
	xfree(src_path);
	xfree(dst_path);
	return rc;
}
```

- **DoD**:
  - [ ] 单测两端路径都能拿到，结构正确

### M12-T2 行级替换 + drop 规则 + 全文前缀替换

- **依赖**: M12-T1
- **预估**: 1.5d
- **关键决策**:
  1. 读 `<dst_work_dir>/<basename(orig)>` 全文
  2. 逐行处理 SBATCH 头
  3. 全文 strstr 多次替换 `src_path` → `dst_path`
  4. 写到 `<dst_work_dir>/<basename>.cd_modified.sh`
  5. **`xfree(job->job_desc->account); = NULL`**（MVP 关键步骤）
- **代码草图**:

```c
static bool _line_is_drop(const char *line)
{
	/* 简化的正则匹配 - 实际可用 regex.h 编译一次复用 */
	if (strstr(line, "#SBATCH") != line && line[0] != '#')
		return false;

	const char *p = line + strspn(line, "# \t");
	if (strncmp(p, "SBATCH", 6)) return false;
	p += 6;
	p += strspn(p, " \t");

	if (strstr(p, "--reservation"))   return true;
	if (strstr(p, "--cross-domain"))  return true;
	if (strstr(p, "--app"))           return true;
	if (strstr(p, "--account"))       return true;
	if (strstr(p, "-A "))             return true;
	if (strstr(p, "-A\t"))            return true;
	return false;
}

static char *_line_replace_partition(const char *line, const char *new_part)
{
	/* 处理 #SBATCH -p X / --partition=X */
	const char *p = strstr(line, "--partition=");
	if (p) {
		size_t prefix_len = (p - line) + strlen("--partition=");
		const char *q = p + strlen("--partition=");
		while (*q && *q != ' ' && *q != '\t' && *q != '\n') q++;
		char *out = NULL;
		xstrfmtcat(out, "%.*s%s%s",
		           (int) prefix_len, line, new_part, q);
		return out;
	}
	p = strstr(line, "-p ");
	if (p) {
		const char *q = p + 3;
		while (*q && *q != ' ' && *q != '\t' && *q != '\n') q++;
		char *out = NULL;
		xstrfmtcat(out, "%.*s%s%s",
		           (int) (p - line + 3), line, new_part, q);
		return out;
	}
	return NULL;
}

static char *_substitute_all(const char *src, const char *find,
                             const char *replace)
{
	if (!find || !*find || !strstr(src, find))
		return xstrdup(src);

	char *out = NULL;
	const char *cur = src;
	const char *p;
	size_t flen = strlen(find);

	while ((p = strstr(cur, find))) {
		xstrncat(out, cur, p - cur);
		xstrcat(out, replace);
		cur = p + flen;
	}
	xstrcat(out, cur);
	return out;
}

static int _do_rewrite(broker_job_t *job, const char *src_path,
                       const char *dst_path, char **out)
{
	char *orig_path = NULL;
	xstrfmtcat(orig_path, "%s/%s", job->dst_work_dir,
	           xbasename(job->script_path));

	/* read */
	FILE *fp = fopen(orig_path, "r");
	if (!fp) {
		error("rewrite: open %s: %m", orig_path);
		xfree(orig_path);
		return SLURM_ERROR;
	}

	char *full = NULL, *transformed = NULL;
	char line[8192];
	while (fgets(line, sizeof(line), fp)) {
		if (_line_is_drop(line)) continue;
		char *replaced = _line_replace_partition(line,
		                                         job->target_partition);
		xstrcat(full, replaced ? replaced : line);
		xfree(replaced);
	}
	fclose(fp);

	transformed = _substitute_all(full, src_path, dst_path);
	xfree(full);

	/* write */
	char *modified_path = NULL;
	xstrfmtcat(modified_path, "%s.cd_modified.sh", orig_path);
	int fd = open(modified_path, O_WRONLY|O_CREAT|O_TRUNC, 0700);
	if (fd < 0 ||
	    write(fd, transformed, strlen(transformed)) < 0) {
		error("rewrite: write %s: %m", modified_path);
		if (fd >= 0) close(fd);
		unlink(modified_path);
		xfree(modified_path); xfree(orig_path); xfree(transformed);
		return SLURM_ERROR;
	}
	close(fd);

	/* 清空 account（关键！） */
	xfree(job->job_desc->account);
	job->job_desc->account = NULL;

	/* 切换 job_desc->script 到新路径
	 * 实际语义视 Slurm submit API 而定：
	 *   - 若 script 字段是 inline 文本 → 用 transformed 内容
	 *   - 若 script 字段是 path → 用 modified_path
	 * 这里假设 path 模式（Slurm sbatch 默认）
	 */
	xfree(job->job_desc->script);
	job->job_desc->script = xstrdup(modified_path);

	*out = modified_path;
	xfree(orig_path);
	xfree(transformed);
	return SLURM_SUCCESS;
}
```

- **风险与坑**:
  - `job_desc->script` 在 Slurm API 中常常是 **inline 文本**（sbatch 把脚本读完一次塞进 RPC）；如果是 inline，则要把 transformed 内容直接赋给 `script` 字段，落地时需 verify
  - SBATCH 解析极其简化（`strstr` 而非真正 lex），可能误判 `#SBATCH --comment="--account in name"`；MVP 接受此局限，文档警告
  - 中文文件名/路径：`xbasename` 在 `xstring.h`，应能处理
- **DoD**:
  - [ ] 给 1 份示例脚本（含 -p / --reservation / --account / source xxx/env.sh），输出脚本 partition 已换 / reservation 已删 / account 已删 / source 路径已替换
  - [ ] `job->job_desc->account == NULL` 在 rewrite 后

### M12-T3 错误处理与回滚

- **依赖**: M12-T2
- **预估**: 0.25d
- **关键决策**:
  1. 任一步失败：log + 返回错误 + 删半成品 modified.sh
  2. 调用方 (M07-T2) 收到非 0 → state FAILED reason="rewrite failed: ..."
- **代码草图**:

主流程 fail 路径已在 T2 草图中体现 unlink + xfree。

- **DoD**:
  - [ ] 脚本不存在 / 没写权限 → SLURM_ERROR
  - [ ] lookup 失败 → ESLURM_BROKER_LOOKUP_FAILED
  - [ ] 半成品文件不残留

---

## 7. 整体 DoD（汇总）

- [ ] 3 子任务全部勾选
- [ ] 端到端：mock 远端 broker rewrite + sbatch，远端 squeue 看到 partition 正确、account 自动选 default
- [ ] valgrind clean
- [ ] 异常注入：脚本带二进制内容 / 非 UTF-8 → 不 crash

## 8. 验证脚本

```bash
# 准备测试脚本
cat > /tmp/orig.sh <<'EOF'
#!/bin/bash
#SBATCH --job-name=gromacs
#SBATCH --partition=xianhcnormal
#SBATCH --reservation=mybooking
#SBATCH --cross-domain
#SBATCH --app=gromacs
#SBATCH --account=team_a
#SBATCH -N 4

source /xian/opt/apps/gromacs-2024.1/setup.sh
gmx mdrun -deffnm prod
EOF

# mock 测试
./tests/broker/test_rewrite \
    --orig /tmp/orig.sh \
    --src-cluster xian_cluster --dst-cluster wz_cluster \
    --target-partition wzhcnormal \
    --src-path /xian/opt/apps/gromacs-2024.1 \
    --dst-path /wz/opt/apps/gromacs-2024.1

# 期望输出 /tmp/orig.sh.cd_modified.sh:
#   - #SBATCH --partition=wzhcnormal
#   - 没有 --reservation / --cross-domain / --app / --account
#   - source /wz/opt/apps/gromacs-2024.1/setup.sh
```

---

## 9. 风险与回滚

| 风险 | 触发 | 缓解 |
|---|---|---|
| `--comment="--account ..."` 误删 | 用户脚本中带 `--account` 子串 | T2 加更精确正则；MVP 文档警告 |
| `job_desc->script` 语义不匹配 | inline vs path | 落地时用 mock submit verify |
| 全文 strstr 性能 O(N*M) | 1MB 脚本 + 多次 src_path | MVP 接受；后续可换 KMP |
| 脚本含 CRLF | Windows 用户提交 | T2 normalize line endings（可选）|

回滚：本模块独立。`git revert rewrite.c/.h + handler_remote 调用`。
