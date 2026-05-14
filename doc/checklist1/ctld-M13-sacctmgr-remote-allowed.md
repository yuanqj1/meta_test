# ctld-M13 sacctmgr remote_allowed 子命令 + DBD user/assoc 列 Checklist (★ v2.0 新增)

> 配套: [doc/Slurmctld跨域详细设计文档MVP_v2.md](../Slurmctld跨域详细设计文档MVP_v2.md) §5.8 / §9.6
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §1.6 / §1.10
> 依赖: ctld-M03（slurmdb_user_rec_t / slurmdb_assoc_rec_t 字段挂载）/ ctld-M04（cd_user_allows_remote 读 assoc.remote_allowed）
> 下游: ctld-M11（端到端验证）

> **v1.5 → v2.0 关键变化**:
> v1.5 用 `user.comment LIKE '%allow_remote%'` 做模糊匹配做用户授权。v2.0 改为 **`user.remote_allowed` / `assoc.remote_allowed`** 显式布尔列：
> 1. 命令行：`sacctmgr modify user/assoc set remote_allowed=true|false` 透传到 SlurmDBD
> 2. SlurmDBD：`user_table` / `<cluster>_assoc_table` 加 `tinyint unsigned default 0` 列
> 3. 协议：`dbd_user_msg_t` / `dbd_assoc_msg_t` pack/unpack 加 `remote_allowed`
> 4. assoc_mgr 内存视图同步该字段，ctld 跨域线程通过 `assoc_ptr->remote_allowed` 读取
> 5. v1.5 → v2.0 数据迁移 SQL 脚本（详见 §6）

---

## 1. 模块目标

### 1.1 sacctmgr 命令行（v2 设计 §5.8.1）

```bash
# 新建用户时同时启用跨域权限
sacctmgr add user name=test1 account=public_acct remote_allowed=true

# 修改已存在用户的跨域权限
sacctmgr modify user test1 set remote_allowed=true
sacctmgr modify user test1 set remote_allowed=false

# 修改 assoc 维度 (即用户在某个 account 下是否允许跨域, 比 user 粒度更细)
sacctmgr modify user test1 account=hpc_acct set remote_allowed=true

# 查询
sacctmgr show user test1 format=user,account,remote_allowed
sacctmgr show assoc where user=test1 format=cluster,account,user,remote_allowed
```

### 1.2 ACL 生效优先级（ctld 跨域线程消费）

```
if (assoc.remote_allowed 显式设置过)  → 用 assoc.remote_allowed
else if (user.remote_allowed)         → 用 user.remote_allowed
else                                   → 视为 0 (默认拒绝)
```

实现见 ctld-M04 §4.6 `cd_user_allows_remote()`。

## 2. 接口契约

### 2.1 SlurmDBD schema (v2.0)

```c
/* src/plugins/accounting_storage/mysql/as_mysql_user.c
 * user_table_fields[] 末尾追加 */
{ "remote_allowed",  "tinyint unsigned default 0" },

/* src/plugins/accounting_storage/mysql/as_mysql_assoc.c
 * assoc_table_fields[] 末尾追加
 * 注意: Slurm 24.05 每个 cluster 有独立 <cluster>_assoc_table
 *       (例: xahc_assoc_table); schema 由 _check_table_columns() 统一管理 */
{ "remote_allowed",  "tinyint unsigned default 0" },
```

### 2.2 `slurmdb_user_rec_t` / `slurmdb_assoc_rec_t` 扩展

```c
typedef struct slurmdb_user_rec {
    /* ... 24.05 已有 ... */
    uint16_t remote_allowed;     /* ★ v2.0: 0=否, 1=允许 */
} slurmdb_user_rec_t;

typedef struct slurmdb_assoc_rec {
    /* ... 24.05 已有 ... */
    uint16_t remote_allowed;     /* ★ v2.0: 0=否, 1=允许; 优先级高于 user 维度 */
} slurmdb_assoc_rec_t;
```

### 2.3 sacctmgr 文件分布（v2 设计 §5.8.2）

| 文件 | 改动 |
|---|---|
| [src/sacctmgr/sacctmgr.c](../../src/sacctmgr/sacctmgr.c) `_set_cond_options()` | 增加 `--remote_allowed` 过滤选项识别 |
| [src/sacctmgr/user_functions.c](../../src/sacctmgr/user_functions.c) `_set_user_rec()` | 解析 `remote_allowed=` 关键字，写入 `slurmdb_user_rec_t::remote_allowed` |
| [src/sacctmgr/assoc_functions.c](../../src/sacctmgr/assoc_functions.c) `_set_assoc_rec()` | 解析 `remote_allowed=` 关键字，写入 `slurmdb_assoc_rec_t::remote_allowed` |
| [src/sacctmgr/print_fields.c](../../src/sacctmgr/print_fields.c) | 注册 `PRINT_REMOTE_ALLOWED` 列（uint16 → "YES"/"NO"） |

### 2.4 assoc_mgr 内存视图同步（v2 设计 §9.6.4）

```c
/* src/common/assoc_mgr.c::_post_user_list() 内部 */
new_user->remote_allowed = user->remote_allowed;

/* src/common/assoc_mgr.c::_post_assoc_list() 内部 */
new_assoc->remote_allowed = assoc->remote_allowed;
```

ctld 跨域线程通过 `job_ptr->assoc_ptr->remote_allowed` 直接 deref（持 `assoc_mgr_lock`），不走 SQL。

---

## 3. 触及文件

| 文件 | 改动 |
|---|---|
| [slurm/slurmdb.h](../../slurm/slurmdb.h) | `slurmdb_user_rec_t` / `slurmdb_assoc_rec_t` 末尾追加 `remote_allowed` 字段 |
| [src/plugins/accounting_storage/mysql/as_mysql_user.c](../../src/plugins/accounting_storage/mysql/as_mysql_user.c) | `user_table_fields[]` 末尾追加 1 列 + `as_mysql_modify_user` SET 子句 |
| [src/plugins/accounting_storage/mysql/as_mysql_assoc.c](../../src/plugins/accounting_storage/mysql/as_mysql_assoc.c) | `assoc_table_fields[]` 末尾追加 1 列 + `as_mysql_modify_assoc` SET 子句 |
| [src/common/slurmdbd_defs.h](../../src/common/slurmdbd_defs.h) | `dbd_user_msg_t` / `dbd_assoc_msg_t` 末尾追加 `remote_allowed` 字段（如果还没在 slurmdb.h 透传） |
| [src/common/slurmdbd_defs.c](../../src/common/slurmdbd_defs.c) | `pack_dbd_user_msg` / `pack_dbd_assoc_msg` 24.05 分支末尾追加 |
| [src/common/assoc_mgr.c](../../src/common/assoc_mgr.c) | `_post_user_list` / `_post_assoc_list` 同步 `remote_allowed` |
| [src/sacctmgr/sacctmgr.c](../../src/sacctmgr/sacctmgr.c) | `_set_cond_options` 加 `--remote_allowed` |
| [src/sacctmgr/user_functions.c](../../src/sacctmgr/user_functions.c) | `_set_user_rec` 解析 `remote_allowed=` |
| [src/sacctmgr/assoc_functions.c](../../src/sacctmgr/assoc_functions.c) | `_set_assoc_rec` 解析 `remote_allowed=` |
| [src/sacctmgr/print_fields.c](../../src/sacctmgr/print_fields.c) | 注册 `PRINT_REMOTE_ALLOWED` |

---

## 4. Checklist

### 4.1 数据结构

- [ ] M13-1 [slurm/slurmdb.h](../../slurm/slurmdb.h) `slurmdb_user_rec_t` 末尾追加 ifdef 块：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        uint16_t remote_allowed;
    #endif
    ```
- [ ] M13-2 同文件 `slurmdb_assoc_rec_t` 末尾同样追加 `remote_allowed` 字段（uint16_t）

### 4.2 SlurmDBD schema

- [ ] M13-3 [src/plugins/accounting_storage/mysql/as_mysql_user.c](../../src/plugins/accounting_storage/mysql/as_mysql_user.c) `user_table_fields[]` 末尾追加 ifdef 块 1 列（§2.1）
- [ ] M13-4 同文件 `as_mysql_modify_user`（或 `as_mysql_get_users`）按 NULL 跳过原则追加 SET 子句：
    ```c
    if (user->remote_allowed != NO_VAL16) {
        xstrfmtcat(query, ", remote_allowed=%u", user->remote_allowed);
    }
    ```
    注意：`remote_allowed = NO_VAL16` 表示客户端未传字段（不修改）；`= 0` 表示用户显式设为 false
- [ ] M13-5 [src/plugins/accounting_storage/mysql/as_mysql_assoc.c](../../src/plugins/accounting_storage/mysql/as_mysql_assoc.c) `assoc_table_fields[]` 末尾追加 ifdef 块 1 列；`as_mysql_modify_assoc` 同样追加 SET 子句
- [ ] M13-6 启动 SlurmDBD + ctld 注册集群后，自动 `ALTER TABLE user_table ADD COLUMN remote_allowed`；`ALTER TABLE <cluster>_assoc_table ADD COLUMN remote_allowed`；用 `DESCRIBE` 验证

### 4.3 DBD 协议序列化

- [ ] M13-7 [src/common/slurmdbd_defs.c](../../src/common/slurmdbd_defs.c) `pack_dbd_user_msg`（实际 24.05 函数名以源码为准；可能在 `slurmdb_pack.c` 中）24.05 分支末尾追加：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
    if (protocol_version >= SLURM_24_05_PROTOCOL_VERSION) {
        pack16(user->remote_allowed, buffer);
    }
    #endif
    ```
- [ ] M13-8 `unpack_dbd_user_msg` 同位置 `safe_unpack16`；旧协议回退 `remote_allowed = 0`
- [ ] M13-9 `pack_dbd_assoc_msg` / `unpack_dbd_assoc_msg` 同样追加 `remote_allowed`
- [ ] M13-10 `slurmdb_pack_user_rec` / `slurmdb_pack_assoc_rec` 同样追加（这是客户端 ↔ DBD 透传通道）

### 4.4 assoc_mgr 内存视图同步

- [ ] M13-11 [src/common/assoc_mgr.c](../../src/common/assoc_mgr.c) `_post_user_list()` 内部循环（24.05 拷贝 `slurmdb_user_rec_t` 字段时）追加：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
        new_user->remote_allowed = user->remote_allowed;
    #endif
    ```
- [ ] M13-12 `_post_assoc_list()` 同样追加 `new_assoc->remote_allowed = assoc->remote_allowed`
- [ ] M13-13 验证：`sacctmgr modify user alice set remote_allowed=true` 后，ctld 内存中 `assoc_mgr_get_user_rec_uid(alice_uid)->remote_allowed == 1`（grep ctld debug2 日志）

### 4.5 sacctmgr 客户端

- [ ] M13-14 [src/sacctmgr/sacctmgr.c](../../src/sacctmgr/sacctmgr.c) `_set_cond_options()`：增加 `--remote_allowed` 选项过滤识别（用于 `sacctmgr show ... where remote_allowed=true`）
- [ ] M13-15 [src/sacctmgr/user_functions.c](../../src/sacctmgr/user_functions.c) `_set_user_rec()` 增加 keyword 解析（v2 设计 §5.8.2）：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
    } else if (!xstrncasecmp(argv[i], "remote_allowed=", 15)) {
        char *val = argv[i] + 15;
        if (!xstrcasecmp(val, "true") || !xstrcasecmp(val, "yes")
            || !xstrcasecmp(val, "1"))
            user->remote_allowed = 1;
        else if (!xstrcasecmp(val, "false") || !xstrcasecmp(val, "no")
                 || !xstrcasecmp(val, "0"))
            user->remote_allowed = 0;
        else {
            fprintf(stderr, "Invalid remote_allowed value: %s\n", val);
            return SLURM_ERROR;
        }
        cond_set++;
    #endif
    ```
- [ ] M13-16 [src/sacctmgr/assoc_functions.c](../../src/sacctmgr/assoc_functions.c) `_set_assoc_rec()` 同样追加 keyword 解析
- [ ] M13-17 [src/sacctmgr/print_fields.c](../../src/sacctmgr/print_fields.c) 注册 `PRINT_REMOTE_ALLOWED` 列：
    ```c
    #ifdef __METASTACK_NEW_CROSS_DOMAIN
    case PRINT_REMOTE_ALLOWED:
        field->print_routine(field,
                             user->remote_allowed ? "YES" : "NO",
                             (curr_inx == field_count));
        break;
    #endif
    ```
- [ ] M13-18 同文件 `_setup_format()` 中 `format=user,account,remote_allowed` 等的字段映射表加 `remote_allowed` 项

### 4.6 编译与验证

- [ ] M13-19 `make -j` 通过；DBD + ctld + sacctmgr 都能 link
- [ ] M13-20 启动后 SlurmDBD 自动 ALTER TABLE 加 `remote_allowed` 列；`SHOW COLUMNS FROM user_table | grep remote_allowed` 看到列
- [ ] M13-21 `sacctmgr modify user alice set remote_allowed=true` 成功；`sacctmgr show user alice format=user,remote_allowed` 输出 `alice YES`
- [ ] M13-22 `sacctmgr show assoc where user=alice format=cluster,account,user,remote_allowed` 看到 assoc 维度
- [ ] M13-23 `sacctmgr modify user alice set remote_allowed=false`；`sacctmgr show user alice format=user,remote_allowed` 输出 `alice NO`
- [ ] M13-24 ctld debug2 日志（`DebugFlags=TraceJobs`）：alice 提交跨域作业时看到 `cross_region: cd_user_allows_remote(alice) → assoc.remote_allowed=1` 或 `=0`

### 4.7 端到端 ACL 验证

- [ ] M13-25 alice user.remote_allowed=true：`sbatch --allow-remote --app=test` → cd_thread Step 2 通过 → 转发成功
- [ ] M13-26 alice user.remote_allowed=false → cd_thread Step 2 失败 → `state_desc=CrossRegionAclDenied`，priority 不变
- [ ] M13-27 alice user.remote_allowed=true 但 assoc(alice@hpc_acct).remote_allowed=false → 在 hpc_acct 下 sbatch 失败（assoc 优先级更高）；切换到其它 account 下 sbatch 成功
- [ ] M13-28 sacctmgr 切换后下轮 cd_thread tick（5s）即刻生效（不重启 ctld）

### 4.8 v1.5 → v2.0 数据迁移（v2 设计 §9.6.5）

- [ ] M13-29 准备迁移 SQL 脚本 `migrate_v15_to_v20.sql`（位置 `doc/migrations/`）：
    ```sql
    -- 升级前对所有 cluster 库执行
    UPDATE user_table u
       SET u.remote_allowed = 1
     WHERE u.comment LIKE '%allow_remote%';

    -- 每个 cluster 库都要跑一次
    UPDATE xahc_assoc_table a
       SET a.remote_allowed = 1
     WHERE a.comment LIKE '%allow_remote%';
    -- ... wz_assoc_table, hf_assoc_table, ...
    ```
- [ ] M13-30 SOP 文档：升级步骤
    1. 停 ctld + DBD
    2. 升级 DBD 到 v2.0（schema 自动 ALTER）
    3. 跑 `migrate_v15_to_v20.sql` 把 comment 子串迁移到 `remote_allowed` 列
    4. 升级 ctld 到 v2.0
    5. 启动 DBD + ctld
    6. 验证 `sacctmgr show user format=user,remote_allowed` 看到正确分布
- [ ] M13-31 迁移完成后 v1.5 的 `comment` 子串保持原样（运维可自行清理），v2.0 ctld 不再读取该字段

---

## 5. 验收标准

1. SlurmDBD 启动时自动 ALTER TABLE 加 `remote_allowed` 列；`DESCRIBE user_table` / `DESCRIBE <cluster>_assoc_table` 看到列
2. `sacctmgr modify user X set remote_allowed=true|false` 命令可用，`sacctmgr show user X format=user,remote_allowed` 显示正确值
3. `sacctmgr modify user X account=Y set remote_allowed=true` assoc 维度可用
4. `sacctmgr show assoc where user=X format=cluster,account,user,remote_allowed` 列出
5. ctld 跨域线程通过 `assoc_ptr->remote_allowed` 读取（不再读 comment）；切换后下轮 tick 实时生效
6. v1.5 → v2.0 SQL 迁移脚本能把老用户授权迁移到新列
7. 兼容性：v2.0 ctld + v1.5 DBD 混部时 ctld 拿到 `remote_allowed=0` 默认拒绝跨域；v1.5 ctld + v2.0 DBD 混部时 ctld 仍读 comment 子串（旧逻辑路径）

## 6. v1.5 → v2.0 数据迁移 SQL 脚本（参考）

```sql
-- migrate_v15_to_v20.sql
-- 升级前对每个 cluster 库执行 (replace clusterX_assoc_table 实际表名)

START TRANSACTION;

-- user 维度
UPDATE user_table u
   SET u.remote_allowed = 1
 WHERE u.comment LIKE '%allow_remote%'
   AND u.remote_allowed = 0;

-- assoc 维度 (每个 cluster 表都要跑)
UPDATE clusterA_assoc_table a SET a.remote_allowed = 1 WHERE a.comment LIKE '%allow_remote%' AND a.remote_allowed = 0;
UPDATE clusterB_assoc_table a SET a.remote_allowed = 1 WHERE a.comment LIKE '%allow_remote%' AND a.remote_allowed = 0;
-- ...

COMMIT;

-- 验证
SELECT name, remote_allowed FROM user_table WHERE remote_allowed = 1 LIMIT 10;
```

## 7. 风险

- **风险 1**: 24.05 SlurmDBD `_check_table_columns()` 自动 ALTER TABLE 失败（权限不足或字段已存在但定义不同）。**降级**: 手写 ALTER TABLE 脚本，部署文档说明
- **风险 2**: `dbd_user_msg_t` / `dbd_assoc_msg_t` 已有字段顺序变化导致 ABI 破坏。**降级**: 字段严格 append-only 加在末尾
- **风险 3**: `NO_VAL16` sentinel 与默认值 0 冲突（`sacctmgr show` 时区分"未设"和"显式 0"）。**降级**: 24.05 已有惯例 — `slurmdb_user_rec_t` 中数值类字段默认设 `NO_VAL`/`NO_VAL16`，sacctmgr 在 `_set_user_rec` 中显式赋值才被 SQL 写入
- **风险 4**: assoc_mgr 视图同步漏改 → ctld 跨域线程读到 stale 0。**降级**: `_post_user_list` / `_post_assoc_list` 必须同步；通过 `sacctmgr modify` 后 ctld 重新拉 assoc_mgr cache 验证
- **风险 5**: `_set_user_rec` keyword `remote_allowed=` 与 24.05 已有 `comment` 等关键字解析顺序冲突。**降级**: 严格用 `xstrncasecmp` 全词匹配，长度按 keyword 实际长度（15）
- **风险 6**: v1.5 部署遗留 user.comment 包含 `allow_remote` 子串，升级后 v2.0 ctld 不再读取，可能误以为权限丢失。**降级**: §6 SQL 迁移脚本必跑；运维 SOP 在升级文档中显式标红
- **风险 7**: `sacctmgr show` format=`remote_allowed` 列名与已有 `RemoteAllow` 等字段冲突。**降级**: 列名严格用 v2 设计 §5.8.1 一致的 `remote_allowed`，全小写
