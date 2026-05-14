# ctld-M10 broker 侧 LEGACY_M04_TRANSITIONAL 清理 + v2 协议适配 Checklist (v2.0)

> 配套: [doc/Broker详细设计文档MVP_v2.md](../Broker详细设计文档MVP_v2.md) §6
> 配套（broker 端原始 M04）: [doc/checklists/M04-rpc.md](../checklists/M04-rpc.md) §10/§11
> 差异蓝图: [doc/跨域调度详设-差异变更说明.md](../跨域调度详设-差异变更说明.md) §2.6
> 依赖: ctld-M01（slurm-native msg_type / payload v2 字段顺序已注册）/ ctld-M03（forward_job_msg_t v2 瘦身字段）
> 下游: ctld-M11

> **v1.5 → v2.0 关键变化**:
> 1. **协议号统一为 8001-8005**（broker 端 `src/slurmbrokerd/proto.h:56-79` 已是这套，无需变更数值）
> 2. **`forward_job_msg_t` payload 大幅瘦身**：broker 端 `brokerd_forward_job_msg_t` 必须同步删除 `target_cluster` / `target_partition` / `account` / `job_desc`，新增 `src_cluster_name` / `src_partition`，`app_name` 改名为 `cd_app_name`
> 3. **`broker_update_remote_state_msg_t` 新增 `remote_cluster_name` / `remote_partition_name`**：broker 必须在首次状态包带这两个字段（详见 broker 详设 §2.7 originator handler 步骤 3e）
> 4. **broker 内部新增 8018/8019 RPC**（broker→broker test-only 探测）：与本 ctld checklist 无关，由 broker 详设 M16/M17/M18/M19 实现
> 5. **`_pack_job_desc/_unpack_job_desc` helper 必删**：v2 删除 `job_desc` 字段后无引用

---

## 1. 模块目标

ctld-M01 完成后，broker 端 4 个 `LEGACY_M04_TRANSITIONAL` 块成为死代码。本模块按依赖反向（先改入口、再删定义）扫除：

- **改名**：把 broker 端使用的 `BROKERD_REQUEST_*` 宏 + `brokerd_*_msg_t` 结构改为 ctld-M01 注册的 slurm-native 名 + struct
- **删除**：proto.h / proto.c / proto_pack.c 中的 LEGACY 宏 / typedef / pack/unpack helper / free 实现
- **同步 v2 字段顺序**：`forward_job_msg_t` 瘦身（删 4 字段、加 2 字段、改 1 字段名）；`broker_update_remote_state_msg_t` / `broker_terminal_state_msg_t` 新增 `remote_cluster_name` / `remote_partition_name`

完成后 `rg LEGACY_M04_TRANSITIONAL src/slurmbrokerd/` 清零。

> **注意**：broker 内部 broker→broker 私有 BRKR 帧 `BROKERD_REQUEST_BROKER_*`（8006-8017）**保留**，那是长期跨集群 RPC，与本模块无关。`BROKERD_REQUEST_BROKER_CANCEL=8016` peer→peer 保留，ctld→broker 路径用新 `REQUEST_BROKER_CANCEL=8005`。

## 2. 改动范围（v2.0 共约 30 处）

### 2.1 改名映射（基础）

| 旧（broker 私有 LEGACY） | 新（slurm-native, ctld-M01 注册） |
|---|---|
| `BROKERD_REQUEST_FORWARD_JOB` | `REQUEST_FORWARD_JOB` |
| `BROKERD_RESPONSE_FORWARD_JOB` | `RESPONSE_FORWARD_JOB` |
| `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` | `REQUEST_BROKER_UPDATE_REMOTE_STATE` |
| `BROKERD_REQUEST_BROKER_TERMINAL_STATE` | `REQUEST_BROKER_TERMINAL_STATE` |
| `brokerd_forward_job_msg_t` | `forward_job_msg_t` (★ v2 字段瘦身) |
| `brokerd_forward_job_resp_msg_t` | `forward_job_resp_msg_t` |
| `brokerd_broker_remote_state_msg_t` | `broker_update_remote_state_msg_t` (★ v2 +2 字段) |
| `brokerd_broker_terminal_state_msg_t` | `broker_terminal_state_msg_t` (★ v2 +2 字段) |
| `brokerd_broker_cancel_msg_t`（ctld→broker 路径用） | `broker_cancel_msg_t`（peer 路径仍用 brokerd_*） |

### 2.2 ★ v2.0 字段同步改造

| 结构 | v1 字段（删/改） | v2 字段（新加） |
|---|---|---|
| `forward_job_msg_t` | 删: `target_cluster` / `target_partition` / `account` / `job_desc` | 加: `src_cluster_name` / `src_partition`；改名: `app_name` → `cd_app_name` |
| `broker_update_remote_state_msg_t` | — | 加: `remote_cluster_name` / `remote_partition_name`（首次状态包必填） |
| `broker_terminal_state_msg_t` | — | 加: `remote_cluster_name` / `remote_partition_name`（同上）|
| `forward_job_resp_msg_t` | — | 加: `error_message`（错误描述） |

### 2.3 不改的部分

- `BROKERD_REQUEST_BROKER_FORWARD_JOB` (8006) / `BROKERD_RESPONSE_BROKER_ACK` (8011) / `BROKERD_REQUEST_BROKER_STAGED_IN` (8012) / `BROKERD_RESPONSE_BROKER_SUBMITTED` (8013) / `BROKERD_REQUEST_BROKER_QUERY_STATUS` (8014) / `BROKERD_RESPONSE_BROKER_STATUS` (8015) / `BROKERD_REQUEST_BROKER_CLEANUP`：broker→broker 永久私有，**保留**
- `BROKERD_REQUEST_BROKER_CANCEL=8016`：peer→peer 私有，**保留**；ctld→broker 路径用新 `REQUEST_BROKER_CANCEL=8005`
- 8018/8019 `BROKERD_REQUEST_BROKER_TEST_ONLY` / `RESPONSE`：broker→broker 永久私有（v2.0 新增），**broker 自己注册** + 与本模块无关

---

## 3. 触及文件 + 行号锚点

| 文件 | 锚点 | 改动 |
|---|---|---|
| [src/slurmbrokerd/handler_ctld.c](../../src/slurmbrokerd/handler_ctld.c) | dispatcher 入口 | 全文替换旧名为新名 + ★ v2 适配 forward_job_msg_t 瘦身字段 |
| [src/slurmbrokerd/egress.c](../../src/slurmbrokerd/egress.c) | `ctld_update_remote_state` / `ctld_inject_terminal_state` | 替换 4 处旧名 + ★ v2 在 8003/8004 必带 `remote_cluster_name` / `remote_partition_name` |
| [src/slurmbrokerd/proto.h](../../src/slurmbrokerd/proto.h) | 56-59、121-139、220-240、247-252 行 | 删 4 宏 + 4 struct + 4 free 声明 |
| [src/slurmbrokerd/proto_pack.c](../../src/slurmbrokerd/proto_pack.c) | 168-235、485-572 行；596-611、664-684 行 dispatcher | 删 4 对 pack/unpack + 4 case + helper `_pack_job_desc`/`_unpack_job_desc` |
| [src/slurmbrokerd/proto.c](../../src/slurmbrokerd/proto.c) | `brokerd_free_*` 实现 + `brokerd_free_msg_data` switch | 删 4 个实现 + 4 case |

---

## 4. Checklist

### 4.1 入口替换 + ★ v2 字段适配

- [ ] M10-1 [src/slurmbrokerd/handler_ctld.c](../../src/slurmbrokerd/handler_ctld.c) 全文替换：
    - `BROKERD_REQUEST_FORWARD_JOB` → `REQUEST_FORWARD_JOB`
    - `BROKERD_RESPONSE_FORWARD_JOB` → `RESPONSE_FORWARD_JOB`
    - `BROKERD_REQUEST_BROKER_CANCEL` → `REQUEST_BROKER_CANCEL`（仅 ctld→broker 入站路径，peer→peer BRKR 帧保留 `BROKERD_*`）
    - `brokerd_forward_job_msg_t` → `forward_job_msg_t`
    - `brokerd_forward_job_resp_msg_t` → `forward_job_resp_msg_t`
    - `brokerd_broker_cancel_msg_t` → `broker_cancel_msg_t`
- [ ] M10-2 [src/slurmbrokerd/handler_ctld.c](../../src/slurmbrokerd/handler_ctld.c) `handle_forward_job` 函数体内适配 v2 字段（详见 broker 详设 §2.7 originator handler）：
    - 不再读 `req->target_cluster` / `req->target_partition` / `req->account` / `req->job_desc`
    - 新读 `req->src_cluster_name` / `req->src_partition` / `req->cd_app_name`
    - 触发 `route_decide(src_cluster, src_partition, cd_app_name)` 拿候选列表
    - 串行调 `egress_test_only_sync(candidate)` 探测，命中 break
    - 全部失败 → 回 ctld `RESPONSE_FORWARD_JOB { error_code=9010/9011/9013, error_message="..." }`
    - 命中 → `broker_job_create(target_cluster=c.cluster, target_partition=c.partition)` + 立即推送首条 8003（含 `remote_cluster_name` / `remote_partition_name`）让 ctld 写入字段
- [ ] M10-3 [src/slurmbrokerd/egress.c](../../src/slurmbrokerd/egress.c) `ctld_update_remote_state` / `ctld_inject_terminal_state` 中替换：
    - `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` → `REQUEST_BROKER_UPDATE_REMOTE_STATE`
    - `BROKERD_REQUEST_BROKER_TERMINAL_STATE` → `REQUEST_BROKER_TERMINAL_STATE`
    - `brokerd_broker_remote_state_msg_t` → `broker_update_remote_state_msg_t`
    - `brokerd_broker_terminal_state_msg_t` → `broker_terminal_state_msg_t`
- [ ] M10-4 [src/slurmbrokerd/egress.c](../../src/slurmbrokerd/egress.c) `ctld_update_remote_state` 在构造 `broker_update_remote_state_msg_t` 时**必填** `remote_cluster_name` / `remote_partition_name`（broker_job_t 已知该作业的 target_cluster / target_partition）：
    ```c
    msg.remote_cluster_name   = xstrdup(broker_job->target_cluster);
    msg.remote_partition_name = xstrdup(broker_job->target_partition);
    /* 其余 6 字段同 v1 */
    ```
- [ ] M10-5 编译 broker：`make -C src/slurmbrokerd -j` 通过（此时 proto.h 中旧宏仍在，handler/egress 已用新名，新名由 ctld-M01 提供，broker 包含 `slurm_protocol_defs.h` 即可解析）

### 4.2 删 proto.h 旧定义

- [ ] M10-6 [src/slurmbrokerd/proto.h](../../src/slurmbrokerd/proto.h) 56-59 行删 4 个 `#define BROKERD_REQUEST_FORWARD_JOB...` / `BROKERD_RESPONSE_FORWARD_JOB` / `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` / `BROKERD_REQUEST_BROKER_TERMINAL_STATE`（共 4 行）
- [ ] M10-7 同文件 121-139、220-240 行删 4 个 typedef struct（`brokerd_forward_job_msg_t`、`brokerd_forward_job_resp_msg_t`、`brokerd_broker_remote_state_msg_t`、`brokerd_broker_terminal_state_msg_t`）
- [ ] M10-8 同文件 247-252 行删 4 个 `extern void brokerd_free_*` 声明
- [ ] M10-9 删除文件中所有 `LEGACY_M04_TRANSITIONAL` 注释行/块说明

### 4.3 删 proto_pack.c 旧实现

- [ ] M10-10 [src/slurmbrokerd/proto_pack.c](../../src/slurmbrokerd/proto_pack.c) 168-235 行删 `_pack_forward_job_msg/_unpack_forward_job_msg`、`_pack_forward_job_resp_msg/_unpack_forward_job_resp_msg`
- [ ] M10-11 同文件 485-572 行删 `_pack_broker_remote_state_msg/_unpack_broker_remote_state_msg`、`_pack_broker_terminal_state_msg/_unpack_broker_terminal_state_msg`
- [ ] M10-12 同文件 dispatcher `brokerd_pack_msg`/`brokerd_unpack_msg`/`brokerd_msg_type_str` 大 switch 删 4 case
- [ ] M10-13 同文件顶部 `_pack_job_desc`/`_unpack_job_desc` static helper（57-119 行）：v2 删除 `job_desc` 字段后必无引用，**强制删除** + `rg _pack_job_desc src/slurmbrokerd/` 验证 0 输出

### 4.4 删 proto.c 旧实现

- [ ] M10-14 [src/slurmbrokerd/proto.c](../../src/slurmbrokerd/proto.c) 删 4 个 `brokerd_free_*_msg` 实现：
    - `brokerd_free_forward_job_msg`
    - `brokerd_free_forward_job_resp_msg`
    - `brokerd_free_broker_remote_state_msg`
    - `brokerd_free_broker_terminal_state_msg`
- [ ] M10-15 同文件 `brokerd_free_msg_data` 大 switch 删 4 case

### 4.5 ★ v2 broker_job_t 同步新字段（broker 详设 §4.1 / §4.3 / §4.4）

> 本节是 broker 内部数据结构改造，不属于 ctld 端 checklist 严格范畴，但与 M10-2 改名同步推进，便于联调。详见 broker 详设 M16 / M17。

- [ ] M10-16 `broker_job_t` 结构新增 v2 字段（broker 详设 §4.1）：
    ```c
    char    **route_attempted;       /* "wz_cluster:wzhcnormal" 字符串数组 */
    uint32_t  route_attempted_count;
    char     *route_name;            /* routes.conf [route:NAME] 名, 用于 cap_release */
    ```
- [ ] M10-17 JSONL 持久化 schema 末尾追加 `"route_attempted":[...]` 可选数组（broker_job_to_json/from_json 同步更新）
- [ ] M10-18 `state_machine_resume_inflight()` INIT 状态恢复后从 `route_attempted` 续探下个候选

### 4.6 全树检查

- [ ] M10-19 `rg LEGACY_M04_TRANSITIONAL src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-20 `rg brokerd_forward_job_msg_t src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-21 `rg brokerd_broker_remote_state_msg_t src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-22 `rg BROKERD_REQUEST_FORWARD_JOB src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-23 `rg "_pack_job_desc|_unpack_job_desc" src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-24 `rg "target_cluster|target_partition" src/slurmbrokerd/handler_ctld.c` **0 输出**（v2 删除）
- [ ] M10-25 `rg "src_cluster_name|src_partition|cd_app_name" src/slurmbrokerd/handler_ctld.c` 多处出现（v2 新增）

### 4.7 编译验证

- [ ] M10-26 `make -j` 全树通过
- [ ] M10-27 `src/slurmbrokerd/inject_broker_forward` 二进制（peer 测试工具）仍能编译——它只走 broker→broker 8006 私有协议，不受影响
- [ ] M10-28 broker 启动 + ctld 启动；ctld → broker 单向 8001 测试通（broker 端能 unpack v2 字段顺序的 `forward_job_msg_t`）；broker → ctld 单向 8003 测试通（ctld 端能拿到 `remote_cluster_name` / `remote_partition_name`）；端到端 M11 联调
- [ ] M10-29 broker 启动日志看到 `info("brokerd: registered RPCs: REQUEST_FORWARD_JOB, RESPONSE_FORWARD_JOB, REQUEST_BROKER_UPDATE_REMOTE_STATE, REQUEST_BROKER_TERMINAL_STATE, REQUEST_BROKER_CANCEL")`，**不再列出** `BROKERD_REQUEST_FORWARD_JOB` 等 LEGACY 名

---

## 5. 验收标准

1. `rg LEGACY src/slurmbrokerd/` 仅余非 M04 的 `LEGACY` 字样（如果有）
2. broker 启动时 `info("brokerd: registered RPCs ...")` 日志不再列出 4 个 LEGACY msg
3. broker handler_ctld.c 内 `handle_forward_job` 实现了 v2 路由决策流程（route_decide + test-only loop + 首次 8003 推送 cluster/partition）
4. broker `_pack_forward_job_msg` 字段顺序与 ctld-M01 §2.2 完全一致（6 个字段，不含 `job_desc`）
5. ctld → broker → ctld 端到端：`sbatch --allow-remote --app=test`（M11.2 验证）链路通

## 6. 风险

- **风险 1**: 改名漏一处导致 link error。**降级**: 用 §4.6 全树 grep 5 个 pattern 兜底
- **风险 2**: M10-13 的 `_pack_job_desc` helper 实际还有 broker→broker 8006 引用。**降级**: 先 `rg _pack_job_desc src/slurmbrokerd/` 看引用，确实无引用再删；有的话保留并加注释说明仅 BRKR 帧用
- **风险 3**: M10-2 `handle_forward_job` 改造范围大（路由决策 + test-only），单 PR 风险高。**降级**: 拆 2 个子 PR — 子 PR (a) 仅改名 + 字段适配（保持 v1 行为：直接接受所有候选，无路由决策）；子 PR (b) 加路由决策 + test-only（依赖 broker M16/M17 路由模块）
- **风险 4**: broker 端 v2 协议字段顺序与 ctld 端 ctld-M01 不一致。**降级**: 双侧 PR 共 review；自动化测试 = 准备一个 mock broker（python pyslurm + slurm-native pack）发 8001 给真实 ctld，验证字段顺序
- **风险 5**: M10-16 ~ M10-18 broker 内部 broker_job_t / JSONL / resume 改造属于 broker 详设范围，超出 ctld checklist 责任边界。**降级**: 在 broker 详设 M16-M19 推进，与本 ctld-M10 同 sprint 协同 review
- **风险 6**: ctld v1.5 + broker v2.0 混部。**降级**: 强制约束 — ctld 与 broker **必须同步升级**；混部不支持，运维 SOP 文档化
