# ctld-M10 broker 侧 LEGACY_M04_TRANSITIONAL 清理 Checklist

> 配套: [doc/Broker详细设计文档MVP_new.md](../Broker详细设计文档MVP_new.md) §6
> 配套（broker 端原始 M04）: [doc/checklists/M04-rpc.md](M04-rpc.md) §10/§11
> 模块化总览: [.cursor/plans/ctld_cross-domain_modular_plan_*.plan.md](../../.cursor/plans/)
> 依赖: ctld-M01（slurm-native msg_type / payload 已注册）
> 下游: ctld-M11

---

## 1. 模块目标

ctld-M01 完成后，broker 端 4 个 `LEGACY_M04_TRANSITIONAL` 块成为死代码，本模块按依赖反向（先改入口、再删定义）扫除。`grep -rn LEGACY_M04_TRANSITIONAL src/slurmbrokerd/` 清零。

> **注意**：broker 内部 broker→broker 私有 BRKR 帧 `BROKERD_REQUEST_BROKER_*`（8010-8017）**保留**，那是 8016 等长期跨集群 RPC，与本模块无关。

## 2. 改动范围（共 27 处）

### 2.1 改名映射

| 旧（broker 私有） | 新（slurm-native, ctld-M01 注册） |
|---|---|
| `BROKERD_REQUEST_FORWARD_JOB` | `REQUEST_FORWARD_JOB` |
| `BROKERD_RESPONSE_FORWARD_JOB` | `RESPONSE_FORWARD_JOB` |
| `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` | `REQUEST_BROKER_UPDATE_REMOTE_STATE` |
| `BROKERD_REQUEST_BROKER_TERMINAL_STATE` | `REQUEST_BROKER_TERMINAL_STATE` |
| `brokerd_forward_job_msg_t` | `forward_job_msg_t` |
| `brokerd_forward_job_resp_msg_t` | `forward_job_resp_msg_t` |
| `brokerd_broker_remote_state_msg_t` | `broker_remote_state_msg_t` |
| `brokerd_broker_terminal_state_msg_t` | `broker_terminal_state_msg_t` |
| `brokerd_broker_cancel_msg_t`（ctld→broker 路径用） | `broker_cancel_msg_t`（peer 路径仍用 brokerd_*） |

### 2.2 不改的部分

- `BROKERD_REQUEST_BROKER_FORWARD_JOB / BROKERD_RESPONSE_BROKER_ACK / BROKERD_REQUEST_BROKER_STAGED_IN / BROKERD_RESPONSE_BROKER_SUBMITTED / BROKERD_REQUEST_BROKER_QUERY_STATUS / BROKERD_RESPONSE_BROKER_STATUS / BROKERD_REQUEST_BROKER_CLEANUP`：broker→broker 永久私有，**保留**
- `BROKERD_REQUEST_BROKER_CANCEL=8016`：peer→peer 私有，**保留**；ctld→broker 路径用新 `REQUEST_BROKER_CANCEL=9104`

---

## 3. 触及文件 + 行号锚点

| 文件 | 锚点 | 改动 |
|---|---|---|
| [src/slurmbrokerd/handler_ctld.c](../../src/slurmbrokerd/handler_ctld.c) | dispatcher 入口 | 全文替换旧名为新名 |
| [src/slurmbrokerd/egress.c](../../src/slurmbrokerd/egress.c) | `ctld_update_remote_state` / `ctld_inject_terminal_state` | 替换 4 处旧名 |
| [src/slurmbrokerd/proto.h](../../src/slurmbrokerd/proto.h) | 56-59、121-139、220-240、247-252 行 | 删 4 宏 + 4 struct + 4 free 声明 |
| [src/slurmbrokerd/proto_pack.c](../../src/slurmbrokerd/proto_pack.c) | 168-235、485-572 行；596-611、664-684 行 dispatcher | 删 4 对 pack/unpack + 4 case + helper `_pack_job_desc/_unpack_job_desc` |
| [src/slurmbrokerd/proto.c](../../src/slurmbrokerd/proto.c) | `brokerd_free_*` 实现 + `brokerd_free_msg_data` switch | 删 4 个实现 + 4 case |

---

## 4. Checklist

### 4.1 入口替换（先改这 2 个，编译还能过）

- [ ] M10-1 [src/slurmbrokerd/handler_ctld.c](../../src/slurmbrokerd/handler_ctld.c) 全文替换：
    - `BROKERD_REQUEST_FORWARD_JOB` → `REQUEST_FORWARD_JOB`
    - `BROKERD_RESPONSE_FORWARD_JOB` → `RESPONSE_FORWARD_JOB`
    - `BROKERD_REQUEST_BROKER_CANCEL` → `REQUEST_BROKER_CANCEL`
    - `brokerd_forward_job_msg_t` → `forward_job_msg_t`
    - `brokerd_forward_job_resp_msg_t` → `forward_job_resp_msg_t`
    - `brokerd_broker_cancel_msg_t` → `broker_cancel_msg_t`
- [ ] M10-2 [src/slurmbrokerd/egress.c](../../src/slurmbrokerd/egress.c) `ctld_update_remote_state` / `ctld_inject_terminal_state` 中替换：
    - `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` → `REQUEST_BROKER_UPDATE_REMOTE_STATE`
    - `BROKERD_REQUEST_BROKER_TERMINAL_STATE` → `REQUEST_BROKER_TERMINAL_STATE`
    - `brokerd_broker_remote_state_msg_t` → `broker_remote_state_msg_t`
    - `brokerd_broker_terminal_state_msg_t` → `broker_terminal_state_msg_t`
- [ ] M10-3 编译 broker：`make -C src/slurmbrokerd -j` 通过（此时 proto.h 中旧宏仍在，handler/egress 已用新名，新名由 ctld-M01 提供，broker 包含 `slurm_protocol_defs.h` 即可解析）

### 4.2 删 proto.h 旧定义

- [ ] M10-4 [src/slurmbrokerd/proto.h](../../src/slurmbrokerd/proto.h) 56-59 行删 4 个 `#define BROKERD_REQUEST_FORWARD_JOB...` / `BROKERD_RESPONSE_FORWARD_JOB` / `BROKERD_REQUEST_BROKER_UPDATE_REMOTE_STATE` / `BROKERD_REQUEST_BROKER_TERMINAL_STATE`（共 4 行）
- [ ] M10-5 同文件 121-139、220-240 行删 4 个 typedef struct（`brokerd_forward_job_msg_t`、`brokerd_forward_job_resp_msg_t`、`brokerd_broker_remote_state_msg_t`、`brokerd_broker_terminal_state_msg_t`）
- [ ] M10-6 同文件 247-252 行删 4 个 `extern void brokerd_free_*` 声明
- [ ] M10-7 删除文件中所有 `LEGACY_M04_TRANSITIONAL` 注释行/块说明

### 4.3 删 proto_pack.c 旧实现

- [ ] M10-8 [src/slurmbrokerd/proto_pack.c](../../src/slurmbrokerd/proto_pack.c) 168-235 行删 `_pack_forward_job_msg/_unpack_forward_job_msg`、`_pack_forward_job_resp_msg/_unpack_forward_job_resp_msg`
- [ ] M10-9 同文件 485-572 行删 `_pack_broker_remote_state_msg/_unpack_broker_remote_state_msg`、`_pack_broker_terminal_state_msg/_unpack_broker_terminal_state_msg`
- [ ] M10-10 同文件 dispatcher `brokerd_pack_msg`/`brokerd_unpack_msg`/`brokerd_msg_type_str` 大 switch 删 4 case
- [ ] M10-11 同文件顶部 `_pack_job_desc`/`_unpack_job_desc` static helper（57-119 行）：检查无引用后删除（broker→broker 7 个永久 RPC 不依赖）

### 4.4 删 proto.c 旧实现

- [ ] M10-12 [src/slurmbrokerd/proto.c](../../src/slurmbrokerd/proto.c) 删 4 个 `brokerd_free_*_msg` 实现：
    - `brokerd_free_forward_job_msg`
    - `brokerd_free_forward_job_resp_msg`
    - `brokerd_free_broker_remote_state_msg`
    - `brokerd_free_broker_terminal_state_msg`
- [ ] M10-13 同文件 `brokerd_free_msg_data` 大 switch 删 4 case

### 4.5 全树检查

- [ ] M10-14 `grep -rn LEGACY_M04_TRANSITIONAL src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-15 `grep -rn brokerd_forward_job_msg_t src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-16 `grep -rn brokerd_broker_remote_state_msg_t src/slurmbrokerd/` 必须 **0 输出**
- [ ] M10-17 `grep -rn BROKERD_REQUEST_FORWARD_JOB src/slurmbrokerd/` 必须 **0 输出**

### 4.6 编译验证

- [ ] M10-18 `make -j` 全树通过
- [ ] M10-19 `src/slurmbrokerd/inject_broker_forward` 二进制（peer 测试工具）仍能编译——它只走 broker→broker 8010 私有协议，不受影响
- [ ] M10-20 broker 启动 + ctld 启动，断电式 sbatch --cross-domain 流程通：M11 联调

---

## 5. 验收标准

1. `grep -rn LEGACY src/slurmbrokerd/` 仅余非 M04 的 `LEGACY` 字样（如果有）
2. broker 启动时 `info("brokerd: registered RPCs ...")` 日志不再列出 4 个 LEGACY msg
3. 端到端 9100/9102/9103/9104 链路（M11 验证）通

## 6. 风险

- **风险 1**: 改名漏一处导致 link error。**降级**: 用 §4.5 全树 grep 4 个 pattern 兜底
- **风险 2**: M10-11 的 `_pack_job_desc` helper 实际还有引用。**降级**: 先 `grep _pack_job_desc src/slurmbrokerd/` 看引用，确实无引用再删；有的话保留
