# DICOM CT 离线取数流水线

面向影像科研数据员的院内 PACS 取数工具：提交 `StudyInstanceUID`，经
**C-FIND 固定本批 SOP 清单 → C-MOVE 发起回传 → C-STORE 接收落盘 →
核对收齐 → 打包 ZIP 下载** 的完整流水线，支持多检查并行、慢回传交错、
重复投送去重、清单外实例拒收、保存失败判失败与重启恢复。

交付分割组前，**重启及每次下载前都会核对 ZIP 归档**（条目可读且与已登记
SOP 清单一致），损坏/丢失自动从已登记原片重建；缺片或重建失败时保留
完成记录、标记不可下载及具体原因，修复数据卷后再次请求即可恢复。

* **API 框架**：FastAPI（自带 Swagger/OpenAPI）
* **DICOM 栈**：pynetdicom 3.x（C-STORE SCP + C-FIND/C-MOVE SCU）、pydicom
* **持久化**：SQLite（WAL，全量落在数据卷）
* **模拟 PACS**：pynetdicom 实现的真实 DICOM 网络对端，内置虚构合成 CT，
  可配置慢回传与重复投送；另带 rogue 投送器演示拒收

---

## 1. 快速开始（Docker Compose）

```bash
# 构建并启动 API + 模拟 PACS（首次会自动生成 8/6/5 个实例的三个虚构 CT 检查）
docker compose up --build -d

# Swagger 文档
open http://localhost:8000/docs

# 一键端到端演示：交错回传 / 重复投送 / 清单外拒收 / 压缩拒收 /
# 保存失败 / 失败批迟到拒收 / 损坏包并发重建 / 缺片阻塞与恢复 / 重启恢复
docker compose run --rm demo
```

端口：

| 服务 | 端口 | 用途 |
|---|---|---|
| api | 8000 | REST API + Swagger |
| api | 11113 | DICOM C-STORE SCP（AE=`RETRIEVER`）|
| pacs | 11112 | 模拟 PACS（AE=`MYPACS`）|

数据卷：`api-data`（SQLite、DICOM 文件、ZIP）、`pacs-data`（虚构 CT）。

---

## 2. 业务规则与实现位置

| 规则 | 实现 |
|---|---|
| 只收**非压缩 CT** | SCP 只公布 Implicit/Explicit VR Little Endian 的 CT 存储上下文，压缩 TS 在协商阶段即被拒；`app/storage.py::validate_dataset` 二次校验 |
| 源端批内**不增删实例** | C-FIND（IMAGE 级）一次性固定清单写入 `instances` 表，`set_manifest` 只允许固定一次 |
| 回传**归入对应批** | C-STORE 按 `StudyInstanceUID` 经 `db.find_batch_for_study` 路由（按 rowid 取最新批） |
| **清单外实例不入包** | `db.is_manifest_uid` 校验，拒收并回 `0xC000`，事件 `STORE_REJECTED` |
| **重复实例不重复计数** | `(batch_id, sop_instance_uid)` 唯一约束 + `register_instance` 条件更新；重复直接回 SUCCESS，不写盘不计数，事件 `STORE_DUPLICATE` |
| **文件写完且登记成功才回 C-STORE 成功** | `handle_c_store` 严格顺序：临时文件 → fsync → rename → 目录 fsync → DB 登记；任一步失败回 `0xA700/0xC000`，登记失败会删除已写文件 |
| **C-MOVE 成功且清单收齐才可下载** | 下载接口仅允许 `COMPLETED`；MOVING/FAILED 返回 409 |
| 传输/保存失败、结束后缺件均**判失败** | C-MOVE 非 0x0000（含 0xA702/0xB000）失败；`missing_uids` 非空失败；写盘异常立即 `fail_batch` |
| **迟到回传不能恢复失败批** | 终态 FAILED 的批收到 C-STORE 一律拒收；DB 层 `transition_state` 禁止任何状态迁出终态 |
| 多检查**并行、慢回传互不阻塞** | 每批独立线程；C-STORE SCP 每关联一线程；C-MOVE 回传与其它批的 API/DICOM 处理无共享阻塞 |
| **重启恢复** | 启动时所有非终态批置 FAILED；COMPLETED 批逐一核对 ZIP，异常即重建（见下） |
| **下载前核对归档** | `app/archive.py::verify_zip`：ZIP 可打开、全部条目 CRC 可读、`MANIFEST.csv` 的 SOP 清单与已登记清单一致，三者缺一即视为异常 |
| **异常即从登记原片重建** | `ArchiveService.ensure_package`：ZIP 丢失/损坏/清单不一致时从完整的已登记 DICOM 重建，重建后自检通过才落位 |
| **临时包不得对外可见** | 重建写入随机命名的 `*.part` 临时文件，自检通过后 `os.replace` 原子落位；下载只服务最终路径；启动时清扫崩溃残留的临时包 |
| **并发下载只重建一次** | 每批一把锁的 single-flight：首个请求重建，其余等待后复核复用结果（事件 `ZIP_REBUILT` 只出现一次） |
| **有效下载不被替换打断** | 核对通过的 ZIP 绝不重建/替换；只有损坏/缺失的归档才被原子替换，rename 不影响已打开的读取句柄 |
| **缺片/重建失败保留完成记录** | 原片缺失或写盘失败：批保持 `COMPLETED`，`downloadable=false` 且 `download_blocked_reason` 记录具体原因，下载返回 409 而非残缺 ZIP；修复数据卷后再次请求自动重试并清除标记 |

### 批状态机

```
PENDING ──C-FIND──▶ FINDING ──清单固定+故障武装──▶ MOVING
                                          │
                  收齐+C-MOVE 成功 ────────┴──▶ COMPLETED（可下载，终态）
                  任何失败/缺件 ──────────────▶ FAILED（终态，迟到回传拒收）
重启时 PENDING/FINDING/MOVING ───────────────▶ FAILED

COMPLETED 归档异常（缺片/重建失败）⇢ downloadable=false + download_blocked_reason
        （状态保持 COMPLETED；修复数据卷后再次下载自动恢复 downloadable=true）
```

---

## 3. REST API

Swagger：`GET /docs`（OpenAPI JSON：`/openapi.json`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/batches` | 提交检查，body：`{"study_instance_uid": "...", "fault_inject": ""}`，`fault_inject` 可选 `save_failure` |
| GET | `/batches` | 列出全部批 |
| GET | `/batches/{id}` | 批状态与进度（`expected_count/received_count/state/downloadable/download_blocked_reason`）|
| GET | `/batches/{id}/events` | 事件流水（接收、重复、拒收、状态迁移、归档核对/重建、故障）|
| GET | `/batches/{id}/instances` | 清单、已收、缺失 SOP 列表 |
| GET | `/batches/{id}/download` | 下载 ZIP（仅 COMPLETED）。下载前核对归档，损坏/丢失自动重建；不可重建时 409 并给出具体原因，不返回残缺 ZIP |
| GET | `/health` | 健康检查 |
| GET | `/internal/pacs-studies` | （演示）C-FIND 列出模拟 PACS 检查 |
| POST | `/internal/suicide` | （演示）退出进程，由 compose 自动重启以演示恢复 |
| POST | `/internal/fault/corrupt-zip/{id}` | （演示）损坏归档：`mode=corrupt`（位翻转）/`truncate`（截断）/`missing`（删除）|
| POST | `/internal/fault/remove-slice/{id}` | （演示）把一个已登记原片移入隔离区，模拟数据卷缺片 |
| POST | `/internal/fault/restore-slices/{id}` | （演示）把隔离区原片放回登记路径，模拟修复数据卷 |

手工调用示例：

```bash
BASE=http://localhost:8000
STUDY=$(curl -s $BASE/internal/pacs-studies | jq -r '.[0].study_instance_uid')

BID=$(curl -s -X POST $BASE/batches -H 'Content-Type: application/json' \
  -d "{\"study_instance_uid\":\"$STUDY\"}" | jq -r .id)

watch "curl -s $BASE/batches/$BID | jq"
curl -s $BASE/batches/$BID/events | jq
curl -OJ $BASE/batches/$BID/download

# 保存失败演示
curl -s -X POST $BASE/batches -H 'Content-Type: application/json' \
  -d "{\"study_instance_uid\":\"$STUDY\",\"fault_inject\":\"save_failure\"}"

# 归档恢复演示：损坏 ZIP 后下载（自动重建，可并发）；缺片后下载（409+原因）
curl -s -X POST "$BASE/internal/fault/corrupt-zip/$BID?mode=corrupt"
curl -OJ $BASE/batches/$BID/download          # 200，已重建
curl -s -X POST $BASE/internal/fault/remove-slice/$BID
curl -s -X POST "$BASE/internal/fault/corrupt-zip/$BID?mode=missing"
curl -s $BASE/batches/$BID/download | jq      # 409，detail 含“原片缺失”
curl -s $BASE/batches/$BID | jq .download_blocked_reason
curl -s -X POST $BASE/internal/fault/restore-slices/$BID   # 修复数据卷
curl -OJ $BASE/batches/$BID/download          # 200，恢复下载
```

---

## 4. 演示场景说明

模拟 PACS 支持环境变量（见 `docker-compose.yml`）：

* `MOVE_DELAY=1.0`：每个实例发送前停顿 1 秒，制造慢回传，便于观察多批交错
* `MOVE_DUPLICATE_EVERY=3`：每 3 个实例额外用一条新关联重发一次，模拟源端重试
  （接收端应回 SUCCESS 但不计数）

rogue 投送器（演示容器/API 镜像内均可运行）：

```bash
# 清单外实例：期望 0xC000 拒收
python -m simulator.rogue_sender off-list   --study <StudyUID> --host api --port 11113
# 声明 JPEG 压缩：期望关联协商被拒
python -m simulator.rogue_sender compressed --study <StudyUID> --host api --port 11113
```

保存失败演示的实现是**真实故障**而非 mock：提交时把批目录 `chmod 555`，
非 root 容器用户写入必然 `PermissionError`，可观察到：
首个实例 C-STORE 回 `0xA700` → 批立即 FAILED → 后续实例全部以“已失败”拒收
→ C-MOVE 最终 `0xA702` → 下载返回 409。

归档恢复演示（`docker compose run --rm demo` 的第 7–9 节）同样是真实故障：
通过 `/internal/fault/*` 接口直接破坏数据卷（位翻转 ZIP、截断 ZIP、
移走原片），随后观察：

* **损坏包 + 并发下载**：8 路并发下载同一个被损坏的 ZIP，全部拿到
  字节一致的完整包，事件流中 `ZIP_REBUILT` 恰好一次（single-flight）；
* **缺片**：抽走一个已登记原片并删除 ZIP 后，下载返回 409 且
  `detail` 说明“原片缺失”，批保持 `COMPLETED`、`downloadable=false`；
  调 `restore-slices`（模拟修复数据卷）后再次下载即恢复 200；
* **重启核对**：退出进程前截断完成批的 ZIP，重启后启动核对自动重建，
  未完成批按失败处理，完成批正常下载。

---

## 5. 目录结构

```
app/
  config.py      环境变量配置（AE、端口、数据卷路径、超时）
  db.py          SQLite schema、状态机、清单/登记去重、下载阻塞标记、重启恢复查询
  storage.py     非压缩 CT 校验、原子写盘+fsync、故障注入
  archive.py     ZIP 核对（条目可读+清单一致）、单飞重建、原子替换、临时包清扫
  dicom.py       C-STORE SCP 处理器、C-FIND/C-MOVE SCU
  pipeline.py    每批后台线程编排与启动恢复
  main.py        FastAPI 路由、lifespan、Swagger、演示故障注入接口
simulator/
  generate_data.py  虚构合成 CT 生成器（确定性模体像素，无真实患者信息）
  pacs.py           模拟 PACS（C-ECHO/C-FIND/C-MOVE，慢回传+重复投送）
  rogue_sender.py   清单外/压缩实例投送器
demo/
  run_demo.py       端到端演示编排（含损坏包/缺片/并发下载/重启恢复）
tests/
  test_db.py       状态机/去重/终态保护/下载阻塞标记/老库迁移单元测试
  test_storage.py  校验/原子写/保存故障单元测试
  test_archive.py  ZIP 核对/单飞重建/缺片阻塞与恢复单元测试
  test_e2e.py      真实 pynetdicom 网络栈端到端测试（13 个场景）
docker-compose.yml  Dockerfile  Dockerfile.pacs  entrypoint.sh
```

## 6. 本地测试（无需 Docker）

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -v
```

端到端测试会在临时目录启动完整的 PACS + API 真实进程对，覆盖：
正常收齐打包、并行交错、重复去重、清单外/压缩/未知 Study 拒收、
保存失败与迟到拒收、409 冲突、重启恢复、损坏包并发重建只一次、
缺片阻塞与修复后恢复、启动核对重建与临时包清扫（共 37 个测试）。

## 7. 接入真实 PACS 的注意事项

1. 在真实 PACS 上注册 AE Title `RETRIEVER` 及本服务主机/11113 端口作为
   C-MOVE 目标，并放通防火墙。
2. 用 `PACS_AET/PACS_HOST/PACS_PORT` 指向真实源端；确认其 C-FIND 支持
   Study Root IMAGE 级查询并返回 `TransferSyntaxUID`。
3. 流水线只接受未压缩 little-endian CT；若源端只存压缩 TS，需在源端配置
   转码，或扩展 `app/dicom.py` 的上下文与 `storage.UNCOMPRESSED_TS` 策略。
4. 数据卷需保证持久化挂载与足够 inode/容量；SQLite 与 DICOM 文件均在
   `DATA_DIR` 下，备份该目录即可完整备份批状态与文件。
5. 完成批的 ZIP 损坏或丢失无需人工干预：重启或下载时会自动核对并从
   已登记原片重建；若提示“原片缺失”，先恢复 `DATA_DIR/batches/` 下
   对应批目录的 DICOM 文件，再次请求下载即可。
