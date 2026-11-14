"""FastAPI 入口：批管理 REST API + Swagger 文档 + DICOM 服务生命周期。"""
from __future__ import annotations

import logging
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from . import dicom, storage
from .archive import ArchiveError, ArchiveService, ArchiveStateError
from .config import settings
from .db import (
    STATE_COMPLETED,
    Database,
    set_db,
)
from .pipeline import Pipeline, recover_on_startup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
)
log = logging.getLogger("api")

# 进程级单例（lifespan 中初始化）
db: Database | None = None
pipeline: Pipeline | None = None
archive: ArchiveService | None = None
_scp_server = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, pipeline, archive, _scp_server
    settings.ensure_dirs()
    db = Database(settings.DB_PATH)
    set_db(db)
    archive = ArchiveService(db)
    recover_on_startup(db, archive)
    _scp_server = dicom.start_store_scp(db)
    pipeline = Pipeline(db, archive)
    log.info("API 就绪，数据卷=%s", settings.DATA_DIR)
    try:
        yield
    finally:
        if _scp_server is not None:
            _scp_server.shutdown()
        if db is not None:
            db.close()


app = FastAPI(
    title="DICOM CT 离线取数流水线",
    version="1.1.0",
    lifespan=lifespan,
    description=(
        "提交 StudyInstanceUID → C-FIND 固定 SOP 清单 → C-MOVE 回传 → "
        "C-STORE 接收非压缩 CT → 核对收齐后打包 ZIP 下载。\n\n"
        "规则：清单外实例不入包、重复实例不重复计数、保存失败即批失败、"
        "失败批迟到回传不可恢复、重启后未完成批判失败。\n\n"
        "归档恢复：重启及每次下载前核对 ZIP（条目可读 + 与已登记 SOP 清单一致），"
        "损坏/丢失即从已登记原片单飞重建（并发只重建一次，临时包不可见，"
        "原子替换不打断有效下载）；原片缺失或重建失败时保留完成记录、"
        "标记不可下载及具体原因，修复数据卷后再次请求即可恢复。"
    ),
)


# ------------------------------------------------------------------ schemas
class BatchCreate(BaseModel):
    study_instance_uid: str = Field(
        ..., description="源端检查的 StudyInstanceUID",
        examples=["1.2.840.10008.1.2.99.1"],
    )
    fault_inject: str = Field(
        default="",
        description="可选故障注入：save_failure=保存失败演示；留空为正常流程",
    )


class BatchView(BaseModel):
    id: str
    study_instance_uid: str
    state: str
    expected_count: int
    received_count: int
    failure_reason: str | None
    zip_path: str | None
    download_blocked_reason: str | None = Field(
        None,
        description="完成批暂不可下载的具体原因（原片缺失/重建写盘失败）；"
        "修复数据卷后再次请求下载即可恢复",
    )
    fault_inject: str
    created_at: str
    updated_at: str
    downloadable: bool = Field(
        ..., description="当前可下载：COMPLETED 且无未恢复的归档异常"
    )


def _view(row: dict) -> BatchView:
    return BatchView(
        id=row["id"],
        study_instance_uid=row["study_instance_uid"],
        state=row["state"],
        expected_count=row["expected_count"],
        received_count=row["received_count"],
        failure_reason=row["failure_reason"],
        zip_path=row["zip_path"],
        download_blocked_reason=row["download_blocked_reason"],
        fault_inject=row["fault_inject"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        downloadable=(
            row["state"] == STATE_COMPLETED
            and not row["download_blocked_reason"]
        ),
    )


# ------------------------------------------------------------------ routes
@app.post("/batches", response_model=BatchView, status_code=201,
          tags=["batches"], summary="提交检查，启动取数批")
def create_batch(body: BatchCreate) -> BatchView:
    if body.fault_inject not in ("", storage.FAULT_SAVE):
        raise HTTPException(400, "fault_inject 只支持 '' 或 'save_failure'")
    uid = body.study_instance_uid.strip()
    if not uid:
        raise HTTPException(400, "study_instance_uid 不能为空")

    if db.has_active_batch_for_study(uid):
        raise HTTPException(409, "该 Study 已有进行中的批，请等待其结束")

    batch_id = uuid.uuid4().hex
    db.create_batch(batch_id, uid, body.fault_inject)
    pipeline.submit(batch_id)
    return _view(db.get_batch(batch_id))


@app.get("/batches", response_model=list[BatchView],
         tags=["batches"], summary="列出全部批")
def list_batches() -> list[BatchView]:
    return [_view(r) for r in db.list_batches()]


@app.get("/batches/{batch_id}", response_model=BatchView,
         tags=["batches"], summary="查询批状态与进度")
def get_batch(batch_id: str) -> BatchView:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    return _view(row)


@app.get("/batches/{batch_id}/events",
         tags=["batches"], summary="查询批事件流水（接收/重复/拒收/状态迁移）")
def get_events(batch_id: str) -> list[dict]:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    return db.list_events(batch_id)


@app.get("/batches/{batch_id}/instances",
         tags=["batches"], summary="查询批清单及每个 SOP 的接收情况")
def get_instances(batch_id: str) -> dict:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    files = db.received_files(batch_id)
    received = {sop: path for sop, path in files}
    return {
        "batch_id": batch_id,
        "state": row["state"],
        "manifest": sorted(db.manifest_uids(batch_id)),
        "received": sorted(received),
        "missing": db.missing_uids(batch_id),
    }


@app.get("/batches/{batch_id}/download",
         tags=["batches"],
         summary="下载 ZIP（下载前核对归档，损坏/丢失自动从登记原片重建）",
         responses={
             200: {"content": {"application/zip": {}},
                   "description": "核对通过（或已重建）的完整 ZIP"},
             404: {"description": "批不存在"},
             409: {"description": "批未到 COMPLETED，或归档不可用"
                                  "（原片缺失/重建写盘失败，detail 含具体原因）"},
         })
def download_zip(batch_id: str) -> FileResponse:
    row = db.get_batch(batch_id)
    if row is None:
        raise HTTPException(404, "批不存在")
    try:
        # 下载前核对：ZIP 条目可读且与已登记 SOP 清单一致；
        # 异常时单飞重建；并发下载只重建一次，有效下载不被替换打断。
        path = archive.ensure_package(batch_id, trigger="下载请求")
    except ArchiveStateError as exc:
        raise HTTPException(409, str(exc))
    except ArchiveError as exc:
        # 保留完成记录，但不可下载；绝不返回残缺 ZIP
        raise HTTPException(409, f"归档暂不可下载：{exc}")
    return FileResponse(
        path, media_type="application/zip",
        filename=f"{batch_id}.zip",
    )


@app.get("/health", tags=["system"], summary="健康检查")
def health() -> dict:
    return {"status": "ok", "store_port": settings.STORE_PORT,
            "ae_title": settings.APP_AET}


# ------------------------------------------------------------------ debug
@app.get("/internal/pacs-studies", tags=["internal"],
         summary="（演示用）STUDY 级 C-FIND 列出模拟 PACS 中的检查")
def pacs_studies() -> list[dict]:
    from pydicom.dataset import Dataset
    from pydicom.uid import ExplicitVRLittleEndian, ImplicitVRLittleEndian
    from pynetdicom import AE
    from pynetdicom.presentation import build_context
    from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

    ae = AE(ae_title=settings.APP_AET)
    ae.requested_contexts = [build_context(
        str(StudyRootQueryRetrieveInformationModelFind),
        [str(ExplicitVRLittleEndian), str(ImplicitVRLittleEndian)])]
    assoc = ae.associate(settings.PACS_HOST, settings.PACS_PORT,
                         ae_title=settings.PACS_AET)
    if not assoc.is_established:
        raise HTTPException(502, "无法连接模拟 PACS")
    try:
        q = Dataset()
        q.QueryRetrieveLevel = "STUDY"
        q.StudyInstanceUID = ""
        q.PatientID = ""
        q.PatientName = ""
        q.Modality = ""
        q.NumberOfStudyRelatedInstances = ""
        out = []
        for status, ident in assoc.send_c_find(
            q, str(StudyRootQueryRetrieveInformationModelFind)
        ):
            if ident is None:
                continue
            out.append({
                "study_instance_uid": str(ident.StudyInstanceUID),
                "patient_id": str(getattr(ident, "PatientID", "")),
                "patient_name": str(getattr(ident, "PatientName", "")),
                "modality": str(getattr(ident, "Modality", "")),
                "instances": int(getattr(ident, "NumberOfStudyRelatedInstances", 0) or 0),
            })
        return out
    finally:
        if assoc.is_alive:
            assoc.release()


@app.post("/internal/suicide", tags=["internal"],
          summary="（演示用）退出进程以演示重启恢复；compose 自动重启")
def suicide(tasks: BackgroundTasks) -> dict:
    def _die() -> None:
        os._exit(1)
    tasks.add_task(_die)
    return {"detail": "进程即将退出，等待 Docker 重启"}


# ------------------------------------------------------- 归档故障注入（演示）
def _quarantine_dir(batch_id: str) -> Path:
    return settings.DATA_DIR / "quarantine" / batch_id


@app.post("/internal/fault/corrupt-zip/{batch_id}", tags=["internal"],
          summary="（演示用）损坏/删除已完成批的 ZIP，模拟数据卷归档损坏")
def fault_corrupt_zip(batch_id: str, mode: str = "corrupt") -> dict:
    """``mode``: corrupt=中部位翻转（条目 CRC 损坏）、truncate=截断
    （结构损坏）、missing=删除（归档丢失）。"""
    if mode not in ("corrupt", "truncate", "missing"):
        raise HTTPException(400, "mode 只支持 corrupt / truncate / missing")
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    path = settings.ZIP_DIR / f"{batch_id}.zip"
    if not path.exists():
        raise HTTPException(404, "ZIP 不存在，无需注入")
    if mode == "missing":
        path.unlink()
        detail = "ZIP 已删除（模拟数据卷丢失归档）"
    elif mode == "truncate":
        data = path.read_bytes()
        path.write_bytes(data[: max(1, len(data) // 3)])
        detail = "ZIP 已截断（模拟结构损坏）"
    else:
        buf = bytearray(path.read_bytes())
        start = max(0, len(buf) // 3)
        for i in range(start, min(len(buf), start + 256)):
            buf[i] ^= 0xFF
        path.write_bytes(bytes(buf))
        detail = "ZIP 已位翻转（模拟条目 CRC 损坏）"
    db.add_event("FAULT_ZIP_CORRUPTED", f"演示故障注入：{detail}", batch_id)
    return {"detail": detail}


@app.post("/internal/fault/remove-slice/{batch_id}", tags=["internal"],
          summary="（演示用）把一个已登记原片移入隔离区，模拟数据卷缺片")
def fault_remove_slice(batch_id: str) -> dict:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    files = db.received_files(batch_id)
    if not files:
        raise HTTPException(409, "批内没有已登记原片")
    quarantine = _quarantine_dir(batch_id)
    quarantine.mkdir(parents=True, exist_ok=True)
    for sop_uid, file_path in files:
        src = Path(file_path)
        if src.is_file():
            dst = quarantine / src.name
            os.replace(src, dst)
            db.add_event(
                "FAULT_SLICE_REMOVED",
                f"演示故障注入：原片 {src.name} 已移入隔离区（模拟缺片）",
                batch_id,
            )
            return {"removed_sop": sop_uid, "quarantined_to": str(dst)}
    raise HTTPException(409, "没有可移除的原片（可能已被移除）")


@app.post("/internal/fault/restore-slices/{batch_id}", tags=["internal"],
          summary="（演示用）把隔离区原片放回登记路径，模拟修复数据卷")
def fault_restore_slices(batch_id: str) -> dict:
    if db.get_batch(batch_id) is None:
        raise HTTPException(404, "批不存在")
    quarantine = _quarantine_dir(batch_id)
    registered = {Path(p).name: p for _, p in db.received_files(batch_id)}
    restored: list[str] = []
    if quarantine.exists():
        for item in sorted(quarantine.iterdir()):
            target = registered.get(item.name)
            if target is None:
                continue
            os.replace(item, target)
            restored.append(item.name)
    if restored:
        db.add_event(
            "FAULT_SLICE_RESTORED",
            f"演示故障恢复：{len(restored)} 个原片已放回数据卷",
            batch_id,
        )
    try:
        quarantine.rmdir()
    except OSError:
        pass
    return {"restored": restored}
