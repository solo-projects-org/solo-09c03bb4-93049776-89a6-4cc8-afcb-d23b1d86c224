"""archive 层：ZIP 核对、单飞重建、原子替换、缺片阻塞与恢复。"""
from __future__ import annotations

import threading
import zipfile
from pathlib import Path

import pytest
from pydicom.uid import CTImageStorage, ExplicitVRLittleEndian

from app import storage
from app.archive import (
    ArchiveError,
    ArchiveService,
    ArchiveStateError,
    canonical_zip_path,
    sweep_stale_tempfiles,
    verify_zip,
)
from app.config import Settings
from app.db import Database, set_db
from simulator.generate_data import build_ct_dataset

CT = str(CTImageStorage)
TS = str(ExplicitVRLittleEndian)
SOPS = ("1.2.3.11", "1.2.3.12", "1.2.3.13")


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    data = tmp_path / "data"
    monkeypatch.setattr(Settings, "DATA_DIR", data)
    monkeypatch.setattr(Settings, "DB_PATH", data / "retrieval.db")
    monkeypatch.setattr(Settings, "BATCHES_DIR", data / "batches")
    monkeypatch.setattr(Settings, "ZIP_DIR", data / "zips")
    Settings.ensure_dirs()
    db = Database(Settings.DB_PATH)
    set_db(db)
    yield data, db
    db.close()


def _completed_batch(db: Database, batch_id: str = "b1") -> None:
    """造一个 COMPLETED 批：清单 3 个 SOP，全部落盘登记，ZIP 已打包。"""
    db.create_batch(batch_id, "1.2.3.9", "")
    db.transition_state(batch_id, ["PENDING"], "FINDING")
    db.set_manifest(batch_id, [(sop, CT, TS) for sop in SOPS])
    db.transition_state(batch_id, ["FINDING"], "MOVING")
    for i, sop in enumerate(SOPS, start=1):
        ds = build_ct_dataset(
            study_uid="1.2.3.9", series_uid="1.2.3.8", sop_uid=sop,
            patient_id="P1", patient_name="T^T", instance_number=i,
            instance_count=len(SOPS),
        )
        path = storage.write_dataset(batch_id, ds, TS)
        assert db.register_instance(batch_id, sop, str(path), TS)
    svc = ArchiveService(db)
    zip_path = svc.build_package(batch_id)
    assert db.complete_batch(batch_id, str(zip_path))


def _corrupt_zip(path: Path) -> None:
    buf = bytearray(path.read_bytes())
    start = len(buf) // 3
    for i in range(start, min(len(buf), start + 128)):
        buf[i] ^= 0xFF
    path.write_bytes(bytes(buf))


# ------------------------------------------------------------------ verify
def test_verify_ok_on_fresh_package(workspace):
    _, db = workspace
    _completed_batch(db)
    assert verify_zip(canonical_zip_path("b1"), db.manifest_uids("b1")) is None


def test_verify_detects_garbage_and_truncation(workspace, tmp_path):
    _, db = workspace
    manifest = set(SOPS)

    garbage = tmp_path / "garbage.zip"
    garbage.write_bytes(b"this is not a zip file at all")
    assert "结构损坏" in verify_zip(garbage, manifest)

    _completed_batch(db)
    good = canonical_zip_path("b1").read_bytes()
    truncated = tmp_path / "truncated.zip"
    truncated.write_bytes(good[: len(good) // 3])
    assert verify_zip(truncated, manifest) is not None


def test_verify_detects_crc_damage(workspace):
    _, db = workspace
    _completed_batch(db)
    path = canonical_zip_path("b1")
    _corrupt_zip(path)
    reason = verify_zip(path, db.manifest_uids("b1"))
    assert reason is not None  # 损坏必须被核对发现（CRC/解压/结构任一）
    assert reason.startswith("ZIP")


def test_verify_detects_manifest_mismatch(workspace, tmp_path):
    _, db = workspace
    _completed_batch(db)
    src = canonical_zip_path("b1")

    # 清单里少一个 SOP（其余条目原样保留）
    altered = tmp_path / "altered.zip"
    with zipfile.ZipFile(src) as zin, \
            zipfile.ZipFile(altered, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            if name == "MANIFEST.csv":
                lines = zin.read(name).decode().splitlines()
                zout.writestr(name, "\n".join(lines[:2]) + "\n")
            else:
                zout.writestr(name, zin.read(name))
    reason = verify_zip(altered, db.manifest_uids("b1"))
    assert "不一致" in reason

    # 缺一个 dicom 条目但清单行还在
    altered2 = tmp_path / "altered2.zip"
    dropped = False
    with zipfile.ZipFile(src) as zin, \
            zipfile.ZipFile(altered2, "w", zipfile.ZIP_DEFLATED) as zout:
        for name in zin.namelist():
            if not dropped and name.startswith("dicom/"):
                dropped = True
                continue
            zout.writestr(name, zin.read(name))
    reason2 = verify_zip(altered2, db.manifest_uids("b1"))
    assert reason2 is not None


# ------------------------------------------------------------------ ensure
def test_ensure_returns_valid_zip_without_rebuild(workspace):
    _, db = workspace
    _completed_batch(db)
    svc = ArchiveService(db)
    before = canonical_zip_path("b1").read_bytes()
    path = svc.ensure_package("b1", trigger="测试")
    assert path.read_bytes() == before  # 有效归档绝不被替换
    kinds = [e["kind"] for e in db.list_events("b1")]
    assert "ZIP_REBUILT" not in kinds and "ZIP_INVALID" not in kinds


def test_ensure_rebuilds_corrupt_zip_once_under_concurrency(workspace):
    _, db = workspace
    _completed_batch(db)
    _corrupt_zip(canonical_zip_path("b1"))
    svc = ArchiveService(db)

    results: list[Path] = []
    errors: list[Exception] = []

    def _worker():
        try:
            results.append(svc.ensure_package("b1", trigger="并发测试"))
        except Exception as exc:  # pragma: no cover - 失败路径由断言暴露
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors
    assert len(results) == 10
    path = canonical_zip_path("b1")
    assert verify_zip(path, db.manifest_uids("b1")) is None
    kinds = [e["kind"] for e in db.list_events("b1")]
    assert kinds.count("ZIP_REBUILT") == 1  # 并发只重建一次
    assert kinds.count("ZIP_INVALID") == 1
    # 无临时包残留
    assert not list(Settings.ZIP_DIR.glob("*.part"))


def test_ensure_rebuilds_missing_zip(workspace):
    _, db = workspace
    _completed_batch(db)
    canonical_zip_path("b1").unlink()
    svc = ArchiveService(db)
    path = svc.ensure_package("b1", trigger="测试")
    assert path.exists()
    assert verify_zip(path, db.manifest_uids("b1")) is None


def test_missing_slice_blocks_but_keeps_completed_and_recovers(workspace):
    data, db = workspace
    _completed_batch(db)
    svc = ArchiveService(db)

    # 抽走一个原片 + 删除 ZIP（模拟数据卷损坏）
    sop, file_path = db.received_files("b1")[0]
    victim = Path(file_path)
    saved = victim.read_bytes()
    victim.unlink()
    canonical_zip_path("b1").unlink()

    with pytest.raises(ArchiveError, match="原片缺失"):
        svc.ensure_package("b1", trigger="测试")
    row = db.get_batch("b1")
    assert row["state"] == "COMPLETED"  # 完成记录保留
    assert "原片缺失" in row["download_blocked_reason"]
    assert not canonical_zip_path("b1").exists()  # 不产出残缺 ZIP
    assert not list(Settings.ZIP_DIR.glob("*.part"))  # 临时包已清理

    # 修复数据卷后再次请求可恢复
    victim.write_bytes(saved)
    path = svc.ensure_package("b1", trigger="测试")
    assert verify_zip(path, db.manifest_uids("b1")) is None
    row = db.get_batch("b1")
    assert row["download_blocked_reason"] is None


def test_rebuild_write_failure_blocks_with_reason(workspace):
    _, db = workspace
    _completed_batch(db)
    canonical_zip_path("b1").unlink()
    svc = ArchiveService(db)

    from unittest import mock

    with mock.patch.object(
        zipfile, "ZipFile", side_effect=OSError("disk full (injected)")
    ):
        with pytest.raises(ArchiveError, match="重建写盘失败"):
            svc.ensure_package("b1", trigger="测试")

    row = db.get_batch("b1")
    assert row["state"] == "COMPLETED"
    assert "重建写盘失败" in row["download_blocked_reason"]
    assert not list(Settings.ZIP_DIR.glob("*.part"))
    # 故障解除后可恢复
    path = svc.ensure_package("b1", trigger="测试")
    assert verify_zip(path, db.manifest_uids("b1")) is None
    assert db.get_batch("b1")["download_blocked_reason"] is None


def test_ensure_rejects_non_completed_batch(workspace):
    _, db = workspace
    db.create_batch("b-moving", "1.2.3.9", "")
    svc = ArchiveService(db)
    with pytest.raises(ArchiveStateError):
        svc.ensure_package("b-moving", trigger="测试")
    with pytest.raises(ArchiveStateError):
        svc.ensure_package("no-such", trigger="测试")


def test_sweep_stale_tempfiles(workspace):
    _, db = workspace
    _completed_batch(db)
    stale = Settings.ZIP_DIR / "b1.deadbeef.part"
    stale.write_bytes(b"residue")
    keep = canonical_zip_path("b1")
    before = keep.read_bytes()
    assert sweep_stale_tempfiles() == 1
    assert not stale.exists()
    assert keep.read_bytes() == before  # 正式包不受影响
