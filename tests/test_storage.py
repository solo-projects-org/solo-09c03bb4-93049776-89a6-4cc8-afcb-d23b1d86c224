"""storage 层：非压缩 CT 校验、落盘与保存故障（ZIP 归档见 test_archive.py）。"""
from __future__ import annotations

import pytest
from pydicom.uid import (
    ExplicitVRLittleEndian,
    ImplicitVRLittleEndian,
    JPEGBaseline8Bit,
    MRImageStorage,
)

from app import storage
from app.config import Settings
from app.db import Database, set_db
from simulator.generate_data import build_ct_dataset


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


def _ds(sop="1.2.3.4", ts=ExplicitVRLittleEndian):
    return build_ct_dataset(
        study_uid="1.2.3.9", series_uid="1.2.3.8", sop_uid=sop,
        patient_id="P1", patient_name="T^T", instance_number=1,
        instance_count=1,
    ), str(ts)


def test_validate_accepts_uncompressed_ct(workspace):
    ds, ts = _ds(ts=ImplicitVRLittleEndian)
    storage.validate_dataset(ds, ts)  # 不抛即通过


def test_validate_rejects_compressed_and_non_ct(workspace):
    ds, _ = _ds()
    with pytest.raises(storage.RejectedError):
        storage.validate_dataset(ds, str(JPEGBaseline8Bit))

    ds2, ts2 = _ds()
    ds2.SOPClassUID = str(MRImageStorage)
    ds2.file_meta.MediaStorageSOPClassUID = str(MRImageStorage)
    with pytest.raises(storage.RejectedError):
        storage.validate_dataset(ds2, ts2)


def test_write_dataset_atomic_and_registered(workspace):
    data, db = workspace
    db.create_batch("b1", "1.2.3.9", "")
    db.transition_state("b1", ["PENDING"], "FINDING")
    db.set_manifest("b1", [
        ("1.2.3.4", "1.2.840.10008.5.1.4.1.1.2", str(ExplicitVRLittleEndian)),
    ])
    db.transition_state("b1", ["FINDING"], "MOVING")

    ds, ts = _ds()
    path = storage.write_dataset("b1", ds, ts)
    assert path.exists()
    assert not list((data / "batches" / "b1").glob("*.part"))
    assert db.register_instance("b1", "1.2.3.4", str(path), ts)


def test_write_failure_on_readonly_dir(workspace):
    data, db = workspace
    db.create_batch("b1", "1.2.3.9", "")
    storage.make_batch_readonly("b1")
    ds, ts = _ds()
    with pytest.raises(storage.StorageError):
        storage.write_dataset("b1", ds, ts)
    # 无 .part 残留
    assert not list((data / "batches" / "b1").glob("*.part"))
    storage.restore_batch_writable("b1")
