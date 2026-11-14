"""文件落盘、DICOM 校验与故障注入。

ZIP 归档的核对/重建/原子替换见 :mod:`app.archive`。
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

import pydicom
from pydicom.uid import ImplicitVRLittleEndian, ExplicitVRLittleEndian
from pynetdicom.sop_class import CTImageStorage

from .config import settings

log = logging.getLogger("storage")

# 只接受未压缩（原始 little-endian）传输语法
UNCOMPRESSED_TS = {
    str(ImplicitVRLittleEndian),
    str(ExplicitVRLittleEndian),
}

# 故障注入模式
FAULT_SAVE = "save_failure"          # 本批首个实例落盘即失败


class StorageError(Exception):
    """保存阶段失败（C-STORE 必须回失败状态）。"""


class RejectedError(Exception):
    """实例被拒收（非 CT / 压缩 / 不属于任何批 / 清单外 / 已失败批）。

    拒收不改变批状态；save_failure 等真正的保存异常才会置批失败。
    """


def batch_dir(batch_id: str) -> Path:
    d = settings.BATCHES_DIR / batch_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def make_batch_readonly(batch_id: str) -> None:
    """保存故障演示：去掉批目录写权限，使后续所有写入抛 PermissionError。

    容器以非 root 用户运行，chmod 555 后 open(...,'wb') 必然失败，
    是真实的磁盘写入失败而非模拟返回值。
    """
    d = batch_dir(batch_id)
    os.chmod(d, 0o555)


def restore_batch_writable(batch_id: str) -> None:
    d = settings.BATCHES_DIR / batch_id
    if d.exists():
        os.chmod(d, 0o755)


def validate_dataset(ds: pydicom.Dataset, ts: str) -> None:
    """只收非压缩 CT；缺关键标识也拒收。"""
    sop_class = str(getattr(ds, "SOPClassUID", ""))
    if sop_class != str(CTImageStorage):
        raise RejectedError(f"非 CT 图像（SOPClassUID={sop_class}），拒收")
    if ts not in UNCOMPRESSED_TS:
        raise RejectedError(f"压缩传输语法 {ts}，仅收未压缩 CT，拒收")
    for tag in ("StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID"):
        if not getattr(ds, tag, ""):
            raise RejectedError(f"缺少 {tag}，拒收")


def safe_filename(ds: pydicom.Dataset) -> str:
    series = str(ds.SeriesInstanceUID).replace(".", "_")
    sop = str(ds.SOPInstanceUID).replace(".", "_")
    return f"{series}__{sop}.dcm"


def write_dataset(batch_id: str, ds: pydicom.Dataset, ts: str) -> Path:
    """原子化落盘：先写临时文件并 fsync，再 rename，最后登记。

    任一步失败都清理残留文件并抛 :class:`StorageError`，由 C-STORE 处理器
    回失败状态并把批置为 FAILED。
    """
    d = batch_dir(batch_id)
    final = d / safe_filename(ds)
    tmp = d / (final.name + ".part")
    try:
        # pynetdicom 的 event.dataset 不带 file_meta，需显式带上协商得到的 TS
        if not hasattr(ds, "file_meta") or ds.file_meta is None:
            ds.file_meta = pydicom.dataset.FileMetaDataset()
        ds.file_meta.MediaStorageSOPClassUID = ds.SOPClassUID
        ds.file_meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
        ds.file_meta.TransferSyntaxUID = pydicom.uid.UID(ts)
        ds.save_as(str(tmp), enforce_file_format=True)
        # 强制刷盘，避免“回复成功但数据未落盘”
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, final)
        fsync_dir(d)
        return final
    except (OSError, PermissionError) as exc:
        for p in (tmp, final):
            try:
                p.unlink()
            except OSError:
                pass
        raise StorageError(f"文件写入失败：{exc}") from exc


def fsync_dir(d: Path) -> None:
    try:
        fd = os.open(str(d), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass
