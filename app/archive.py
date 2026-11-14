"""已完成批 ZIP 归档的核对、单飞重建与原子替换。

规则
====
* **重启及下载前核对**：ZIP 必须可打开、全部条目 CRC 可读、
  ``MANIFEST.csv`` 中的 SOP 清单与已登记清单一致。
* **异常即重建**：ZIP 丢失 / 损坏 / 清单不一致时，从完整的已登记
  DICOM 原片重建；每批单飞（single-flight），并发下载只重建一次，
  其余请求等待后复用重建结果。
* **临时包不得对外可见**：重建写入带随机名的 ``*.part`` 临时文件，
  自检通过后才 ``os.replace`` 原子落位；下载只服务最终路径，
  启动时清扫上次崩溃可能残留的临时包。
* **已受理的有效下载不能被替换打断**：核对通过的 ZIP 绝不重建、
  绝不替换；只有损坏/缺失的归档才会被原子替换（POSIX 下 rename
  不影响已打开的文件句柄，正在流式传输的响应不受干扰）。
* **保留完成记录**：原片缺失或重建写盘失败时，批保持 COMPLETED，
  仅记录 ``download_blocked_reason`` 并拒绝下载（不返回残缺 ZIP）；
  数据卷修复后，下一次下载请求会自动重试并清除该标记。
"""
from __future__ import annotations

import logging
import os
import threading
import uuid
import zipfile
from pathlib import Path

from .config import settings
from .db import STATE_COMPLETED, Database
from .storage import fsync_dir

log = logging.getLogger("archive")

MANIFEST_HEADER = "sop_instance_uid,file_name"


class ArchiveError(Exception):
    """归档不可用：原片缺失 / 重建写盘失败 / 重建后自检不过。

    出现该错误时批保持 COMPLETED，仅标记不可下载并给出具体原因。
    """


class ArchiveStateError(Exception):
    """批状态不允许下载（非 COMPLETED）。"""


def canonical_zip_path(batch_id: str) -> Path:
    return settings.ZIP_DIR / f"{batch_id}.zip"


def verify_zip(path: Path, manifest_uids: set[str]) -> str | None:
    """核对 ZIP：条目可读且与已登记 SOP 清单一致。

    返回 ``None`` 表示核对通过；否则返回人类可读的异常原因。
    本函数是“是否可信”的判官，自身绝不抛异常。
    """
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
            if "MANIFEST.csv" not in names:
                return "ZIP 缺少 MANIFEST.csv"
            # 逐条目解压校验 => 任何条目不可读都会被发现
            bad = zf.testzip()
            if bad is not None:
                return f"ZIP 条目 {bad} CRC 校验失败（文件已损坏）"
            manifest_raw = zf.read("MANIFEST.csv").decode("utf-8")
    except zipfile.BadZipFile as exc:
        return f"ZIP 结构损坏：{exc}"
    except OSError as exc:
        return f"ZIP 读取失败：{exc}"
    except Exception as exc:  # 解压错误（zlib 等）一律视为条目不可读
        return f"ZIP 条目不可读：{exc}"

    rows = [line.strip() for line in manifest_raw.splitlines() if line.strip()]
    if not rows or rows[0] != MANIFEST_HEADER:
        return "MANIFEST.csv 表头不符"
    sop_in_zip: set[str] = set()
    file_names: set[str] = set()
    for line in rows[1:]:
        parts = line.split(",")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return f"MANIFEST.csv 行格式错误：{line!r}"
        sop_in_zip.add(parts[0])
        file_names.add(parts[1])
    if sop_in_zip != set(manifest_uids):
        missing = sorted(set(manifest_uids) - sop_in_zip)
        extra = sorted(sop_in_zip - set(manifest_uids))
        return (
            f"ZIP 清单与已登记 SOP 不一致（缺 {len(missing)} 个"
            f"{'，示例 ' + '、'.join(missing[:3]) if missing else ''}；"
            f"多 {len(extra)} 个）"
        )
    dicom_entries = {
        n for n in names if n.startswith("dicom/") and not n.endswith("/")
    }
    if len(dicom_entries) != len(manifest_uids):
        return (
            f"ZIP 内 DICOM 条目数 {len(dicom_entries)} "
            f"与已登记数 {len(manifest_uids)} 不符"
        )
    for name in file_names:
        if f"dicom/{name}" not in dicom_entries:
            return f"ZIP 缺少清单声明的文件 dicom/{name}"
    return None


class ArchiveService:
    """归档核对与重建服务（每批单飞）。"""

    def __init__(self, db: Database) -> None:
        self.db = db
        self._guard = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    def _lock_for(self, batch_id: str) -> threading.Lock:
        with self._guard:
            lock = self._locks.get(batch_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[batch_id] = lock
            return lock

    # ------------------------------------------------------------ 公开入口
    def ensure_package(self, batch_id: str, *, trigger: str) -> Path:
        """核对并（必要时）重建归档，返回可下载的最终 ZIP 路径。

        * 批非 COMPLETED —— :class:`ArchiveStateError`
        * 归档损坏/缺失且无法重建（原片缺失、写盘失败）——
          :class:`ArchiveError`，同时把具体原因记入批的
          ``download_blocked_reason``（保留完成记录，不返回残缺 ZIP）。
        """
        row = self.db.get_batch(batch_id)
        if row is None:
            raise ArchiveStateError("批不存在")
        if row["state"] != STATE_COMPLETED:
            raise ArchiveStateError(
                f"批状态 {row['state']} 不可下载；"
                "仅 C-MOVE 成功且清单收齐的批可下载"
            )

        path = canonical_zip_path(batch_id)
        reason = self._invalid_reason(batch_id, path)
        if reason is None:
            # 有效归档：绝不重建/替换；清除可能存在的旧阻塞标记
            self.db.clear_download_blocked(batch_id)
            return path

        # 需要重建：每批单飞，并发下载只重建一次
        lock = self._lock_for(batch_id)
        with lock:
            # 拿到锁后复核：排在后面的请求直接复用已重建结果
            reason = self._invalid_reason(batch_id, path)
            if reason is None:
                self.db.clear_download_blocked(batch_id)
                return path
            self.db.add_event(
                "ZIP_INVALID",
                f"归档核对未通过（{trigger}）：{reason}，开始从已登记原片重建",
                batch_id,
            )
            try:
                self._rebuild(batch_id, path)
            except ArchiveError as exc:
                self.db.set_download_blocked(batch_id, str(exc))
                self.db.add_event("ZIP_REBUILD_FAILED", str(exc), batch_id)
                log.warning("批 %s 归档重建失败（%s）：%s", batch_id, trigger, exc)
                raise
            self.db.clear_download_blocked(batch_id)
            self.db.add_event(
                "ZIP_REBUILT",
                f"ZIP 已重建并通过核对（{trigger}；原异常：{reason}）",
                batch_id,
            )
            log.info("批 %s 归档已重建（%s）", batch_id, trigger)
            return path

    def build_package(self, batch_id: str) -> Path:
        """完成时首次打包（流水线单线程调用，无需单飞锁）。"""
        path = canonical_zip_path(batch_id)
        self._rebuild(batch_id, path)
        return path

    # ------------------------------------------------------------ 内部实现
    def _invalid_reason(self, batch_id: str, path: Path) -> str | None:
        if not path.exists():
            return "ZIP 文件缺失（数据卷可能已损坏）"
        return verify_zip(path, self.db.manifest_uids(batch_id))

    def _rebuild(self, batch_id: str, path: Path) -> None:
        """从已登记原片重建 ZIP：唯一临时文件 -> fsync -> 自检 -> 原子落位。

        任一步失败都清理临时文件并抛 :class:`ArchiveError`，
        绝不留下对外可见的残缺包，也绝不以坏包替换现有归档。
        """
        files = self.db.received_files(batch_id)
        manifest = self.db.manifest_uids(batch_id)
        unregistered = sorted(manifest - {sop for sop, _ in files})
        if unregistered:
            raise ArchiveError(
                f"登记记录不完整：{len(unregistered)} 个 SOP 无落盘记录"
                f"（示例：{'、'.join(unregistered[:3])}），无法重建"
            )
        absent = [(sop, p) for sop, p in files if not Path(p).is_file()]
        if absent:
            sample = "、".join(sop for sop, _ in absent[:3])
            raise ArchiveError(
                f"原片缺失：{len(absent)} 个已登记 DICOM 不在数据卷"
                f"（示例 SOP：{sample}），请修复数据卷后重试"
            )

        # 随机名临时包：并发/重试不互相覆盖，且对外不可见
        tmp = settings.ZIP_DIR / f"{batch_id}.{uuid.uuid4().hex}.part"
        try:
            with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
                lines = [MANIFEST_HEADER]
                used_names: set[str] = set()
                for sop_uid, file_path in files:
                    src = Path(file_path)
                    name = src.name
                    if name in used_names:  # 极端重名兜底
                        name = f"{sop_uid.replace('.', '_')}.dcm"
                    used_names.add(name)
                    zf.write(src, arcname=f"dicom/{name}")
                    lines.append(f"{sop_uid},{name}")
                zf.writestr("MANIFEST.csv", "\n".join(lines) + "\n")
            # 落盘确认，避免“回复成功但数据未写入卷”
            with open(tmp, "rb") as fh:
                os.fsync(fh.fileno())
            # 落位前自检：绝不以残缺包替换现有归档
            err = verify_zip(tmp, manifest)
            if err:
                raise ArchiveError(f"重建后自检未通过：{err}")
            os.replace(tmp, path)  # 原子替换；已打开的读取句柄不受影响
            fsync_dir(settings.ZIP_DIR)
        except ArchiveError:
            tmp.unlink(missing_ok=True)
            raise
        except OSError as exc:
            tmp.unlink(missing_ok=True)
            raise ArchiveError(f"重建写盘失败：{exc}") from exc


def sweep_stale_tempfiles() -> int:
    """清理 ZIP 目录中上次崩溃残留的临时包（启动时调用）。"""
    removed = 0
    for stale in settings.ZIP_DIR.glob("*.part"):
        try:
            stale.unlink()
            removed += 1
            log.info("清理残留临时包 %s", stale)
        except OSError:
            log.warning("无法清理残留临时包 %s", stale)
    return removed
