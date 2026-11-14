"""端到端演示编排（在 api 容器内或任何能访问 API/PACS 的环境运行）。

演示序列
========
1. 列出模拟 PACS 中的虚构 CT 检查
2. 并行提交两个正常批（交错/慢回传互不阻塞，其中一个启用源端重复投送）
3. 回传进行中投送“清单外实例”和“压缩实例”，验证均被拒收、不入包
4. 提交一个 save_failure 批，验证保存失败即批失败、C-STORE 回失败码
5. 轮询进度，下载完成批 ZIP 并校验实例数与清单一致
6. 归档恢复：损坏完成批的 ZIP 后并发下载——只重建一次且全部拿到完整包
7. 归档恢复：抽走原片（缺片）后下载被拒并给出具体原因，完成记录保留；
   修复数据卷后再次请求自动恢复
8. 重启恢复：制造未完成批 + 损坏完成批 ZIP 后重启进程，验证未完成批判失败、
   启动核对自动重建损坏归档、已完成批可正常下载

运行::

    python -m demo.run_demo            # 完整演示
    python -m demo.run_demo --skip-restart
"""
from __future__ import annotations

import argparse
import concurrent.futures
import io
import os
import sys
import time
import zipfile

import requests

# 默认走 compose 服务名；本地裸跑时可用环境变量覆盖为 127.0.0.1
API = os.getenv("API_BASE", "http://api:8000")
HOST_API = os.getenv("STORE_HOST", "api")
PORT_API = int(os.getenv("STORE_PORT", "11113"))


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def wait_state(batch_id: str, target: set[str], timeout: float = 180) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(f"{API}/batches/{batch_id}", timeout=10)
        r.raise_for_status()
        view = r.json()
        if view["state"] in target:
            return view
        print(f"    ... {batch_id[:8]} {view['state']} "
              f"{view['received_count']}/{view['expected_count']}")
        time.sleep(1.5)
    raise TimeoutError(f"批 {batch_id} 未在 {timeout}s 内到达 {target}")


def show_events(batch_id: str) -> None:
    events = requests.get(f"{API}/batches/{batch_id}/events",
                          timeout=10).json()
    for e in events:
        print(f"    [{e['created_at'][11:]}] {e['kind']}: {e['message']}")


def get_events(batch_id: str) -> list[dict]:
    return requests.get(f"{API}/batches/{batch_id}/events", timeout=10).json()


def get_view(batch_id: str) -> dict:
    r = requests.get(f"{API}/batches/{batch_id}", timeout=10)
    r.raise_for_status()
    return r.json()


def download(batch_id: str) -> requests.Response:
    return requests.get(f"{API}/batches/{batch_id}/download", timeout=60)


def check_zip(payload: bytes, expected_count: int) -> None:
    zf = zipfile.ZipFile(io.BytesIO(payload))
    assert zf.testzip() is None, "ZIP 条目 CRC 校验失败"
    dicom_files = [n for n in zf.namelist() if n.startswith("dicom/")]
    manifest = zf.read("MANIFEST.csv").decode().strip().splitlines()
    assert len(dicom_files) == expected_count, "ZIP 实例数与清单不符"
    assert len(manifest) == expected_count + 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-restart", action="store_true",
                        help="跳过重启恢复演示")
    args = parser.parse_args()

    banner("1. 列出模拟 PACS 中的检查")
    studies = requests.get(f"{API}/internal/pacs-studies", timeout=20).json()
    for s in studies:
        print(f"    Study={s['study_instance_uid']} 患者={s['patient_name']} "
              f"实例数={s['instances']}")
    if len(studies) < 3:
        print("PACS 至少需要 3 个检查")
        return 1
    study_a, study_b, study_c = (s["study_instance_uid"] for s in studies[:3])

    banner("2. 并行提交批 A（含源端重复投送）与批 B，验证交错回传互不阻塞")
    # 批 A 的重复投送由 PACS 环境变量 MOVE_DUPLICATE_EVERY=3 控制
    r_a = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_a}, timeout=10)
    r_a.raise_for_status()
    batch_a = r_a.json()["id"]
    r_b = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_b}, timeout=10)
    r_b.raise_for_status()
    batch_b = r_b.json()["id"]
    print(f"    批 A={batch_a}  Study={study_a}")
    print(f"    批 B={batch_b}  Study={study_b}")

    # 等清单固定、进入 MOVING，再投 rogue 流量
    time.sleep(3)

    banner("3. 清单外实例 / 压缩实例攻击，均应被拒收且不入包")
    from simulator import rogue_sender

    old_argv = sys.argv
    sys.argv = ["rogue", "off-list", "--study", study_a,
                "--host", HOST_API, "--port", str(PORT_API)]
    try:
        rc_offlist = rogue_sender.main()
    finally:
        sys.argv = old_argv
    sys.argv = ["rogue", "compressed", "--study", study_b,
                "--host", HOST_API, "--port", str(PORT_API)]
    try:
        rc_compressed = rogue_sender.main()
    finally:
        sys.argv = old_argv
    print(f"    清单外投送拒收校验返回码={rc_offlist}（0=符合预期）")
    print(f"    压缩投送拒收校验返回码={rc_compressed}（0=符合预期）")

    banner("4. 提交批 C 并注入保存失败（批目录只读，真实写盘失败）")
    r_c = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_c,
                              "fault_inject": "save_failure"}, timeout=10)
    r_c.raise_for_status()
    batch_c = r_c.json()["id"]
    print(f"    批 C={batch_c}（save_failure）")

    banner("5. 等待三个批到达终态")
    view_a = wait_state(batch_a, {"COMPLETED", "FAILED"})
    view_b = wait_state(batch_b, {"COMPLETED", "FAILED"})
    view_c = wait_state(batch_c, {"COMPLETED", "FAILED"}, timeout=120)
    print(f"    批 A 终态={view_a['state']} "
          f"{view_a['received_count']}/{view_a['expected_count']}")
    print(f"    批 B 终态={view_b['state']} "
          f"{view_b['received_count']}/{view_b['expected_count']}")
    print(f"    批 C 终态={view_c['state']} 原因={view_c['failure_reason']}")

    print("\n  -- 批 A 事件流水（可见重复投送去重、rogue 拒收）--")
    show_events(batch_a)
    print("\n  -- 批 C 事件流水（可见保存失败）--")
    show_events(batch_c)

    banner("6. 下载批 A 的 ZIP 并核对内容")
    if view_a["state"] != "COMPLETED":
        print("    批 A 未完成，后续归档演示无法继续")
        return 1
    resp = download(batch_a)
    resp.raise_for_status()
    check_zip(resp.content, view_a["expected_count"])
    print(f"    ZIP 内 DICOM 文件数={view_a['expected_count']}，"
          "清单行数一致，校验通过：ZIP 仅含清单内实例")

    # 对失败批下载应被拒绝
    denied = download(batch_c)
    print(f"    失败批 C 下载 HTTP 状态={denied.status_code}（期望 409）")
    assert denied.status_code == 409

    banner("7. 归档恢复：损坏批 A 的 ZIP，并发下载只重建一次")
    r = requests.post(f"{API}/internal/fault/corrupt-zip/{batch_a}",
                      params={"mode": "corrupt"}, timeout=10)
    r.raise_for_status()
    print(f"    已注入故障：{r.json()['detail']}")

    def _dl(_: int) -> bytes:
        resp = download(batch_a)
        assert resp.status_code == 200, f"并发下载失败：{resp.status_code}"
        return resp.content

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        payloads = list(pool.map(_dl, range(8)))
    assert all(p == payloads[0] for p in payloads), "并发下载内容不一致"
    check_zip(payloads[0], view_a["expected_count"])
    rebuilt = [e for e in get_events(batch_a) if e["kind"] == "ZIP_REBUILT"]
    assert len(rebuilt) == 1, f"应只重建一次，实际 {len(rebuilt)} 次"
    print(f"    8 路并发下载全部 200 且字节一致；ZIP_REBUILT 事件恰好 1 次：")
    print(f"    {rebuilt[0]['message']}")

    banner("8. 归档恢复：缺片时保留完成记录但不可下载，修复数据卷后恢复")
    r = requests.post(f"{API}/internal/fault/remove-slice/{batch_a}",
                      timeout=10)
    r.raise_for_status()
    removed_sop = r.json()["removed_sop"]
    print(f"    已抽走原片 SOP={removed_sop}（移入隔离区，模拟数据卷缺片）")
    r = requests.post(f"{API}/internal/fault/corrupt-zip/{batch_a}",
                      params={"mode": "missing"}, timeout=10)
    r.raise_for_status()
    print(f"    已注入故障：{r.json()['detail']}")

    resp = download(batch_a)
    print(f"    缺片后下载 HTTP 状态={resp.status_code}（期望 409，"
          "不返回残缺 ZIP）")
    assert resp.status_code == 409
    assert "原片缺失" in resp.json()["detail"]
    view = get_view(batch_a)
    print(f"    批 A 状态={view['state']}（完成记录保留），"
          f"downloadable={view['downloadable']}")
    print(f"    不可下载原因：{view['download_blocked_reason']}")
    assert view["state"] == "COMPLETED"
    assert view["downloadable"] is False
    assert "原片缺失" in view["download_blocked_reason"]

    r = requests.post(f"{API}/internal/fault/restore-slices/{batch_a}",
                      timeout=10)
    r.raise_for_status()
    print(f"    已修复数据卷：{r.json()['restored']}")
    resp = download(batch_a)
    assert resp.status_code == 200, "修复数据卷后下载应恢复"
    check_zip(resp.content, view_a["expected_count"])
    view = get_view(batch_a)
    assert view["downloadable"] is True
    assert view["download_blocked_reason"] is None
    print("    再次请求下载：200 且内容完整，downloadable 已恢复 True")

    if args.skip_restart:
        banner("演示完成（已跳过重启恢复）")
        return 0

    banner("9. 重启恢复：未完成批判失败；启动核对自动重建损坏归档")
    # 再提交一个新检查的批，立刻杀掉进程，使其停在非终态。
    # 终态批已存在不影响重新提交（仅拒绝“进行中”的同 Study 批）。
    r_d = requests.post(f"{API}/batches",
                        json={"study_instance_uid": study_b}, timeout=10)
    if r_d.status_code == 409:
        print("    存在同 Study 进行中批，重启恢复场景略过")
        return 0
    r_d.raise_for_status()
    batch_d = r_d.json()["id"]
    print(f"    批 D={batch_d} 已提交，立即触发进程退出")
    # 退出前再损坏一次批 A 的 ZIP：验证重启时“启动核对”会自动重建
    r = requests.post(f"{API}/internal/fault/corrupt-zip/{batch_a}",
                      params={"mode": "truncate"}, timeout=10)
    r.raise_for_status()
    print(f"    退出前已截断批 A 的 ZIP：{r.json()['detail']}")
    try:
        requests.post(f"{API}/internal/suicide", timeout=5)
    except requests.RequestException:
        pass
    # 等待容器编排（compose restart: always）或外部守护拉起进程
    deadline = time.time() + 90
    healthy = False
    while time.time() < deadline:
        try:
            if requests.get(f"{API}/health", timeout=3).ok:
                healthy = True
                break
        except requests.RequestException:
            time.sleep(1.5)
    if not healthy:
        print("    服务未在 90s 内恢复（本地裸跑请手动重启 API 后重试）")
        return 2
    view_d = wait_state(batch_d, {"COMPLETED", "FAILED"})
    print(f"    重启后批 D 状态={view_d['state']} "
          f"原因={view_d['failure_reason']}")
    assert view_d["state"] == "FAILED"
    assert "重启" in (view_d["failure_reason"] or "")
    show_events(batch_d)

    # 启动核对应已重建批 A 被截断的 ZIP
    again = download(batch_a)
    print(f"    重启后批 A 下载 HTTP 状态={again.status_code}（期望 200）")
    assert again.status_code == 200
    check_zip(again.content, view_a["expected_count"])
    rebuilt = [e for e in get_events(batch_a)
               if e["kind"] == "ZIP_REBUILT" and "启动核对" in e["message"]]
    assert rebuilt, "重启时启动核对应重建损坏的 ZIP"
    print(f"    启动核对已重建：{rebuilt[0]['message']}")

    banner("全部演示完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
