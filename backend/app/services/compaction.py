"""小文件合并服务：合并后级联Iceberg过期清理和Metric聚合更新。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.block_file import (
    ACTIVE_TASK_STATUS,
    _size_key,
    active_task_numbers,
    is_occupied,
    resolve_block_file,
)
from app.store import store


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "合并"
    LIST_FIELDS = ["任务编号", "目标表", "文件数量", "合并策略", "触发阈值", "执行内存", "合并耗时", "释放空间"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "文件数量"]
    STATUS_ORDER = ["待执行", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "合并中", "取消合并": "已完成", "标记失败": "合并失败"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """状态流转后同步阻塞文件候选数据：完成则落账，失败则释放占用。"""
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return entry, message
        status = entry.get("status")
        if status == "已完成":
            self._settle_files(entry, merged=True)
            if str(entry.get("合并耗时", "")) in ("未执行", "进行中...", ""):
                entry["合并耗时"] = "9.8s"
            if str(entry.get("释放空间", "")) in ("未执行", ""):
                entry["释放空间"] = self._estimate_release(entry)
        elif status == "合并失败":
            # 失败释放本任务占用，文件回到待合并，可重新核对后再派发；
            # 只释放仍挂在该任务名下的文件，不影响其他任务的占用。
            self._settle_files(entry, merged=False)
        return entry, message

    def accept_candidate_files(
        self, entry_id: int, refs: list[Any]
    ) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
        """以阻塞文件候选数据接受可执行文件。

        返回已被别的任务处理或仍有资格的路径，供调用方再次筛选：
        - 仍有资格：本任务已持有或本次新占有的文件；
        - 已被别的任务处理：被其他进行中任务占用、或已合并完成的文件；
        - 无法受理：候选数据里查不到的路径。
        """
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档", {}
        if entry.get("status") not in ACTIVE_TASK_STATUS:
            return None, f"{self.ENTITY}当前状态为{entry.get('status')}，不可受理阻塞文件", {}

        task_no = str(entry.get("任务编号", ""))
        active_tasks = active_task_numbers()
        manifest: list[str] = list(entry.get("文件清单") or [])
        eligible: list[str] = []
        taken: list[dict[str, Any]] = []
        invalid: list[Any] = []

        for ref in refs:
            row = resolve_block_file(ref)
            if row is None:
                invalid.append(ref)
                continue
            path = str(row.get("文件路径", ""))
            if path in eligible or any(t.get("文件路径") == path for t in taken):
                continue  # 同一文件的不同引用形式只受理一次
            holder = str(row.get("占用任务", "") or "").strip()
            if holder == task_no:
                # 本任务已持有，直接确认资格。
                if path not in manifest:
                    manifest.append(path)
                eligible.append(path)
            elif row.get("status") == "已合并":
                taken.append({"文件路径": path, "原因": "文件已合并"})
            elif is_occupied(row, active_tasks):
                taken.append({
                    "文件路径": path,
                    "占用任务": holder,
                    "确认人": row.get("确认人", ""),
                })
            else:
                # 空闲文件：登记为本任务占用，纳入合并清单。
                row["status"] = "已派发"
                row["占用任务"] = task_no
                if not str(row.get("确认人", "")).strip():
                    row["确认人"] = str(entry.get("确认人", ""))
                if path not in manifest:
                    manifest.append(path)
                eligible.append(path)

        entry["文件清单"] = manifest
        entry["文件数量"] = len(manifest)
        detail = {"仍有资格": eligible, "已被别的任务处理": taken, "无法受理": invalid}
        message = (f"{self.ENTITY}{task_no}受理{len(eligible)}个文件，"
                   f"{len(taken)}个已被别的任务处理，{len(invalid)}个无法受理")
        return entry, message, detail

    def _settle_files(self, entry: dict[str, Any], *, merged: bool) -> None:
        """按任务文件清单落账或释放阻塞文件。"""
        task_no = str(entry.get("任务编号", ""))
        for path in entry.get("文件清单") or []:
            row = resolve_block_file(path)
            if row is None:
                continue
            if merged:
                # 保留占用任务与确认人，合并来源可追查。
                row["status"] = "已合并"
            elif str(row.get("占用任务", "")) == task_no:
                row["status"] = "待合并"
                row["占用任务"] = ""
                row["确认人"] = ""

    def _estimate_release(self, entry: dict[str, Any]) -> str:
        """按清单内文件大小估算释放空间。"""
        total = sum(
            _size_key(row.get("文件大小"))
            for path in entry.get("文件清单") or []
            if (row := resolve_block_file(path)) is not None
        )
        if total <= 0:
            return "待统计"
        if total >= 1024:
            return f"{total / 1024:.1f}GB"
        return f"{int(total)}MB"

    def trigger_expire_and_metric(self, entry_id: int) -> dict[str, Any] | None:
        """合并完成后触发expire_snap清理和metric_aggr聚合更新。

        数据流链路: compaction -> expire_snap + metric_aggr
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已完成":
            return None

        table = str(entry.get("目标表", "unknown"))
        results: dict[str, Any] = {"expire_snap": None, "metric_aggr": None}

        # 触发 expire_snap
        for exp in store.rows("expire_snap"):
            if str(exp.get("目标表", "")) == table:
                exp["status"] = "预演完成"
                exp["pending"] = True
                exp["abnormal"] = False
                exp["预演结果"] = f"可清理{entry.get('文件数量', 0) * 3}个历史快照"
                results["expire_snap"] = exp
                break

        # 创建 metric_aggr 聚合
        metric_rows = store.rows("metric_aggr")
        new_metric_id = max((int(r.get("id", 0)) for r in metric_rows), default=0) + 1
        metric = {
            "id": new_metric_id,
            "窗口编号": f"METR-{new_metric_id:04d}",
            "数据流": table,
            "聚合键": "合并批次",
            "聚合函数": "SUM(file_count), AVG(file_size)",
            "窗口宽度": "1h",
            "输出频率": "每次合并后",
            "延迟更新": "实时",
            "最后写入": "2026-11-18 10:42:00",
            "status": "聚合中",
            "pending": True,
            "abnormal": False,
        }
        metric_rows.append(metric)
        results["metric_aggr"] = metric

        return results
