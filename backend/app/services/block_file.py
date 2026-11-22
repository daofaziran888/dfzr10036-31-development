"""阻塞文件候选数据服务：跨落盘管道检索待合并文件，核对确认范围并派发合并。

数据流链路: sink_writer(写入阻塞) -> block_file(候选) -> compaction(派发合并)

职责边界：
- 检索：按目标表、阻塞时间、清单状态跨所有写入阻塞的落盘管道查询待合并文件；
- 核对：提交范围变动时重新核对每个文件当前是否仍可派发；
- 派发：把值班员确认的文件范围登记成合并任务，占用文件并留下审计，
  任何派发都不会重新取得已经被其他任务占用的文件，失败重试也不会重复占用。
"""
from __future__ import annotations

from typing import Any

from app.store import store

# 候选文件状态：待合并（空闲）和已派发（被任务占用但未合并完）都会出现在检索结果里。
CANDIDATE_STATUS = ("待合并", "已派发")
# 占用文件的合并任务只有处于这些状态时才视为“在使用”，否则占用视为已释放。
ACTIVE_TASK_STATUS = ("待执行", "合并中")
# 列表允许排序的字段，默认按阻塞时间升序，id 兜底保证翻页稳定。
SORTABLE_FIELDS = ("文件编号", "文件路径", "目标表", "源管道", "阻塞时间", "清单状态", "文件大小")
# 演示环境时间冻结在 2026-11-18，与种子数据保持一致。
DISPATCH_TIME = "2026-11-18 11:05:00"


def resolve_block_file(ref: Any) -> dict[str, Any] | None:
    """按 id / 文件编号 / 文件路径定位一个阻塞文件。"""
    text = str(ref).strip()
    if not text:
        return None
    for row in store.rows("block_file"):
        if text.isdigit() and int(row.get("id", 0)) == int(text):
            return row
        if text in (str(row.get("文件编号", "")), str(row.get("文件路径", ""))):
            return row
    return None


def active_task_numbers() -> set[str]:
    """仍在使用文件的合并任务编号集合。"""
    return {
        str(row.get("任务编号", ""))
        for row in store.rows("compaction")
        if row.get("status") in ACTIVE_TASK_STATUS
    }


def is_occupied(row: dict[str, Any], active_tasks: set[str]) -> bool:
    """文件是否被某个进行中的任务占用；已结束任务的占用视为已释放。"""
    holder = str(row.get("占用任务", "") or "").strip()
    return bool(holder) and holder in active_tasks


def _size_key(value: Any) -> float:
    """把 9MB / 1.2GB 这类大小换算成 MB 数值，保证排序符合直觉。"""
    text = str(value or "").strip().upper()
    for suffix, factor in (("GB", 1024.0), ("MB", 1.0), ("KB", 1 / 1024.0)):
        if text.endswith(suffix):
            try:
                return float(text[: -len(suffix)].strip()) * factor
            except ValueError:
                return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


class BlockFileService:
    MODULE = "block_file"
    ENTITY = "阻塞文件"

    # ---- 检索 ----

    def _blocked_pipelines(self) -> dict[str, dict[str, Any]]:
        return {
            str(row.get("管道编号", "")): row
            for row in store.rows("sink_writer")
            if row.get("status") == "写入阻塞"
        }

    def _view(self, row: dict[str, Any], pipelines: dict[str, dict[str, Any]],
              active_tasks: set[str]) -> dict[str, Any]:
        item = dict(row)
        pipeline = pipelines.get(str(row.get("源管道", "")), {})
        item["数据流"] = pipeline.get("数据流", "")
        item["可派发"] = not is_occupied(row, active_tasks)
        return item

    def list_candidates(
        self,
        *,
        目标表: str | None = None,
        源管道: str | None = None,
        清单状态: str | None = None,
        阻塞时间起: str | None = None,
        阻塞时间止: str | None = None,
        sort: str = "阻塞时间",
        order: str = "asc",
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """跨落盘管道检索阻塞管道的待合并数据文件。

        只纳入当前处于写入阻塞的管道名下的文件；已合并的文件退出候选。
        total 在过滤之后、翻页之前计算，保证翻页显示的文件数与查询依据一致。
        """
        pipelines = self._blocked_pipelines()
        active_tasks = active_task_numbers()
        rows = [
            row for row in store.rows(self.MODULE)
            if row.get("status") in CANDIDATE_STATUS
            and str(row.get("源管道", "")) in pipelines
        ]
        for field, expected in (("目标表", 目标表), ("源管道", 源管道), ("清单状态", 清单状态)):
            if expected:
                rows = [
                    row for row in rows
                    if expected.casefold() in str(row.get(field, "")).casefold()
                ]
        if 阻塞时间起:
            rows = [row for row in rows if str(row.get("阻塞时间", "")) >= 阻塞时间起]
        if 阻塞时间止:
            rows = [row for row in rows if str(row.get("阻塞时间", "")) <= 阻塞时间止]

        sort_field = sort if sort in SORTABLE_FIELDS else "阻塞时间"
        reverse = order.lower() == "desc"
        if sort_field == "文件大小":
            rows.sort(key=lambda row: (_size_key(row.get("文件大小")), int(row.get("id", 0))),
                      reverse=reverse)
        else:
            rows.sort(key=lambda row: (str(row.get(sort_field, "")), int(row.get("id", 0))),
                      reverse=reverse)

        total = len(rows)
        start = (page - 1) * size
        page_rows = rows[start:start + size]
        return [self._view(row, pipelines, active_tasks) for row in page_rows], total

    # ---- 核对 ----

    def recheck(self, refs: list[Any]) -> dict[str, Any]:
        """提交范围变动时重新核对：逐个文件确认当前是否仍可派发。"""
        active_tasks = active_task_numbers()
        可派发: list[str] = []
        已占用: list[dict[str, Any]] = []
        已合并: list[str] = []
        缺失: list[Any] = []
        for ref in refs:
            row = resolve_block_file(ref)
            if row is None:
                缺失.append(ref)
                continue
            path = str(row.get("文件路径", ""))
            if row.get("status") == "已合并":
                已合并.append(path)
            elif is_occupied(row, active_tasks):
                已占用.append({
                    "文件路径": path,
                    "占用任务": row.get("占用任务", ""),
                    "确认人": row.get("确认人", ""),
                })
            else:
                可派发.append(path)
        return {
            "ok": True,
            "message": f"核对{len(refs)}个文件：可派发{len(可派发)}、已占用{len(已占用)}、"
                       f"已合并{len(已合并)}、缺失{len(缺失)}",
            "总数": len(refs),
            "可派发": 可派发,
            "已占用": 已占用,
            "已合并": 已合并,
            "缺失": 缺失,
        }

    # ---- 派发 ----

    def dispatch(
        self,
        *,
        refs: list[Any],
        operator: str,
        request_id: str = "",
        strategy: str = "",
        memory: str = "",
    ) -> dict[str, Any]:
        """把值班员确认的文件范围提交维护：登记合并任务并占用文件。

        - 已经被其他任务占用的文件只回报冲突，绝不重新取得；
        - 携带相同请求编号的重试直接返回既有任务，不重复建单、不重复占用；
        - 源管道、值班员和确认文件范围写入任务与审计，保证可追查。
        """
        if not operator:
            return {"ok": False, "message": "缺少值班员，无法登记确认人", "tasks": []}
        if not refs:
            return {"ok": False, "message": "提交范围为空，没有可派发的文件", "tasks": []}

        if request_id:
            existing = [
                row for row in store.rows("compaction")
                if str(row.get("请求编号", "")) == request_id
            ]
            if existing:
                accepted = [p for task in existing for p in task.get("文件清单", [])]
                return {
                    "ok": True,
                    "message": f"请求编号{request_id}已派发过，返回既有{len(existing)}个任务，"
                               "不重复占用文件",
                    "tasks": existing,
                    "已受理": accepted,
                    "占用冲突": [],
                    "无法受理": [],
                    "重复提交": True,
                }

        active_tasks = active_task_numbers()
        free: list[dict[str, Any]] = []
        conflicts: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []
        seen: set[int] = set()
        for ref in refs:
            row = resolve_block_file(ref)
            if row is None:
                invalid.append({"引用": ref, "原因": "文件不存在"})
                continue
            if int(row.get("id", 0)) in seen:
                continue  # 同一文件的不同引用形式只受理一次
            seen.add(int(row.get("id", 0)))
            path = str(row.get("文件路径", ""))
            if row.get("status") == "已合并":
                invalid.append({"引用": ref, "文件路径": path, "原因": "文件已合并"})
            elif str(row.get("源管道", "")) not in self._blocked_pipelines():
                invalid.append({"引用": ref, "文件路径": path, "原因": "源管道未处于写入阻塞"})
            elif is_occupied(row, active_tasks):
                conflicts.append({
                    "文件路径": path,
                    "占用任务": row.get("占用任务", ""),
                    "确认人": row.get("确认人", ""),
                })
            else:
                free.append(row)

        if not free:
            return {
                "ok": False,
                "message": f"没有可派发的文件：{len(conflicts)}个被其他任务占用、"
                           f"{len(invalid)}个不可受理",
                "tasks": [],
                "已受理": [],
                "占用冲突": conflicts,
                "无法受理": invalid,
            }

        # 按目标表分组，每张表登记一个合并任务。
        by_table: dict[str, list[dict[str, Any]]] = {}
        for row in free:
            by_table.setdefault(str(row.get("目标表", "")), []).append(row)

        comp_rows = store.rows("compaction")
        next_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
        pipelines = self._blocked_pipelines()
        tasks: list[dict[str, Any]] = []
        accepted: list[str] = []
        for table, files in by_table.items():
            task_no = f"CMP-{next_id:04d}"
            paths = [str(f.get("文件路径", "")) for f in files]
            sources = sorted({str(f.get("源管道", "")) for f in files})
            task = {
                "id": next_id,
                "任务编号": task_no,
                "目标表": table,
                "文件数量": len(paths),
                "合并策略": strategy or "BinPack-按大小合并",
                "触发阈值": "阻塞文件派发",
                "执行内存": memory or "2GB",
                "合并耗时": "未执行",
                "释放空间": "未执行",
                "status": "待执行",
                "pending": True,
                "abnormal": False,
                "文件清单": paths,
                "源管道": "、".join(sources),
                "确认人": operator,
                "请求编号": request_id,
            }
            comp_rows.append(task)
            next_id += 1
            for row in files:
                row["status"] = "已派发"
                row["占用任务"] = task_no
                row["确认人"] = operator
            accepted.extend(paths)
            tasks.append(task)
            self._audit_dispatch(task, sources, pipelines, operator)

        return {
            "ok": True,
            "message": f"已派发{len(tasks)}个合并任务，受理{len(accepted)}个文件；"
                       f"{len(conflicts)}个被占用跳过、{len(invalid)}个不可受理",
            "tasks": tasks,
            "已受理": accepted,
            "占用冲突": conflicts,
            "无法受理": invalid,
            "重复提交": False,
        }

    def _audit_dispatch(self, task: dict[str, Any], sources: list[str],
                        pipelines: dict[str, dict[str, Any]], operator: str) -> None:
        """登记派发审计：源管道、值班员和确认文件范围留痕可追查。"""
        audit_rows = store.rows("stream_audit")
        new_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        streams = "、".join(sorted({
            str(pipelines.get(src, {}).get("数据流", "")) for src in sources
        } - {""})) or str(task.get("目标表", ""))
        audit_rows.append({
            "id": new_id,
            "status": "已记录",
            "pending": True,
            "abnormal": False,
            "记录编号": f"SA-{new_id:04d}",
            "数据流": streams,
            "操作类型": "阻塞文件派发合并",
            "影响表": str(task.get("目标表", "")),
            "操作人": operator,
            "操作时间": DISPATCH_TIME,
            "审计结论": f"派发{task.get('任务编号', '')}受理{task.get('文件数量', 0)}个阻塞文件"
                        f"（源管道：{task.get('源管道', '')}）",
            "合规标记": "合规",
        })
