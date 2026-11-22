"""阻塞文件候选数据接口：跨落盘管道检索、范围核对、派发合并与合并侧受理。

注意：/api/blocked-files 与资源路由的 /api/{module} 形状相同，
本路由必须在 resource 之前注册，否则会被动态模块路由抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query, Request
from pydantic import BaseModel, Field

from app.schemas import PageResult
from app.services.block_file import BlockFileService
from app.services.compaction import CompactionService

router = APIRouter(prefix="/api", tags=["阻塞文件候选"])

block_files = BlockFileService()
compaction = CompactionService()


class RecheckPayload(BaseModel):
    files: list[Any] = Field(default_factory=list)


class DispatchPayload(BaseModel):
    files: list[Any] = Field(default_factory=list)
    值班员: str = ""
    请求编号: str = ""
    合并策略: str = ""
    执行内存: str = ""


class AcceptPayload(BaseModel):
    files: list[Any] = Field(default_factory=list)


@router.get("/blocked-files", response_model=PageResult[dict])
def list_blocked_files(
    request: Request,
    sort: str = Query(default="阻塞时间"),
    order: str = Query(default="asc"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """跨落盘管道检索阻塞管道的待合并数据文件。

    查询依据：目标表、源管道、清单状态、阻塞时间区间；排序翻页后返回，
    total 与查询依据保持一致。
    """
    params = request.query_params
    items, total = block_files.list_candidates(
        目标表=(params.get("目标表") or "").strip() or None,
        源管道=(params.get("源管道") or "").strip() or None,
        清单状态=(params.get("清单状态") or "").strip() or None,
        阻塞时间起=(params.get("阻塞时间起") or "").strip() or None,
        阻塞时间止=(params.get("阻塞时间止") or "").strip() or None,
        sort=sort.strip() or "阻塞时间",
        order=order.strip() or "asc",
        page=page,
        size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/blocked-files/recheck")
def recheck_blocked_files(payload: RecheckPayload) -> dict[str, Any]:
    """提交范围变动时重新核对：逐个文件回报可派发 / 已占用 / 已合并 / 缺失。"""
    return block_files.recheck(payload.files)


@router.post("/blocked-files/dispatch")
def dispatch_blocked_files(payload: DispatchPayload) -> dict[str, Any]:
    """把值班员确认的文件范围提交维护：登记合并任务并占用文件。

    已被其他任务占用的文件只回报冲突不重复取得；相同请求编号的重试
    直接返回既有任务，不会重复占用。
    """
    return block_files.dispatch(
        refs=payload.files,
        operator=payload.值班员.strip(),
        request_id=payload.请求编号.strip(),
        strategy=payload.合并策略.strip(),
        memory=payload.执行内存.strip(),
    )


@router.post("/compaction/{entry_id}/accept-files")
def accept_candidate_files(entry_id: int, payload: AcceptPayload) -> dict[str, Any]:
    """小文件合并以阻塞文件候选数据接受可执行文件。

    返回已被别的任务处理或仍有资格的路径，供再次筛选。
    """
    entry, message, detail = compaction.accept_candidate_files(entry_id, payload.files)
    return {"ok": entry is not None, "message": message, "entry": entry, **detail}
