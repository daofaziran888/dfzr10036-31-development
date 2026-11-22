"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import block_file, resource

# block_file 含固定路径 /api/blocked-files，必须先于 resource 的 /api/{module} 注册。
ROUTERS = [block_file, resource]
