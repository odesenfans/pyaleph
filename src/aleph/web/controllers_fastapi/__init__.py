"""
Register API routers here.
"""

from .main_routers import api_v0_router, api_ws0_router, api_v1_router
from .info import router as info_router
from .main import router as main_router


api_v0_router.include_router(main_router)
api_v0_router.include_router(info_router)
