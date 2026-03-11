from .user import router as user_router
from .admin import router as admin_router
from .errors import router as errors_router

__all__ = ["user_router", "admin_router", "errors_router"]
