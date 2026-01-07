from fastapi import APIRouter

# Only import routers that don't require authentication dependencies
from .health import router as health_router
from .tables import router as tables_router
from .ai_sql import router as ai_sql_router
from .table_assets import router as table_assets_router

# Comment out routers with authentication dependencies for now
# from .login import router as login_router
# from .logout import router as logout_router
# from .posts import router as posts_router
# from .rate_limits import router as rate_limits_router
# from .tasks import router as tasks_router
# from .tiers import router as tiers_router
# from .users import router as users_router

router = APIRouter(prefix="/v1")
router.include_router(health_router)
router.include_router(tables_router)
router.include_router(ai_sql_router)
router.include_router(table_assets_router)

# Comment out routers with authentication dependencies for now
# router.include_router(login_router)
# router.include_router(logout_router)
# router.include_router(users_router)
# router.include_router(posts_router)
# router.include_router(tasks_router)
# router.include_router(tiers_router)
# router.include_router(rate_limits_router)
