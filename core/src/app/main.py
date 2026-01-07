from .api import router
from .core.config import settings
from .core.setup import create_application, lifespan_factory

# Create application with default lifespan and enable table creation
app = create_application(
    router=router,
    settings=settings,
    lifespan=lifespan_factory(settings, create_tables_on_start=True)
)
