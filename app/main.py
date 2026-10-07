from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.application.payments import Payments
from app.bootstrap import Database
from app.infrastructure.config import Settings, get_settings
from app.presentation.http.payments import router


def create_app(settings: Settings | None = None, payments: Payments | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        database = None
        if payments is None:
            database = Database(settings.database_url)
            app.state.payments = database.payments
        try:
            yield
        finally:
            if database is not None:
                await database.close()

    app = FastAPI(title="Async Payment Service", lifespan=lifespan)
    app.state.api_key = settings.api_key
    if payments is not None:
        app.state.payments = payments
    app.include_router(router)
    return app


app = create_app()
