from types import TracebackType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.persistence.repositories import SqlOutboxRepository, SqlPaymentRepository


class SqlUnitOfWork:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def __aenter__(self) -> "SqlUnitOfWork":
        self._session = self._sessions()
        self.payments = SqlPaymentRepository(self._session)
        self.outbox = SqlOutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        # Closing rolls back any uncommitted work, including cancellation.
        await self._session.close()

    async def commit(self) -> None:
        await self._session.commit()
