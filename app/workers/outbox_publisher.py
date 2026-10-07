import asyncio
import logging

from app.application.outbox import PublishOutbox
from app.application.retry import RetryPolicy
from app.bootstrap import Database, create_broker, utc_now
from app.infrastructure.config import get_settings
from app.infrastructure.rabbit import RabbitEventPublisher

logger = logging.getLogger(__name__)


async def run() -> None:
    settings = get_settings()
    database = Database(settings.database_url)
    try:
        async with create_broker(settings) as broker:
            publisher = RabbitEventPublisher(broker)
            await publisher.declare_topology()
            publish = PublishOutbox(database.uow, publisher, RetryPolicy(), utc_now, asyncio.sleep)
            while True:
                try:
                    count = await publish.execute()
                    if count:
                        logger.info("published %s outbox events", count)
                except Exception:
                    logger.exception("outbox polling failed; will retry")
                await asyncio.sleep(settings.outbox_poll_interval_seconds)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(run())
