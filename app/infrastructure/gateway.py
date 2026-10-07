import random

from app.application.ports import Sleep
from app.domain.payments import GatewayResult, Payment


class SimulatedPaymentGateway:
    def __init__(self, sleep: Sleep, random_source: random.Random) -> None:
        self._sleep = sleep
        self._random = random_source

    async def process(self, payment: Payment) -> GatewayResult:
        await self._sleep(self._random.uniform(2, 5))
        succeeded = self._random.random() < 0.9
        return GatewayResult(succeeded, None if succeeded else "Simulated gateway failure")
