from httpx import AsyncClient, HTTPError

from app.application.ports import DeliveryError
from app.domain.payments import PaymentNotification


class HttpWebhookTransport:
    def __init__(self, client: AsyncClient, timeout: float = 3.0) -> None:
        self._client = client
        self._timeout = timeout

    async def send(self, notification: PaymentNotification) -> None:
        event_type = f"payment.{notification.status.value}"
        payload = {
            "event_id": str(notification.event_id),
            "event_type": event_type,
            "payment_id": str(notification.payment_id),
            "status": notification.status.value,
            "amount": format(notification.amount, ".2f"),
            "currency": notification.currency.value,
            "processed_at": notification.processed_at.isoformat()
            if notification.processed_at
            else None,
            "metadata": notification.metadata,
        }
        try:
            response = await self._client.post(
                notification.url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Payment-Event": event_type,
                    "X-Payment-Id": str(notification.payment_id),
                },
                timeout=self._timeout,
            )
        except HTTPError as exc:
            raise DeliveryError(str(exc)) from exc
        if not 200 <= response.status_code < 300:
            raise DeliveryError(f"HTTP {response.status_code}")
