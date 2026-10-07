from app.infrastructure.rabbit import RabbitEventPublisher


class Broker:
    def __init__(self):
        self.messages = []
        self.queues = []

    async def publish(self, payload, **kwargs):
        self.messages.append((payload, kwargs))

    async def declare_queue(self, queue):
        self.queues.append(queue)


async def test_transport_owns_routing_persistence_and_seconds():
    broker = Broker()
    publisher = RabbitEventPublisher(broker)
    await publisher.declare_topology()
    await publisher.publish_payment({"payment_id": "id"})
    await publisher.retry_payment({"payment_id": "id"}, 2)
    await publisher.dead_letter({"payment_id": "id"})
    assert [q.name for q in broker.queues] == [
        "payments.new",
        "payments.new.retry",
        "payments.new.dlq",
    ]
    assert all(q.durable for q in broker.queues)
    assert [options["routing_key"] for _, options in broker.messages] == [
        "payments.new",
        "payments.new.retry",
        "payments.new.dlq",
    ]
    assert all(o["persist"] and o["mandatory"] for _, o in broker.messages)
    assert broker.messages[1][1]["expiration"] == 2
