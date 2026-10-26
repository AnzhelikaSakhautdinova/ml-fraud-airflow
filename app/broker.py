"""At-least-once processing: acknowledge only after confirmed output delivery."""
import json
import logging
import os
import signal

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9095')


def producer():
    return Producer({'bootstrap.servers': BOOTSTRAP, 'enable.idempotence': True,
                     'delivery.timeout.ms': 30000})


def publish(client, topic, value, key=None):
    errors = []
    def delivered(error, message):
        if error:
            errors.append(str(error))
    client.produce(topic, key=key, value=json.dumps(value, allow_nan=False).encode(), on_delivery=delivered)
    remaining = client.flush(35)
    if errors or remaining:
        raise RuntimeError(f'Kafka delivery failed: {errors}, remaining={remaining}')


def run(topic, group, handler):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    consumer = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': group,
                         'auto.offset.reset': 'earliest', 'enable.auto.commit': False})
    dead_letters = producer()
    stopping = False
    def stop(*args):
        nonlocal stopping
        stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer.subscribe([topic])
    try:
        while not stopping:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            try:
                value = json.loads(message.value())
                handler(value)
            except (ValueError, TypeError, KeyError, UnicodeError) as error:
                logging.warning('Rejected topic=%s offset=%s: %s', topic, message.offset(), error)
                publish(dead_letters, topic + '_dlq', {
                    'topic': topic, 'partition': message.partition(), 'offset': message.offset(),
                    'error': str(error), 'raw': (message.value() or b'').decode(errors='replace')})
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
