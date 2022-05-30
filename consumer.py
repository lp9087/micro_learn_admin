from confluent_kafka import Consumer, KafkaError
import json, os, django


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()

import core.listeners
from core.models import KafkaError


consumer = Consumer({
    'bootstrap.servers': 'localhost:9091',
    'group.id': 'myGroup',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

consumer.subscribe(['checkout_topic'])
while True:
    msg = consumer.poll(0.1)
    if msg is None:
        continue
    elif not msg.error():
        print(msg.value())
    elif msg.error().code() == KafkaError._PARTITION_EOF:
        print("End of partition reached")
    else:
        print("Error")

    print(msg.key())

    print(msg.key())
    try:
        getattr(core.listeners, msg.key().decode('utf-8'))(json.loads(msg.value()))
    except Exception as e:
        print(e)
        KafkaError.objects.create(
            key=msg.key(),
            value=msg.value(),
            error=e,
        )

consumer.close()




