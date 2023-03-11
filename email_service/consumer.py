from confluent_kafka import Consumer
import json, os, django
from django.core.mail import send_mail


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()

consumer = Consumer({
    'bootstrap.servers': 'your-bootstrap-server-here',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'your-api-key-here',
    'sasl.password': 'your-api-secret-here',
    'sasl.mechanism': 'PLAIN',
    'group.id': 'myGroup', # any value
    'auto.offset.reset': 'earliest' # get messages as soon as they arrive
})

consumer.subscribe(['your-topic-name']) # subscribe to events topic
print("Now consuming, Waiting for messages...")


while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    if msg.error():
        print(f"Consumer error: {msg.error()}")

    print(f"Recieved message: {msg.value()}")

    message = json.loads(msg.value())

    send_mail(
        subject="Welcome with love from Kafka",
        message=message['message'],
        from_email="kafkatutorial@email.com",
        recipient_list=[f"{message['email']}"]
    )

consumer.close()