import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def start_consuming(self, callback):
        def on_message(ch, method, properties, body):
            ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)

            callback(body, ack, nack)


        self.channel.basic_consume(queue=self.queue_name, on_message_callback=on_message, auto_ack=False)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
