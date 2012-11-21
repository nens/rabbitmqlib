# (c) Nelen & Schuurmans.  MIT licensed, see LICENSE.rst.
from __future__ import unicode_literals
import json
import pika


class Rabbit:

    def __init__(self, server, user, password, vhost):
        credentials = pika.PlainCredentials(user, password)
        conn_params = pika.ConnectionParameters(
            server,
            virtual_host=vhost,
            credentials=credentials
        )
        conn_broker = pika.BlockingConnection(conn_params)
        self.channel = conn_broker.channel()


class Producer(Rabbit):

    def send(self, msg, exchange, routing_key):
        queue = b"%s_%s" % (exchange, routing_key)
        ch = self.channel
        ch.exchange_declare(
            exchange=exchange,
            auto_delete=False
        )
        ch.queue_declare(
            queue=queue
        )
        ch.queue_bind(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key
        )
        ch.basic_publish(
            body=json.dumps(msg),
            exchange=exchange,
            routing_key=routing_key
        )


class Consumer(Rabbit):

    def queue_name(self):
        return ""

    def consume(self):
        ch = self.channel
        ch.queue_declare(queue=self.queue_name())
        ch.basic_consume(
            self._callback,
            queue=self.queue_name(),
            consumer_tag=self.__class__.__name__
        )
        ch.start_consuming()

    def callback(self, body):
        return

    def _callback(self, channel, method, header, body):
        self.callback(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)
