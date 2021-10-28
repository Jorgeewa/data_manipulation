import pika
import os
from dotenv import load_dotenv
load_dotenv()


class RabbitMq:

	def __init__(self):
		self.username = os.getenv("rabbit_username")
		self.password = os.getenv("rabbit_password")
		self.host = os.getenv("rabbit_host")
		self.vhost = os.getenv("rabbit_vhost")
		self.port = os.getenv("rabbit_port")

	def connect(self, queue) -> None:
		credentials = pika.PlainCredentials(self.username, self.password)
		self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host, self.port, self.vhost, credentials))
		self.channel = self.connection.channel()
		self.channel.queue_declare(queue=queue, durable=True)
		return self.connection

	def get_channel(self) -> pika.BlockingConnection:
		return self.channel

	def close_connection(self) -> None:
		self.connection.close()


