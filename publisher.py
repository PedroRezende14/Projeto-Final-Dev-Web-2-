from typing import Dict
import pika  # type: ignore
import json

class RabbitmqPublisher:
    def __init__(self) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "admin"
        self.__password = "123456"
        self.__exchange = "envio_fila1"
        self.__routing_key = ""
        self.__channel = self.__create_channel()

    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )

        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()

        channel.exchange_declare(
            exchange=self.__exchange,
            exchange_type="direct",
            durable=True
        )

        return channel

    def send_message(self, body: Dict):
        try:
            self.__channel.basic_publish(
                exchange=self.__exchange,
                routing_key=self.__routing_key,
                body=json.dumps(body),
                properties=pika.BasicProperties(
                    delivery_mode=2
                )
            )
            print(f"Mensagem publicada: {body}")
        except Exception as e:
            print(f"Erro ao publicar mensagem: {e}")