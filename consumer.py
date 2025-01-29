import pika  # type: ignore
import json


class RabbitMQConsumer:
    def __init__(self, callback) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "admin"
        self.__password = "123456"
        self.__queue = "Fila1"
        self.__callback = callback
        self.__channel = self.create_channel()

    def create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )
        channel = pika.BlockingConnection(connection_parameters).channel()
        channel.queue_declare(
            queue=self.__queue,
            durable=True
        )
        channel.basic_consume(
            queue=self.__queue,
            auto_ack=True,
            on_message_callback=self.__callback
        )
        return channel

    def start(self):
        print(f'Escutando RabbitMQ na porta {self.__port} \n')
        self.__channel.start_consuming()









class RabbitmqPublisherBanco:
    def __init__(self) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "admin"
        self.__password = "123456"
        self.__exchange = "banco_exchange"
        self.__routing_key = "banco_routing_key"  
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

    def send_message(self, message: dict):
        if not isinstance(message, dict):
            print("Erro: A mensagem precisa ser um dicionário JSON.")
            return

        try:
            self.__channel.basic_publish(
                exchange=self.__exchange,
                routing_key=self.__routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2  
                )
            )
            print(f"Mensagem enviada para a exchange {self.__exchange}: {message} \n")
        except Exception as e:
            print(f"Erro ao publicar mensagem: {e}")


def validar_dados(dados):

    campos_obrigatorios = {"nome", "email", "mensagem"}
    if not isinstance(dados, dict):
        print("Erro: O corpo da mensagem não é um dicionário.")
        return False
    if not campos_obrigatorios.issubset(dados.keys()):
        print(f"Erro: Dados incompletos. Campos obrigatórios: {campos_obrigatorios}")
        return False
    return True


def minha_callback(ch, method, properties, body):
    try:
        dados = json.loads(body)
        print(f"Dados recebidos: {dados}")

        if validar_dados(dados):
            print("Dados válidos \n")

            publisher = RabbitmqPublisherBanco()
            publisher.send_message(dados)

        else:
            print("Dados inválidos. Ignorando mensagem.")
    except json.JSONDecodeError:
        print("Erro: O corpo da mensagem não é um JSON válido.")
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")


if __name__ == "__main__":
   
    rabbitmq_consumer = RabbitMQConsumer(minha_callback)
    rabbitmq_consumer.start()
