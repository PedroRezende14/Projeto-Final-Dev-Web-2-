import pika  # type: ignore
import json
import mysql.connector # type: ignore


DB_CONFIG = {
    "host": "localhost",
    "user": "app_user",
    "password": "app_password",
    "database": "app_database"
}

class RabbitMQConsumer:
    def __init__(self, callback) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "admin"
        self.__password = "123456"
        self.__queue = "banco_fila"
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

def salvar_no_banco(dados):
    """
    Insere os dados na tabela 'mensagens'.
    """
    try:
        conexao = mysql.connector.connect(**DB_CONFIG)
        cursor = conexao.cursor()

        sql = "INSERT INTO mensagens (nome, email, mensagem) VALUES (%s, %s, %s)"
        valores = (dados["nome"], dados["email"], dados["mensagem"])
        
        cursor.execute(sql, valores)
        conexao.commit()

        print(f"Dados inseridos no banco: {dados} \n")
    except mysql.connector.Error as err:
        print(f"Erro ao inserir dados no banco: {err}")
    finally:
        if cursor:
            cursor.close()
        if conexao:
            conexao.close()

def minha_callback(ch, method, properties, body):
    try:
        dados = json.loads(body)
        print(f"Dados recebidos: {dados} \n")

        salvar_no_banco(dados)

    except json.JSONDecodeError:
        print("Erro: O corpo da mensagem não é um JSON válido.")
    except Exception as e:
        print(f"Erro ao processar mensagem: {e}")

rabbitmq_consumer = RabbitMQConsumer(minha_callback)
rabbitmq_consumer.start()
