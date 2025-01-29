import pika # type: ignore

class Conectar:
    def __init__(self) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "admin"
        self.__password = "123456"
        self.__exchange = "fila_exchange"
        self.__routing_key = ""
        self.__channel = self.__create_channel()

    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            ),
            virtual_host='/'
        )
        connection = pika.BlockingConnection(connection_parameters)
        return connection.channel()

conectar = Conectar()
channel = conectar._Conectar__channel  
channel.exchange_declare(exchange='fila_exchange', exchange_type='direct', durable=True)
channel.exchange_declare(exchange='banco_exchange', exchange_type='direct', durable=True)

channel.queue_declare(queue='Fila1', durable=True)
channel.queue_declare(queue='banco_fila', durable=True)

channel.queue_bind(exchange='fila_exchange', queue='Fila1', routing_key='')
channel.queue_bind(exchange='banco_exchange', queue='banco_fila', routing_key='banco_routing_key')

print("Exchanges e Queues criadas com sucesso!")

conectar._Conectar__channel.connection.close()  
