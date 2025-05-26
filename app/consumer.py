import pika
import json
import psycopg2
from pymongo import MongoClient
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        # Configurações PostgreSQL
        self.pg_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'appdb',
            'user': 'user',
            'password': 'password'
        }
        
        # Configurações MongoDB
        self.mongo_config = {
            'host': 'localhost',
            'port': 27017,
            'username': 'root',
            'password': 'example',
            'database': 'appdb'
        }
        
        self.pg_connection = None
        self.mongo_client = None
        self.mongo_db = None
        
    def connect_postgresql(self):
        try:
            self.pg_connection = psycopg2.connect(**self.pg_config)
            logger.info("Conectado ao PostgreSQL com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar PostgreSQL: {e}")
            return False
    
    def connect_mongodb(self):
        try:
            connection_string = f"mongodb://{self.mongo_config['username']}:{self.mongo_config['password']}@{self.mongo_config['host']}:{self.mongo_config['port']}/{self.mongo_config['database']}?authSource=admin"
            self.mongo_client = MongoClient(connection_string)
            self.mongo_db = self.mongo_client[self.mongo_config['database']]
            # Teste a conexão
            self.mongo_client.admin.command('ping')
            logger.info("Conectado ao MongoDB com sucesso")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar MongoDB: {e}")
            return False
    
    def insert_postgresql(self, dados):
        try:
            if not self.pg_connection or self.pg_connection.closed:
                if not self.connect_postgresql():
                    return False
            
            cursor = self.pg_connection.cursor()
            nome = dados.get('nome', 'Nome não informado')
            idade = dados.get('idade', 0)  
            sexo = dados.get('sexo', 'Não informado')
            
            query = """
                INSERT INTO pessoa (nome, idade, sexo) 
                VALUES (%s, %s, %s)
            """
            cursor.execute(query, (nome, idade, sexo))
            self.pg_connection.commit()
            cursor.close()
            
            logger.info(f"Dados inseridos no PostgreSQL: {nome}, {idade}, {sexo}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao inserir no PostgreSQL: {e}")
            if self.pg_connection:
                self.pg_connection.rollback()
            return False
    
    def insert_mongodb(self, dados):
        try:
            if not self.mongo_client:
                if not self.connect_mongodb():
                    return False
            
            documento = {
                'telefone': dados.get('telefone', dados.get('phone', 'Não informado')),
                'mensagem': dados.get('mensagem', dados.get('message', 'Mensagem não informada')),
                'email': dados.get('email', 'email@nao-informado.com'),
                'data_criacao': datetime.now()
            }
            
            collection = self.mongo_db['mensagem']
            result = collection.insert_one(documento)
            
            logger.info(f"Dados inseridos no MongoDB com ID: {result.inserted_id}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao inserir no MongoDB: {e}")
            return False
    
    def close_connections(self):
        if self.pg_connection and not self.pg_connection.closed:
            self.pg_connection.close()
            logger.info("Conexão PostgreSQL fechada")
        
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Conexão MongoDB fechada")


class RabbitMQConsumer:
    def __init__(self, callback, host="localhost"):
        self.__host = host
        self.__port = 5672
        self.__username = "user"
        self.__password = "1234"
        self.__queue = "envio_fila1"
        self.__routing_key = ""
        self.__callback = callback
        self.__channel = self.__create_channel()

    def __create_channel(self):
        try:
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
        except Exception as e:
            logger.error(f"Erro ao criar canal RabbitMQ: {e}")
            raise

    def start(self):
        logger.info(f"Escutando RabbitMQ na porta {self.__port}")
        try:
            self.__channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Parando o consumer...")
            self.__channel.stop_consuming()
        except Exception as e:
            logger.error(f"Erro no consumer: {e}")


class RabbitmqPublisherBanco:
    def __init__(self):
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "admin"
        self.__password = "123456"
        self.__exchange = "envio_banco"
        self.__routing_key = ""  
        self.__channel = self.__create_channel()

    def __create_channel(self):
        try:
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
        except Exception as e:
            logger.error(f"Erro ao criar canal do publisher: {e}")
            raise

    def send_message(self, message: dict):
        if not isinstance(message, dict):
            logger.error("A mensagem precisa ser um dicionário JSON.")
            return False

        try:
            self.__channel.basic_publish(
                exchange=self.__exchange,
                routing_key=self.__routing_key,
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2  
                )
            )
            logger.info(f"Mensagem enviada para a exchange {self.__exchange}: {message}")
            return True
        except Exception as e:
            logger.error(f"Erro ao publicar mensagem: {e}")
            return False


def validar_dados(dados):
    """Valida se os dados contêm os campos mínimos necessários"""
    if not isinstance(dados, dict):
        logger.error("O corpo da mensagem não é um dicionário.")
        return False
    
    has_basic_fields = (
        dados.get('nome') or dados.get('email') or 
        dados.get('mensagem') or dados.get('telefone')
    )
    
    if not has_basic_fields:
        logger.error("Dados insuficientes. Necessário pelo menos um campo: nome, email, mensagem ou telefone")
        return False
    
    return True

db_manager = DatabaseManager()

def processar_callback(ch, method, properties, body):
    """Callback principal que processa as mensagens recebidas"""
    try:
        dados = json.loads(body)
        logger.info(f"Dados recebidos: {dados}")

        if not validar_dados(dados):
            logger.warning("Dados inválidos. Ignorando mensagem.")
            return

        logger.info("Dados válidos. Processando...")

        # Inserir nos bancos de dados
        pg_success = db_manager.insert_postgresql(dados)
        mongo_success = db_manager.insert_mongodb(dados)

        if pg_success and mongo_success:
            logger.info("Dados inseridos com sucesso em ambos os bancos!")
            
            # Opcionalmente, enviar mensagem de confirmação
            try:
                publisher = RabbitmqPublisherBanco()
                confirmation_msg = {
                    "status": "success",
                    "message": "Dados processados com sucesso",
                    "original_data": dados,
                    "timestamp": datetime.now().isoformat()
                }
                publisher.send_message(confirmation_msg)
            except Exception as e:
                logger.error(f"Erro ao enviar confirmação: {e}")
                
        else:
            error_msg = []
            if not pg_success:
                error_msg.append("PostgreSQL")
            if not mongo_success:
                error_msg.append("MongoDB")
            logger.error(f"Falha ao inserir em: {', '.join(error_msg)}")

    except json.JSONDecodeError:
        logger.error("O corpo da mensagem não é um JSON válido.")
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {e}")


def main():
    """Função principal"""
    try:
        # Conectar aos bancos de dados
        logger.info("Conectando aos bancos de dados...")
        pg_connected = db_manager.connect_postgresql()
        mongo_connected = db_manager.connect_mongodb()
        
        if not pg_connected or not mongo_connected:
            logger.error("Falha ao conectar aos bancos de dados")
            return
        
        # Iniciar consumer
        logger.info("Iniciando consumer RabbitMQ...")
        rabbitmq_consumer = RabbitMQConsumer(processar_callback)
        rabbitmq_consumer.start()
        
    except KeyboardInterrupt:
        logger.info("Aplicação interrompida pelo usuário")
    except Exception as e:
        logger.error(f"Erro na aplicação: {e}")
    finally:
        # Fechar conexões
        db_manager.close_connections()
        logger.info("Aplicação finalizada")


if __name__ == "__main__":
    main()