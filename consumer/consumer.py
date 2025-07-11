import pika
import json
import psycopg2
from pymongo import MongoClient
from datetime import datetime
import logging
import threading
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.pg_config = {
            'host': 'postgres',
            'port': 5432,
            'database': 'appdb',
            'user': 'user',
            'password': 'password'
        }

        self.mongo_config = {
            'host': 'mongo',
            'port': 27017,
            'username': 'root',
            'password': 'example',
            'database': 'appdb'
        }

        self.pg_connection = None
        self.mongo_client = None
        self.mongo_db = None
        self.pg_status = False
        self.mongo_status = False

    def test_postgresql_connection(self):
        """Testa a conexão com PostgreSQL sem manter a conexão ativa"""
        try:
            test_conn = psycopg2.connect(**self.pg_config)
            test_conn.close()
            return True
        except Exception as e:
            logger.debug(f"PostgreSQL offline: {e}")
            return False

    def test_mongodb_connection(self):
        """Testa a conexão com MongoDB sem manter a conexão ativa"""
        try:
            connection_string = f"mongodb://{self.mongo_config['username']}:{self.mongo_config['password']}@{self.mongo_config['host']}:{self.mongo_config['port']}/{self.mongo_config['database']}?authSource=admin"
            test_client = MongoClient(connection_string, serverSelectionTimeoutMS=2000)
            test_client.admin.command('ping')
            test_client.close()
            return True
        except Exception as e:
            logger.debug(f"MongoDB offline: {e}")
            return False

    def check_databases_status(self):
        """Verifica o status de ambos os bancos de dados"""
        pg_status = self.test_postgresql_connection()
        mongo_status = self.test_mongodb_connection()
        
        # Log apenas quando há mudança de status
        if pg_status != self.pg_status:
            if pg_status:
                logger.info("PostgreSQL voltou a ficar online")
            else:
                logger.warning("PostgreSQL ficou offline")
            self.pg_status = pg_status
            
        if mongo_status != self.mongo_status:
            if mongo_status:
                logger.info("MongoDB voltou a ficar online")
            else:
                logger.warning("MongoDB ficou offline")
            self.mongo_status = mongo_status
            
        return pg_status and mongo_status

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


class DatabaseHealthChecker:
    def __init__(self, db_manager, rabbitmq_consumer):
        self.db_manager = db_manager
        self.rabbitmq_consumer = rabbitmq_consumer
        self.running = False
        self.thread = None
        self.databases_online = True

    def start_monitoring(self):
        """Inicia o monitoramento dos bancos de dados"""
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        logger.info("Monitoramento de bancos de dados iniciado")

    def stop_monitoring(self):
        """Para o monitoramento dos bancos de dados"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join()
        logger.info("Monitoramento de bancos de dados parado")

    def _monitor_loop(self):
        """Loop principal de monitoramento"""
        while self.running:
            try:
                databases_status = self.db_manager.check_databases_status()
                
                if databases_status != self.databases_online:
                    self.databases_online = databases_status
                    
                    if not databases_status:
                        logger.warning("Um ou mais bancos estão offline. Parando consumer...")
                        self.rabbitmq_consumer.stop_consuming()
                        logger.info("Aguardando reconexão dos bancos...")
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Erro no monitoramento de bancos: {e}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Erro no monitoramento de bancos: {e}")
                time.sleep(1)
    
    def _reconnect_databases(self):
        """Tenta reconectar aos bancos de dados"""
        try:
            self.db_manager.close_connections()
            pg_connected = self.db_manager.connect_postgresql()
            mongo_connected = self.db_manager.connect_mongodb()
            
            return pg_connected and mongo_connected
        except Exception as e:
            logger.error(f"Erro na reconexão: {e}")
            return False


class RabbitmqConsumer:
    def __init__(self, callback, host="rabbit") -> None:
        self.__host = host
        self.__port = 5672
        self.__username = "user"
        self.__password = "1234"
        self.__queue = "envio_fila1"
        self.__routing_key = ""
        self.__callback = callback
        self.__connection = None
        self.__channel = None
        self.consuming = False

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
            self.__connection = pika.BlockingConnection(connection_parameters)
            channel = self.__connection.channel()

            channel.queue_declare(
                queue=self.__queue,
                durable=True
            )

            channel.basic_consume(
                queue=self.__queue,
                auto_ack=True,
                on_message_callback=lambda ch, method, properties, body: self.__callback(self, ch, method, properties, body)
            )

            return channel
        except Exception as e:
            logger.error(f"Erro ao criar canal RabbitMQ: {e}")
            raise

    def start_consuming(self):
        """Inicia o consumo das mensagens"""
        if self.consuming:
            logger.warning("Consumer já está em execução")
            return
            
        try:
            if not self.__channel or self.__connection.is_closed:
                self.__channel = self.__create_channel()
            
            self.consuming = True
            logger.info("Consumer RabbitMQ iniciado")
            
            while self.consuming:
                try:
                    self.__connection.process_data_events(time_limit=0.5)
                except pika.exceptions.ConnectionClosedByBroker:
                    logger.warning("Conexão fechada pelo broker. Tentando reconectar...")
                    self.consuming = False
                    break
                except pika.exceptions.AMQPChannelError as err:
                    logger.error(f"Erro de canal: {err}. Recriando canal...")
                    self.consuming = False
                    break
                except pika.exceptions.AMQPConnectionError:
                    logger.error("Erro de conexão AMQP. Tentando reconectar...")
                    self.consuming = False
                    break
                except Exception as e:
                    logger.error(f"Erro inesperado: {e}")
                    self.consuming = False
                    break
                    
        except Exception as e:
            self.consuming = False
            logger.error(f"Erro ao iniciar consumo: {e}")
            raise

    def stop_consuming(self):
        """Para o consumo das mensagens"""
        if not self.consuming:
            return
            
        try:
            self.consuming = False
            if self.__channel:
                self.__channel.stop_consuming()
            logger.info("Consumo RabbitMQ parado")
        except Exception as e:
            logger.error(f"Erro ao parar consumo: {e}")

    def start(self):
        """Método principal para iniciar o consumer"""
        logger.info(f"Escutando RabbitMQ na porta {self.__port}")
        try:
            self.__channel = self.__create_channel()
            self.start_consuming()
        except KeyboardInterrupt:
            logger.info("Parando o consumer...")
            self.stop_consuming()
            if self.__connection and not self.__connection.is_closed:
                self.__connection.close()
        except Exception as e:
            logger.error(f"Erro no consumer: {e}")
            if self.__connection and not self.__connection.is_closed:
                self.__connection.close()

    def close_connection(self):
        """Fecha a conexão com RabbitMQ"""
        self.stop_consuming()
        if self.__connection and not self.__connection.is_closed:
            self.__connection.close()


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


def processar_callback(consumer, ch, method, properties, body):
    """Callback principal que processa as mensagens recebidas"""
    try:
        dados = json.loads(body)
        logger.info(f"Dados recebidos: {dados}")

        if not validar_dados(dados):
            logger.warning("Dados inválidos. Ignorando mensagem.")
            return

        logger.info("Dados válidos. Processando...")

        pg_success = db_manager.insert_postgresql(dados)
        mongo_success = db_manager.insert_mongodb(dados)

        if pg_success and mongo_success:
            logger.info("Dados inseridos com sucesso em ambos os bancos!")
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
    health_checker = None
    rabbitmq_consumer = None
    consumer_thread = None
    should_restart = True 
    
    try:
        logger.info("=== INICIANDO APLICAÇÃO ===")
        
        while True:
            try:
                if should_restart:
                    should_restart = False
                    logger.info("Verificando conectividade dos bancos...")
                    
                    databases_online = db_manager.check_databases_status()
                    
                    if databases_online:
                        logger.info("Ambos os bancos estão online. Conectando...")
                        pg_connected = db_manager.connect_postgresql()
                        mongo_connected = db_manager.connect_mongodb()
                        
                        if pg_connected and mongo_connected:
                            logger.info("Conexões estabelecidas com sucesso!")
                            
                        
                            if health_checker:
                                health_checker.stop_monitoring()
                            if rabbitmq_consumer:
                                rabbitmq_consumer.close_connection()
                            if consumer_thread and consumer_thread.is_alive():
                                rabbitmq_consumer.stop_consuming()
                                consumer_thread.join(timeout=2)
                            
                       
                            rabbitmq_consumer = RabbitmqConsumer(processar_callback, host="rabbit")
                            
                            
                            health_checker = DatabaseHealthChecker(db_manager, rabbitmq_consumer)
                            health_checker.start_monitoring()
                            
                           
                            consumer_thread = threading.Thread(
                                target=rabbitmq_consumer.start_consuming, 
                                daemon=True
                            )
                            consumer_thread.start()
                            
                            logger.info("Sistema operacional! Monitorando bancos de dados...")
                        else:
                            should_restart = True
                            logger.warning("Falha ao conectar em um ou ambos os bancos. Tentando novamente...")
                            time.sleep(5)
                            continue
                    else:
                        should_restart = True
                        logger.warning("Um ou mais bancos estão offline. Tentando novamente em 5 segundos...")
                        time.sleep(5)
                        continue
                
                
                if not db_manager.check_databases_status():
                    should_restart = True
                    logger.warning("Um dos bancos caiu! Reiniciando...")
                    continue
             
                if consumer_thread and not consumer_thread.is_alive():
                    should_restart = True
                    logger.warning("Thread do consumer morreu. Reiniciando...")
                    continue
                
                time.sleep(1)
                    
            except KeyboardInterrupt:
                logger.info("Aplicação interrompida pelo usuário")
                break
            except Exception as e:
                should_restart = True
                logger.error(f"Erro no loop principal: {e}")
                logger.info("Tentando novamente em 5 segundos...")
                time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Aplicação interrompida pelo usuário")
    except Exception as e:
        logger.error(f"Erro crítico na aplicação: {e}")
    finally:
        logger.info("Finalizando aplicação...")
        if health_checker:
            health_checker.stop_monitoring()
        if rabbitmq_consumer:
            rabbitmq_consumer.close_connection()
        if consumer_thread and consumer_thread.is_alive():
            rabbitmq_consumer.stop_consuming()
            consumer_thread.join(timeout=2)
        db_manager.close_connections()
        logger.info("Aplicação finalizada")
        
if __name__ == "__main__":
    main()