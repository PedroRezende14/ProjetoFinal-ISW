from flask import Flask, request, render_template, redirect, url_for
from typing import Dict
import pika
import json

app = Flask(__name__)

class RabbitmqPublisher:
    def __init__(self, host="rabbit") -> None:
        self.__host = host
        self.__port = 5672
        self.__username = "user"
        self.__password = "1234"
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

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/enviar", methods=["POST"])
def enviar():
    nome = request.form.get("nome")
    email = request.form.get("email")
    mensagem = request.form.get("mensagem")
    telefone = request.form.get("telefone")
    idade = request.form.get("idade")
    sexo = request.form.get("sexo")

    if not nome or not email or not mensagem or not telefone or not idade or not sexo:
        return "Todos os campos são obrigatórios!", 400

    dados = {
        "nome": nome,
        "email": email,
        "mensagem": mensagem,
        "telefone": telefone,
        "idade": idade,
        "sexo": sexo
    }

    try:
        publisher = RabbitmqPublisher(host="rabbit")
        publisher.send_message(dados)
        print(f"Dados enviados para o RabbitMQ: {dados}")
    except Exception as e:
        print(f"Erro ao enviar mensagem para o RabbitMQ: {e}")
        return "Erro ao processar a solicitação. Tente novamente mais tarde.", 500

    return redirect(url_for("index"))

if __name__ == "__main__":
   app.run(host='0.0.0.0', port=5000)
