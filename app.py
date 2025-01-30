from flask import Flask, request, render_template, redirect, url_for  # type: ignore
from publisher import RabbitmqPublisher  

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/enviar", methods=["POST"])
def enviar():
    nome = request.form.get("nome")
    email = request.form.get("email")
    mensagem = request.form.get("mensagem")

    if not nome or not email or not mensagem:
        return "Todos os campos são obrigatórios!", 400  

    dados = {
        "nome": nome,
        "email": email,
        "mensagem": mensagem
    }
    try:
 
        publisher = RabbitmqPublisher()
        publisher.send_message(dados)
        print(f"Dados enviados para o RabbitMQ: {dados}")
    except Exception as e:
     
        print(f"Erro ao enviar mensagem para o RabbitMQ: {e}")
        return "Erro ao processar a solicitação. Tente novamente mais tarde.", 500

    return redirect(url_for("index"))

if __name__ == "__main__":
    app.run(debug=True)  