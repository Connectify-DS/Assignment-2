from flask import Flask
from flask import request
from flask import jsonify

app = Flask(__name__)

@app.route('/')
def index():
    return "Hello World!"

@app.route('/topics', methods=['POST'])
def createTopic():
    topicName = request.form.get('topic_name')
    try:
        #Yahica's Function
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/topics', methods=['GET'])
def createTopic():
    try:
        #Yahica's Function
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/consumer/register', methods=['POST'])
def createTopic():
    topicName = request.form.get('topic_name')
    try:
        #Yahica's Function
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/producer/register', methods=['POST'])
def createTopic():
    topicName = request.form.get('topic_name')
    try:
        #Yahica's Function
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/producer/produce', methods=['GET'])
def createTopic():
    topicName = request.form.get('topic_name')
    producerID = request.form.get('producer_id')
    message = request.form.get('message')
    try:
        #Yahica's Function
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/consumer/consume', methods=['GET'])
def createTopic():
    topicName = request.form.get('topic_name')
    consumerId = request.form.get('consumer_id')
    try:
        #Yahica's Function
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/size', methods=['GET'])
def createTopic():
    topicName = request.form.get('topic_name')
    consumerId = request.form.get('consumer_id')
    try:
        #Yahica's Function
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

if __name__ == "__main__":
    app.run(debug=True)