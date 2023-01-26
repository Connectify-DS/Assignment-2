from flask import Flask
from flask import request
from flask import jsonify
from message_queue_system import MessageQueueSystem

app = Flask(__name__)
mqs = MessageQueueSystem()

@app.route('/')
def index():
    return "Hello World!"

@app.route('/topics', methods=['POST'])
def createTopic():
    topicName = request.form.get('topic_name')
    try:
        mqs.create_topic(topic_name=topicName)
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/topics', methods=['GET'])
def listTopic():
    try:
        print(mqs.list_topics())
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/consumer/register', methods=['POST'])
def registerConsumer():
    topicName = request.form.get('topic_name')
    try:
        mqs.register_consumer(topic_name=topicName)
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/producer/register', methods=['POST'])
def registerProducer():
    topicName = request.form.get('topic_name')
    try:
        mqs.register_producer(topic_name=topicName)
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/producer/produce', methods=['GET'])
def publish():
    topicName = request.form.get('topic_name')
    producerID = request.form.get('producer_id')
    message = request.form.get('message')
    try:
        mqs.enqueue(topic_name=topicName, producer_id=producerID, message=message)
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/consumer/consume', methods=['GET'])
def retrieve():
    topicName = request.form.get('topic_name')
    consumerId = request.form.get('consumer_id')
    try:
        mqs.dequeue(topic=topicName, consumer_id=consumerId)
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

@app.route('/size', methods=['GET'])
def getSize():
    topicName = request.form.get('topic_name')
    consumerId = request.form.get('consumer_id')
    try:
        print(mqs.size(topic=topicName, consumer_id=consumerId))
        raise Exception("Test error")
    except:
        return jsonify("Error Message")

if __name__ == "__main__":
    app.run(debug=True)