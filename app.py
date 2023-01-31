from flask import Flask
from flask import request
from flask import jsonify
from message_queue_system import MessageQueueSystem

IS_PERSISTENT = True
mqs = MessageQueueSystem(persistent=IS_PERSISTENT)
app = Flask(__name__)

@app.before_first_request
def create_tables():
    if IS_PERSISTENT:
        mqs.message_table.create_table()
        mqs.consumer_table.create_table()
        mqs.producer_table.create_table()
        mqs.topic_table.create_table()

@app.route('/')
def index():
    return "DS-Connectify", 200

@app.route('/topics', methods=['POST'])
def createTopic():
    req = request.json
    topicName = req['topic_name']
    try:
        mqs.create_topic(topic_name=topicName)
        resp = {
            "status": "success",
            "message": f'Topic {topicName} created successfully',
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/topics', methods=['GET'])
def listTopic():
    try:
        topics = mqs.list_topics()
        resp = {
            "status": "success",
            "topics": topics,
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/consumer/register', methods=['POST'])
def registerConsumer():
    req = request.json
    topicName = req['topic_name']
    try:
        consumerId = mqs.register_consumer(topic_name=topicName)
        resp = {
            "status": "success",
            "consumer_id": consumerId[0],
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/producer/register', methods=['POST'])
def registerProducer():
    req = request.json
    topicName = req['topic_name']
    try:
        producerId = mqs.register_producer(topic_name=topicName)
        resp = {
            "status": "success",
            "producer_id": producerId,
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/producer/produce', methods=['POST'])
def publish():
    req = request.json
    topicName = req['topic_name']
    producerID = req['producer_id']
    message = req['message']
    try:
        mqs.enqueue(topic_name=topicName, producer_id=producerID, message=message)
        resp = {
            "status": "success",
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/consumer/consume', methods=['GET'])
def retrieve():
    req = request.json
    topicName = req['topic_name']
    consumerId = req['consumer_id']
    try:
        message = mqs.dequeue(topic_name=topicName, consumer_id=consumerId)
        resp = {
            "status": "success",
            "message": str(message.message),
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/size', methods=['GET'])
def getSize():
    req = request.json
    topicName = req['topic_name']
    consumerId = req['consumer_id']
    try:
        queuesize = mqs.size(topic=topicName, consumer_id=consumerId)
        resp = {
            "status": "success",
            "size": queuesize,
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

if __name__ == "__main__":
    app.run(debug=True)
