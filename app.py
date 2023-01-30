from flask import Flask
from flask import request
from flask import jsonify
from message_queue_system import MessageQueueSystem

mqs = MessageQueueSystem(persistent=True)
app = Flask(__name__)

@app.route('/')
def index():
    return "Hello World!"

@app.route('/topics', methods=['POST'])
def createTopic():
    req = request.json
    topicName = req['topic_name']
    print(topicName)
    try:
        mqs.create_topic(topic_name=topicName)
        resp = {
            "status": "success",
            "message": f'Topic {topicName} created successfully',
        }
        return jsonify(resp)
        # raise Exception("Test error")
    except:
        resp = {
            "status": "failure",
            "message": "Error in Creating Topic",
        }
        return jsonify(resp)

@app.route('/topics', methods=['GET'])
def listTopic():
    try:
        topics = mqs.list_topics()
        resp = {
            "status": "success",
            "topics": topics,
        }
        return jsonify(resp)
        # raise Exception("Test error")
    except:
        resp = {
            "status": "failure",
            "message": "Error in Listing Topics",
        }
        return jsonify(resp)

@app.route('/consumer/register', methods=['POST'])
def registerConsumer():
    req = request.json
    topicName = req['topic_name']
    try:
        consumerId = mqs.register_consumer(topic_name=topicName)
        resp = {
            "status": "success",
            "consumer_id": consumerId,
        }
        return jsonify(resp)
        # raise Exception("Test error")
    except:
        resp = {
            "status": "failure",
            "message": "Error in Registering Consumer",
        }
        return jsonify(resp)

@app.route('/producer/register', methods=['POST'])
def registerProducer():
    req = request.json
    topicName = req['topic_name']
    try:
        producerId = mqs.register_producer(topic_name=topicName)
        resp = {
            "status": "success",
            "consumer_id": producerId,
        }
        return jsonify(resp)
        # raise Exception("Test error")
    except:
        resp = {
            "status": "failure",
            "message": "Error in Registering Producer",
        }
        return jsonify(resp)

@app.route('/producer/produce', methods=['GET'])
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
        return jsonify(resp)
        # raise Exception("Test error")
    except:
        resp = {
            "status": "failure",
            "message": "Error in publishing message",
        }
        return jsonify(resp)

@app.route('/consumer/consume', methods=['GET'])
def retrieve():
    req = request.json
    topicName = req['topic_name']
    consumerId = req['consumer_id']
    try:
        message = mqs.dequeue(topic=topicName, consumer_id=consumerId)
        resp = {
            "status": "success",
            "message": message,
        }
        return jsonify(resp)
        # raise Exception("Test error")
    except:
        resp = {
            "status": "failure",
            "message": "Error in recieving message",
        }
        return jsonify(resp)

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
        return jsonify(resp)
        # raise Exception("Test error")
    except:
        resp = {
            "status": "failure",
            "message": "Error in calculating Size",
        }
        return jsonify(resp)

if __name__ == "__main__":
    app.run(debug=True)