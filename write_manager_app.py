"""
Flask app to create a broker manager
"""
from flask import Flask
from flask import request
from flask import jsonify
from models import writeManager
import yaml
import argparse

config=None
with open('configs/wm.yaml') as f:
    config = yaml.safe_load(f)

app = Flask(__name__)
wm = writeManager(config=config)

@app.route('/broker', methods=['POST'])
def addBroker():
    req = request.json
    if req is None or 'port' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        port = req['port']
        bid = wm.add_broker(port=port)
        resp = {
            "status": "success",
            "message": f'Broker ID {bid} created successfully',
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/topics', methods=['POST'])
def addTopic():
    req = request.json
    if req is None or 'topic_name' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        topic_name = req['topic_name']
        resp = wm.add_topic(topic_name=topic_name)
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/topics', methods=['GET'])
def listTopic():
    """
    List all topics
    """
    try:
        topics = wm.list_topics()
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

@app.route('/partition', methods=['POST'])
def addPartition():
    req = request.json
    if req is None or 'topic_name' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        topic_name = req['topic_name']
        partition_name,broker_port = wm.add_partition(topic_name=topic_name)
        resp = {
            "status": "success",
            "message": f'Partition created for {topic_name}: {partition_name} at broker port {broker_port}',
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
    if req is None or 'topic_name' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        topic_name = req['topic_name']
        pid = wm.register_producer(topic_name=topic_name)
        resp = {
            "status": "success",
            "message": f'Producer ID {pid} registered for topic {topic_name}',
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
    if req is None or 'producer_id' not in req or 'topic_name' not in req or 'message' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        producer_id = req['producer_id']
        topic_name = req['topic_name']
        message = req['message']
        _ = wm.produce_message(producer_id=producer_id, topic_name=topic_name, message=message)
        resp = {
            "status": "success",
            "message": f'Producer ID {producer_id} published in topic {topic_name}',
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

if __name__ == "__main__":
    app.run(debug=False, port=config['SERVER_PORT'])
