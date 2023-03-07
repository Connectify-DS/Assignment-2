"""
Flask app to create a read manager
"""
from flask import Flask
from flask import request
from flask import jsonify
from models import readManager
import yaml
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='config file path', type=str)
args = parser.parse_args()
config=None
with open(args.config) as f:
    config = yaml.safe_load(f)

app = Flask(__name__)
rm = readManager(config=config)

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
        rm.add_broker(port=port)
        resp = {
            "status": "success",
            "message": f'Broker with port {port} added successfully',
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
        rm.add_topic(topic_name)
        resp = {
            "status": "success",
            "message": f"Topic {topic_name} added",
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
    """
    List all topics
    """
    try:
        topics = rm.list_topics()
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
    if req is None or 'topic_name' not in req or 'partition_name' not in req or 'broker_id' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        topic_name = req['topic_name']
        partition_name = req['partition_name']
        broker_id = req['broker_id']
        rm.add_partition(topic_name, partition_name, broker_id)
        resp = {
            "status": "success",
            "message": f'Partition created for {topic_name}: {partition_name} at broker id {broker_id}',
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
    if req is None or 'topic_name' not in req or 'sync' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        topic_name = req['topic_name']
        sync = req["sync"]
        cid = rm.register_consumer(topic_name=topic_name, sync=sync)
        
        resp = {
            "status": "success",
            "message": f'Consumer ID {cid} subscribed to topic {topic_name}',
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
    if req is None or 'consumer_id' not in req or 'topic_name' not in req or 'sync' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        consumer_id = req['consumer_id']
        topic_name = req['topic_name']
        sync = req["sync"]
        act_message=rm.consume_message(consumer_id=consumer_id, topic_name=topic_name, sync=sync)
        resp = {
            "status": "success",
            "message": f"Consumer ID {consumer_id} retrieved message from topic {topic_name}: {act_message}",
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