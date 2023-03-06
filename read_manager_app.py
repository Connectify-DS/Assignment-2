"""
Flask app to create a message queue system
"""
from flask import Flask
from flask import request
from flask import jsonify
from models import readManager
import yaml
import argparse

config=None
with open('configs/rm.yaml') as f:
    config = yaml.safe_load(f)

app = Flask(__name__)
rm = readManager(config=config)

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
        resp = {
            "status": "success",
            "message": f"Topic {topic_name} added",
        }
        return jsonify(resp), 200
    except Exception as e:
        print(e)
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
    if req is None or 'topic_name' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        topic_name = req['topic_name']
        cid = rm.register_consumer(topic_name=topic_name)
        resp = {
            "status": "success",
            "message": f'Consumer ID {cid} registered for topic {topic_name}',
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

@app.route('/consumer/consume', methods=['POST'])
def retrieve():
    req = request.json
    if req is None or 'consumer_id' not in req or 'topic_name' not in req:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    try:
        consumer_id = req['consumer_id']
        topic_name = req['topic_name']
        rm.consume_message(consumer_id=consumer_id, topic_name=topic_name)
        resp = {
            "status": "success",
            "message": f'Consumer ID {consumer_id} published in topic {topic_name}',
        }
        return jsonify(resp), 200
    except Exception as e:
        resp = {
            "status": "failure",
            "message": str(e),
        }
        return jsonify(resp), 400

if __name__ == "__main__":
    app.run(debug=True, port=config['SERVER_PORT'])
