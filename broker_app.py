"""
Flask app to create a message queue system
"""
from flask import Flask
from flask import request
from flask import jsonify
from models import Broker
import yaml
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='config file path', type=str)
args = parser.parse_args()

with open(args.config) as f:
    config = yaml.safe_load(f)

mqs = Broker(config=config)
app = Flask(__name__)

# Create tables if persistent: Use only for testing. During actual runs. Tables should not
# be dropped and recreated
@app.before_first_request
def create_tables():
    """
    Create tables if persistent
    """
    if config['IS_PERSISTENT']:
        mqs.reset_dbms()

# Routes
@app.route('/')
def index():
    return "DS-Connectify", 200

@app.route('/topics', methods=['POST'])
def createTopic():
    """
    Create a topic
    """
    req = request.json
    if req is None:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
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
    """
    List all topics
    """
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

@app.route('/producer/produce', methods=['POST'])
def publish():
    """
    Publish a message to the queue
    """
    req = request.json
    if req is None:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    
    ## Check whether producer ID is valid and it is registered under the topic in the Broker Manager
    topicName = req['topic_name']
    message = req['message']
    
    try:
        mqs.enqueue(topic_name=topicName, message=message)
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
    """
    Retrieve a message from the queue
    """
    req = request.json
    if req is None:
        resp = {
            "status": "failure",
            "message": "Required fields absent in request",
        }
        return jsonify(resp), 400
    topicName = req['topic_name']
    offset = req['offset']
    try:
        message = mqs.dequeue(topic_name=topicName, offset=int(offset))
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

if __name__ == "__main__":
    app.run(debug=True)
