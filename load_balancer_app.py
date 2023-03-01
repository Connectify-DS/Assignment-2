"""
Flask app to create a message queue system
"""
from flask import Flask
from flask import request
from flask import jsonify
import requests
import argparse
import yaml

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help='config file path', type=str)
args = parser.parse_args()
config=None
with open(args.config) as f:
    config = yaml.safe_load(f)

app = Flask(__name__)

# Routes
@app.route('/')
def index():
    return "DS-Connectify-Load-Balancer", 200

def handle_request(url,data,forward_to,method="POST"):
    """
    Code to handle post requests to Write Manager or Read Manager URL. 
    Made a function to reduce redundancy
    """
    try:   
        if method=="GET":
            r = requests.get(url, json = data)
        elif method=="POST":
            r = requests.post(url, json = data)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        resp={
            "status": "failure",
            "message": str(errh),
        }
        return jsonify(resp),400
    except requests.exceptions.ConnectionError as errc:
        resp={
            "status": "failure",
            "message": str(errc),
        }
        return jsonify(resp),400

    if r is None:
        resp={
            "status": "failure",
            "message": f"Got empty request from {forward_to}",
        }
        return jsonify(resp),400
    
    response = r.json()
    if response["status"] == "success":
        resp={
            "status": "success",
            "message": response["message"]
        }
        return jsonify(resp), 200
    else:
        resp={
            "status": "failure",
            "message": f"{forward_to} Failed: "+ str(response["message"])
        }
        return jsonify(resp), 400

@app.route('/broker',methods=['POST'])
def addBroker():
    """
    Add a new Broker Server
    """
    wm_request_url = config['WRITE_MANAGER_URL'] +  "/broker"
    return handle_request(wm_request_url,request.json,"Write Manager")

@app.route('/topics',methods=['POST'])
def addTopic():
    """
    Add a new Topic
    """
    wm_request_url = config['WRITE_MANAGER_URL'] +  "/topics"
    return handle_request(wm_request_url,request.json,"Write Manager")

@app.route('/partition',methods=['POST'])
def addPartition():
    """
    Create a new Partition for a Topic
    """
    wm_request_url = config['WRITE_MANAGER_URL'] +  "/partition"
    return handle_request(wm_request_url,request.json,"Write Manager")

@app.route('/topics',methods=['GET'])
def listTopics():
    """
    List all the created topics
    """
    wm_request_url = config['WRITE_MANAGER_URL'] +  "/topics"
    return handle_request(wm_request_url,request.json,"Write Manager","GET")

@app.route('/producer/register',methods=['POST'])
def registerProducer():
    """
    Register a producer to a topic
    """
    wm_request_url = config['WRITE_MANAGER_URL'] +  "/producer/register"
    return handle_request(wm_request_url,request.json,"Write Manager")

@app.route('/producer/produce',methods=['POST'])
def produceMessage():
    """
    Allow produce to send message of a topic
    """
    wm_request_url = config['WRITE_MANAGER_URL'] +  "/producer/produce"
    return handle_request(wm_request_url,request.json,"Write Manager")
  
   
