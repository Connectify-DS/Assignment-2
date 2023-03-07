"""
Flask app to create a load balancer
"""
from flask import Flask, redirect
from flask import request
from flask import jsonify
import requests
import argparse
from urllib3.exceptions import InsecureRequestWarning
import yaml
from urllib3 import disable_warnings

disable_warnings(InsecureRequestWarning)

# parser = argparse.ArgumentParser()
# parser.add_argument('-c', '--config', help='config file path', type=str)
# args = parser.parse_args()
config=None
with open('configs/load_balancer.yaml') as f:
    config = yaml.safe_load(f)

app = Flask(__name__)

num_rms=config['NUM_READMANAGERS']
curr_id=0

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
            if data==None:
                r = requests.get(url)
            else:
                r = requests.get(url, json = data)
        elif method=="POST":
            if data==None:
                r = requests.post(url)
            else:
                r = requests.post(url, json = data)
        r.raise_for_status()
    except requests.exceptions.HTTPError as errh:
        if errh.response.status_code==400:
            resp={
                "status": "failure",
                "message": f"{forward_to} Failed: "+ str(errh.response.json()["message"])
            }
            return jsonify(resp),400
        resp={
            "status": "failure",
            "message": "HTTP Error: "+str(errh),
        }
        return jsonify(resp),400
    except requests.exceptions.ConnectionError as errc:
        resp={
            "status": "failure",
            "message": "HTTP Connection Error: "+str(errc),
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
        return jsonify(response), 200
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
    return handle_request(wm_request_url,None,"Write Manager","GET")

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

@app.route('/consumer/consume', methods=['GET'])
def consumeMessage():
    """
    Forward Message to RM to consume message
    """
    global curr_id
    rm_request_url=config['READ_MANAGER_URLS'][curr_id]+"/consumer/consume"
    curr_id=(curr_id+1)%num_rms
    data=request.json
    data["sync"]=1
    return handle_request(rm_request_url,data,"Read Manager","GET")

@app.route('/consumer/register', methods=['POST'])
def registerConsumer():
    """
    Forward message to RM to register consumer
    """
    global curr_id
    rm_request_url=config['READ_MANAGER_URLS'][curr_id]+"/consumer/register"
    curr_id=(curr_id+1)%num_rms
    data=request.json
    data["sync"]=1
    return handle_request(rm_request_url,data,"Read Manager")
  
if __name__ == "__main__":
    app.run(debug=False,port=config['SERVER_PORT'])
