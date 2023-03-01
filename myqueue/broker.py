import requests

class MyBroker:
    def __init__(self, url):
        self.base_url = url

    # consumer function to list all the topics  
    # Returns True on Success
    def create_topic(self,topic_name:str):
        topics_url = self.base_url +  "/topics"
        data = {"topic_name" : topic_name}
        r = None

        try:
            r = requests.post(topics_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
        
        if r is None:
            print(f"Null Response")
            return False
        
        response = r.json()
        
        if response["status"] == "success":
            print("Topic Created")
            return True
        else:
            return False
        

    # list topics in the broker
    # Returns None on Failure
    def list_topics(self, topic_name):
        list_url = self.base_url + "/topics"
        r = None
        
        try:
            r = requests.get(list_url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errhttp :
            print(f"HTTP error:{errhttp}")
        except requests.exceptions.ConnectionError as errcon :
            print(f"HTTP error:{errcon}")
        
        if r is None:
            print(f"Null Response")
            return

        response = r.json()

        if response["status"] == "success":
            return response['topics']
        else:
            print(f"Failed to list topics")
       
    # Publish Message to Topic
    # Returns True on Success
    def publish_message(self, topic_name, message):
        publish_url = self.base_url +  "/producer/produce"
        data = {"topic_name" : topic_name,
                'message': message}
        r = None
        
        try:
            r = requests.post(publish_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
        
        if r is None:
            print(f"Null Response")
            return False
        
        response = r.json()

        if response["status"] == "success":
            return True
        return False  
    
    # Recieve Message Of Topic
    # Returns None on Failure
    def consume_message(self, topic_name:str,offset:int):
        consume_url = self.base_url +  "/consumer/consume"
        data = {"topic_name" : topic_name,
                'offset': str(offset)}
        r = None
        
        try:
            r = requests.get(consume_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
        
        if r is None:
            print(f"Null Response")
            return
        
        response = r.json()

        if response["status"] == "success":
            return response['message']
     



