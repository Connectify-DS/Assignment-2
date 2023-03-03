import requests

class MyBroker:
    # consumer function to list all the topics  
    # Returns True on Success
    @staticmethod
    def create_topic(url:str, topic_name:str):
        topics_url = url +  "/topics"
        data = {"topic_name" : topic_name}
        r = None

        try:
            r = requests.post(topics_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            raise errh
        except requests.exceptions.ConnectionError as errc:
            raise errc
        
        if r is None:
            raise Exception("Null response")
        
        response = r.json()
        if response["status"]=="failure":
            raise Exception(f"{self.base_url}: Failed to create topic")
        return response
        

    # list topics in the broker
    # Returns None on Failure
    @staticmethod
    def list_topics(url:str, topic_name:str):
        list_url = url + "/topics"
        r = None
        
        try:
            r = requests.get(list_url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errhttp :
            raise errhttp
        except requests.exceptions.ConnectionError as errcon :
            raise errcon
        
        if r is None:
            raise Exception("Null response")

        response = r.json()
        if response["status"]=="failure":
            raise Exception(f"{self.base_url}: Failed to create topic")
        return response
       
    # Publish Message to Topic
    # Returns True on Success
    @staticmethod
    def publish_message(url:str, topic_name:str, message:str):
        publish_url = url +  "/producer/produce"
        data = {"topic_name" : topic_name,
                'message': message}
        r = None
        
        try:
            r = requests.post(publish_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            raise errh
        except requests.exceptions.ConnectionError as errc:
            raise errc
        
        if r is None:
            raise Exception("Null response")
        
        response = r.json()

        if response["status"]=="failure":
            raise Exception(f"{self.base_url}: Failed to publish message")
        return response
    
    # Recieve Message Of Topic
    # Returns None on Failure
    @staticmethod
    def consume_message(url:str, topic_name:str, offset:int):
        consume_url = url +  "/consumer/consume"
        data = {"topic_name" : topic_name,
                'offset': str(offset)}
        r = None
        
        try:
            r = requests.get(consume_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            raise errh
        except requests.exceptions.ConnectionError as errc:
            raise errc
        
        if r is None:
            raise Exception("Null response")
        
        response = r.json()

        if response["status"]=="failure":
            raise Exception(f"{self.base_url}: Failed to consume message")
        return response
     



