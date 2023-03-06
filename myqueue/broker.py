import requests

class MyBroker:
    @staticmethod
    def add_broker(broker_port:str, rms):
        for rm in rms:
            url = "http://127.0.0.1:" + str(rm) + "/broker"
            data = {"port": broker_port}
            r = None

            try:
                r = requests.post(url, json = data)
                r.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                if errh.response.status_code==400:
                    raise Exception(f"{url} Failed: "+ str(errh.response.json()["message"]))
                raise errh
            except requests.exceptions.ConnectionError as errc:
                raise errc
            
            if r is None:
                raise Exception("Null response")
            
            response = r.json()
            if response["status"]=="failure":
                raise Exception(f"{url}: Failed to create topic")
            return response
        
    @staticmethod
    def add_topic(topic_name:str, rms):
        for rm in rms:
            url = "http://127.0.0.1:" + str(rm) + "/topics"
            data = {"topic_name": topic_name}
            r = None

            try:
                r = requests.post(url, json = data)
                r.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                if errh.response.status_code==400:
                    raise Exception(f"{url} Failed: "+ str(errh.response.json()["message"]))
                raise errh
            except requests.exceptions.ConnectionError as errc:
                raise errc
            
            if r is None:
                raise Exception("Null response")
            
            response = r.json()
            if response["status"]=="failure":
                raise Exception(f"{url}: Failed to create topic")
            return response
    
    @staticmethod
    def create_partition(url:str, topic_name:str, partition_name:str, broker_id:str, rms):
        #create partiton in the broker
        topics_url = url +  "/topics"
        data = {"topic_name" : partition_name}
        r = None

        try:
            r = requests.post(topics_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            if errh.response.status_code==400:
                raise Exception(f"{topics_url} Failed: "+ str(errh.response.json()["message"]))
            raise errh
        except requests.exceptions.ConnectionError as errc:
            raise errc
        
        if r is None:
            raise Exception("Null response")
        
        response = r.json()
        if response["status"]=="failure":
            raise Exception(f"{url}: Failed to create topic")
        
        #update all rms
        for rm in rms:
            rm_url = "http://127.0.0.1:" + str(rm) + '/partition'
            data = {"topic_name" : topic_name, 
                    "partition_name" : partition_name,
                    "broker_id": broker_id}
            r = None

            try:
                r = requests.post(rm_url, json = data)
                r.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                if errh.response.status_code==400:
                    raise Exception(f"{rm_url} Failed: "+ str(errh.response.json()["message"]))
                raise errh
            except requests.exceptions.ConnectionError as errc:
                raise errc
            
            if r is None:
                raise Exception("Null response")
            
            response = r.json()
            if response["status"]=="failure":
                raise Exception(f"{url}: Failed to create topic")
            

    # list topics in the broker
    # Returns None on Failure
    @staticmethod
    def list_topics(url:str, topic_name:str):
        list_url = url + "/topics"
        r = None
        
        try:
            r = requests.get(list_url)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh :
            if errh.response.status_code==400:
                raise Exception(f"{list_url} Failed: "+ str(errh.response.json()["message"]))
            raise errh
        except requests.exceptions.ConnectionError as errcon :
            raise errcon
        
        if r is None:
            raise Exception("Null response")

        response = r.json()
        if response["status"]=="failure":
            raise Exception(f"{url}: Failed to create topic")
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
            if errh.response.status_code==400:
                raise Exception(f"{publish_url} Failed: "+ str(errh.response.json()["message"]))
            raise errh
        except requests.exceptions.ConnectionError as errc:
            raise errc
        
        if r is None:
            raise Exception("Null response")
        
        response = r.json()

        if response["status"]=="failure":
            raise Exception(f"{url}: Failed to publish message")
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
            if errh.response.status_code==400:
                raise Exception(f"{consume_url} Failed: "+ str(errh.response.json()["message"]))
            raise errh
        except requests.exceptions.ConnectionError as errc:
            raise errc
        
        if r is None:
            raise Exception("Null response")
        
        response = r.json()

        if response["status"]=="failure":
            raise Exception(f"{url}: Failed to consume message")
        return response
     



