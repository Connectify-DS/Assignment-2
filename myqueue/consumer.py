import requests

class MyConsumer:

    def __init__(self, topics, broker):

        self.topics = topics
        self.broker = broker 
        self.id_topic_map = {}

        #subscribing to the topics

        for topic_name in topics:
            if topic_name in self.id_topic_map.keys():
                print(f"Consumer already registered to {topic_name}")
                continue
            
            subscribe_url = self.broker + "/consumer/register"

            data = {"topic_name" : topic_name}

            try:
                r = requests.post(subscribe_url, json = data)
                r.raise_for_status()
            except requests.exceptions.HTTPError as errhttp :
                print(f"HTTP error:{errhttp}")
            except requests.exceptions.ConnectionError as errcon :
                print(f"HTTP error:{errcon}")
            
            subscribe_response = r.json()

            if subscribe_response["status"] == "success":
                consumer_id = subscribe_response["consumer_id"]
                self.id_topic_map[topic_name] = consumer_id
                print(f"Registered consumer_id {consumer_id} for topic {topic_name}")

    # consumer function to list all the topics  
    def get_all_topics(self):

        topics_url = self.base_url +  "/topics"
        data = {"topic_name" : topic_name}

        try:
            r = requests.get(topics_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
        
        response = r.json()
        
        if response["status"] == "success":
            print("Active Topics : ")
            for topic_name in response["topics"]:
                print(topic_name)
        
        return

    # get next message from the topic name 
    def get_next(self, topic_name):

        if topic_name not in self.id_topic_map.keys():
            print(f"Topic name : {topic_name} not subscribed")
            return

        consume_url = self.broker + "consumer/consume"
        
        data = {
            "topic_name" : topic_name, 
            "consumer_id": self.id_topic_map[topic_name]
        }
        
        try:
            r = requests.post(consume_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errhttp :
            print(f"HTTP error:{errhttp}")
        except requests.exceptions.ConnectionError as errcon :
            print(f"HTTP error:{errcon}")

        response = r.json()

        if response["status"] == "success":
            print("Message recieved successfully")

        else:
            print(f"Failed, {response['message']}")
       
        return response  


    def subscribe_new_topic(self, topic_name):

        register_url = self.broker +  "/consumer/register"
        data = {"topic_name" : topic_name}
        
        try:
            r = requests.post(register_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            print ("Http Error:",errh)
        except requests.exceptions.ConnectionError as errc:
            print ("Error Connecting:",errc)
        
        response = r.json()

        if response["status"] == "success":
            consumer_id = response["consumer_id"]
            self.id_topic_map[topic_name] = consumer_id
            print(f"Registered consumer_id {consumer_id} for topic {topic_name}")
        return         
