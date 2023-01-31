import requests


class MyProducer:

    def __init__(self, topics, broker):
        
        self.broker = broker 
        self.topics = topics

        # gets the list of the topics which have already been created
        topics_url = self.broker + '/topics'
        self.id_topic_map = {}
        
        producer_url = self.broker + "/producer/register"
        
        for topic in topics:
            if topic in self.id_topic_map.keys():
                continue
            data = {"topic_name" : topic}
            try:
                r = requests.post(producer_url, json = data)
                r.raise_for_status()
            except requests.exceptions.HTTPError as errhttp :
                print(f"HTTP error:{errhttp}")
            except requests.exceptions.ConnectionError as errcon :
                print(f"HTTP error:{errcon}")
            
            producer_response = r.json()

            if producer_response["status"] == "success":
                producer_id = producer_response["producer_id"]
                self.id_topic_map[topic] = producer_id

    def add_new_topic(self, topic):

        producer_url = self.broker + "/producer/register"
        
        if topic in self.id_topic_map.keys():
            return 
        data = {"topic_name" : topic}
        try:
            r = requests.post(producer_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errhttp :
            print(f"HTTP error:{errhttp}")
        except requests.exceptions.ConnectionError as errcon :
            print(f"HTTP error:{errcon}")
        
        producer_response = r.json()

        if producer_response["status"] == "success":
            producer_id = producer_response["producer_id"]
            self.id_topic_map[topic] = producer_id

        return 
            
    def send(self, topic, message):
        
        if topic not in self.id_topic_map.keys():
            print(f"Topic not registered {topic}")
            return 

        send_url = self.broker + '/producer/produce'
        
        data = {
            "topic_name" : topic, 
            "producer_id" : self.id_topic_map[topic], 
            "message" : message 
        }

        try:
            r = requests.post(send_url, json = data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as errhttp :
            print(f"HTTP error:{errhttp}")
        except requests.exceptions.ConnectionError as errcon :
            print(f"HTTP error:{errcon}")

        response = r.json()

        if response["status"] == "success":
            print(f"Message sent successfully:{message}")
        else:
            print(f"Failed, {response['message']}")
       
        return


if __name__ == "__main__":

    pass