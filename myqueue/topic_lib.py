import requests


class ServerFunctions:

    def __init__(self, broker):

        self.broker = broker

    def CreateTopic(self, topic_name):

        topic_url = self.broker + "/topics"
        data = {"topic_name": topic_name}

        try:
            r = requests.post(topic_url, json = data)
            r.raise_for_status()

        except requests.exceptions.HTTPError as errhttp :
            print(f"HTTP error:{errhttp}")
        except requests.exceptions.ConnectionError as errcon :
            print(f"HTTP error:{errcon}")

        response = r.json()

        if response["status"] == "success":
            print(f"Topic : {topic_name} created successfully")
        else:
            print(f"Topic : {topic_name} ERROR in creation")
        

    def ListTopics(self):

        topics_url = self.broker + "/topics"
        data = {"topic_name": ""}

        try:
            r = requests.get(topics_url, json = data)
            r.raise_for_status()

        except requests.exceptions.HTTPError as errhttp :
            print(f"HTTP error:{errhttp}")
        except requests.exceptions.ConnectionError as errcon :
            print(f"HTTP error:{errcon}")
        
        topic_response = r.json()
        

        if topic_response["status"] == "success":
            topic_list = topic_response["topics"]
            print("Active Topics : ")
            for topics in topic_list:
                print(topics)
        else:
            print("Failure in listing topics")