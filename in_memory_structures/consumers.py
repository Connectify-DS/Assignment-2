from models import Consumer

class ConsumerTable:
    def __init__(self):
        self.consumer_entry = {}
        self.consumer_id_auto_inc = 0

    def get_consumer(self,consumer_id):
        return self.consumer_entry[consumer_id]

    def create_consumer(self,consumer_id,topic_name):
        self.consumer_entry[consumer_id] = Consumer(consumer_id,topic_name)

    def register_to_topic(self,topic_name):
        cur_consumer_id = self.consumer_id_auto_inc+1
        self.consumer_id_auto_inc += 1
        self.create_consumer(cur_consumer_id,topic_name)
        return cur_consumer_id
