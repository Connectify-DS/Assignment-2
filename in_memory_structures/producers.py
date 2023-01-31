from models import Producer

class ProducerTable:
    def __init__(self):
        self.producer_entry = {}
        self.producer_id_auto_inc = 0

    def get_producer(self,producer_id):
        return self.producer_entry[producer_id]

    def create_producer(self,producer_id,topic_name):
        self.producer_entry[producer_id] = Producer(producer_id,topic_name)

    def register_new_producer_to_topic(self,topic_name):
        cur_producer_id = self.producer_id_auto_inc+1
        self.producer_id_auto_inc += 1
        self.create_producer(cur_producer_id,topic_name)
        return cur_producer_id
