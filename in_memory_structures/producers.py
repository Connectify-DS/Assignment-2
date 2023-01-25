class ProducerTable:
    # This will ideally convert to a Table. Currently a strcture to be maintained in memory.
    def __init__(self):
        self.producer_entry = {}
        self.producer_id_auto_inc = 0

    def get_producer(self,producer_id):
        return self.producer_entry[producer_id]

    def create_producer(self,producer_id,topic_name):
        self.producer_entry[producer_id] = Producer(producer_id,topic_name)

    def register_new_producer_to_topic(self,topic_name):
        # this needs to be atomic
        cur_producer_id = self.producer_id_auto_inc+1
        self.producer_id_auto_inc += 1
        # atomic code ends here
        self.create_producer(cur_producer_id,topic_name)
        return cur_producer_id

class Producer:
    def __init__(self, producer_id, producer_topic):
        self.producer_id = producer_id
        self.producer_topic = producer_topic

        
