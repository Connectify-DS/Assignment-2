from database_structures import ProducerDBMS

class ProducerTable:
    def __init__(self):
        self.producer_dbms = ProducerDBMS()

    def get_producer(self,producer_id):
        return self.producer_dbms.select_producer(producer_id=producer_id)

    def register_new_producer_to_topic(self,topic_name):
        cur_producer_id=self.producer_dbms.create_producer(topic_name)
        return cur_producer_id

class Producer:
    def __init__(self, producer_id, producer_topic):
        self.producer_id = producer_id
        self.producer_topic = producer_topic

        
