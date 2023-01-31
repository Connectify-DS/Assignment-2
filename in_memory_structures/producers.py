from models import Producer

class ProducerTable:
    """
    In memory structure to store all the producers
    """
    def __init__(self):
        self.producer_entry = {}
        self.producer_id_auto_inc = 0

    def get_producer(self,producer_id):
        """
        Get the producer object from the producer id
        """
        return self.producer_entry[producer_id]

    def create_producer(self,producer_id,topic_name):
        """
        Create a producer object and store it in the producer table
        """
        self.producer_entry[producer_id] = Producer(producer_id,topic_name)

    def register_new_producer_to_topic(self,topic_name):
        """
        Register a new producer to a topic
        """
        cur_producer_id = self.producer_id_auto_inc+1
        self.producer_id_auto_inc += 1
        self.create_producer(cur_producer_id,topic_name)
        return cur_producer_id
