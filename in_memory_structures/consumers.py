from models import Consumer

class ConsumerTable:
    """
    This class is used to store the consumer information
    """
    def __init__(self):
        self.consumer_entry = {}
        self.consumer_id_auto_inc = 0

    def get_consumer(self,consumer_id):
        """
        This function is used to get the consumer information
        """
        return self.consumer_entry[consumer_id]

    def create_consumer(self,consumer_id,topic_name):
        """
        This function is used to create a consumer
        """
        self.consumer_entry[consumer_id] = Consumer(consumer_id,topic_name)

    def register_to_topic(self,topic_name):
        """
        Register a consumer to a topic
        """
        cur_consumer_id = self.consumer_id_auto_inc+1
        self.consumer_id_auto_inc += 1
        self.create_consumer(cur_consumer_id,topic_name)
        temp = []
        temp.append(cur_consumer_id)
        return temp
    
    def increase_offset(self, consumer_id):
        """
        Change the offset of the consumer
        """
        self.consumer_entry[consumer_id].cur_topic_queue_offset += 1
        return self.consumer_entry[consumer_id].cur_topic_queue_offset-1
