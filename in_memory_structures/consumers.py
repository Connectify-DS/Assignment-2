DEFUALT_OFFSET = 0

class ConsumerTable:
    # This will ideally convert to a Table. Currently a strcture to be maintained in memory.
    def __init__(self):
        self.consumer_entry = {}
        self.consumer_id_auto_inc = 0

    def get_consumer(self,consumer_id):
        return self.consumer_entry[consumer_id]

    def create_consumer(self,consumer_id,topic_name):
        self.consumer_entry[consumer_id] = Consumer(consumer_id,topic_name)

    def register_to_topic(self,topic_name):
        # this needs to be atomic
        cur_consumer_id = self.consumer_id_auto_inc+1
        self.consumer_id_auto_inc += 1
        # atomic code ends here
        self.create_consumer(cur_consumer_id,topic_name)
        return cur_consumer_id

class Consumer:
    def __init__(self, consumer_id,topic_name,cur_topic_queue_offset=DEFUALT_OFFSET):
        self.consumer_id = consumer_id
        self.topic_name = topic_name
        self.cur_topic_queue_offset = cur_topic_queue_offset

    def get_next_message(self,topic_queue):
        message_id = topic_queue.get_at_offset(self.cur_topic_queue_offset)
        if message_id:
            self.cur_topic_queue_offset += 1
        return message_id

    def get_count_messages_to_fetch(self,topic_table):
        topic_queue = topic_table.get_topic_queue(self.topic_name)
        return topic_queue.size() - self.cur_topic_queue_offset

