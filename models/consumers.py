DEFUALT_OFFSET = 0

class Consumer:
    def __init__(self, consumer_id,topic_name,cur_topic_queue_offset=DEFUALT_OFFSET):
        self.consumer_id = consumer_id
        self.topic_name = topic_name
        self.cur_topic_queue_offset = cur_topic_queue_offset

    def get_next_message(self,topic_queue, ofset):
        message_id = topic_queue.get_at_offset(ofset)
        return message_id

    def get_count_messages_to_fetch(self,topic_table):
        topic_queue = topic_table.get_topic_queue(self.topic_name)
        return topic_queue.size() - self.cur_topic_queue_offset
