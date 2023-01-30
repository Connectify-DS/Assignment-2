from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS

class MessageQueueSystem:
    def __init__(self,persistent=True):
        if persistent:
            self.consumer_table = ConsumerDBMS()
            self.message_table = MessageDBMS()
            self.producer_table = ProducerDBMS()
            self.topic_table = TopicDBMS()
        else:
            self.consumer_table = ConsumerTable()
            self.message_table = MessageTable()
            self.producer_table = ProducerTable()
            self.topic_table = TopicTable()

    def create_topic(self, topic_name: str):
        try:
            topics = self.list_topics()
            if topic_name in topics:
                raise Exception("Topic already exists")
            return self.topic_table.create_topic_queue(topic_name)
        except Exception as e:
            raise e

    def list_topics(self):
        return self.topic_table.get_topic_list()

    def register_producer(self, topic_name: str):
        topics = self.list_topics()
        if topic_name not in topics:
            self.create_topic(topic_name)
        return self.producer_table.register_new_producer_to_topic(topic_name)
        
    def enqueue(self, topic_name: str, producer_id: int, message: str):
        try:
            topics = self.list_topics()
            if topic_name not in topics:
                raise Exception("Topic does not exist")
        except Exception as e:
            raise e

        try:
            producer = self.producer_table.get_producer(producer_id)
            if producer.producer_topic != topic_name:
                raise Exception("This Topic is not registered under this Id")
            topic_queue = self.topic_table.get_topic_queue(topic_name)
            message_id = self.message_table.add_message(message)
            topic_queue.enqueue(message_id)
        except Exception as e:
            raise e
        

    def register_consumer(self, topic_name: str):
        try:
            topics = self.list_topics()
            if topic_name not in topics:
                raise Exception("Topic does not exist")
            return self.consumer_table.register_to_topic(topic_name)
        except Exception as e:
            raise e

    def dequeue(self, topic_name: str, consumer_id: int):
        try:
            topics = self.list_topics()
            if topic_name not in topics:
                raise Exception("Topic does not exist")
        except Exception as e:
            raise e
        
        try:
            consumer = self.consumer_table.get_consumer(consumer_id)
            topic_name = consumer.topic_name
            topic_queue = self.topic_table.get_topic_queue(topic_name)
            if topic_name != topic_name:
                raise Exception("This Topic is not subscribed under this Id")
            else:
                message_id = consumer.get_next_message(topic_queue)
                if message_id:
                    message_data = self.message_table.get_message(message_id)
                    return message_data
        except Exception as e:
            raise e

    def size(self, topic: str, consumer_id: int):
        consumer = self.consumer_table.get_consumer(consumer_id)
        return consumer.get_count_messages_to_fetch(self.topic_table)
