from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS
import psycopg2

class MessageQueueSystem:
    def __init__(self,persistent):
        if persistent:
            self.conn = psycopg2.connect(database = "mqsdb", user = "postgres", password = "mayank", 
                                host = "127.0.0.1", port = "5432")
            self.cur=self.conn.cursor()

            self.consumer_table = ConsumerDBMS(self.conn, self.cur)
            self.message_table = MessageDBMS(self.conn, self.cur)
            self.producer_table = ProducerDBMS(self.conn, self.cur)
            self.topic_table = TopicDBMS(self.conn, self.cur)

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
            size_rem = self.size(topic_name, consumer_id)
            if size_rem == 0:
                raise Exception("No messages left to retrieve")
        except Exception as e:
            raise e
        
        try:
            consumer = self.consumer_table.get_consumer(consumer_id)
            topicName = consumer.topic_name
            ofset = self.consumer_table.increase_offset(consumer_id)
            topic_queue = self.topic_table.get_topic_queue(topic_name)
            if topicName != topic_name:
                raise Exception("This Topic is not subscribed under this Id")
            else:
                message_id = consumer.get_next_message(topic_queue, ofset)
                if message_id:
                    message_data = self.message_table.get_message(message_id)
                    return message_data
        except Exception as e:
            raise e

    def size(self, topic: str, consumer_id: int):
        try:
            consumer = self.consumer_table.get_consumer(consumer_id)
            if consumer.topic_name != topic:
                raise Exception("Invalid topic")
            return consumer.get_count_messages_to_fetch(self.topic_table)
        except Exception as e:
            raise e