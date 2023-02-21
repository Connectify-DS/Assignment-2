import psycopg2
import threading
from config import *
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS

class MessageQueueSystem:
    """
    This class is the main class of the system. It is responsible for the creation of topics,
    registering producers and consumers, and the enqueueing and dequeueing of messages.
    """
    def __init__(self,persistent):
        self.persistent = persistent
        if self.persistent:
            # Connect to the database
            self.conn = psycopg2.connect(database = DATABASE, user = USER, password = PASSWORD, 
                                host = HOST, port = PORT)
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            self.consumer_table = ConsumerDBMS(self.conn, self.cur)
            self.message_table = MessageDBMS(self.conn, self.cur)
            self.producer_table = ProducerDBMS(self.conn, self.cur)
            self.topic_table = TopicDBMS(self.conn, self.cur)
            self.reset_dbms()

            self.register_lock=threading.Lock()
            self.enqueue_lock=threading.Lock()
            self.deque_lock=threading.Lock()

        else:
            # Create the tables if they don't exist in memory
            self.consumer_table = ConsumerTable()
            self.message_table = MessageTable()
            self.producer_table = ProducerTable()
            self.topic_table = TopicTable()

    def reset_dbms(self):
    
        self.cur.execute("""
            DROP TABLE IF EXISTS CONSUMERS, PRODUCERS, MESSAGES, TOPICS;
        """)

        self.conn.commit()

        self.consumer_table.create_table()
        self.producer_table.create_table()
        self.message_table.create_table()
        self.topic_table.create_table()

        self.conn.commit()

    def create_topic(self, topic_name: str):
        """
        Creates a new topic with the given name.
        """
        try:
            topics = self.list_topics()
            if topic_name in topics:
                raise Exception("Topic already exists")
            return self.topic_table.create_topic_queue(topic_name)
        except Exception as e:
            raise e

    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        return self.topic_table.get_topic_list()

    def register_producer(self, topic_name: str):
        """
        Registers a new producer to the given topic.
        """
        self.register_lock.acquire()
        topics = self.list_topics()
        if topic_name not in topics:
            self.create_topic(topic_name)
        id=self.producer_table.register_new_producer_to_topic(topic_name)
        self.register_lock.release()
        return id
        
    def enqueue(self, topic_name: str, producer_id: int, message: str):
        """
        Enqueues a new message to the given topic.
        """
        self.enqueue_lock.acquire()
        try:
            topics = self.list_topics()
            # print(topics, topic_name)
            if topics is None:
                raise Exception("No topic registered")
            if topic_name not in topics:
                raise Exception("Topic does not exist")
        except Exception as e:
            self.enqueue_lock.release()
            raise e

        try:
            producer = self.producer_table.get_producer(producer_id)
            if producer is None:
                raise Exception(f"Invalid producer id {producer_id}")
            if producer.producer_topic != topic_name:
                raise Exception("This Topic is not registered under this Id")
            topic_queue = self.topic_table.get_topic_queue(topic_name)
            if topic_queue is None:
                raise Exception(f"Topic name {topic_name} not found")
            message_id = self.message_table.add_message(message)
            topic_queue.enqueue(message_id)
            self.enqueue_lock.release()
        except Exception as e:
            self.enqueue_lock.release()
            raise e
        
    def register_consumer(self, topic_name: str):
        """
        Registers a new consumer to the given topic.
        """
        self.register_lock.acquire()
        try:
            topics = self.list_topics()
            if topic_name not in topics:
                raise Exception("Topic does not exist")
            id = self.consumer_table.register_to_topic(topic_name)
            self.register_lock.release()
            return id
        except Exception as e:
            self.register_lock.release()
            # print(e)
            raise e

    def dequeue(self, topic_name: str, consumer_id: int):
        """
        Removes the next message from the given topic.
        """
        self.deque_lock.acquire()
        try:
            topics = self.list_topics()
            if topics is None:
                raise Exception("No topic registered")
            if topic_name not in topics:
                raise Exception("Topic does not exist")
            size_rem = self.size(topic_name, consumer_id)
            if size_rem == 0:
                raise Exception("No messages left to retrieve")
        except Exception as e:
            self.deque_lock.release()
            raise e
        
        try:
            consumer = self.consumer_table.get_consumer(consumer_id)
            if consumer is None:
                raise Exception(f"Invalid consumer id {consumer_id}")
            topicName = consumer.topic_name
            ofset = self.consumer_table.increase_offset(consumer_id)
            topic_queue = self.topic_table.get_topic_queue(topic_name)
            if topic_queue is None:
                raise Exception(f"Topic name {topic_name} not found")
            if topicName != topic_name:
                raise Exception("This Topic is not subscribed under this Id")
            else:
                if self.persistent:
                    message_id = consumer.get_next_message(topic_queue, ofset+1)
                else:
                    message_id = consumer.get_next_message(topic_queue, ofset)
                if message_id:
                    message_data = self.message_table.get_message(message_id)
                    self.deque_lock.release()
                    return message_data
            self.deque_lock.release()
        except Exception as e:
            self.deque_lock.release()
            raise e

    def size(self, topic: str, consumer_id: int):
        """
        Returns the number of messages left to be dequeued by the given consumer.
        """
        try:
            consumer = self.consumer_table.get_consumer(consumer_id)
            if consumer.topic_name != topic:
                raise Exception("Invalid topic")
            return consumer.get_count_messages_to_fetch(self.topic_table)
        except Exception as e:
            raise e