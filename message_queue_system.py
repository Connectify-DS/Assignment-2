from in_memory_structures.consumers import ConsumerTable
from in_memory_structures.message import MessageTable
from in_memory_structures.producers import ProducerTable
from in_memory_structures.topics import TopicTable


class MessageQueueSystem:
    def __init__(self):
        self.consumer_table = ConsumerTable()
        self.message_table = MessageTable()
        self.producer_table = ProducerTable()
        self.topic_table = TopicTable()
        self.active_readers = 0
        self.active_writers = 0

    def create_topic(self, topic_name: str):
        return self.topic_table.create_topic_queue(topic_name)

    def list_topics(self):
        return self.topic_table.get_topic_list()

    def register_producer(self, topic_name: str):
        return self.producer_table.register_new_producer_to_topic(topic_name)

    def enqueue(self, topic_name: str, producer_id: int, message: str):
        producer = self.producer_table.get_producer(producer_id)
        # check if correctly subscribed.
        if producer.producer_topic == topic_name:
            topic_queue = self.topic_table.get_topic_queue(topic_name)
            if topic_queue.can_write():
                topic_queue.get_writer_lock()
                message_id = self.message_table.add_message(message)
                topic_queue.enqueue(message_id)
                topic_queue.release_reader_lock()

    def register_consumer(self, topic_name: str):
        return self.consumer_table.register_to_topic(topic_name)

    def dequeue(self, topic: str, consumer_id: int):
        consumer = self.consumer_table.get_consumer(consumer_id)
        topic_name = consumer.topic_name
        topic_queue = self.topic_table.get_topic_queue(topic_name)
        if topic == topic_name:
            topic_queue.get_reader_lock()
            message_id = consumer.get_next_message(topic_queue)
            topic_queue.release_reader_lock()
            if message_id:
                message_data = self.message_table.get_message(message_id)
                return message_data

    def size(self, topic: str, consumer_id: int):
        consumer = self.consumer_table.get_consumer(consumer_id)
        return consumer.get_count_messages_to_fetch(self.topic_table)
