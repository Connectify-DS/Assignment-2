import psycopg2
from config import *
from broker import broker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS
# TODO:
# Move list_topics register producer and register consumer 

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url. 
class writeManager:
    def __init__(self, init_brokers = 1, ispersistent=True) -> None:
        self.partition_broker = {}
        self.topic_numPartitions = {}
        self.broker_port = {}
        self.num_brokers = init_brokers
        self.ispersistent = ispersistent

        if self.ispersistent:
            # Connect to the database
            self.conn = psycopg2.connect(database = DATABASE, user = USER, password = PASSWORD, 
                                host = HOST, port = PORT)
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            self.consumer_table = ConsumerDBMS(self.conn, self.cur)
            self.message_table = MessageDBMS(self.conn, self.cur)
            self.producer_table = ProducerDBMS(self.conn, self.cur)
            self.topic_table = TopicDBMS(self.conn, self.cur)

        else:
            # Create the tables if they don't exist in memory
            self.consumer_table = ConsumerTable()
            self.message_table = MessageTable()
            self.producer_table = ProducerTable()
            self.topic_table = TopicTable()
        
    def add_broker(self):
        # port = insert random port
        self.num_brokers += 1
        #create new broker server here
        # new_broker = broker(persistent=self.ispersistent, brokerID=self.num_brokers)
        self.broker_port[self.num_brokers] = port

    def add_topic(self, topic_name):
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