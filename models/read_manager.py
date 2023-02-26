import psycopg2
from config import *
from broker import broker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS
# TODO:
# Move list_topics register producer and register consumer 
# Write a MyReadManager class that sends requests to flask server

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url. 
class readManager:
    def __init__(self, config,init_brokers = 1):
        self.partition_broker = {}
        self.topic_numPartitions = {}
        self.broker_port = {}
        self.brokers=[] ## List of instances of MyBroker Class
        self.num_brokers = init_brokers
        self.ispersistent = config['IS_PERSISTENT']

        if self.ispersistent:
            # Connect to the database
            self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            self.consumer_table = ConsumerDBMS(self.conn, self.cur) 
            # self.message_table = MessageDBMS(self.conn, self.cur) In Broker
            # self.producer_table = ProducerDBMS(self.conn, self.cur) In Write Manager
            # self.topic_table = TopicDBMS(self.conn, self.cur) In Broker

        else:
            # Create the tables if they don't exist in memory
            self.consumer_table = ConsumerTable() 
            # self.message_table = MessageTable()
            # self.producer_table = ProducerTable()
            # self.topic_table = TopicTable()
        
    def add_broker(self,port):
        # port = insert random port
        self.num_brokers += 1
        # Create new broker server here: By Running Flask App
        # Create Instance of MyBroker Class with base url (LOCALHOST:PORT) to send requests
        # new_broker = MyBroker(LOCALHOST:PORT) -> self.brokers.append(new_broker)
        self.broker_port[self.num_brokers] = port

    def add_topic(self, topic_name):
        # Handle Metadata of Read Manager -> Do not request broker
        pass

    def add_partition(self,topic_name):
        # Handle Metadata of Read Manager -> Do not request broker
        # This function may not be useful. 
        pass

    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        # Return from Metadata of Read Manager
        pass

    def register_consumer(self,topic_name):
        # Check if Topic Exists. If not Return Error
        # Add to Consumer Table (register_to_topic Function) -> Returns ID
        # Handle Metadata

        ## Health Check: 
        #       Add new consumer to the healthcheck list with the ID
        #       Save the current time (time.datetime) as the time of creation
        #       You will also have to maintain the last use time (currently empty)
        pass

    def consume_message(self,consumer_id,topic_name):
        # Check if Topic is subscribed by consumer can publish to the topic.
        # Assign Partition (Round Robin)
        # Retrieve offset : consumer table increase_offset function
        # Call the appropriate broker consume_message function

        ## Health Check: 
        #       Update the last use time of the consumer based on the consumer id
        pass

    def health_check(self):
        # This function will check the last use time of the consumers and log whether 
        # any consumer has not produced a message for a long time (set arbitrary threshold for now)
        pass

