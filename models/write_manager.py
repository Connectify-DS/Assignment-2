import psycopg2
from config import *
from broker import broker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS
import random
# TODO:
# Move list_topics register producer and register consumer 
# Write a MyWriteManager class that sends requests to flask server

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url. 
class writeManager:
    def __init__(self, config, init_brokers = 1):
        self.topics = []
        self.partition_broker = {}      #Partition -> Broker
        self.topic_numPartitions = {}  #Topic -> num_partition
        self.broker_port = {}  #Broker ID -> Broker Port
        self.brokers=[] ## List of id of MyBroker Class
        self.producer_topic = {}

        self.num_producers = 0
        self.num_brokers = init_brokers
        self.ispersistent = config['IS_PERSISTENT']

        if self.ispersistent:
            # Connect to the database
            self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            # self.consumer_table = ConsumerDBMS(self.conn, self.cur) In Read Manager
            # self.message_table = MessageDBMS(self.conn, self.cur) In Broker
            self.producer_table = ProducerDBMS(self.conn, self.cur)
            # self.topic_table = TopicDBMS(self.conn, self.cur) In Broker

        else:
            # Create the tables if they don't exist in memory
            # self.consumer_table = ConsumerTable() 
            # self.message_table = MessageTable()
            self.producer_table = ProducerTable()
            # self.topic_table = TopicTable()
        
    def add_broker(self,port):
        ## Note: You will have to request read manager to add this broker too.
        # port = insert random port
        self.num_brokers += 1
        broker_id = self.num_brokers
        self.brokers.append(broker_id)
        # Create new broker server here: By Running Flask App
        # Create Instance of MyBroker Class with base url (LOCALHOST:PORT) to send requests
        # new_broker = MyBroker(LOCALHOST:PORT) -> self.brokers.append(new_broker)
        self.broker_port[self.num_brokers] = port
        return broker_id

    def add_topic(self, topic_name):
        ## Need to send request to read manager too.
        # Check if topic already exists
        # Create first partition for the topic by requesting a MyBroker instance (create_topic method)
        # Handle Metadata of Write Manager
        try:
            if topic_name in self.topics:
                raise Exception("Topic already exists")
        except Exception as e:
            raise e

        self.topics.append(topic_name)
        #Selecting random broker
        curr_id = random.choice(self.brokers)
        curr_port = self.broker_port[curr_id]

        #Send topic and partition request to broker server port with following partition id
        partition_id = topic_name + ".1"
        self.partition_broker[partition_id] = curr_id
        self.topic_numPartitions[topic_name] = 1
        return partition_id, curr_id

    def add_partition(self,topic_name):
        ## Need to send request to read manager too.
        # Choose a Broker (Round Robin / Random)
        # Create the partition by calling create_topic of MyBroker instance
        curr_id = random.choice(self.brokers)
        curr_port = self.broker_port[curr_id]

        self.topic_numPartitions[topic_name] += 1
        partition_id = topic_name + "." + self.topic_numPartitions[topic_name]
        self.partition_broker[partition_id] = curr_id
        #Send request to broker server for creating new partition
        return partition_id, curr_id

    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        # Return from Metadata of Write Manager
        return self.topics

    def register_producer(self,topic_name):
        # Check if Topic Exists. If not Create Topic
        # Add to Producer Table (register_new_producer_to_topic Function) -> Returns ID
        # Handle Metadata

        ## Health Check: 
        #       Add new producer to the healthcheck list with the ID
        #       Save the current time (time.datetime) as the time of creation
        #       You will also have to maintain the last use time (currently empty)
        if topic_name not in self.topics:
            self.add_topic(topic_name)

        self.num_producers += 1
        self.producer_topic[self.num_producers] = topic_name 
        return self.num_producers

    def produce_message(self,producer_id,topic_name,message):
        # Check if Producer can publish to the topic.
        # Assign / Create Partition (Round Robin)
        # Call the appropriate broker publish_message function

        ## Health Check: 
        #       Update the last use time of the producer based on the producer id
        try:
            if self.producer_topic[producer_id] != topic_name:
                raise Exception("ProducerId is not subscribed to the topic")
        except Exception as e:
            raise e

        curr_partition = random.randbytes(1,self.topic_numPartitions[topic_name])
        partition_id = topic_name + "." + curr_partition

        curr_broker = self.partition_broker[partition_id]
        curr_port = self.broker_port[curr_broker]
        #send request to broker for enque message
        return partition_id

    def health_check(self):
        # This function will check the last use time of the producers and log whether 
        # any producer has not produced a message for a long time (set arbitrary threshold for now)
        pass

