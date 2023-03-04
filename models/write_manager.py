import psycopg2
from config import *
# from broker import broker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import ConsumerDBMS, ProducerDBMS, TopicDBMS, MessageDBMS
from myqueue import MyBroker
import random
import yaml
import os
# TODO:
# Move list_topics register producer and register consumer 
# Write a MyWriteManager class that sends requests to flask server

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url. 
class writeManager:
    def __init__(self, config, init_brokers = 2):
        self.topics = []
        self.topics_offset = {}
        self.partition_broker = {}      #Partition -> Broker ID
        self.topic_numPartitions = {}  #Topic -> num_partition
        self.broker_port = {} ## List of id to broker_port
        self.brokerId = []
        self.producer_topic = {}

        self.num_producers = 0
        self.num_brokers = 0
        self.ispersistent = config['IS_PERSISTENT']
        self.curr_port = 1000

        ##HARD CODING BROKERS
        for i in range(init_brokers):
            self.brokerId.append(i)
            self.broker_port[i] = self.curr_port
            self.curr_port += 100
            self.num_brokers += 1

        if self.ispersistent:
            # Connect to the database
            self.conn = psycopg2.connect(database = config['DATABASE'], user = config['USER'], password = config['PASSWORD'], 
                                host = config['HOST'], port = config['PORT'])
            self.cur=self.conn.cursor()

            # Create the tables if they don't exist
            # self.consumer_table = ConsumerDBMS(self.conn, self.cur) In Read Manager
            # self.message_table = MessageDBMS(self.conn, self.cur) In Broker
            # self.producer_table = ProducerDBMS(self.conn, self.cur)
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
        self.brokerId.append(broker_id)
        # Create new broker server here: By Running Flask App
        # Create Instance of MyBroker Class with base url (LOCALHOST:PORT) to send requests
        # new_broker = MyBroker(LOCALHOST:PORT) -> self.broker_port.append(new_broker)
        port = self.curr_port
        self.curr_port += 100
        self.broker_port[broker_id] = port

        #generate yaml file
        config = {'IS_PERSISTENT': self.ispersistent,
                    'SERVER_PORT': port,
                    'USER': 'postgres',
                    'PASSWORD': 'mayank',
                    'DATABASE': 'mqsdb',
                    'HOST': '127.0.0.1',
                    'PORT': '5432'}
        with open(f'../configs/broker{broker_id}.yaml', 'w') as f:
            yaml.dump(config, f)
        
        #start a new server
        os.system('python3 ../broker_app.py -c ../configs/broker{broker_id}.yaml')
        return broker_id

    def add_topic(self, topic_name):
        ## Need to send request to read manager too.
        # Check if topic already exists
        # Create first partition for the topic by requesting a MyBroker instance (create_topic method)
        # Handle Metadata of Write Manager
        # if topic_name in self.topics:
        #     raise Exception("Topic already exists")


        self.topics.append(topic_name)
        self.topics_offset[topic_name] = 0
        #Selecting random broker
        curr_id = random.choice(self.brokerId)
        broker_port = self.broker_port[curr_id]
        url = "http://127.0.0.1:" + str(broker_port)
        #Send topic and partition request to broker server port with following partition id
        partition_id = topic_name + ".1"
        self.partition_broker[partition_id] = curr_id
        self.topic_numPartitions[topic_name] = 1
        return MyBroker.create_topic(url, partition_id)


    def add_partition(self,topic_name):
        ## Need to send request to read manager too.
        # Choose a Broker (Round Robin / Random)
        # Create the partition by calling create_topic of MyBroker instance
        curr_id = random.choice(self.broker_port)
        broker_port = self.broker_port(curr_id)
        url = "http://127.0.0.1:" + str(broker_port)

        self.topic_numPartitions[topic_name] += 1
        partition_id = topic_name + "." + self.topic_numPartitions[topic_name]
        self.partition_broker[partition_id] = curr_id
        #Send request to broker server for creating new partition

        MyBroker.create_topic(url, partition_id)
        return partition_id


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
        if producer_id not in self.producer_topic:
            raise Exception("Invalid ProducerId")
        if self.producer_topic[producer_id] != topic_name:
            raise Exception("ProducerId is not subscribed to the topic")

        # curr_partition = random.randint(1,self.topic_numPartitions[topic_name])
        curr_partition = self.topics_offset[topic_name]%self.topic_numPartitions[topic_name] + 1
        self.topics_offset[topic_name] = (self.topics_offset[topic_name] + 1)%self.topic_numPartitions[topic_name]
        partition_id = topic_name + "." + str(curr_partition)

        curr_id = self.partition_broker[partition_id]
        broker_port = self.broker_port[curr_id]
        url = "http://127.0.0.1:" + str(broker_port)
        #send request to broker for enque message
 
        return MyBroker.publish_message(url, partition_id, message)


    def health_check(self):
        # This function will check the last use time of the producers and log whether 
        # any producer has not produced a message for a long time (set arbitrary threshold for now)
        pass

