import psycopg2
from config import *
# from broker import broker
from in_memory_structures import ConsumerTable, ProducerTable, TopicTable, MessageTable
from database_structures import BrokerDBMS,TopicDBMS_WM,PartitionDMBS, ProducerDBMS
from myqueue import MyBroker
import random
import yaml
import os

# Note:
# Use MyBroker class from myqueue folder to create, publish, consume, list topics as that class
# requests the broker server. Initialise it with the broker url. 
class writeManager:
    def __init__(self, config):
        self.ispersistent = config['IS_PERSISTENT']
        self.init_brokers=config['INIT_BROKERS']

        if self.ispersistent:
            self.topic_dbms=TopicDBMS_WM(config)
            self.broker_dbms=BrokerDBMS(config)
            self.partition_dbms=PartitionDMBS(config)
            self.producer_dbms=ProducerDBMS(config)

            # self.num_producers = self.producer_dbms.get_num_producers()
            # self.num_brokers = self.broker_dbms.get_num_brokers()
            self.drop_tables()
            self.create_tables()
        else:
            self.topics = []
            self.topics_offset = {}
            self.partition_broker = {}      #Partition -> Broker ID
            self.topic_numPartitions = {}  #Topic -> num_partition
            self.broker_port = {} ## List of id to broker_port
            self.brokerId = []
            self.producer_topic = {}

            self.num_producers = 0
            self.num_brokers = 0
        
        self.curr_port = 1000

        ##HARD CODING BROKERS
        for i in range(self.init_brokers):
            if self.ispersistent:
                self.broker_dbms.add_new_broker(str(self.curr_port))
            else:
                self.brokerId.append(i)
                self.broker_port[i] = self.curr_port
                self.num_brokers += 1
            
            self.curr_port += 100

    def create_tables(self):
        self.broker_dbms.create_table()
        self.topic_dbms.create_table()
        self.partition_dbms.create_table()
        self.producer_dbms.create_table()

    def drop_tables(self):
        self.broker_dbms.cur.execute("""
            DROP TABLE IF EXISTS BROKERS, PRODUCERS, PARTITIONS, TOPICS_WM;
        """)

        self.broker_dbms.conn.commit()


        
    def add_broker(self,port):
        ## Note: You will have to request read manager to add this broker too.
        if self.ispersistent:
            port = self.curr_port
            self.curr_port += 100
            broker_id=self.broker_dbms.add_new_broker(str(port))
        else:
            broker_id = self.num_brokers
            self.brokerId.append(broker_id)
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
        
        if topic_name in self.list_topics():
            raise Exception("Topic already exists")

        if self.ispersistent:
            self.topic_dbms.add_topic(topic_name)
            
            broker_id,broker_port = self.broker_dbms.get_random_broker()

            partition_name = topic_name + ".1"
            self.partition_dbms.add_partition(partition_name,broker_id)
            print(f"broker Port: {broker_port}")
        else:
            self.topics.append(topic_name)
            self.topics_offset[topic_name] = 0
            self.topic_numPartitions[topic_name] = 1

            broker_id=random.choice(self.brokerId)
            broker_port = self.broker_port[broker_id]
            
            partition_name = topic_name + ".1"
            self.partition_broker[partition_name] = broker_id

        url = "http://127.0.0.1:" + str(broker_port)
        
        return MyBroker.create_topic(url, partition_name)


    def add_partition(self,topic_name):
        ## Need to send request to read manager too.
        # Choose a Broker (Round Robin / Random)
        # Create the partition by calling create_topic of MyBroker instance

        if self.ispersistent:
            broker_id,broker_port = self.broker_dbms.get_random_broker()
            num_partitions=self.topic_dbms.add_partition(topic_name)

            partition_name = topic_name + "." + str(num_partitions)

            self.partition_dbms.add_partition(partition_name,broker_id)
        else:
            curr_id = random.choice(self.brokerId)
            broker_port = self.broker_port[curr_id]

            self.topic_numPartitions[topic_name] += 1
            partition_name = topic_name + "." + str(self.topic_numPartitions[topic_name])

            self.partition_broker[partition_name] = curr_id

        url = "http://127.0.0.1:" + str(broker_port)
        MyBroker.create_topic(url, partition_name)
        return partition_name,broker_port


    def list_topics(self):
        """
        Returns a list of all the topics in the system.
        """
        # Return from Metadata of Write Manager
        if self.ispersistent:
            return self.topic_dbms.list_topics()
        else:
            return self.topics


    def register_producer(self,topic_name):
        # Check if Topic Exists. If not Create Topic
        # Add to Producer Table (register_new_producer_to_topic Function) -> Returns ID
        # Handle Metadata

        ## Health Check: 
        #       Add new producer to the healthcheck list with the ID
        #       Save the current time (time.datetime) as the time of creation
        #       You will also have to maintain the last use time (currently empty)
        if topic_name not in self.list_topics():
            self.add_topic(topic_name)

        if self.ispersistent:
            producer_id=self.producer_dbms.add_producer(topic_name)
            return producer_id
        else:
            self.num_producers += 1
            self.producer_topic[self.num_producers] = topic_name
            return self.num_producers


    def produce_message(self,producer_id,topic_name,message):
        # Check if Producer can publish to the topic.
        # Assign / Create Partition (Round Robin)
        # Call the appropriate broker publish_message function

        ## Health Check: 
        #       Update the last use time of the producer based on the producer id

        if self.ispersistent:
            if not self.producer_dbms.check_producer_id(producer_id):
                raise Exception("Invalid ProducerId")
            if not self.producer_dbms.check_producer_topic_link(producer_id,topic_name):
                raise Exception("ProducerId is not subscribed to the topic")
            
            curr_partition=self.topic_dbms.get_current_partition(topic_name) ## round robin
            partition_name = topic_name + "." + str(curr_partition)

            broker_port=self.partition_dbms.get_broker_port_from_partition(partition_name)
        else:
            if producer_id not in self.producer_topic:
                raise Exception("Invalid ProducerId")
            if self.producer_topic[producer_id] != topic_name:
                raise Exception("ProducerId is not subscribed to the topic")
            
            curr_partition = random.randint(1,self.topic_numPartitions[topic_name])
            curr_partition = self.topics_offset[topic_name]%self.topic_numPartitions[topic_name] + 1
            self.topics_offset[topic_name] = (self.topics_offset[topic_name] + 1)%self.topic_numPartitions[topic_name]

            partition_name = topic_name + "." + str(curr_partition)
            curr_id = self.partition_broker[partition_name]
            broker_port = self.broker_port[curr_id]

        url = "http://127.0.0.1:" + str(broker_port)
 
        return MyBroker.publish_message(url, partition_name, message)


    def health_check(self):
        # This function will check the last use time of the producers and log whether 
        # any producer has not produced a message for a long time (set arbitrary threshold for now)
        pass

