import sys
sys.path.append("..")
from database_structures import BrokerDBMS, TopicDBMS_WM, ProducerDBMS, PartitionDMBS

import yaml

config=None
with open('../configs/wm.yaml') as f:
    config = yaml.safe_load(f)

topic_dbms=TopicDBMS_WM(config)
broker_dbms=BrokerDBMS(config)
partition_dbms=PartitionDMBS(config)
producer_dbms=ProducerDBMS(config)

def create_tables():
    broker_dbms.create_table()
    topic_dbms.create_table()
    partition_dbms.create_table()
    producer_dbms.create_table()

def drop_tables():
    broker_dbms.cur.execute("""
        DROP TABLE IF EXISTS BROKERS, PRODUCERS, PARTITIONS, TOPICS_WM;
    """)

    broker_dbms.conn.commit()

def test_topics():
    print("Adding Topic Test1")
    topic_id=topic_dbms.add_topic("test1")
    print(f"Added Successfully, ID={topic_id}")

    print("Adding Partition to topic")
    num_partitions=topic_dbms.add_partition("test1")
    print(f"Added new partition, current number of partitions: {num_partitions}")

    print("Adding Topic Test2")
    topic_id=topic_dbms.add_topic("test2")
    print(f"Added Successfully, ID={topic_id}")

    print("Listing Topics")
    print(topic_dbms.list_topics())

    print("Checking Round Robin")
    print("First: ",topic_dbms.get_current_partition("test1"))
    print("Second: ",topic_dbms.get_current_partition("test1"))
    print("Third: ",topic_dbms.get_current_partition("test1"))

def test_brokers():
    print("Adding new broker")
    id=broker_dbms.add_new_broker("1234")
    print(f"Broker Added ID: {id}")

    print("Adding another broker")
    id=broker_dbms.add_new_broker("1252")
    print(f"Broker Added ID: {id}")

    print("Adding another broker")
    id=broker_dbms.add_new_broker("1122")
    print(f"Broker Added ID: {id}")
    
    try: 
        print("Adding another broker")
        id=broker_dbms.add_new_broker("1122")
        print(f"Broker Added ID: {id}")
    except Exception as e:
        print(e)

    print("Number of brokers: ",broker_dbms.get_num_brokers()) 

    print("Getting Random Broker")
    id,port=broker_dbms.get_random_broker()
    print(f"ID : {id}, PORT : {port}")

    print("Getting Random Broker")
    id,port=broker_dbms.get_random_broker()
    print(f"ID : {id}, PORT : {port}")

def test_partitions():
    # print("Adding new broker")
    # id=broker_dbms.add_new_broker("1234")
    # print(f"Broker Added ID: {id}")

    # print("Adding another broker")
    # id=broker_dbms.add_new_broker("1252")
    # print(f"Broker Added ID: {id}")

    print("Adding Partition")
    id=partition_dbms.add_partition("test1.1",1)
    print(f"Partition Added: ID {id}")

    print("Adding Partition")
    id=partition_dbms.add_partition("test1.2",2)
    print(f"Partition Added: ID {id}")

    print("Getting Broker PORT")
    print(partition_dbms.get_broker_port_from_partition("test1.1"))
    print(partition_dbms.get_broker_port_from_partition("test1.2"))

def test_producer():
    print("Adding Producer")
    id=producer_dbms.add_producer("test1")
    print(f"Added Producer ID: {id}")

    print("Adding Producer")
    id=producer_dbms.add_producer("test2")
    print(f"Added Producer ID: {id}")

    print("Number of Producers: ",producer_dbms.get_num_producers())



if __name__=="__main__":
    drop_tables()
    create_tables()

    print("Testing Topics")
    test_topics()
    print("\nTesting Brokers")
    test_brokers()
    print("\nTesting Partitions")
    test_partitions()
    print("\nTesting Producer")
    test_producer()
