import yaml
from database_structures.health_dbms import HealthDBMS
from database_structures import BrokerDBMS, TopicDBMS_WM, ConsumerDBMS, PartitionDMBS
import sys

sys.path.append("..")


config = None
with open('../configs/wm.yaml') as f:
    config = yaml.safe_load(f)

topic_dbms = TopicDBMS_WM(config)
broker_dbms = BrokerDBMS(config)
partition_dbms = PartitionDMBS(config)
consumer_dbms = ConsumerDBMS(config)
health_dbms = HealthDBMS(config)


def create_tables():
    broker_dbms.create_table()
    topic_dbms.create_table()
    partition_dbms.create_table()
    consumer_dbms.create_table()
    health_dbms.create_table()


def drop_tables():
    broker_dbms.cur.execute("""
        DROP TABLE IF EXISTS BROKERS, CONSUMERS, PARTITIONS, TOPICS_WM, HEALTHLOG;
    """)

    broker_dbms.conn.commit()


def test_init():
    print("Adding Topic Test1")
    topic_id = topic_dbms.add_topic("test1")
    print(f"Added Successfully, ID={topic_id}")

    print("Adding Partition to topic")
    partition_id = topic_dbms.add_partition("test1")
    print(f"Added new partition, current partition ID: {partition_id}")

    print("Adding Topic Test2")
    topic_id = topic_dbms.add_topic("test2")
    print(f"Added Successfully, ID={topic_id}")

    print("Adding Partition to topic")
    partition_id = topic_dbms.add_partition("test2")
    print(f"Added new partition, current partition ID: {partition_id}")


def test_consumer():
    print("Adding Consumer")
    num_partitions = topic_dbms.get_num_partitions("test1")
    id1 = consumer_dbms.add_consumer("test1", num_partitions)
    print(f"Added Consumer ID: {id1}")

    print("Adding Consumer")
    num_partitions = topic_dbms.get_num_partitions("test2")
    id2 = consumer_dbms.add_consumer("test2", num_partitions)
    print(f"Added Consumer ID: {id2}")

    try:
        print("Adding Consumer whose topic does not exist")
        num_partitions = topic_dbms.get_num_partitions("test3")
        id = consumer_dbms.add_consumer("test3", num_partitions)
        print(f"Added Consumer ID: {id}")
    except Exception as e:
        print(e)

    print("Number of Consumers: ", consumer_dbms.get_num_consumers())

    print("Checking if Consumer ID 1 exists")
    print(consumer_dbms.check_consumer_id(1))

    print("Checking if Consumer ID 5 exists")
    print(consumer_dbms.check_consumer_id(5))

    print("Checking if link between Consumer ID 1 and Topic Name test1 exists")
    print(consumer_dbms.check_consumer_topic_link(1, "test1"))

    print("Checking if link between Consumer ID 1 and Topic Name test3 exists")
    print(consumer_dbms.check_consumer_topic_link(1, "test3"))

    print("Checking if link between Consumer ID 5 and Topic Name test1 exists")
    print(consumer_dbms.check_consumer_topic_link(5, "test1"))

    print("Consuming Message")
    curr_partition = topic_dbms.get_current_partition("test1")
    partition_name = "test1" + "." + str(curr_partition)
    offset = consumer_dbms.get_offset(id1, partition_name)
    print(f"Offset: {offset}, Partition: {curr_partition}")

    curr_partition = topic_dbms.get_current_partition("test1")
    partition_name = "test1" + "." + str(curr_partition)
    offset = consumer_dbms.get_offset(id1, partition_name)
    print(f"Offset: {offset}, Partition: {curr_partition}")

    curr_partition = topic_dbms.get_current_partition("test1")
    partition_name = "test1" + "." + str(curr_partition)
    offset = consumer_dbms.get_offset(id1, partition_name)
    print(f"Offset: {offset}, Partition: {curr_partition}")

    curr_partition = topic_dbms.get_current_partition("test1")
    partition_name = "test1" + "." + str(curr_partition)
    offset = consumer_dbms.get_offset(id1, partition_name)
    print(f"Offset: {offset}, Partition: {curr_partition}")

    print("Checking consumption when adding partition")
    num_partitions = topic_dbms.add_partition("test1")
    partition_name = "test1" + "." + str(num_partitions)
    consumer_dbms.add_partition("test1", partition_name)

    print("Consuming Message")
    curr_partition = topic_dbms.get_current_partition("test1")
    partition_name = "test1" + "." + str(curr_partition)
    offset = consumer_dbms.get_offset(id1, partition_name)
    print(f"Offset: {offset}, Partition: {curr_partition}")

    curr_partition = topic_dbms.get_current_partition("test1")
    partition_name = "test1" + "." + str(curr_partition)
    offset = consumer_dbms.get_offset(id1, partition_name)
    print(f"Offset: {offset}, Partition: {curr_partition}")

    curr_partition = topic_dbms.get_current_partition("test1")
    partition_name = "test1" + "." + str(curr_partition)
    offset = consumer_dbms.get_offset(id1, partition_name)
    print(f"Offset: {offset}, Partition: {curr_partition}")


if __name__ == "__main__":
    drop_tables()
    create_tables()

    print("Testing Init")
    test_init()
    print("\nTesting Consumer")
    test_consumer()
