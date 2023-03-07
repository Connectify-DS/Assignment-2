"""
Runners for the consumers and producers in the message queue system with In-Memory Structures.
"""
import psycopg2
import threading
from config import *

# psycopg2.connect(database = DATABASE, user = USER, password = PASSWORD, 
#                                 host = HOST, port = PORT)
# psycopg2.connect(database = DATABASE, user = USER, password = PASSWORD, 
#                                 host = HOST, port = PORT)                                
from models.broker import Broker

broker = Broker(persistent=True)
print("MQS Created")

print("\nCreating Topic abcd")
broker.create_topic("abcd")
print("Topic Created")
print("Topics: ", broker.list_topics())

print("\nCreating Topic abc")
broker.create_topic("abc")
print("Topics: ",broker.list_topics())

print("\nRegistering Producer to abcd and abc")
print("Producer ID ",broker.register_producer("abcd"))
print("Producer ID ",broker.register_producer("abc"))

print("\nEnqueuing Messages")
broker.enqueue("abcd", 1, "First Enqueue Message!!")
broker.enqueue("abcd", 1, "2nd Enqueue Message!!")

print("\nRegistering Consumer to Topic abcd")
print("Consumer ID ", broker.register_consumer("abcd"))
print("Messages: ")
print(broker.dequeue("abcd", 1).message)
print(broker.dequeue("abcd", 1).message)

print("\nRegistering new Consumer to Topic abcd")
print("Consumer ID ",broker.register_consumer("abcd"))
print("Messages: ")
print(broker.dequeue("abcd", 2).message)
print(broker.dequeue("abcd", 2).message)
