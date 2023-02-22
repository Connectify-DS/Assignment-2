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
from message_queue_system import MessageQueueSystem

message_queue_system = MessageQueueSystem(persistent=True)
print("MQS Created")

print("\nCreating Topic abcd")
message_queue_system.create_topic("abcd")
print("Topic Created")
print("Topics: ", message_queue_system.list_topics())

print("\nCreating Topic abc")
message_queue_system.create_topic("abc")
print("Topics: ",message_queue_system.list_topics())

print("\nRegistering Producer to abcd and abc")
print("Producer ID ",message_queue_system.register_producer("abcd"))
print("Producer ID ",message_queue_system.register_producer("abc"))

print("\nEnqueuing Messages")
message_queue_system.enqueue("abcd", 1, "First Enqueue Message!!")
message_queue_system.enqueue("abcd", 1, "2nd Enqueue Message!!")

print("\nRegistering Consumer to Topic abcd")
print("Consumer ID ", message_queue_system.register_consumer("abcd"))
print("Messages: ")
print(message_queue_system.dequeue("abcd", 1).message)
print(message_queue_system.dequeue("abcd", 1).message)

print("\nRegistering new Consumer to Topic abcd")
print("Consumer ID ",message_queue_system.register_consumer("abcd"))
print("Messages: ")
print(message_queue_system.dequeue("abcd", 2).message)
print(message_queue_system.dequeue("abcd", 2).message)
