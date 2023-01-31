"""
Runners for the consumers and producers in the message queue system with In-Memory Structures.
"""

from message_queue_system import MessageQueueSystem

message_queue_system = MessageQueueSystem(persistent=False)
message_queue_system.create_topic("abcd")
print(message_queue_system.list_topics())
message_queue_system.create_topic("abc")
print(message_queue_system.list_topics())
print(message_queue_system.register_producer("abcd"))
print(message_queue_system.register_producer("abc"))
print(message_queue_system.enqueue("abcd", 1, "First Enqueue Message!!"))
print(message_queue_system.message_table.message_table_entries[1].message)
print(message_queue_system.message_table.last_id)
print(message_queue_system.enqueue("abcd", 1, "2nd Enqueue Message!!"))
print(message_queue_system.register_consumer("abcd"))
print(message_queue_system.dequeue("abcd", 1).message)
print(message_queue_system.dequeue("abcd", 1).message)
print(message_queue_system.register_consumer("abcd"))
print(message_queue_system.dequeue("abcd", 2).message)
print(message_queue_system.dequeue("abcd", 2).message)
