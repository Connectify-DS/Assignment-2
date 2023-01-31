from myqueue import *

broker = "http://127.0.0.1:5000"
server = ServerFunctions(broker)

#Creating Topics
server.CreateTopic("Test_Topic_1")
server.CreateTopic("Test_Topic_2")
server.ListTopics()

consumer1 = MyConsumer(["Test_Topic_1"],broker)
consumer2 = MyConsumer(["Test_Topic_1", "Test_Topic_2"],broker)

producer1 = MyProducer(["Test_Topic_1"], broker)
producer2 = MyProducer(["Test_Topic_2", "Test_Topic_3"], broker)

