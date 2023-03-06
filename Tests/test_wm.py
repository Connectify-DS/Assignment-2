import sys
sys.path.append("..")
import yaml
from models import writeManager,readManager

config=None
with open('../configs/rm1.yaml') as f:
    config = yaml.safe_load(f)
rm = readManager(config=config)

config=None
with open('../configs/wm.yaml') as f:
    config = yaml.safe_load(f)
wm=writeManager(config=config)

if __name__=="__main__":
    print("List Topics: ")
    print(wm.list_topics())

    try:
        print("Adding Topic test1")
        wm.add_topic("test1")

        print("Adding Topic test2")
        wm.add_topic("test2")
    except Exception as e:
        print(e)

    print("List Topics: ")
    print(wm.list_topics())

    print("Adding Partition to test1")
    pname,port=wm.add_partition("test1")
    print(f"Partition name {pname}, Port: {port}")

    print("Registering producer to existing topic")
    pid1=wm.register_producer("test1")

    print("Registering producer to new topic")
    pid2=wm.register_producer("test3")

    print("List Topics: ")
    print(wm.list_topics())

    print("Producing Messages")
    wm.produce_message(pid1,"test1","Test Message 1")
    wm.produce_message(pid1,"test1","Test Message 2")
    wm.produce_message(pid1,"test1","Test Message 1")
    wm.produce_message(pid1,"test1","Test Message 2")
    wm.produce_message(pid1,"test1","Test Message 1")
    wm.produce_message(pid1,"test1","Test Message 2")

    wm.produce_message(pid2,"test3","Test Message 1")
    wm.produce_message(pid2,"test3","Test Message 2")
    wm.produce_message(pid2,"test3","Test Message 1")
    wm.produce_message(pid2,"test3","Test Message 2")
    wm.produce_message(pid2,"test3","Test Message 1")
    wm.produce_message(pid2,"test3","Test Message 2")

    print("Producing Message to unregistered topic")
    try:
        wm.produce_message(pid1,"test2","Error")
    except Exception as e:
        print(e)

    print("Producing Message via unregistered producer")
    try:
        wm.produce_message(4,"test2","Error")
    except Exception as e:
        print(e)

    cid1=rm.register_consumer("test1")
    try:
        message=rm.consume_message(cid1,"test1")
        print(message)
    except Exception as e:
        pass
    try:
        message=rm.consume_message(cid1,"test1")
        print(message)
    except Exception as e:
        pass
    try:
        message=rm.consume_message(cid1,"test1")
        print(message)
    except Exception as e:
        pass
    try:
        message=rm.consume_message(cid1,"test1")
        print(message)
    except Exception as e:
        pass
    try:
        message=rm.consume_message(cid1,"test1")
        print(message)
    except Exception as e:
        pass
    try:
        message=rm.consume_message(cid1,"test1")
        print(message)
    except Exception as e:
        pass
    try:
        message=rm.consume_message(cid1,"test1")
        print(message)
    except Exception as e:
        pass

