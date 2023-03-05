import sys
sys.path.append("..")
import yaml
from models import readManager

config=None
with open('../configs/rm.yaml') as f:
    config = yaml.safe_load(f)

rm = readManager(config=config)

if __name__=="__main__":
    print("List Topics: ")
    print(rm.list_topics())

    try:
        print("Adding Topic test1")
        rm.add_topic("test1",1)

        print("Adding Topic test2")
        rm.add_topic("test2",2)
    except Exception as e:
        print(e)

    print("List Topics: ")
    print(rm.list_topics())

    print("Adding Partition to test1")
    rm.add_partition("test1","test1.2",1)

    print("Registering consumer to existing topic")
    cid1=rm.register_consumer("test1")
    
    try:
        print("Registering consumer to new topic")
        cid2=rm.register_consumer("test3")
    except Exception as e:
        print(e)

    print("List Topics: ")
    print(rm.list_topics())
