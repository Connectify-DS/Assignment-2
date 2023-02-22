import threading
from myqueue import *
import random
import time
from time import sleep
import sys

HOST = "127.0.0.1"
PORT = 5000
base_url = f"http://{HOST}:{PORT}"

p1 = MyProducer(topics=["T-1", "T-2", "T-3"], broker=base_url)
p2 = MyProducer(topics=["T-1", "T-2", "T-3"], broker=base_url)
p3 = MyProducer(topics=["T-1", "T-2", "T-3"], broker=base_url)
p4 = MyProducer(topics=["T-1", "T-2", "T-3"], broker=base_url)
p5 = MyProducer(topics=["T-1", "T-2", "T-3"], broker=base_url)

c1 = MyConsumer(topics=["T-1", "T-2", "T-3"], broker=base_url)
c2 = MyConsumer(topics=["T-1",  "T-3"], broker=base_url)
c3 = MyConsumer(topics=["T-1",  "T-3"], broker=base_url)

class SysRedirect(object):
    def __init__(self):
        self.terminal = sys.stdout                  # To continue writing to terminal
        self.log={}                                 # A dictionary of file pointers for file logging

    def register(self,filename):                    # To start redirecting to filename
        ident = threading.currentThread().ident     # Get thread ident (thanks @michscoots)
        if ident in self.log:                       # If already in dictionary :
            self.log[ident].close()                 # Closing current file pointer
        self.log[ident] = open(filename, "w")       # Creating a new file pointed associated with thread id

    def write(self, message):
        self.terminal.write(message)                # Write in terminal (comment this line to remove terminal logging)
        ident = threading.currentThread().ident     # Get Thread id
        if ident in self.log:                       # Check if file pointer exists
            self.log[ident].write(message)          # write in file
        else:                                       # if no file pointer 
            for ident in self.log:                  # write in all thread (this can be replaced by a Write in terminal)
                 self.log[ident].write(message)  
    def flush(self):
        #this flush method is needed for python 3 compatibility.
        #this handles the flush command by doing nothing.
        #you might want to specify some extra behavior here.
        pass   

sys.stdout=SysRedirect() 

def run_producer(file_path,log_path,producer):
    sys.stdout.register(log_path)
    with open(file_path, "r") as f:
        i=0
        for line in f:
            i+=1
            if i>10:
                break
            line=line.strip()
            topic = line.split('\t')[-1]
            message= '\t'.join(line.split('\t')[:-1])
            print(topic)
            print(message)
            response = producer.send(topic=topic, message=message)
            time.sleep(random.uniform(0,1))

def run_consumer(consumer,log_path,isT2=False):
    sys.stdout.register(log_path)
    i=0
    while i<=10:
        i+=1
        x = consumer.get_next("T-1")
        if x is not None:
            print(x)
        sleep(random.uniform(0, 0.5))
        y=None
        if isT2:
            y = consumer.get_next("T-2")
            if y is not None:
                print(y)
            sleep(random.uniform(0, 0.5))
        z = consumer.get_next("T-3")
        if z is not None:
            print(z)
        sleep(random.uniform(0, 0.5))
        if x is None and y is None and z is None:
            break

p1_t=threading.Thread(target=run_producer,args=("Tests/test_asgn1/producer_1.txt","Tests/logs/log_p1.log",p1,))
p2_t=threading.Thread(target=run_producer,args=("Tests/test_asgn1/producer_2.txt","Tests/logs/log_p2.log",p2,))
p3_t=threading.Thread(target=run_producer,args=("Tests/test_asgn1/producer_3.txt","Tests/logs/log_p3.log",p3,))
p4_t=threading.Thread(target=run_producer,args=("Tests/test_asgn1/producer_4.txt","Tests/logs/log_p4.log",p4,))
p5_t=threading.Thread(target=run_producer,args=("Tests/test_asgn1/producer_5.txt","Tests/logs/log_p5.log",p5,))

c1_t=threading.Thread(target=run_consumer,args=(c1,"Tests/logs/log_c1.log",True,))
c2_t=threading.Thread(target=run_consumer,args=(c2,"Tests/logs/log_c2.log",))
c3_t=threading.Thread(target=run_consumer,args=(c3,"Tests/logs/log_c3.log",))

p1_t.start()
p2_t.start()
p3_t.start()
p4_t.start()
p5_t.start()

c1_t.start()
c2_t.start()
c3_t.start()

p1_t.join()
p2_t.join()
p3_t.join()
p4_t.join()
p5_t.join()

c1_t.join()
c2_t.join()
c3_t.join()