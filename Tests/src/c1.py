from time import sleep
import random
from myqueue import *
HOST = "127.0.0.1"
PORT = 5000
base_url = f"http://{HOST}:{PORT}"

c1 = MyConsumer(topics=["T-1", "T-2", "T-3"], broker=base_url)

i=0
while i<=10:
    i+=1
    x = c1.get_next("T-1")
    if x is not None:
        print(x)
    sleep(random.uniform(0, 0.5))
    y = c1.get_next("T-2")
    if y is not None:
        print(y)
    sleep(random.uniform(0, 0.5))
    z = c1.get_next("T-3")
    if z is not None:
        print(z)
    sleep(random.uniform(0, 0.5))
    if x is None and y is None and z is None:
        break
