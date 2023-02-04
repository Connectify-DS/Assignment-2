from myqueue import *
HOST = "127.0.0.1"
PORT = 5000
base_url = f"http://{HOST}:{PORT}"
from time import sleep
import random

c2 = MyConsumer(topics=["T-1",  "T-3"], broker=base_url)

while True:
    x = c2.get_next("T-1")
    if x is not None:
        print(x)
    sleep(random.uniform(0, 0.5))
    y = c2.get_next("T-3")
    if y is not None:
        print(y)
    sleep(random.uniform(0, 0.5))
    if x is None and y is None:
        break