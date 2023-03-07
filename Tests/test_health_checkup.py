import sys
import time
sys.path.append("..")
from database_structures.health_dbms import HealthDBMS
import yaml



config = None
with open('../configs/wm.yaml') as f:
    config = yaml.safe_load(f)

health_dbms = HealthDBMS(config)
health_dbms.create_table()
print("Created Table")
tm=time.time()
print(tm)
print(health_dbms.add_update_health_log("producer", 1,  tm ))
print(health_dbms.add_update_health_log("producer", 2,  tm-2000 ))
print(health_dbms.get_last_active_time_stamp("producer",1))
print(health_dbms.get_inactive_actors("producer",1000))
print(health_dbms.add_update_health_log("consumer", 1,  tm ))
print(health_dbms.add_update_health_log("consumer", 2,  tm-2000 ))
print(health_dbms.get_last_active_time_stamp("consumer",1))
print(health_dbms.get_inactive_actors("consumer",1000))