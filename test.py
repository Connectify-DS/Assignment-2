import os
import yaml
import execfile

config = {'IS_PERSISTENT': True,
                    'SERVER_PORT': 1111,
                    'USER': 'postgres',
                    'PASSWORD': 'mayank',
                    'DATABASE': 'mqsdb',
                    'HOST': '127.0.0.1',
                    'PORT': '5432'}
with open(f'configs/broker10.yaml', 'w') as f:
    yaml.dump(config, f)

#start a new server
execfile('python broker_app.py -c configs/broker10.yaml')

print("DONEEEEE")