# DS-Connectify

Implementation of a distributed queue

Authors:\
Mayank Kumar (19CS30029)\
Ishan Goel (19CS30052)\
Shrinivas Khiste (19CS30043)\
Yashica Patodia (19CS10067)\
Shashwat Shukla (19CS10056)\
\
\
• Modify the configurations in `config.py` file for PostgreSQL\
• Create a database named `mqsdb` in postgres using the command `create database mqsdb;`

The files `unit_test1.py` and `unit_test2.py` tests the in-memory and database structures respectively\
These can be run directly using `python unit_test1.py` or `python unit_test2.py`

• Run `python app.py` to start the server\
• To use the persistent layer, ensure that `IS_PERSISTENT` in app.py is `True`\
• To run without persistent layer, ensure that `IS_PERSISTENT` in app.py is `False`\
• Run `python Test_Main.py` to test the main code

TODO:
Ishan:
- [ ] handle requests from wm to rm
- [ ] make rm app server