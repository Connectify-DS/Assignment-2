import psycopg2

from in_memory_structures import TopicQueue

class TopicDBMS:
    def __init__(self):
        self.conn = psycopg2.connect(database = "mqsdb", user = "postgres", password = "mayank", 
                                host = "127.0.0.1", port = "5432")
        self.cur=self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE TOPICS
            ID INT PRIMARY KEY NOT NULL,
            NAME TEXT NOT NULL,
            MESSAGES INT[],
        """)

        self.conn.commit()

    def create_topic(self,name):
        self.cur.execute("""
            INSERT INTO TOPICS (NAME) 
            VALUES (%s)
        """,name)

        id=self.cur.fetchone()[0]
        
        self.conn.commit()

        return id

    def get_topic_queue(self,topic_id):
        self.cur.execute("""
            SELECT * FROM TopicsS
            WHERE ID = %s
        """,topic_id)

        row=self.cur.fetchone()

        return TopicQueue(
                Topics=row[1]
            )

if __name__=="__main__":
    dbms=TopicsDBMS()
    dbms.create_table()