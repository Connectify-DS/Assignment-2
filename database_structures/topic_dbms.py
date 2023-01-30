import psycopg2

class TopicDBMS:
    def __init__(self):
        self.conn = psycopg2.connect(database = "mqsdb", user = "postgres", password = "mayank", 
                                host = "127.0.0.1", port = "5432")
        self.cur=self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE TOPICS(
            ID SERIAL PRIMARY KEY NOT NULL,
            NAME TEXT NOT NULL UNIQUE,
            MESSAGES INT[]);
        """)

        self.conn.commit()

    def create_topic_queue(self,name):
        self.cur.execute("""
            INSERT INTO TOPICS (NAME) 
            VALUES (%s)
            RETURNING ID
        """,(name,))

        id=self.cur.fetchone()[0]
        
        self.conn.commit()

        return id

    def get_topic_list(self):
        self.cur.execute("""
            SELECT NAME FROM TOPICS
        """)

        row = self.cur.fetchall()
        topics = []
        for val in row:
            topics.append(val[0])
        return topics

    def get_topic_queue(self,topic_name):
        self.cur.execute("""
            SELECT * FROM TOPICS
            WHERE NAME = %s
        """,(topic_name,))

        row=self.cur.fetchone()

        return TopicQueueDBMS(
            topic_name=row[1],
            cur=self.cur,
            conn=self.conn
        )

class TopicQueueDBMS:
    def __init__(self, topic_name,cur,conn):
        self.topic_name = topic_name
        self.cur=cur
        self.conn=conn

    def enqueue(self, message):
        if self.size()==0:
            self.cur.execute("""
                UPDATE TOPICS 
                SET MESSAGES = ARRAY[%s]
                WHERE NAME=%s
            """,(message,self.topic_name,))
        else:
            self.cur.execute("""
                UPDATE TOPICS 
                SET MESSAGES = ARRAY_APPEND(MESSAGES,%s) 
                WHERE NAME=%s
            """,(message,self.topic_name,))

        self.conn.commit()
        

    def get_at_offset(self, offset):
        self.cur.execute("""
            SELECT MESSAGES[%s] FROM TOPICS 
            WHERE NAME=%s
        """,(str(offset),self.topic_name,))
        
        return self.cur.fetchone()

    def size(self):
        self.cur.execute("""
            SELECT MESSAGES FROM TOPICS 
            WHERE NAME=%s
        """,(self.topic_name,))

        row=self.cur.fetchone()

        if row[0]==None:
            return 0
        else:
            return len(row[0])

    def can_write(self):
        return True

if __name__=="__main__":
    dbms=TopicDBMS()
    dbms.create_table()