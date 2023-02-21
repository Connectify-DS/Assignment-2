class TopicDBMS:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur=cur

    def create_table(self):
        try:
            self.cur.execute("""
                CREATE TABLE IF NOT EXISTS TOPICS(
                ID SERIAL PRIMARY KEY NOT NULL,
                NAME TEXT NOT NULL UNIQUE,
                MESSAGES INT[]);
            """)

            self.conn.commit()
        except:
            self.conn.rollback()

    def create_topic_queue(self,name):
        try:
            self.cur.execute("""
                INSERT INTO TOPICS (NAME) 
                VALUES (%s)
                RETURNING ID
            """,(name,))

            id=self.cur.fetchone()[0]
            
            self.conn.commit()

            return id
        except:
            self.conn.rollback()

    def get_topic_list(self):
        try:
            self.cur.execute("""
                SELECT NAME FROM TOPICS
            """)

            row = self.cur.fetchall()
            topics = []
            for val in row:
                topics.append(val[0])
            return topics
        except:
            self.conn.rollback()

    def get_topic_queue(self,topic_name):
        try:
            self.cur.execute("""
                SELECT * FROM TOPICS
                WHERE NAME = %s
            """,(topic_name,))

            row=self.cur.fetchone()
            # print("In Get Topic Queue, Topic Name = ",row)

            return TopicQueueDBMS(
                topic_name=row[1],
                cur=self.cur,
                conn=self.conn
            )
        except:
            self.conn.rollback()

class TopicQueueDBMS:
    def __init__(self, topic_name,cur,conn):
        self.topic_name = topic_name
        self.cur=cur
        self.conn=conn

    def enqueue(self, message):
        try:
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
        except:
            self.conn.rollback()
        

    def get_at_offset(self, offset):
        try:
            self.cur.execute("""
                SELECT MESSAGES[%s] FROM TOPICS 
                WHERE NAME=%s
            """,(str(offset),self.topic_name,))
            
            return self.cur.fetchone()
        except:
            self.conn.rollback()

    def size(self):
        try:
            self.cur.execute("""
                SELECT MESSAGES FROM TOPICS 
                WHERE NAME=%s
            """,(self.topic_name,))

            row=self.cur.fetchone()
            # print(row[0])
            
            if row[0]==None:
                return 0
            else:
                return len(row[0])
        except:
            self.conn.rollback()
