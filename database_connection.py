import psycopg2
import json 

class DBConnection:
    def __init__(self, host="localhost", port = 5432, database="TPC-H", user="postgres", password="cz4031"):
        self.conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        self.cur = self.conn.cursor()

    def execute(self,query):
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        return query_results

    def close(self):
        self.cur.close()
        self.conn.close()

