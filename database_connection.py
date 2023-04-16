import psycopg2
import json 

class DBConnection:
    '''
    Connects to local database with correct configuration.
    '''
    def __init__(self, config_path = "Project2\db_config.json"):
        with open(config_path, "r") as file:
            self.config = json.load(file)
        self.conn = psycopg2.connect(host=self.config["host"], port=self.config["port"], database=self.config["database"], user=self.config["user"], password=self.config["password"])
        self.cur = self.conn.cursor()

    def execute(self,query):
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        return query_results

    def close(self):
        self.cur.close()
        self.conn.close()

