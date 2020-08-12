import psycopg2 

class PGSQLLib:
    def __init__(self, host:str, port:str, database:str, user:str, password:str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password