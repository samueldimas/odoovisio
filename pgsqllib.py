import psycopg2 

class PGSQLLib:
    def __init__(self, host:str, port:str, database:str, user:str, password:str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    def execute(self, argument:str):
        try:
            connection = psycopg2.connect(host=self.host, 
            port=self.port, 
            database=self.database, 
            user=self.user, 
            password=self.password
            )

            cursor = connection.cursor()

            cursor.execute(argument)
            connection.commit()
            
        except psycopg2.Error as e:
            print("PostgreSQL connection error:", e)

        finally:
            if connection:
                cursor.close()
                connection.close()
    
    def execute_select(self, argument:str):
        records = None
        try:
            connection = psycopg2.connect(host=self.host, 
            port=self.port, 
            database=self.database, 
            user=self.user, 
            password=self.password
            )

            cursor = connection.cursor()

            cursor.execute(argument)
            records = cursor.fetchall()
            
        except psycopg2.Error as e:
            print("PostgreSQL connection error:", e)

        finally:
            if connection:
                cursor.close()
                connection.close()
        
        return records