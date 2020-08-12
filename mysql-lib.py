import mysql.connector

class MySQLLib:
    def __init__(self, host:str, database:str, username:str, password:str):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
    
    def select_query(self, table_name:str, field:str='*'):
        connection = None
        query = 'select %s from %s'.format(field, table_name)
        records = None

        try:
            connection = mysql.connect(host=self.host, 
            database=self.database, 
            user=self.username, 
            password=self.password)

            cursor = connection.cursor()
            cursor.execute(query)

            records = cursor.fetchall()


        except mysql.connector.Error as e:
            print("MySQL connection error:", e)

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

        return records
        