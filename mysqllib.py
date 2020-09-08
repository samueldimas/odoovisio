import mysql.connector

class MySQLLib:
    def __init__(self, host:str, database:str, user:str, password:str):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
    
    def select_query(self, table_name:str, field:str):
        query = 'select ' + field + ' from ' + table_name 
        records = None

        try:
            connection = mysql.connector.connect(host=self.host, 
            database=self.database, 
            user=self.user, 
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

    def select_where_query(self, table_name:str, field:str):
        connection = None
        query = 'select * from ob_business_partner where status <> "N" order by idClient asc'
        records = None

        try:
            connection = mysql.connector.connect(host=self.host, 
            database=self.database, 
            user=self.user, 
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

    def execute_update(self, argument:str):
        connection = None
        
        try:
            connection = mysql.connector.connect(
                host=self.host, 
                database=self.database,
                user=self.user,
                password=self.password
            )

            cursor = connection.cursor()
            cursor.execute(argument)
            connection.commit()
        
        except mysql.connector.Error as e:
            print("MySQL connection error:", e)
        
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def execute(self, argument:str):
        records = None

        try:
            connection = mysql.connector.connect(host=self.host, 
            database=self.database, 
            user=self.user, 
            password=self.password)

            cursor = connection.cursor()
            cursor.execute(argument)

            records = cursor.fetchall()

        except mysql.connector.Error as e:
            print("MySQL connection error:", e)

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

        return records