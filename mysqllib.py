import mysql.connector

class MySQLLib:
    def __init__(self, host:str, database:str, user:str, password:str):
        '''Fungsi constructor

        Keyword arguments:
        host -- alamat host dari database
        database -- nama dari database
        user -- username dari database 
        password -- password dari database

        '''
        self.host = host
        self.database = database
        self.user = user
        self.password = password
  
    def execute(self, argument:str):
        '''Fungsi untuk mengeksekusi query selain select di MySQL
           misal INSERT atau UPDATE
           
        Arguments:
        argument -- query select yang ingin dieksekusi
        
        Return value:
        records -- list of tuples dari masing-masing record yang di-fetch
                   
        '''
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
    
    def execute_select(self, argument:str):
        '''Fungsi untuk mengeksekusi query SELECT di MySQL
        
        Arguments:
        argument -- query select yang ingin dieksekusi
        
        Return value:
        records -- list of tuples dari masing-masing record yang di-fetch
                   
        '''
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