import psycopg2 

class PGSQLLib:
    def __init__(self, host:str, port:str, database:str, user:str, password:str):
        '''Fungsi constructor

        Keyword arguments:
        host -- alamat host dari database
        port -- port dari database
        database -- nama dari database
        user -- username dari database 
        password -- password dari database

        '''
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
    
    def execute(self, argument:str):
        '''Fungsi untuk mengeksekusi query selain SELECT di PostgreSQL
           misal INSERT atau UPDATE
           
        Arguments:
        argument -- query select yang ingin dieksekusi
                   
        '''
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
        '''Fungsi untuk mengeksekusi query SELECT di MySQL
        
        Arguments:
        argument -- query select yang ingin dieksekusi
        
        Return value:
        records -- list of tuples dari masing-masing record yang di-fetch
                   
        '''
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