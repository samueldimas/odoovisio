import psycopg2

def main():
    ''' Connect to the PostgreSQL database server''' 
    connection = None

    try:
        print("Connecing to the PostgreSQL database server")
        connection = psycopg2.connect(host='localhost', port='3000', database='odoovisio', user='samuel', password='samuel')

        '''Create cursor of the connection'''
        cursor = connection.cursor()

        '''Execute a statement'''
        print('PostgreSQL dattabase version:')
        cursor.execute('select * from public.account_account;')

        '''Displaying  PGSQL database version'''
        records = cursor.fetchall()
        for row in records:
            print(row[1])

        '''Close the connection with the PGSQL database'''
        cursor.close()

    except (Exception, psycopg2.DatabaseError) as e:
        print(e)

    finally:
        if connection is not None:
            connection.close()
            print('Database connection closed')

if __name__ == '__main__':
    main()