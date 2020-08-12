from mysql.connector import Error
import mysql.connector
import psycopg2

def main():
    try:
        connection = mysql.connector.connect(host='192.168.1.33',
        database='nusasms_v2',
        user='odoo',
        password='Odoo2020!',
        )
        if connection.is_connected():
            sql_select_query = "select * from ob_business_partner"
            cursor = connection.cursor()
            cursor.execute(sql_select_query)
            records = cursor.fetchall()
            print("Total number of rows in public.account_account is:", cursor.rowcount)
            
            dct = {}
            print("\nPrinting each laptop record")
            print(records)
            
    except Error as e:
        print("Error while connection to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == '__main__':
    main()