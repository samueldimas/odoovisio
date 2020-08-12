from mysqllib import MySQLLib

def main():
    db = MySQLLib(host='192.168.1.33', 
    database='nusasms_v2', 
    user='odoo', 
    password='Odoo2020!'
    )

    result = db.select_query('ob_business_partner', '*')
    print(result)

if __name__ == '__main__':
    main()

