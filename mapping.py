from mysqllib import MySQLLib
from pgsqllib import PGSQLLib
import datetime

def main():
    db_source = MySQLLib(host='192.168.1.33', 
    database='nusasms_v2', 
    user='odoo', 
    password='Odoo2020!'
    )

    client_key_partner_list = db_source.execute('select client_key from ob_business_partner')
    client_key_invoice_list = db_source.execute('select client_key from ob_invoice')

    lst = []
    lst2 = []
    for tup in client_key_partner_list:
        lst.append(str(tup[0]).lower())
    for tup2 in client_key_invoice_list:
        lst2.append(str(tup2[0]).lower())
   
    mylist = list(dict.fromkeys(lst2))

    new_lst = [c for c in mylist if not c in lst]

    print(new_lst)
    
if __name__ == '__main__':
    main()