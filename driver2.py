from mysqllib import MySQLLib
from pgsqllib import PGSQLLib

def main():
    db_source = MySQLLib(host='192.168.1.33', 
    database='nusasms_v2', 
    user='odoo', 
    password='Odoo2020!'
    )
    
    db_dest = PGSQLLib(host='localhost', 
    port='3000', 
    database='odoovisio2', 
    user='samuel', 
    password='samuel'
    )

    select_result = db_source.select_query("ob_business_partner", "*")
    full_entry = []

    for row in select_result:
        ## ORDER todo
        order_entry = []

        # id id
        # organization company_id
        # invoice_no number
        # invoice_date date_invoice
        # client_key partner_id
        # address partner_id
        # desc none
        # payment_term none
        # price_list company_id
        # product product_id
        # quantity quantity
        # unit_price price_unit
        # subtotal price_subtotal
        # actual_payment price_total
        # tax none
        # line_no none
        # currency currency_id
        # status none
        # err_msg none
        # isMigrated none
        # payment_channel none
        # payment_method none
        # doku_cost none
        # transaction_cost none

        ## ORDER LINE todo
        order_line_entry = []

        ## INVOICE todo
        invoice_entry = []

        ## INVOICE LINE todo
        invoice_line_entry = []

        ## MOVE todo
        move_entry = []

        ## MOVE LINE todo
        move_line_entry = []

        return NotImplementedError

if __name__ == "__main__":
    main()