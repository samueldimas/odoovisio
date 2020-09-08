from mysqllib import MySQLLib
from pgsqllib import PGSQLLib
import datetime

def main():
    db_source = MySQLLib(host='192.168.1.33', 
    database='nusasms_v2', 
    user='odoo', 
    password='Odoo2020!'
    )

    db_dest = PGSQLLib(host='localhost', 
    database='odoovisio2', 
    user='samuel', 
    password='samuel'
    )

    select_result =  db_source.execute("select * from ob_business_partner")

    for row in select_result:
        partner_entry = {}
        contact_entry = {}
    ''' Used field(s)   : 1:organization    -> company_id, 
                          2:client_key      -> number
                          3:client_name    -> date_order, validity_date
                          4:category     -> partner_id
                         12:subtotal        -> amount_untaxed, amount_untaxed_signed
                         13:actual_payment  -> amount_total, amount_total_signed, amount_total_company_signed
                         14:tax             -> amount_tax
                         16:currency        -> currency_id
        Unused field(s) : 0:id, 5:price_list, 6:payment_term, 7:phone, 8:phone_2, 9:up_name,
        10:quantity, 11:unit_price, 15:line_no, 17:status 18:err_msg, 19:isMigrated,
        20:payment_channel, 21:payment_method, 22:doku_cost, 23:transaction_cost
    '''

    
    partner_insert_query = "insert into res_partner (name, company_id, create_date, display_name, lang, \
        active, type, street, zip, city, state_id, country_id, phone, is_company, color, partner_share, \
        commercial_partner_id, commercial_company_name, create_uid, write_uid, write_date, message_bounce, \
        picking_warn, invoice_warn, supplier_rank, customer_rank, sale_warn) values (\
        '{}', {}, '{}', '{}', '{}',\
        {}, '{}', '{}', '{}', '{}', {}, {}, '{}', {}, {}, {},\
        {}, '{}', {}, {}, '{}', {},\
        '{}', '{}', {}, {}, '{}')".format()

if __name__ == '__main__':
    main()