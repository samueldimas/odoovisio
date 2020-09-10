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
    port='4000', 
    database='odoovisio2', 
    user='samuel', 
    password='samuel'
    )

    select_result = db_source.select_query("ob_invoice", "*")

    i = 0
    for row in select_result:
        ''' Initialize default values (dipakai di banyak query)'''
        time = str(datetime.datetime.now()).split(".")[0]
        product_id = 0
        company_id = 0
        partner_id = 0

        ## ORDER todo
        order_entry = {}
        ''' Used field(s)   : 1:organization    -> company_id, 
                              3:invoice_date    -> date_order, validity_date
                              4:client_key      -> partner_id
                             12:subtotal        -> amount_untaxed
                             13:actual_payment  -> amount_total
                             14:tax             -> amount_tax
            Unused field(s) : 0:id, 2:invoice_no, 5:address, 6:desc, 7:payment_term, 8:price_list, 9:product,
            10:quantity, 11:unit_price, 15:line_no, 16:currency, 17:status 18:err_msg, 19:isMigrated,
            20:payment_channel, 21:payment_method, 22:doku_cost, 23:transaction_cost
        '''
        if row[1] == "VIS":
            order_entry["company_id"] = 2
        elif row[1] == "" or row[1] is None:
            '''Ada kasus di mana organization string kosong atau None (null di MySQL)''' 
            order_entry["company_id"] = 2
        else: 
            order_entry["company_id"] = 3
        company_id = order_entry["company_id"]

        order_entry["date_order"] = "{} 00:00:00".format(row[3].strftime("%Y-%m-%d"))
        order_entry["validity_date"] = row[3].strftime("%Y-%m-%d")

        '''
        Ada banyak client_key di table ob_invoice yang tidak sesuai dengan 
        table ob_business_partner jadi harus dipetakan terlebih dahulu 
        '''
        if (row[4] == 'PT NUTRIFOOD INDONESIA_1'):
            order_client = 'PT NUTRIFOOD INDONESIA'
        elif (row[4] == 'gotravindo'):
            order_client = 'PT Gistrav Inspira Indonesia'
        elif (row[4] == 'TUGURE'):
            order_client = 'PT TUGU REASURANSI INDONESIA'
        elif (row[4] == 'Up. Tommy'):
            order_client = 'Bimbel Matica'
        elif row[4] == 'PT. Adhi Karya , Proyek Eastern Green':
            order_client = 'PT. Adhi Karya,Proyek Eastern Green'
        elif row[4] == 'Yayasan Lembaga Alkitab Indonesia':
            order_client = 'Lembaga Alkitab Indonesia'
        elif row[4] == 'Raja Mobil':
            order_client = 'PT. Raja Mobil Media'
        elif row[4] == 'Santa Fe':
            order_client = 'Santa Fe Relocation Services'
        elif row[4] == 'BPJS Kesehatan Cabang Jakarta Utara':
            order_client = 'BPJS Kesehatan Cabang Jakarta Utara MPKP'
        elif row[4] == 'BPJS KESEHATAN TANGERANG':
            order_client = 'BPJS KESEHATAN TANGERANG BIDANG PENJAMIN'
        elif row[4] == 'PT. Sale Stock Indonesia':
            order_client = 'PT Sale Stock Indonesia'
        elif row[4] == 'BPJS Ketenagakerjaan Purwakarta':
            order_client = 'BPJS Ketenagakerjaan Cabang Purwakarta'
        elif row[4] == 'PT Nusa Cipta Pratama (Apartement Silkwood)':
            order_client = 'PT Nusa Cipta Pratama'
        elif row[4] == 'Melilea':
            order_client = 'PT. Melilea Internasional Indonesia'
        elif row[4] == 'KEDAI SAYUR':
            order_client = 'PT.Jaring Solusi Indonesia'
        elif row[4] == '\toceanesia':
            order_client = 'oceanesia'
        elif row[4] == 'md media':
            order_client = 'PT. Metra Digital Media'
        elif row[4] == 'PT Dhecyber Flow Indonesia':
            order_client = 'PT. Dhecyber Flow Indonesia'
        elif row[4] == 'PT Mobile Stone Indonesia':
            order_client = 'firestorm'
        elif row[4] == 'IDMarco':
            order_client = 'PT. IDMarco Perkasa Indonesia'
        elif row[4] == 'BPR SARANA UTAMA MULTIDANA':
            order_client = 'PT. BPR Sarana Utama Multidana'
        elif row[4] == 'PT. Global Oase Indonesia':
            order_client = 'PT Global Oase Indonesia'
        elif row[4] == "Daarul Qur'an Jambi":
            order_client = "Daarul Qur''an Jambi"
        elif row[4] == "":
            order_client = "Training Nusa"   
        else:
            print(row[4])
            order_client = row[4]

        
        partner_id_query = "select id_odoo from ob_business_partner where client_key = '{}'".format(order_client)
        partner_id = db_source.execute(partner_id_query)[0][0]
        order_entry["partner_id"] = partner_id
        
        order_entry["amount_untaxed"] = row[12]
        order_entry["amount_total"] = row[13]

        amount_tax = row[13] - row[12]
        order_entry["amount_tax"] = amount_tax

        sale_order_name = db_dest.execute_select("select name from sale_order where id in (select max(id) from sale_order)")[0][0]
        name_sequence = int(sale_order_name.split("O")[1]) + 1
        new_order_name = "SO{:04d}".format(name_sequence)

        # INSERT QUERY SALE_ORDER_LINE
        insert_sale_order_query = "insert into sale_order (name, state, date_order, validity_date, require_signature, \
            require_payment, create_date, confirmation_date, user_id, partner_id, partner_invoice_id, \
            partner_shipping_id, pricelist_id, invoice_status, amount_untaxed, amount_tax, amount_total, \
            currency_rate, payment_term_id, company_id, team_id, create_uid, write_uid, write_date, \
            picking_policy, warehouse_id) values (\
            '{}', '{}', '{}', '{}', {},\
            {}, '{}', '{}', {}, {}, {},\
            {}, {}, '{}', {}, {}, {},\
            {}, {}, {}, {}, {}, {}, '{}', \
            '{}', {})".format(
            new_order_name, 'sale', order_entry["date_order"], order_entry["validity_date"], 'false', 
            'true', time, order_entry["date_order"], 2, order_entry["partner_id"], order_entry["partner_id"], 
            order_entry["partner_id"], 1, 'invoiced', order_entry["amount_untaxed"], order_entry["amount_tax"], order_entry["amount_total"], 
            1, 1, order_entry["company_id"], 1, 2, 2, time, 
            'direct', 1
            )
        # print(insert_sale_order_query)
        db_dest.execute(insert_sale_order_query)

        ## ORDER LINE todo
        order_line_entry = {}
        ''' Used field(s)   : 1:organization    -> company_id, 
                              4:client_key      -> partner_id
                              9:product         -> product_id
                             10:quantity        -> product_uom_qty, qty_invoiced
                             11:unit_price      -> price_unit
                             12:subtotal        -> price_subtotal
                             13:actual_payment  -> price_total
                             14:tax             -> price_tax
                             16:currency        -> currency_id
            Unused field(s) : 0:id, 2:invoice_no, 3:invoice_date, 5:address, 6:desc, 7:payment_term, 8:price_list, 
            15:line_no, , 17:status 18:err_msg, 19:isMigrated, 20:payment_channel, 21:payment_method, 
            22:doku_cost, 23:transaction_cost
        '''
        order_line_entry["company_id"] = company_id
        order_line_entry["partner_id"] = partner_id
        
        if row[9] == '' or row[9] is None:
            order_line_entry["product_id"] = 1
        else:
            if row[1] == 'VIS':
                order_line_entry["product_id"] = row[9]
            else:
                order_line_entry["product_id"] = int(row[9]) + 30
        product_id = order_line_entry["product_id"]

        order_line_entry["product_uom_qty"] = row[10]
        order_line_entry["qty_invoiced"] = row[10]
        order_line_entry["price_unit"] = row[11]
        order_line_entry["price_subtotal"] = row[12]
        order_line_entry["price_total"] = row[13]
        order_line_entry["price_tax"] = row[13] - row[12]
        order_line_entry["currency_id"] = 12

        # TODO GET ORDER_ID FOR SALE_ORDER_LINE
        order_id_query = "select max(id) from sale_order"
        order_id = db_dest.execute_select(order_id_query)[0][0]

        # TODO PRICE_REDUCE, PRICE_REDUCE_TAXINC, PRICE_REDUCE_TAXEXCL
        price_reduce = row[11]
        price_reduce_taxinc = row[11] + row[11] * 0.1
        price_reduce_taxexcl = price_reduce

        insert_sale_order_line_query = "insert into sale_order_line (name, order_id, sequence, invoice_status,\
            price_unit, price_subtotal, price_tax, price_total, price_reduce, price_reduce_taxinc, \
            price_reduce_taxexcl, discount, product_id, product_uom_qty, product_uom, qty_delivered_method, \
            qty_delivered, qty_delivered_manual, qty_to_invoice, qty_invoiced, untaxed_amount_invoiced, \
            untaxed_amount_to_invoice, salesman_id, currency_id, company_id, order_partner_id, is_expense, \
            is_downpayment, state, customer_lead, create_uid, create_date, write_uid, write_date) values (\
            '{}', {}, 1, 'invoiced', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 1, 'manual', 0, 0, 0, {}, {}, 0, 2, {}, \
            {}, {}, false, false, 'sale', 0, 2, '{}', 2, '{}')".format(new_order_name, order_id, order_line_entry["price_unit"], 
            order_line_entry["price_subtotal"], order_line_entry["price_tax"], order_line_entry["price_total"], 
            price_reduce, price_reduce_taxinc, price_reduce_taxexcl, 0, order_line_entry["product_id"], 
            order_line_entry["product_uom_qty"], order_line_entry["product_uom_qty"], 
            order_line_entry["price_subtotal"], order_line_entry["currency_id"], order_line_entry["company_id"], 
            order_line_entry["partner_id"], time, time)
        # print(insert_sale_order_line_query)
        db_dest.execute(insert_sale_order_line_query)

        ## INVOICE todo
        invoice_entry = {}
        ''' Used field(s)   : 1:organization    -> company_id, 
                              2:invoice_no      -> number
                              3:invoice_date    -> date_order, validity_date
                              4:client_key      -> partner_id
                             12:subtotal        -> amount_untaxed, amount_untaxed_signed
                             13:actual_payment  -> amount_total, amount_total_signed, amount_total_company_signed
                             14:tax             -> amount_tax
                             16:currency        -> currency_id
            Unused field(s) : 0:id, 5:address, 6:desc, 7:payment_term, 8:price_list, 9:product,
            10:quantity, 11:unit_price, 15:line_no, 17:status 18:err_msg, 19:isMigrated,
            20:payment_channel, 21:payment_method, 22:doku_cost, 23:transaction_cost
        '''
        invoice_entry['company_id'] = 3
        invoice_entry["number"] = row[2]
        invoice_entry["date_invoice"] = row[3].strftime('%Y-%m-%d')

        invoice_entry["partner_id"] = partner_id
        if row[4] == "Daarul Qur'an Jambi":
            client_key = "Daarul Qur''an Jambi"
        else:
            client_key = row[4]

        invoice_entry["amount_untaxed"] = row[12]
        invoice_entry["amount_untaxed_signed"] = row[12]
        invoice_entry["amount_tax"] = row[13] - row[12]
        invoice_entry["amount_total"] = row[13]
        invoice_entry["amount_total_signed"] = row[13]
        invoice_entry["amount_total_company_signed"] = row[13]
        invoice_entry["residual"] = 0
        invoice_entry["residual_signed"] = 0
        invoice_entry["residual_company_signed"] = 0
        invoice_entry["currency_id"] = 12

        # TODO MAPPING ID_PIUTANG
        id_sms_vis = [1, 2, 3, 4, 5, 28]
        id_sms_vml = [el + 30 for el in id_sms_vis]

        id_hosting_sim_card_vis = [8, 9, 19, 21, 23]
        id_hosting_sim_card_vml = [el + 30 for el in id_hosting_sim_card_vis]

        id_gsuite_vis = [7, 10, 11, 12, 13, 14, 15, 16, 17]
        id_gsuite_vml = [el + 30 for el in id_gsuite_vis]

        id_domain_vis = 30
        id_domain_vml = 60
        
        id_website_vis = 29
        id_website_vml = 59

        id_piutang = 481

        if row[1] == 'VIS':
            if product_id in id_sms_vis:
                id_piutang = 481
            elif product_id in id_hosting_sim_card_vis:
                id_piutang = 482
            elif product_id in id_gsuite_vis:
                id_piutang = 483
            elif product_id == id_domain_vis:
                id_piutang = 484
            elif product_id == id_website_vis:
                id_piutang = 485
        else:
            if product_id in id_sms_vml:
                id_piutang = 486
            elif product_id in id_hosting_sim_card_vml:
                id_piutang = 487
            elif product_id in id_gsuite_vml:
                id_piutang = 488
            elif product_id == id_domain_vml:
                id_piutang = 489
            elif product_id == id_website_vml:
                id_piutang = 490

        # TODO PREPROCESS INVOICE REFERENCE
        reference_key = int(invoice_entry["number"].split("/")[2])
        invoice_reference = "{}/{:04d}".format(invoice_entry["number"], reference_key)
        invoice_amount_tax = invoice_entry["amount_untaxed"] * 0.1

        insert_invoice_query = "insert into account_invoice (name, company_id, number, date_invoice, partner_id, \
            currency_id, type, move_name, reference, state, sent, date_due, payment_term_id, date, account_id, \
            amount_untaxed, amount_untaxed_signed, amount_tax, amount_total, amount_total_signed, \
            amount_total_company_signed, journal_id, reconciled, residual, residual_signed, \
            residual_company_signed, user_id, commercial_partner_id, vendor_display_name, create_uid, \
            write_uid, create_date, write_date, team_id, partner_shipping_id) values (\
            '{}', {}, '{}', '{}', {}, \
            {}, '{}', '{}', '{}', '{}', {}, '{}', {}, '{}', {}, \
            {}, {}, {}, {}, {}, \
            {}, {}, {}, {}, {}, \
            {}, {}, {}, '{}', {}, \
            {}, '{}', '{}', {}, {})".format('', invoice_entry["company_id"], invoice_entry["number"], invoice_entry["date_invoice"], invoice_entry["partner_id"],
            invoice_entry["currency_id"], 'out_invoice', invoice_entry["number"], invoice_reference, 'paid', 'true', invoice_entry["date_invoice"], 1, invoice_entry["date_invoice"], id_piutang,
            invoice_entry["amount_untaxed"], invoice_entry["amount_untaxed_signed"], invoice_entry["amount_tax"], invoice_entry["amount_total"], invoice_entry["amount_total_signed"],
            invoice_entry["amount_total_company_signed"], 1, 'false', invoice_entry["residual"], invoice_entry["residual_signed"], 
            invoice_entry["residual_company_signed"], 2, invoice_entry["partner_id"], client_key, 2,
            2, time, time, 1, invoice_entry["partner_id"])
        # print(insert_invoice_query)

        db_dest.execute(insert_invoice_query)

        ## INVOICE LINE todo
        invoice_line_entry = {}
        ''' Used field(s)   : 1:organization    -> company_id, 
                              2:invoice_no      -> invoice_id (preprocessed)
                              3:invoice_date    -> date_invoice
                              4:client_key      -> partner_id
                              9:product         -> product_id
                             10:quantity        -> quantity
                             11:unit_price      -> price_unit
                             12:subtotal        -> price_subtotal, price_subtotal_signed
                             13:actual_payment  -> price_total
                             16:currency        -> currency_id
            Unused field(s) : 0:id, 5:address, 6:desc, 7:payment_term, 8:price_list, 14:tax, 15:line_no, 
            17:status 18:err_msg, 19:isMigrated, 20:payment_channel, 21:payment_method, 22:doku_cost, 
            23:transaction_cost
        '''
        invoice_line_entry["company_id"] = company_id

        invoice_id_query = "select max(id) from account_invoice"
        invoice_id = db_dest.execute_select(invoice_id_query)[0][0]

        invoice_line_entry["date_invoice"] = row[3].strftime('%Y-%m-%d')
        invoice_line_entry["partner_id"] = partner_id
        invoice_line_entry["product_id"] = product_id
        invoice_line_entry["quantity"] = row[10]
        invoice_line_entry["price_unit"] = row[11]
        invoice_line_entry["price_subtotal"] = row[12]
        invoice_line_entry["price_subtotal_signed"] = row[12]
        invoice_line_entry["quantity"] = 0
        invoice_line_entry["discount"] = 0
        invoice_line_entry["price_total"] = row[13]
        invoice_line_entry["currency_id"] = 12

        name_product_query = "select name from product_template where id = {}".format(product_id)
        name_product = db_dest.execute_select(name_product_query)[0][0]

        # INSERT QUERY ACCOUNT_INVOICE_LINE
        insert_invoice_line_query = "insert into account_invoice_line (name, origin, sequence, invoice_id, uom_id, product_id, account_id, \
            price_unit, price_subtotal, price_total, price_subtotal_signed, quantity, discount, \
            company_id, partner_id, currency_id, is_rounding_line, create_uid,\
            create_date, write_uid, write_date) values ('{}', '{}', {}, {}, {}, {}, {}, \
            {}, {}, {}, {}, {}, {}, \
            {}, {}, {}, {}, {}, \
            '{}', {}, '{}')".format(name_product, new_order_name, 1, invoice_id, 1, invoice_line_entry["product_id"], id_piutang,
            invoice_line_entry["price_unit"], invoice_line_entry["price_subtotal"], invoice_line_entry["price_total"], invoice_line_entry["price_subtotal_signed"], invoice_line_entry["quantity"], invoice_line_entry["discount"], 
            invoice_line_entry["company_id"], invoice_line_entry["partner_id"], invoice_line_entry["currency_id"], 'false', 2, 
            time, 2, time)
        # print(insert_invoice_line_query)
        db_dest.execute(insert_invoice_line_query)

        ## MOVE todo
        move_entry = {}
        ''' Used field(s)   : 1:organization    -> company_id, 
                              2:invoice_no      -> number
                              3:invoice_date    -> date_order, validity_date
                              4:client_key      -> partner_id
                             13:actual_payment  -> amount_total, amount_total_signed, amount_total_company_signed
                             16:currency        -> currency_id
            Unused field(s) : 0:id, 5:address, 6:desc, 7:payment_term, 8:price_list, 9:product, 10:quantity, 
            11:unit_price, 12:subtotal, 14:tax, 15:line_no, 17:status 18:err_msg, 19:isMigrated,
            20:payment_channel, 21:payment_method, 22:doku_cost, 23:transaction_cost
        '''
        move_entry["company_id"] = company_id

        move_entry["name"] = row[2]
        move_entry["ref"] = invoice_reference

        move_entry["date"] = row[3].strftime('%Y-%m-%d')
        move_entry["partner_id"] = partner_id
        move_entry["amount"] = row[13]
        move_entry["currency_id"] = 12

        # ACCOUNT_MOVE INSERT QUERY
        insert_move_query = "insert into account_move (name, ref, date, journal_id, currency_id, state, \
            partner_id, amount, company_id, matched_percentage, auto_reverse, create_uid, create_date, \
            write_uid, write_date) values ('{}', '{}', '{}', 1, {}, 'posted',\
            {}, {}, {}, 0, false, 2, '{}', 2, '{}')".format(
            move_entry["name"], move_entry["ref"], move_entry["date"], move_entry["currency_id"],
            move_entry["partner_id"], move_entry["amount"], move_entry["company_id"], time, time)
        # print(insert_move_query)
        db_dest.execute(insert_move_query)

        ## TODO ADD move_id TO ACCOUNT_INVOICE
        new_move_id = db_dest.execute_select("select max(id) from account_move")[0][0]
        new_invoice_id = db_dest.execute_select("select max(id) from account_invoice")[0][0]
        update_move_query = "update account_invoice set move_id = {} where id = {}".format(new_move_id, new_invoice_id)
        # db_dest.execute(update_move_query)

        ## MOVE LINE todo
        move_line_entry = {}
        ''' Used field(s)   : 1:organization    -> company_id, 
                              2:invoice_no      -> ref (preprocessed)
                              3:invoice_date    -> date, date_maturity
                              4:client_key      -> partner_id
                              9:product         -> product_id
                             10:quantity        -> quantity_1
                             11:unit_price      -> price_unit
                             12:subtotal        -> <various>
                             13:actual_payment  -> <various>
                             14:tax             -> tax_credit(preprocessed)
                             16:currency        -> company_currency_id
            Unused field(s) : 0:id, 5:address, 6:desc, 7:payment_term, 8:price_list, 15:line_no, 
            17:status 18:err_msg, 19:isMigrated, 20:payment_channel, 21:payment_method, 22:doku_cost, 
            23:transaction_cost
        '''
        move_line_entry["company_id"] = company_id
        move_line_entry["ref"] = invoice_reference
        move_line_entry["date"] = row[3].strftime('%Y-%m-%d')
        move_line_entry["date_maturity"] = row[3].strftime('%Y-%m-%d')
        move_line_entry["partner_id"] = partner_id
        move_line_entry["product_id"] = product_id
        move_line_entry["currency_id"] = 12

        ## TODO GET name FROM PRODUCT_TEMPLATE
        name_1_query = "select name from product_template where id = {}".format(product_id)
        name_1 = db_dest.execute_select(name_1_query)[0][0]

        move_line_entry["quantity_1"] = row[10] 
        move_line_entry["quantity_2"] = 1
        move_line_entry["quantity_3"] = 1
        move_line_entry["credit"] = row[12]
        move_line_entry["debit"] = row[13]

        ## TODO GET TAX_LINE_ID
        if row[14] == '0':
            move_line_entry["tax_line_id"] = 19
        else:
            move_line_entry["tax_line_id"] = 20
        tax_credit = int(row[13]) - int(row[12])

        move_line_entry["company_currency_id"] = 12

        # GET RECENT INVOICE REF QUERY
        invoice_ref_query = "select reference from account_invoice where id = {}".format(new_move_id)
        invoice_ref = db_dest.execute_select(invoice_ref_query)
        # invoice_ref = None

        # GET TAX NAME QUERY
        tax_name_query = "select name from account_tax where id = {}".format(move_line_entry["tax_line_id"])
        tax_name = db_dest.execute_select(tax_name_query)[0][0]

        # INSERT QUERY MOVE LINE BEFORE PAYMENT
        insert_move_line_query_1 = "insert into account_move_line (name, quantity, product_uom_id, product_id, \
            debit, credit, balance, debit_cash_basis, credit_cash_basis, balance_cash_basis, amount_currency, \
            company_currency_id, currency_id, amount_residual, amount_residual_currency, tax_base_amount, \
            account_id, move_id, ref, payment_id, reconciled, full_reconcile_id, \
            journal_id, blocked, date_maturity, date, tax_line_id, company_id, invoice_id, \
            partner_id, user_type_id, tax_exigible, create_uid, create_date, write_uid, write_date) values (\
            '{}', {}, {}, {},\
            {}, {}, {}, {}, {}, {}, {}, \
            {}, {}, {}, {}, {}, \
            {}, {}, '{}', {}, {}, {},\
            {}, {}, '{}', '{}', {}, {}, {}, \
            {}, {}, {}, {}, '{}', {}, '{}')".format(name_1, move_line_entry["quantity_1"], 1, move_line_entry["product_id"], 
            0, move_line_entry["credit"], int(move_line_entry["credit"]) * -1, 0, move_line_entry["credit"], int(move_line_entry["credit"]) * -1, 0, 
            move_line_entry["company_currency_id"], 'null', 0, 0, 0, 
            87, new_move_id, invoice_ref, 'null', 'false', 'null', 
            1, 'false', move_line_entry["date_maturity"], move_line_entry["date"], 'null', move_line_entry["company_id"], invoice_id, 
            move_line_entry["partner_id"], 14, 'true', 2, time, 2, time
            )
        # print(insert_move_line_query_1)
        db_dest.execute(insert_move_line_query_1)

        insert_move_line_query_2 = "insert into account_move_line (name, quantity, product_uom_id, product_id, \
            debit, credit, balance, debit_cash_basis, credit_cash_basis, balance_cash_basis, amount_currency, \
            company_currency_id, currency_id, amount_residual, amount_residual_currency, tax_base_amount, \
            account_id, move_id, ref, payment_id, reconciled, full_reconcile_id, \
            journal_id, blocked, date_maturity, date, tax_line_id, company_id, invoice_id, \
            partner_id, user_type_id, tax_exigible, create_uid, create_date, write_uid, write_date) values (\
            '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', {}, {}, {}, {}, {}, \
            '{}', '{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, '{}')".format(tax_name, 1, 'null', 'null', 0, 
            tax_credit, tax_credit * -1, 0, tax_credit, tax_credit * -1, 0,
            move_line_entry["company_currency_id"], 'null', 0, 0, move_line_entry["credit"],
            60, new_move_id, invoice_ref, 'null', 'false', 'null', 1, 'false ', move_line_entry["date_maturity"], 
            move_line_entry["date"], move_line_entry["tax_line_id"], move_line_entry["company_id"], invoice_id,
            move_line_entry["partner_id"], 9, 'true', 2, time, 2, time)
        # print(insert_move_line_query_2)
        db_dest.execute(insert_move_line_query_2)
        
        insert_move_line_query_3 = "insert into account_move_line (name, quantity, product_uom_id, product_id, \
            debit, credit, balance, debit_cash_basis, credit_cash_basis, balance_cash_basis, amount_currency, \
            company_currency_id, currency_id, amount_residual, amount_residual_currency, tax_base_amount, \
            account_id, move_id, ref, payment_id, reconciled, full_reconcile_id, \
            journal_id, blocked, date_maturity, date, tax_line_id, company_id, invoice_id, \
            partner_id, user_type_id, tax_exigible, create_uid, create_date, write_uid, write_date) values (\
            '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', {}, {}, {}, {}, {}, \
            '{}', '{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, '{}')".format('', 1, 'null', 'null', 
            move_line_entry["debit"], 0, move_line_entry["debit"], move_line_entry["debit"], 0, move_line_entry["debit"], 0,
            move_line_entry["company_currency_id"], 'null', 0, 0, 0,
            id_piutang, new_move_id, invoice_ref, 'null', 'false', 'null',
            1, 'false', move_line_entry["date_maturity"], move_line_entry["date"], 'null', move_line_entry["company_id"], invoice_id,
            move_line_entry["partner_id"], 1, 'true', 2, time, 2, time)
        # print(insert_move_line_query_3)
        db_dest.execute(insert_move_line_query_3)

        ## PAYMENT TODO

        # INSERT QUERY MOVE AND MOVE LINE AFTER PAYMENT

        # GET MOVE NAME FOR BANK MOVE LINES
        bank_move_name_query = "select name from account_move where journal_id = 7"
        bank_move_list = db_dest.execute_select(bank_move_name_query)
        # print(bank_move_list)

        if (len(bank_move_list) == 0):
            new_bank_move_name = "BNK1/2020/0001"
        else:
            next_move_sequence = int(bank_move_list[-1][0].split("/")[2]) + 1 
            new_bank_move_name = "BNK1/2020/{:04d}".format(next_move_sequence)

        payment_name_move_line = "Customer Payment: {}".format(move_entry["name"])
    
        insert_move_query_2 = "insert into account_move (name, ref, date, journal_id, currency_id, state, \
            partner_id, amount, company_id, matched_percentage, auto_reverse, create_uid, create_date, \
            write_uid, write_date) values ('{}', '{}', '{}', 7, {}, 'posted',\
            {}, {}, {}, 0, false, 2, '{}', 2, '{}')".format(
            new_bank_move_name, move_line_entry["ref"], move_line_entry["date"], move_line_entry["currency_id"],
            move_entry["partner_id"], move_entry["amount"], move_entry["company_id"], time, time)
        # print(insert_move_query_2)
        db_dest.execute(insert_move_query_2)

        new_move_id_payment = db_dest.execute_select("select max(id) from account_move")[0][0]
        # new_move_id_payment = 1

        insert_move_line_query_4 = "insert into account_move_line (name, \
            debit, credit, balance, debit_cash_basis, credit_cash_basis, balance_cash_basis, amount_currency, \
            company_currency_id, currency_id, amount_residual, amount_residual_currency, tax_base_amount, \
            account_id, move_id, ref, payment_id, reconciled, full_reconcile_id, \
            journal_id, blocked, date_maturity, date, tax_line_id, company_id, invoice_id, \
            partner_id, user_type_id, tax_exigible, create_uid, create_date, write_uid, write_date) values (\
            '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', {}, {}, {}, {}, {}, \
            '{}', '{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, '{}')".format(payment_name_move_line,
            0, move_line_entry["debit"], int(move_line_entry["debit"]) * -1, 0, move_line_entry["debit"], int(move_line_entry["debit"]) * -1, 0,
            move_line_entry["company_currency_id"], 'null', 0, 0, 0,
            id_piutang, new_move_id_payment, move_line_entry["ref"], 'null', 'false', 'null',
            7, 'false', move_line_entry["date_maturity"], move_line_entry["date"], 'null', move_line_entry["company_id"], 'null',
            move_line_entry["partner_id"], 1, 'true', 2, time, 2, time)
        # print(insert_move_line_query_4)
        db_dest.execute(insert_move_line_query_4)

        move_line_payment_list = db_dest.execute_select("select * from account_payment")

        if (len(move_line_payment_list)) == 0:
            move_line_payment_name = "CUST.IN/2020/00001"
        else:
            move_line_payment_sequence = db_dest.execute_select("select name from account_payment where \
                id in (select max(id) from account_payment)")[0][0]
            next_payment_sequence = int(move_line_payment_sequence.split("/")[2]) + 1
            move_line_payment_name = "CUST.IN/2020/{:05d}".format(next_payment_sequence)
        # move_line_payment_name = "CUST.IN/2020/0001"

        insert_move_line_query_5 = "insert into account_move_line (name, \
            debit, credit, balance, debit_cash_basis, credit_cash_basis, balance_cash_basis, amount_currency, \
            company_currency_id, currency_id, amount_residual, amount_residual_currency, tax_base_amount, \
            account_id, move_id, ref, payment_id, reconciled, full_reconcile_id, \
            journal_id, blocked, date_maturity, date, tax_line_id, company_id, invoice_id, \
            partner_id, user_type_id, tax_exigible, create_uid, create_date, write_uid, write_date) values (\
            '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{}', {}, {}, {}, {}, {}, \
            '{}', '{}', {}, {}, {}, {}, {}, {}, {}, '{}', {}, '{}')".format(move_line_payment_name,
            move_line_entry["debit"], 0, move_line_entry["debit"], move_line_entry["debit"], 0, move_line_entry["debit"], 0,
            move_line_entry["company_currency_id"], 'null', move_line_entry["debit"], 0, 0,
            159, new_move_id_payment, move_line_entry["ref"], 'null', 'false', 'null',
            7, 'false', move_line_entry["date_maturity"], move_line_entry["date"], 'null', move_line_entry["company_id"], 'null',
            move_line_entry["partner_id"], 3, 'true', 2, time, 2, time)
        # print(insert_move_line_query_5)
        db_dest.execute(insert_move_line_query_5)

        payment_entry = {}
        ''' Used field(s)   : 1:organization    -> company_id, 
                              3:invoice_date    -> payment_date
                              4:client_key      -> partner_id
                             13:actual_payment  -> amount
                             16:currency        -> currency_id
            Unused field(s) : 0:id, 2:invoice_no, 5:address, 6:desc, 7:payment_term, 8:price_list, 9:product, 10:quantity, 
            11:unit_price, 12:subtotal, 14:tax, 15:line_no, 17:status 18:err_msg, 19:isMigrated,
            20:payment_channel, 21:payment_method, 22:doku_cost, 23:transaction_cost
        '''
        payment_entry["payment_date"] = row[3].strftime('%Y-%m-%d')
        payment_entry["partner_id"] = partner_id
        payment_entry["amount"] = row[13]
        payment_entry["currency_id"] = 12
        payment_entry["communication"] = invoice_reference
        
        # ACCOUNT_PAYMENT INSERT QUERY
        insert_payment_query = "insert into account_payment (name, state, payment_type, move_name, \
            multi, payment_method_id, partner_type, partner_id, amount, currency_id, payment_date, communication, \
            journal_id, payment_difference_handling, writeofF_label, create_uid, \
            create_date, write_uid, write_date) values ('{}', '{}', '{}', '{}', \
            '{}', {}, '{}', {}, {}, '{}', '{}', '{}',\
            '{}', '{}', '{}', {}, '{}', {}, '{}')".format(move_line_payment_name, 'posted', 'inbound', new_bank_move_name,
            'false', 1, 'customer', payment_entry["partner_id"], payment_entry["amount"], payment_entry["currency_id"], payment_entry["payment_date"], payment_entry["communication"],
            7, 'open', 'Write-Off', 2, time, 2, time)
        # print(insert_payment_query)
        db_dest.execute(insert_payment_query)
         
        ## TODO UPDATE PAYMENT ID MOVE LINE
        new_payment_id = db_dest.execute_select("select max(id) from account_payment")[0][0]
        selected_move_line_id = db_dest.execute_select("select max(id) from account_move_line")[0][0]
        db_dest.execute("update account_move_line set payment_id = {} where id = {}".format(new_payment_id, selected_move_line_id))
        db_dest.execute("update account_move_line set payment_id = {} where id = {}".format(new_payment_id, selected_move_line_id - 1))

        ## TODO UPDATE INVOICE STATUS QUERY
        # update_invoice_status_query = "update ob_invoice set status = {} where client_key = '{}'".format(
        #     'S', item[1])
        # db.execute_update(update_status_query)

        ## TODO UPDATE INVOICE MIGRATION QUERY
        # update_invoice_migration_query = "update ob_invoice set isMigrated = {} where client_key = '{}'".format(
        #     '1', item[1])
        # db.execute_update(update_migration_query)

         ## TODO UPDATE PAYMENT STATUS QUERY
        # update_payment_status_query = "update ob_payment set status = {} where client_key = '{}'".format(
        #     'S', item[1])
        # db.execute_update(update_status_query)

        ## TODO UPDATE PAYMENT MIGRATION QUERY
        # update_payment_migration_query = "update ob_payment set isMigrated = {} where client_key = '{}'".format(
        #     '1', item[1])
        # db.execute_update(update_migration_query)
        i += 1

if __name__ == "__main__":
    main()