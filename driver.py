from mysqllib import MySQLLib
from pgsqllib import PGSQLLib
import datetime

def main():
    db = MySQLLib(host='192.168.1.33', 
    database='nusasms_v2', 
    user='odoo', 
    password='Odoo2020!'
    )
    
    destination = PGSQLLib(host='localhost', 
    port='4000', 
    database='odoovisio2', 
    user='samuel', 
    password='samuel'
    )

    select_result = db.select_query("ob_business_partner", "*")
    
    full_entry = []

    for row in select_result:
        entry = []
        contact_entry = []

        '''Filtering partner attributes'''
        # (row[0]) SKIP
        
        # (row[1]) organization -> company_id
        entry.append(2) if row[1] == 'VIS' else entry.append(3)

        # (row[2]) client_key -> name
        key_processed = str(row[2]).replace("'", "")
        entry.append(key_processed)
            
        # (row[3]) client_name -> display_name
        name_processed = str(row[3]).replace("'", "")
        entry.append(name_processed)

        # (row[4]) category -> is_company (True)
        entry.append('true')

        # (row[5]) SKIP (row[6]) SKIP

        # (row[7]) phone -> phone
        entry.append(row[7])

        # (row[8]) SKIP (row[9]) up_name (row[10]) (row[11]) SKIP
        
        # (row[12]) street_1 -> street
        entry.append(row[12])

        # (row[13]) city_1 -> city
        entry.append(row[13])

        # (row[14]) province_1 -> province
        province_dict = {
            "sulawesi tenggara":641, "dki jakarta":619, "banten":616, "lampung":630, "jawa Barat":621,
            "jakarta":619, "tangerang":616, "-": "null", "jawa timur":623, "dki jaya":619, "nusa tenggara barat":633,
            "sumatera selatan":644, "dki jakarta raya":619, "d.i yogyakarta":646, "testing": "null", 
            "Jakarta Timur":619, "sumatera utara":645, "sumatera barat":643, "bali":614, "sulawesi barat":638,
            "": "null", "kalimantan selatan":625, "riau":637, "sulawesi selatan":639, "kalimantan narat":624,
            "bangka belitung":615, "asd":"null", "sumbar":643, "jakarta selatan":619, "jatim":623, "jambi":620, 
            "aceh":613, "sul-sel":639, "papua":635, "ntb":633, "sumatra barat":643, "kalimantan":626, 
            "jawabarat":621, "tamalate":639, "jogjakarta":646, "dki":619, "sumut":645, "sumatera utara":645, 
            "bekasi":621, "dukasaha":"null", "jakarta utara":619, "jaakarta":619, "bandung":621, "diy":646, 
            "papua barat":636, "kota bandung":621,"sumatera selatan":644, "sulawesi tengah":640, 
            "jakarta pusat":619, "maluku utara":632, "central java":622, "surabaya":623, "sulawes barat":638, 
            "di. yogyakarta":646, "jakbar":619, "Jawa":619, "tes":"null", "sultra":641, "sulsel":639, "maluku":631, 
            "jabar":621, "indonesia":619, "jawatimur":623,"west jakarta":619, "jawa bara":621, "south sulawesi":639, 
            "d.k.i. jakarta":619, "singapore":"null","china":"null", "jawattimur":623, "sumberjaya":621, 
            "d.i. yogyakarta":646, "ntt":634, "jakarta dki":619,"nusa tenggara timur":634, "kaltim":627, 
            "kalimantan":625, "daerah istimewa yogy":646, "provinsi":621,"seoul":"null", "banyumas":622, 
            "jk":619, "sumaterabarat":643, "jakut":619, "chase":"null", "daerah istimewa yogyakarta":646, 
            "kepri":629, "cengkareng":619, "kalsel":625, "provinsi 1":"null", "d i yogyakarta":646, 
            "sumtera utara":645, "jateng":622, "maluku":631, "korea":"", "greater jakarta":619, "east java":623, 
            "daerah khusus ibukot":619, "kalimantan selatan":625, "timor leste":"null", "jakarta raya":619, 
            "asahan":645, "pluit":619, "jayapura":635, "jawa tegah":622, "13430":619, "bengkulu":617, 
            "jawa tengan":622, "jawa barat": 621, "jakarta barat": 619, "jawa tengah":622, "sulawesi utara": 642,
            "di yogyakarta": 646, "jakarta timur": 619, "sumatera barat":643, "kalimantan barat": 624, 
            "kepulauan riau": 629, "yogyakarta": 646, "kalimantan timur": 627, "dki jakarta ": 619, 
            "jawa barat ": 621, "kalimantan tengah": 626, "sumut": 642, "sumatra utara": 645, "jawa timur ": 623,
            "sumatra selatan": 644, "nusa tenggara barat ": 633, "jawa":619, "sumberjaya ":619, " ": "null",
            "jakarta ": 619, "jawa tengah ":622, "mqluku": 631, "korea ": "null", "banten ": 616, 
            "kalimantanselatan": 625, "jakarta barat ": 619, 
        }
        entry.append(province_dict[row[14].lower()])

        # (row[15]) postal_1 -> zip
        entry.append(row[15])

        # (row[16]) country_1 -> country_id
        entry.append(100) if row[16] == "ID" else entry.append("null")

        '''Filtering contact attributes''' 
        # (row[0]) SKIP
        
        # (row[1]) organization -> company_id
        contact_entry.append(2) if row[1] == 'VIS' else contact_entry.append(3)

        # (row[2]) client_key -> name
        contact_entry.append(row[9])
            
        # (row[3]) client_name -> display_name
        contact_entry.append(row[9])

        # (row[4]) category -> is_company (True)
        contact_entry.append('true')

        # (row[5]) SKIP (row[6]) SKIP

        # (row[7]) phone -> phone
        contact_entry.append(row[7])

        # (row[8]) SKIP (row[9]) up_name (row[10]) (row[11]) SKIP
        
        # (row[12]) street_1 -> street
        contact_entry.append(row[12])

        # (row[13]) city_1 -> city
        contact_entry.append(row[13])

        # (row[14]) province_1 -> state_id
        contact_entry.append(province_dict[row[14].lower()])

        # (row[15]) postal_1 -> zip
        contact_entry.append(row[15])

        # (row[16]) country_1 -> country_id
        contact_entry.append(100) if row[16] == "ID" else contact_entry.append("null")

        full_entry.append(entry)
        full_entry.append(contact_entry)

    for i in range(len(full_entry)):
        item = full_entry[i]
        time = str(datetime.datetime.now()).split(".")[0]
        
        if i % 2 == 0: 
            query = "insert into res_partner (company_id, name, display_name, commercial_company_name, \
                is_company, phone, street, city, state_id, zip, country_id, active, customer, supplier, \
                employee, \"type\", color, partner_share, message_bounce, invoice_warn, picking_warn, \
                sale_warn, create_date, create_uid, write_uid, lang) values ({}, '{}', '{}', '{}', {}, '{}', \
                '{}', '{}', {}, '{}', {}, true, true, false, false, 'contact', 0, true, 0, 'no-message', \
                'no-message', 'no-message', '{}', 2, 2, 'en_US');".format(item[0], item[1], item[2], item[2], 
                item[3], item[4], item[5], item[6], item[7], item[8], item[9], time)
            destination.execute(query)

            new_partner_id = destination.execute_select("select max(id) from res_partner")[0][0]

            update_query = "update ob_business_partner set id_odoo = {} where client_key = '{}'".format(
                new_partner_id, item[1])
            db.execute_update(update_query)

            ## UPDATE STATUS QUERY
            # update_status_query = "update ob_business_partner set status = {} where client_key = '{}'".format(
            #     'S', item[1])
            # db.execute_update(update_status_query)

            ## UPDATE MIGRATION QUERY
            # update_migration_query = "update ob_business_partner set isMigrated = {} where client_key = '{}'".format(
            #     '1', item[1])
            # db.execute_update(update_migration_query)

            update_commercial_query = "update res_partner set commercial_partner_id = {} where id = {}".format(
            new_partner_id, new_partner_id)
            destination.execute(update_commercial_query)
        else:
            new_partner_id = destination.execute_select("select max(id) from res_partner")[0][0]
            query = "insert into res_partner (company_id, name, display_name, commercial_company_name, \
                is_company, phone, street, city, state_id, zip, country_id, active, customer, supplier, \
                employee, \"type\", color, partner_share, message_bounce, invoice_warn, picking_warn, sale_warn, \
                parent_id, create_date, create_uid, write_uid, lang) values ({}, '{}', '{}', '{}', {}, '{}', '{}', \
                '{}', {}, '{}', {}, true, true, false, false, 'contact', 0, true, 0, 'no-message', 'no-message', \
                'no-message', {}, '{}', 2, 2, 'en_US');".format(item[0], item[1], item[2], item[2], item[3], 
                item[4], item[5], item[6], item[7], item[8], item[9], new_partner_id, time)
            destination.execute(query)
            
            current_partner_id = destination.execute_select("select max(id) from res_partner")[0][0]
            update_commercial_query = "update res_partner set commercial_partner_id = {} where id = {}".format(
            new_partner_id, current_partner_id)
            destination.execute(update_commercial_query)

if __name__ == '__main__':
    main()