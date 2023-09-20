import itertools
import requests
import csv
import pyodbc
import html
import json

class Scraper:

    def __init__(self, user, Pass, IP, database):
        self.user = user
        self.Pass = Pass
        self.IP = IP
        self.database = database

    def connect_sql(self):
        self.conn = pyodbc.connect("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format("ODBC Driver 17 for SQL Server", self.IP, self.database, self.user, self.Pass))
        self.cursor = self.conn.cursor()

    def get_existing_purchase_orders(self, table):
        self.cursor.execute('DELETE FROM ###.dbo.'+table)
        self.cursor.execute('SELECT id FROM ###.dbo.'+table)
        self.IDS = [each[0] for each in self.cursor.fetchall()]

    def purchase_orders(self, Bearer, table):
        i = 1
        while True:
            headers = {'Content-type': 'application/json','Authorization': 'Bearer '+Bearer}
            r = requests.get('https://api.tradegecko.com/purchase_orders?limit=250&page='+str(i), headers = headers)
            if r.json()['purchase_orders'] == []:
                break
            for x in r.json()['purchase_orders']:
                try:
                    id_ = x['id']
                except:
                    id_ = ''
                try:
                    created_at = x['created_at']
                except:
                    created_at = ''
                try:
                    updated_at = x['updated_at']
                except:
                    updated_at = ''
                try:
                    billing_address_id = x['billing_address_id']
                except:
                    billing_address_id = ''
                try:
                    company_id = x['company_id']
                except:
                    company_id = ''
                try:
                    currency_id = x['currency_id']
                except:
                    currency_id = ''
                try:
                    stock_location_id = x['stock_location_id']
                except:
                    stock_location_id = ''
                try:
                    supplier_address_id = x['supplier_address_id']
                except:
                    supplier_address_id = ''
                try:
                    default_price_list_id = x['default_price_list_id']
                except:
                    default_price_list_id = ''
                try:
                    document_url = x['document_url']
                except:
                    document_url = ''
                try:
                    due_at = x['due_at']
                except:
                    due_at = ''
                try:
                    issued_at = x['issued_at']
                except:
                    issued_at = ''
                try:
                    email = x['email']
                except:
                    email = ''
                try:
                    notes = x['notes']
                except:
                    notes = ''
                try:
                    order_number = x['order_number']
                except:
                    order_number = ''
                try:
                    payment_due_at = x['payment_due_at']
                except:
                    payment_due_at = ''
                try:
                    procurement_status = x['procurement_status']
                except:
                    procurement_status = ''
                try:
                    reference_number = x['reference_number']
                except:
                    reference_number = ''
                try:
                    status = x['status']
                except:
                    status = ''
                try:
                    tax_treatment = x['tax_treatment']
                except:
                    tax_treatment = ''
                try:
                    received_at = x['received_at']
                except:
                    received_at = ''
                try:
                    total = x['total']
                except:
                    total = ''
                try:
                    tags = ", ".join([str(each) for each in x['tags']])
                except:
                    tags = ''
                try:
                    source_id = x['source_id']
                except:
                    source_id = ''
                try:
                    cached_quantity = x['cached_quantity']
                except:
                    cached_quantity = ''
                try:
                    procurement_ids = ", ".join([str(each) for each in x['procurement_ids']])
                except:
                    procurement_ids = ''
                try:
                    purchase_order_line_item_ids = ", ".join([str(each) for each in x['purchase_order_line_item_ids']])
                except:
                    purchase_order_line_item_ids = ''
                try:
                    default_price_type_id = x['default_price_type_id']
                except:
                    default_price_type_id = ''
                try:
                    tax_type = x['tax_type']
                except:
                    tax_type = ''
                if str(id_) not in self.IDS:
                    self.cursor.execute("""
                                    INSERT INTO ###.dbo.{} (id, created_at, updated_at, billing_address_id, company_id, currency_id, stock_location_id, supplier_address_id, default_price_list_id,
                                    document_url, due_at, issued_at, email,
                                    notes, order_number, payment_due_at, procurement_status, reference_number, status, tax_treatment, received_at, total, tags, source_id, cached_quantity,
                                    procurement_ids, purchase_order_line_item_ids, default_price_type_id, tax_type)
                                    VALUES
                                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """.format(table), id_, created_at, updated_at, billing_address_id, company_id, currency_id, stock_location_id, supplier_address_id, default_price_list_id,
                                    document_url, due_at, issued_at, email,
                                    notes, order_number, payment_due_at, procurement_status, reference_number, status, tax_treatment, received_at, total, tags, source_id, cached_quantity,
                                    procurement_ids, purchase_order_line_item_ids, default_price_type_id, tax_type)
            self.cursor.commit()
            i+=1

    def get_existing_purchase_order_line_items(self, table):
        self.cursor.execute('DELETE FROM ###.dbo.{}'.format(table))
        self.cursor.execute('SELECT purchase_order_id FROM ###.dbo.{}'.format(table))
        self.Line_item_IDS = [each[0] for each in self.cursor.fetchall()]

    def purchase_order_line_items(self, Bearer, table):
        i = 1
        while True:
            headers = {'Content-type': 'application/json','Authorization': 'Bearer '+Bearer}
            r = requests.get('https://api.tradegecko.com/purchase_order_line_items?limit=250&page='+str(i), headers = headers)
            if r.json()['purchase_order_line_items'] == []:
                break
            for x in r.json()['purchase_order_line_items']:
                try:
                    id_ = x['id']
                except:
                    id_ = ''
                try:
                    created_at = x['created_at']
                except:
                    created_at = ''
                try:
                    updated_at = x['updated_at']
                except:
                    updated_at = ''
                try:
                    procurement_id = x['procurement_id']
                except:
                    procurement_id = ''
                try:
                    purchase_order_id = x['purchase_order_id']
                except:
                    purchase_order_id = ''
                try:
                    tax_type_id = x['tax_type_id']
                except:
                    tax_type_id = ''
                try:
                    variant_id = x['variant_id']
                except:
                    variant_id = ''
                try:
                    base_price = x['base_price']
                except:
                    base_price = ''
                try:
                    freeform = x['freeform']
                except:
                    freeform = ''
                try:
                    image_url = x['image_url']
                except:
                    image_url = ''
                try:
                    label = x['label']
                except:
                    label = ''
                try:
                    position = x['position']
                except:
                    position = ''
                try:
                    price = x['price']
                except:
                    price = ''
                try:
                    quantity = x['quantity']
                except:
                    quantity = ''
                try:
                    tax_rate_override = x['tax_rate_override']
                except:
                    tax_rate_override = ''
                try:
                    extra_cost_value = x['extra_cost_value']
                except:
                    extra_cost_value = ''
                try:
                    tax_rate = x['tax_rate']
                except:
                    tax_rate = ''
                if str(purchase_order_id) not in self.Line_item_IDS:
                    self.cursor.execute("""
                                    INSERT INTO ###.dbo.{} (id, created_at, updated_at, procurement_id, purchase_order_id, tax_type_id, variant_id, base_price, freeform,
                                    image_url, label, position, price,
                                    quantity, tax_rate_override, extra_cost_value, tax_rate)
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".format(table), id_, created_at, updated_at, procurement_id, purchase_order_id, tax_type_id, variant_id, base_price, freeform,
                                    image_url, label, position, price,
                                    quantity, tax_rate_override, extra_cost_value, tax_rate)
            self.cursor.commit()
            i+=1

    def get_existing_variants(self, table):
        self.cursor.execute('DELETE FROM ###.dbo.{}'.format(table))
        self.cursor.execute('SELECT id FROM ###.dbo.{}'.format(table))
        self.variants_IDS = [each[0] for each in self.cursor.fetchall()]

    def variants(self, Bearer, table):
        i = 1
        while True:
            headers = {'Content-type': 'application/json','Authorization': 'Bearer '+Bearer}
            r = requests.get('https://api.tradegecko.com/variants?limit=250&page='+str(i), headers = headers)
            try:
                if r.json()['variants'] == []:
                    break
            except:
                print('Varient Not Found ERROR')
                break
            for x in r.json()['variants']:
                try:
                    id_ = x['id']
                except:
                    id_ = ''
                try:
                    created_at = x['created_at']
                except:
                    created_at = ''
                try:
                    updated_at = x['updated_at']
                except:
                    updated_at = ''
                try:
                    product_id = x['product_id']
                except:
                    product_id = ''
                try:
                    default_ledger_account_id = x['default_ledger_account_id']
                except:
                    default_ledger_account_id = ''
                try:
                    default_cost_of_goods_ledger_id = x['default_cost_of_goods_ledger_id']
                except:
                    default_cost_of_goods_ledger_id = ''
                try:
                    buy_price = x['buy_price']
                except:
                    buy_price = ''
                try:
                    committed_stock = x['committed_stock']
                except:
                    committed_stock = ''
                try:
                    available_stock = x['available_stock']
                except:
                    available_stock = ''
                try:
                    production_committed_stock = x['production_committed_stock']
                except:
                    production_committed_stock = ''
                try:
                    incoming_stock = x['incoming_stock']
                except:
                    incoming_stock = ''
                try:
                    production_incoming_stock = x['production_incoming_stock']
                except:
                    production_incoming_stock = ''
                try:
                    composite = x['composite']
                except:
                    composite = ''
                try:
                    packsize = x['packsize']
                except:
                    packsize = ''
                try:
                    raw_material = x['raw_material']
                except:
                    raw_material = ''
                try:
                    description = x['description']
                except:
                    description = ''
                try:
                    shipping_description = x['shipping_description']
                except:
                    shipping_description = ''
                try:
                    is_online = x['is_online']
                except:
                    is_online = ''
                try:
                    keep_selling = x['keep_selling']
                except:
                    keep_selling = ''
                try:
                    keep_selling_b2b = x['keep_selling_b2b']
                except:
                    keep_selling_b2b = ''
                try:
                    last_cost_price = x['last_cost_price']
                except:
                    last_cost_price = ''
                try:
                    manage_stock = x['manage_stock']
                except:
                    manage_stock = ''
                try:
                    max_online = x['max_online']
                except:
                    max_online = ''
                try:
                    moving_average_cost = x['moving_average_cost']
                except:
                    moving_average_cost = ''
                try:
                    name = x['name']
                except:
                    name = ''
                try:
                    online_ordering = x['online_ordering']
                except:
                    online_ordering = ''
                try:
                    opt1 = x['opt1']
                except:
                    opt1 = ''
                try:
                    opt2 = x['opt2']
                except:
                    opt2 = ''
                try:
                    opt3 = x['opt3']
                except:
                    opt3 = ''
                try:
                    position = x['position']
                except:
                    position = ''
                try:
                    product_name = x['product_name']
                except:
                    product_name = ''
                try:
                    product_status = x['product_status']
                except:
                    product_status = ''
                try:
                    product_type = x['product_type']
                except:
                    product_type = ''
                try:
                    purchasable = x['purchasable']
                except:
                    purchasable = ''
                try:
                    manufacturable = x['manufacturable']
                except:
                    manufacturable = ''
                try:
                    retail_price = x['retail_price']
                except:
                    retail_price = ''
                try:
                    sellable = x['sellable']
                except:
                    sellable = ''
                try:
                    sku = x['sku']
                except:
                    sku = ''
                try:
                    status = x['status']
                except:
                    status = ''
                try:
                    stock_on_hand = x['stock_on_hand']
                except:
                    stock_on_hand = ''
                try:
                    supplier_code = x['supplier_code']
                except:
                    supplier_code = ''
                try:
                    taxable = x['taxable']
                except:
                    taxable = ''
                try:
                    upc = x['upc']
                except:
                    upc = ''
                try:
                    weight = x['weight']
                except:
                    weight = ''
                try:
                    weight_unit = x['weight_unit']
                except:
                    weight_unit = ''
                try:
                    weight_value = x['weight_value']
                except:
                    weight_value = ''
                try:
                    wholesale_price = x['wholesale_price']
                except:
                    wholesale_price = ''
                try:
                    volume = x['volume']
                except:
                    volume = ''
                try:
                    volume_unit = x['volume_unit']
                except:
                    volume_unit = ''
                try:
                    moq = x['moq']
                except:
                    moq = ''
                try:
                    stock_batchable = x['stock_batchable']
                except:
                    stock_batchable = ''
                try:
                    country_of_origin = x['country_of_origin']
                except:
                    country_of_origin = ''
                try:
                    hs_code = x['hs_code']
                except:
                    hs_code = ''
                try:
                    initial_cost_price_confirmed = x['initial_cost_price_confirmed']
                except:
                    initial_cost_price_confirmed = ''
                try:
                    total_committed_stock = x['total_committed_stock']
                except:
                    total_committed_stock = ''
                try:
                    total_incoming_stock = x['total_incoming_stock']
                except:
                    total_incoming_stock = ''
                try:
                    current_version_variant_id = x['current_version_variant_id']
                except:
                    current_version_variant_id = ''
                try:
                    image_ids = ", ".join(x['id'])
                except:
                    image_ids = ''
                if str(id_) not in self.variants_IDS:
                    self.cursor.execute("""
                                    INSERT INTO ###.dbo.{} (id, created_at, updated_at, product_id, default_ledger_account_id, default_cost_of_goods_ledger_id, buy_price, committed_stock, available_stock,
                                    production_committed_stock, incoming_stock, production_incoming_stock, composite, packsize, raw_material, description, shipping_description, is_online, keep_selling, keep_selling_b2b,
                                    last_cost_price, manage_stock, max_online, moving_average_cost, name, online_ordering, opt1, opt2, opt3, position, product_name, product_status, product_type, purchasable, manufacturable,
                                    retail_price, sellable, sku, status, stock_on_hand, supplier_code, taxable, upc, weight, weight_unit, weight_value, wholesale_price, volume, volume_unit, moq, stock_batchable, country_of_origin,
                                    hs_code, initial_cost_price_confirmed, total_committed_stock, total_incoming_stock, current_version_variant_id, image_ids)
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""".format(table),
                                        id_, created_at, updated_at, product_id, default_ledger_account_id, default_cost_of_goods_ledger_id, buy_price, committed_stock, available_stock,
                                    production_committed_stock, incoming_stock, production_incoming_stock, composite, packsize, raw_material, description, shipping_description, is_online, keep_selling, keep_selling_b2b,
                                    last_cost_price, manage_stock, max_online, moving_average_cost, name, online_ordering, opt1, opt2, opt3, position, product_name, product_status, product_type, purchasable, manufacturable,
                                    retail_price, sellable, sku, status, stock_on_hand, supplier_code, taxable, upc, weight, weight_unit, weight_value, wholesale_price, volume, volume_unit, moq, stock_batchable, country_of_origin,
                                    hs_code, initial_cost_price_confirmed, total_committed_stock, total_incoming_stock, current_version_variant_id, image_ids)
            self.cursor.commit()
            i+=1

    def get_existing_locations(self, table):
        self.cursor.execute('DELETE FROM ###.dbo.'+table)
        self.cursor.execute('SELECT id FROM ###.dbo.'+table)
        self.locations_ids = [each[0] for each in self.cursor.fetchall()]

    def locations(self, Bearer, table):
        i = 1
        while True:
            headers = {'Content-type': 'application/json','Authorization': 'Bearer '+Bearer}
            r = requests.get('https://api.tradegecko.com/locations?limit=250&page='+str(i), headers = headers)
            if r.json()['locations'] == []:
                break
            for x in r.json()['locations']:
                try:
                    id_ = x['id']
                except:
                    id_ = ''
                try:
                    created_at = x['created_at']
                except:
                    created_at = ''
                try:
                    updated_at = x['updated_at']
                except:
                    updated_at = ''
                try:
                    address1 = x['address1']
                except:
                    address1 = ''
                try:
                    address2 = x['address2']
                except:
                    address2 = ''
                try:
                    city = x['city']
                except:
                    city = ''
                try:
                    country = x['country']
                except:
                    country = ''
                try:
                    holds_stock = x['holds_stock']
                except:
                    holds_stock = ''
                try:
                    label = x['label']
                except:
                    label = ''
                try:
                    state = x['state']
                except:
                    state = ''
                try:
                    status = x['status']
                except:
                    status = ''
                try:
                    suburb = x['suburb']
                except:
                    suburb = ''
                try:
                    zip_code = x['zip_code']
                except:
                    zip_code = ''
                try:
                    virtual = x['virtual']
                except:
                    virtual = ''
                if str(id_) not in self.locations_ids:
                    self.cursor.execute("""
                                    INSERT INTO ###.dbo.{} (id, created_at, updated_at, address1, address2, city, country, holds_stock, label,
                                    state, status, suburb, zip_code, virtual)
                                    VALUES
                                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """.format(table), id_, created_at, updated_at, address1, address2, city, country, holds_stock, label,
                                    state, status, suburb, zip_code, virtual)
            self.cursor.commit()
            i+=1

                    
pupnaps = 'QkBbNRKhX_-7Kgpe3tqx8Qf2kVs7I6A4xt8q-lGULdk'
swished = 'rsMe3RO_ogowvq3Y059jEcH_fRorcajONDLCuws6c30'
calming_blankets = 'AeEHkD13tSzaGrzYS01YIuNIICz6W2Z7umpDSGJ3EpQ'
the_Oodie = 'KMSAa_PLZZn1p_Sn7myw-6b_TQcdo0-pVEc14RaQyak'   

if __name__ == '__main__':

    print("TG-Oodie")
    d = Scraper(user='###', Pass='######', IP='###', database='###')
    d.connect_sql()
    
    d.get_existing_purchase_orders('the_Oodie_purchase_orders_tradegecko')
    d.purchase_orders(the_Oodie, 'the_Oodie_purchase_orders_tradegecko')
    
    d.get_existing_purchase_order_line_items('the_Oodie_purchase_order_line_items_tradegecko')
    d.purchase_order_line_items(the_Oodie, 'the_Oodie_purchase_order_line_items_tradegecko')
    
    d.get_existing_variants('the_Oodie_variants_tradegecko')
    d.variants(the_Oodie, 'the_Oodie_variants_tradegecko')

    d.get_existing_locations('the_Oodie_Locations')
    d.locations(the_Oodie, 'the_Oodie_Locations')

#---------------------------------------------------------------------------------------------------------------------

    print("TG-Pupnaps")
    a = Scraper(user='###', Pass='######', IP='###', database='###')
    a.connect_sql()
    
    a.get_existing_purchase_orders('pupnaps_purchase_orders_tradegecko')
    a.purchase_orders(pupnaps, 'pupnaps_purchase_orders_tradegecko')
    
    a.get_existing_purchase_order_line_items('pupnaps_purchase_order_line_items_tradegecko')
    a.purchase_order_line_items(pupnaps, 'pupnaps_purchase_order_line_items_tradegecko')
    
    a.get_existing_variants('pupnaps_variants_tradegecko')
    a.variants(pupnaps, 'pupnaps_variants_tradegecko')

    a.get_existing_locations('pupnaps_Locations')
    a.locations(pupnaps, 'pupnaps_Locations')
    
    #---------------------------------------------------------------------------------------------------------------------
    
    print("TG-Swished")
    b = Scraper(user='###', Pass='######', IP='###', database='###')
    b.connect_sql()
    
    b.get_existing_purchase_orders('swished_purchase_orders_tradegecko')
    b.purchase_orders(swished, 'swished_purchase_orders_tradegecko')
    
    b.get_existing_purchase_order_line_items('swished_purchase_order_line_items_tradegecko')
    b.purchase_order_line_items(swished, 'swished_purchase_order_line_items_tradegecko')
    
    b.get_existing_variants('swished_variants_tradegecko')
    b.variants(swished, 'swished_variants_tradegecko')

    b.get_existing_locations('swished_Locations')
    b.locations(swished, 'swished_Locations')
    
    #---------------------------------------------------------------------------------------------------------------------
    
    print("TG-CB")
    c = Scraper(user='###', Pass='######', IP='###', database='###')
    c.connect_sql()
    
    c.get_existing_purchase_orders('calming_blankets_purchase_orders_tradegecko')
    c.purchase_orders(calming_blankets, 'calming_blankets_purchase_orders_tradegecko')
    
    c.get_existing_purchase_order_line_items('calming_blankets_purchase_order_line_items_tradegecko')
    c.purchase_order_line_items(calming_blankets, 'calming_blankets_purchase_order_line_items_tradegecko')
    
    c.get_existing_variants('calming_blankets_variants_tradegecko')
    c.variants(calming_blankets, 'calming_blankets_variants_tradegecko')

    c.get_existing_locations('calming_blankets_Locations')
    c.locations(calming_blankets, 'calming_blankets_Locations')
    
    #---------------------------------------------------------------------------------------------------------------------
    

    
