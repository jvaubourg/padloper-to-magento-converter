import mysql.connector
import re

mySQLconnection = mysql.connector.connect(host='127.0.0.1', database='processwire', user='processwire', password='processwire')

def q(query):
  cursor = mySQLconnection.cursor()
  cursor.execute(query)
  records = cursor.fetchall()
  return records

def q1(query):
  cursor = mySQLconnection.cursor()
  cursor.execute(query)
  record = cursor.fetchone()
  return record

def getProduct(product_id, variation_id, product_type):
  is_simple = (product_type == 'simple')
  is_configurable = (product_type == 'configurable')
  is_virtual = (product_type == 'virtual')

  p = {}

  # sku
  p['sku'] = 'PW-' + str(product_id if is_configurable else variation_id)

  # store_view_code
  p['store_view_code'] = ''

  # attribute_set_code
  p['attribute_set_code'] = 'Default'

  # product_type
  p['product_type'] = product_type

  # categories
  p['categories'] = "Nutrition & Alimentation certifiée Bio - Natürlich Pferd"

  # product_websites
  p['product_websites'] = 'base'
  
  # name
  r = q1("select data from field_title where pages_id=%d limit 1" % (product_id if is_configurable else variation_id))
  title = re.sub(r'^[0-9]+\s+', '', r[0])
  p['name'] = title

  # description
  if is_virtual:
    p['description'] = ''
  else:
    r = q1("select data from field_body where pages_id=%d limit 1" % product_id)
    p['description'] = '<div class="text">' + r[0] + '</div>'

  # short_description
  if is_virtual:
    p['short_description'] = ''
  else:
    r = q1("select data from field_product_description where pages_id=%d limit 1" % product_id)
    p['short_description'] = r[0]

  # weight
  extracted_weight = re.search(r'\b([0-9]+[.,]?[0-9]*)\s*(mg|g|gr|kg)\b', title.lower())
  weight = 0.0 # in g
  if extracted_weight:
    extracted_unit = extracted_weight.group(2)
    extracted_weight = float(extracted_weight.group(1))
    if extracted_unit == 'mg':
      weight = extracted_weight / 1000
    elif extracted_unit == 'gr' or extracted_unit == 'g':
      weight = extracted_weight
    elif extracted_unit == 'kg':
      weight = extracted_weight * 1000
  p['weight'] = weight

  # product_online
  p['product_online'] = 1

  # tax_class_name
  p['tax_class_name'] = 'Taxable Goods'
  
  # visibility
  if is_virtual:
    p['visibility'] = 'Not Visible Individually'
  else:
    p['visibility'] = 'Catalog, Search'

  # price
  r = q1("select data from field_product_price where pages_id=%d limit 1" % product_id)
  price = r[0]
  if not is_configurable:
    r = q1("select data from field_product_price where pages_id=%d limit 1" % variation_id)
    price = r[0] if (r and r[0] != 0) else price
  p['price'] = price

  # url_key
  r = q1("select name from pages where id=%d limit 1" % product_id)
  url = r[0]
  if not is_configurable:
    r = q1("select name from pages where id=%d limit 1" % variation_id)
    url = url + r[0]
  p['url_key'] = url

  # meta_title
  r = q1("select data from field_meta_description where pages_id=%d limit 1" % (product_id if is_configurable else variation_id))
  meta = r[0]
  p['meta_title'] = meta

  # meta_keywords
  p['meta_keywords'] = meta

  # meta_description
  p['meta_description'] = meta

  # base_image
  images = []
  r = q1("select data from field_image where pages_id=%d limit 1" % product_id)
  img_name = '/%c/%c/%s' % (r[0][0], r[0][1], r[0])
  images.append(img_name)
  p['base_image'] = img_name

  # base_image_label
  p['base_image_label'] = '' # field_image/description

  # small_image
  p['small_image'] = img_name
 
  # small_image_label
  p['small_image_label'] = ''

  # thumbnail_image
  p['thumbnail_image'] = img_name

  # thumbnail_image_label
  p['thumbnail_image_label'] = ''

  # swatch_image
  p['swatch_image'] = img_name

  # swatch_image_label
  p['swatch_image_label'] = ''

  # created_at
  r = q1("select date_format(created, '%%d/%%m/%%Y %%H:%%i') from pages where id=%d limit 1" % (product_id if is_configurable else variation_id))
  p['created_at'] = r[0]

  # updated_at
  r = q1("select date_format(modified, '%%d/%%m/%%Y %%H:%%i') from pages where id=%d limit 1" % (product_id if is_configurable else variation_id))
  p['updated_at'] = r[0]

  # additional_images
  for r in q("select data from field_images where pages_id=%d order by sort" % product_id):
    img_name = '/%c/%c/%s' % (r[0][0], r[0][1], r[0])
    images.append(img_name)
  p['additional_images'] = ','.join(images)

  # display_product_options_in
  p['display_product_options_in'] = 'Block after Info Column'

  return p

def getProductsFromCat(cat_id):
  products = []

  for product in q("select pr.data from field_cat_products pr, pages pa where pr.data=pa.id and pa.status=1 and pr.pages_id=%d order by pr.sort" % cat_id):
    variations = q("select v.data from field_variations v, pages p where v.data=p.id and p.status=1 and v.pages_id=%d order by v.sort" % product[0])
    
    if len(variations) > 1:
      products.append(getProduct(product[0], variations[0][0], 'configurable'))

      for variation in variations:
        products.append(getProduct(product[0], variation[0], 'virtual'))

    elif len(variations) == 1:
      products.append(getProduct(product[0], variations[0][0], 'simple'))

  return products

for p in getProductsFromCat(5684):
  print(p)

mySQLconnection.close()