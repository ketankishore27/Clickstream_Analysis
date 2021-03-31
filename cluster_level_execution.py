# -*- coding: utf-8 -*-
"""
Created on Thu Jul 30 09:24:03 2020

@author: ketankishore
"""


from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql.functions import when, udf
from pyspark.sql.types import *
from pyspark.sql.window import Window
from functions_used import *
import pyspark.sql.functions as F

conf = SparkConf() \
       .setMaster("local[*]") \
       .setAppName("Adobe Clickstream Analytics")

sc = SparkContext(conf = conf).getOrCreate()
spark = SparkSession(sparkContext=sc)
#SparkContext.clearCache()

data_clickstream = spark.read.parquet('Raw_Data6.parquet')
data_clickstream = data_clickstream[~(data_clickstream['mcvisid'].isNull() | (data_clickstream['mcvisid'] == 0).cast(BooleanType()))]

url_check_udf = udf(check_mobile_only)
mobile_no_decode = udf(number_decode)

data_clickstream = data_clickstream.withColumn('parsed_mobile', when(data_clickstream['page_url'].isNotNull(), url_check_udf(data_clickstream['page_url'])))
data_clickstream = data_clickstream.withColumn('parsed_mobile_number', when(data_clickstream['mobile_no'].isNotNull(), mobile_no_decode(data_clickstream['mobile_no']))
                                                                      .when(data_clickstream['parsed_mobile'].isNotNull(), data_clickstream['parsed_mobile'])
                                                                      .when(data_clickstream['mobile_no_non_otp'].isNotNull(), mobile_no_decode(data_clickstream['mobile_no_non_otp']))
                                                                      .when(data_clickstream['app_mobile_number'].isNotNull(), mobile_no_decode(data_clickstream['app_mobile_number'])))

data_clickstream = data_clickstream.withColumn('spa_url_parse', when(data_clickstream['post_pagename'].isNotNull() & 
                                                                     F.split(data_clickstream['post_pagename'], ':')[0].contains('spa') & 
                                                                     (F.size(F.split(data_clickstream['post_pagename'], ':')) > 1), 
                                                                     F.split(data_clickstream['post_pagename'], ':')[1])
                                               
                                                               .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNotNull() &
                                                                     F.split(data_clickstream['click_context'], ':')[0].contains('spa') & 
                                                                     (F.size(F.split(data_clickstream['click_context'], ':')) > 1), 
                                                                     F.split(data_clickstream['click_context'], ':')[1])
                                               
                                                               .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNull() & 
                                                                     data_clickstream['first_hit_pagename'].isNotNull() & 
                                                                     F.split(data_clickstream['first_hit_pagename'], ':')[0].contains('spa') & 
                                                                     (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1), 
                                                                     F.split(data_clickstream['first_hit_pagename'], ':')[1])
                                              )                                              

data_clickstream = data_clickstream.withColumn('es_url_parse', when(data_clickstream['post_pagename'].isNotNull() & 
                                                                    F.split(data_clickstream['post_pagename'], ':')[0].contains('es') & 
                                                                    (F.size(F.split(data_clickstream['post_pagename'], ':')) > 1), 
                                                                    F.split(data_clickstream['post_pagename'], ':')[1])
                                               
                                                              .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNotNull() &
                                                                    F.split(data_clickstream['click_context'], ':')[0].contains('es') & 
                                                                    (F.size(F.split(data_clickstream['click_context'], ':')) > 1), 
                                                                    F.split(data_clickstream['click_context'], ':')[1])
                                               
                                                              .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNull() & 
                                                                    data_clickstream['first_hit_pagename'].isNotNull() & 
                                                                    F.split(data_clickstream['first_hit_pagename'], ':')[0].contains('es') &
                                                                    (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1), 
                                                                    F.split(data_clickstream['first_hit_pagename'], ':')[1])
                                              )

data_clickstream = data_clickstream.withColumn('insurance_url_parse', when(data_clickstream['post_pagename'].isNotNull() & 
                                                                           F.split(data_clickstream['post_pagename'], ':')[0].contains('insurance') & 
                                                                           ((F.size(F.split(data_clickstream['post_pagename'], ':')) < 4) &
                                                                           (F.size(F.split(data_clickstream['post_pagename'], ':')) > 1)
                                                                           ), 
                                                                           F.split(data_clickstream['post_pagename'], ':')[F.size(F.split(data_clickstream['post_pagename'], ':')) - 1])

                                                                     .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNotNull() &
                                                                           F.split(data_clickstream['click_context'], ':')[0].contains('insurance') & 
                                                                           ((F.size(F.split(data_clickstream['click_context'], ':')) < 4) &
                                                                           (F.size(F.split(data_clickstream['click_context'], ':')) > 1)
                                                                           ), 
                                                                           F.split(data_clickstream['click_context'], ':')[F.size(F.split(data_clickstream['click_context'], ':')) - 1])

                                                                     .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNull() & 
                                                                           data_clickstream['first_hit_pagename'].isNotNull() & 
                                                                           F.split(data_clickstream['first_hit_pagename'], ':')[0].contains('insurance') & 
                                                                           ((F.size(F.split(data_clickstream['first_hit_pagename'], ':')) < 4) &
                                                                           (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1)
                                                                           ), 
                                                                           F.split(data_clickstream['first_hit_pagename'], ':')[F.size(F.split(data_clickstream['first_hit_pagename'], ':')) - 1])
                                              )

data_clickstream = data_clickstream.withColumn('loan_url_parse', when(data_clickstream['post_pagename'].isNotNull() & 
                                                                       F.split(data_clickstream['post_pagename'], ':')[0].contains('loans') & 
                                                                       ((F.size(F.split(data_clickstream['post_pagename'], ':')) < 4) &
                                                                       (F.size(F.split(data_clickstream['post_pagename'], ':')) > 1)
                                                                       ), 
                                                                       F.split(data_clickstream['post_pagename'], ':')[F.size(F.split(data_clickstream['post_pagename'], ':')) - 1])

                                                                .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNotNull() &
                                                                       F.split(data_clickstream['click_context'], ':')[0].contains('loans') & 
                                                                       ((F.size(F.split(data_clickstream['click_context'], ':')) < 4) &
                                                                       (F.size(F.split(data_clickstream['click_context'], ':')) > 1)
                                                                       ), 
                                                                       F.split(data_clickstream['click_context'], ':')[F.size(F.split(data_clickstream['click_context'], ':')) - 1])

                                                                .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNull() &
                                                                      data_clickstream['first_hit_pagename'].isNotNull() & 
                                                                       F.split(data_clickstream['first_hit_pagename'], ':')[0].contains('loans') & 
                                                                       ((F.size(F.split(data_clickstream['first_hit_pagename'], ':')) < 4) &
                                                                       (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1)
                                                                       ), 
                                                                       F.split(data_clickstream['first_hit_pagename'], ':')[F.size(F.split(data_clickstream['first_hit_pagename'], ':')) - 1])
                                       
                                              )

# data_clickstream = data_clickstream.withColumn('legal_and_compliance_parse', when(data_clickstream['post_pagename'].isNotNull() & 
#                                                                                   F.split(data_clickstream['post_pagename'], ':')[0].contains('legal-and-compliance') & 
#                                                                                   (F.size(F.split(data_clickstream['post_pagename'], ':')) > 1), 
#                                                                                   F.split(data_clickstream['post_pagename'], ':')[1])
                                               
#                                                                             .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNotNull() &
#                                                                                   F.split(data_clickstream['click_context'], ':')[0].contains('legal-and-compliance') & 
#                                                                                   (F.size(F.split(data_clickstream['click_context'], ':')) > 1), 
#                                                                                   F.split(data_clickstream['click_context'], ':')[1])
                                               
#                                                                             .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNull() &
#                                                                                   data_clickstream['first_hit_pagename'].isNotNull() & 
#                                                                                   F.split(data_clickstream['first_hit_pagename'], ':')[0].contains('legal-and-compliance') & 
#                                                                                   (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1), 
#                                                                                   F.split(data_clickstream['first_hit_pagename'], ':')[1])
#                                               )

# data_clickstream = data_clickstream.withColumn('market_insight_parse', when(data_clickstream['post_pagename'].isNotNull() & 
#                                                                            F.split(data_clickstream['post_pagename'], ':')[0].contains('markets-insights') & 
#                                                                            ((F.size(F.split(data_clickstream['post_pagename'], ':')) < 5) &
#                                                                            (F.size(F.split(data_clickstream['post_pagename'], ':')) > 1)
#                                                                            ), 
#                                                                            F.split(data_clickstream['post_pagename'], ':')[F.size(F.split(data_clickstream['post_pagename'], ':')) - 1])

#                                                                       .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNotNull() &
#                                                                            F.split(data_clickstream['click_context'], ':')[0].contains('markets-insights') & 
#                                                                            ((F.size(F.split(data_clickstream['click_context'], ':')) < 5) &
#                                                                            (F.size(F.split(data_clickstream['click_context'], ':')) > 1)
#                                                                            ), 
#                                                                            F.split(data_clickstream['click_context'], ':')[F.size(F.split(data_clickstream['click_context'], ':')) - 1])

#                                                                       .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNull() &
#                                                                             data_clickstream['first_hit_pagename'].isNotNull() &
#                                                                            F.split(data_clickstream['first_hit_pagename'], ':')[0].contains('markets-insights') & 
#                                                                            ((F.size(F.split(data_clickstream['first_hit_pagename'], ':')) < 5) &
#                                                                            (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1)
#                                                                            ), 
#                                                                            F.split(data_clickstream['first_hit_pagename'], ':')[F.size(F.split(data_clickstream['first_hit_pagename'], ':')) - 1])
#                                               )

# data_clickstream = data_clickstream.withColumn('discover_parse', when(data_clickstream['post_pagename'].isNotNull() & 
#                                                                        F.split(data_clickstream['post_pagename'], ':')[0].contains('discover') & 
#                                                                        ((F.size(F.split(data_clickstream['post_pagename'], ':')) < 5) &
#                                                                        (F.size(F.split(data_clickstream['post_pagename'], ':')) > 1)
#                                                                        ), 
#                                                                        F.split(data_clickstream['post_pagename'], ':')[F.size(F.split(data_clickstream['post_pagename'], ':')) - 1])

#                                                                 .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNotNull() &
#                                                                        F.split(data_clickstream['click_context'], ':')[0].contains('discover') & 
#                                                                        ((F.size(F.split(data_clickstream['click_context'], ':')) < 5) &
#                                                                        (F.size(F.split(data_clickstream['click_context'], ':')) > 1)
#                                                                        ), 
#                                                                        F.split(data_clickstream['click_context'], ':')[F.size(F.split(data_clickstream['click_context'], ':')) - 1])
                                               
#                                                                 .when(data_clickstream['post_pagename'].isNull() & data_clickstream['click_context'].isNull() & 
#                                                                       data_clickstream['first_hit_pagename'].isNotNull() &
#                                                                        F.split(data_clickstream['first_hit_pagename'], ':')[0].contains('discover') & 
#                                                                        ((F.size(F.split(data_clickstream['first_hit_pagename'], ':')) < 5) &
#                                                                        (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1)
#                                                                        ), 
#                                                                        F.split(data_clickstream['first_hit_pagename'], ':')[F.size(F.split(data_clickstream['first_hit_pagename'], ':')) - 1])
#                                               )

data_clickstream = data_clickstream.withColumn('First_Hit_Page', when(data_clickstream['first_hit_pagename'].isNotNull() & 
                                                                     (F.size(F.split(data_clickstream['first_hit_pagename'], ':')) > 1), 
                                                                     F.split(data_clickstream['first_hit_pagename'], ':')[1]))

lookup_product = spark.read.csv('export_all_products_2020_06_12_12_58_12.csv', inferSchema=  True, header = True).toPandas()

lookup_dict = {}

for key in lookup_product.itertuples():
    lookup_dict[str(key[2])] = key[1]
    
br_variable = sc.broadcast(lookup_dict)


check_product_udf = udf(check_product_code)
get_catg_val = udf(lambda x: br_variable.value.get(x))
check_for_numeric = udf(is_numeric, BooleanType())

# data_clickstream = data_clickstream.withColumn('Product_Code', when(data_clickstream['page_url'].isNotNull() & 
#                                                                     data_clickstream['page_url'].contains('prodCode='), check_product_udf(data_clickstream['page_url']))
#                                                               .when(data_clickstream['post_product_list'].isNotNull() &
#                                                                    (F.split(F.split(data_clickstream['post_product_list'], ',')[0], ';')[1] != ''),
#                                                                     F.split(F.split(data_clickstream['post_product_list'], ',')[0], ';')[1])
#                                                               .when(data_clickstream['product_category'].isNotNull() & 
#                                                                     (~(data_clickstream['page_url'].contains('prodCode='))) & data_clickstream['post_product_list'].isNull(), 
#                                                                     data_clickstream['product_category'])) 

# data_clickstream = data_clickstream.withColumn('Product_Code', when(check_for_numeric(data_clickstream['Product_Code']) == True, 
#                                                                      get_catg_val(data_clickstream['Product_Code'])).otherwise(data_clickstream['Product_Code']))

check_search_udf = udf(find_searched_item)
data_clickstream = data_clickstream.withColumn('Searched_Item', when(data_clickstream['page_url'].isNotNull(), check_search_udf(data_clickstream['page_url']))) 

values_to_fill = {
                    'spa_url_parse': '',
                    'es_url_parse': '',
                    'insurance_url_parse': '',
                    'loan_url_parse': '', 
                    # 'legal_and_compliance_parse': '', 
                    # 'market_insight_parse': '', 
                    # 'discover_parse': '',
                    # 'Product_Code': '',
                    'Searched_Item': ''
                }

data_clickstream = data_clickstream.fillna(values_to_fill)

data_clickstream = data_clickstream.withColumn('Date_Field', F.split(data_clickstream['date_time'], ' ')[0])
data_clickstream = data_clickstream.withColumn('Hour_Field', F.split(F.split(data_clickstream['date_time'], ' ')[1], ':')[0])
data_clickstream = data_clickstream.withColumn("time_interval",(data_clickstream['date_time'].cast('timestamp').cast("long") - F.lag(data_clickstream['date_time'].cast('timestamp').cast("long"), +1)
                                    .over(Window.partitionBy(['mcvisid', 'Date_Field', 'Hour_Field']).orderBy("date_time"))).cast("long") / 60).fillna({'time_interval': 0})

data_clickstream = data_clickstream.withColumn('time_interval', F.round(data_clickstream['time_interval'], 2))
data_clickstream = data_clickstream.withColumn("time_difference", F.lag("time_interval", -1, 0).over(Window.partitionBy("mcvisid").orderBy("date_time")))

final_columns = ['mcvisid', 'parsed_mobile_number', 'Final_Product', 'Date_Field', 'Hour_Field',
                 'date_time', 'time_difference']

data_clickstream = data_clickstream.withColumn('Final_Product', F.concat(data_clickstream["spa_url_parse"], data_clickstream["es_url_parse"], 
                                                                   data_clickstream["insurance_url_parse"], data_clickstream["loan_url_parse"], 
                                                                   data_clickstream["Searched_Item"]))[final_columns]

data_clickstream = data_clickstream.withColumn("parsed_mobile_number", F.last('parsed_mobile_number', True).over(Window.partitionBy('mcvisid')
                                                                        .rowsBetween(-F.sys.maxsize, 0)))

#lookup_table = data_clickstream[['mcvisid', 'parsed_mobile_number']].dropna().dropDuplicates(['mcvisid', 'parsed_mobile_number']).withColumnRenamed('parsed_mobile_number', 'Mobile_Number')

#data_clickstream = data_clickstream.join(lookup_table, on=['mcvisid'], how='inner')

# data_clickstream = data_clickstream.groupBy(['mcvisid', 'Mobile_Number', 'Product_Code', 'Final_Product']).agg(F.max(data_clickstream['date_time']).alias('Most_Recent_Timestamp'), 
#                                       F.max(data_clickstream['time_difference']).alias('Max_Time_Spent_on_a_single_visit'), 
#                                       F.count(data_clickstream['Final_Product']).alias('Number_of_Time_Visited')).orderBy('Mobile_Number', ascending = False)

# Can be commented out
# data_clickstream = data_clickstream.groupBy(['mcvisid', 'parsed_mobile_number', 'Final_Product']).agg(F.max(data_clickstream['date_time']).alias('Visit_Time'), 
#                                       F.max(data_clickstream['time_difference']).alias('Visit_Duration'))
#      no uncomment                                F.count(data_clickstream['Final_Product']).alias('Visit_Count_in_this_Hour'))

# data_clickstream = data_clickstream.withColumn('Visit_Duration', F.round(data_clickstream['Visit_Duration'], 2))

#data_clickstream.write.mode("overwrite").parquet("JUNE_Results5.parquet")
#data_clickstream.uncache()

#data_clickstream = spark.read.parquet("JUNE_Results5.parquet")

lookup_table = data_clickstream[['mcvisid', 'parsed_mobile_number']].dropna().dropDuplicates(['mcvisid', 'parsed_mobile_number']).withColumnRenamed('parsed_mobile_number', 'Mobile_Number')

data_clickstream = data_clickstream.join(lookup_table, on=['mcvisid'], how='inner')

data_clickstream = data_clickstream.groupBy(['mcvisid', 'Mobile_Number', 'Final_Product', 'Date_Field', 'Hour_Field']).agg(F.max(data_clickstream['date_time']).alias('Visit_Time'), 
                                      F.sum(data_clickstream['time_difference']).alias('Visit_Duration'))

data_clickstream = data_clickstream.withColumn('Visit_Duration', F.round(data_clickstream['Visit_Duration'], 2))

output_columns = ['mcvisid', 'Mobile_Number', 'Final_Product', 'Visit_Time', 'Visit_Duration']

data_clickstream = data_clickstream.orderBy('Mobile_Number', ascending = False)[output_columns]
data_clickstream = data_clickstream[check_for_numeric(data_clickstream['Mobile_Number'])]
data_clickstream = data_clickstream.withColumnRenamed('Final_Product', 'Products')

#final_frame.coalesce(1).write.mode("overwrite").option('header', 'true').csv("JUNE_Results4_final.csv")
#final_frame.write.mode("overwrite").parquet("JUNE_Results5_final.parquet")

#data_clickstream = spark.read.parquet("JUNE_Results5_final.parquet")
sms_data = spark.read.csv("sms_data_new.txt", sep = '~', inferSchema = True, header = True)

sms_data_clicked = sms_data[sms_data['clicked'] == 'Yes']
sms_data_all = sms_data[sms_data['sms_status'] == 'Success']

# This write is for SMS sends which were clicked

sms_clicked_group = sms_data_clicked.groupBy(['mobile_no']).agg(
    F.collect_list(sms_data_clicked['sent_date']).alias('sms_clicked_sent_date'), 
    F.collect_list(sms_data_clicked['product_group']).alias('product_list_clicked'))

sms_all_group = sms_data_all.groupBy(['mobile_no']).agg(
    F.collect_list(sms_data_all['sent_date']).alias('sms_all_sent_date'), 
    F.collect_list(sms_data_all['product_group']).alias('product_list_all'))

stream_sms_cols = ['Mobile_Number', 'Products', 'Visit_Time', 'Visit_Duration',
                   'sms_clicked_sent_date', 'product_list_clicked', 'sms_all_sent_date',
                   'product_list_all']
stream_sms_clicked = data_clickstream.join(sms_clicked_group, 
                     data_clickstream['Mobile_Number'] == sms_clicked_group['mobile_no'], 
                     how='left')
stream_sms_clicked = stream_sms_clicked.drop('mobile_no')


click_sms_data = stream_sms_clicked.join(sms_all_group, 
                     stream_sms_clicked['Mobile_Number'] == sms_all_group['mobile_no'], 
                     how='left')[stream_sms_cols]

click_sms_data = click_sms_data[click_sms_data['Products'].isNotNull() & 
                                (click_sms_data['Products'] != ' ').cast(BooleanType())]

click_sms_data = click_sms_data[~(click_sms_data['Products'].contains('/'))]
click_sms_data = click_sms_data[click_sms_data['Products'].isNotNull()]

stringified = udf(stringify, StringType())
click_sms_data = click_sms_data.withColumn('sms_clicked_sent_date', stringified(click_sms_data['sms_clicked_sent_date']))
click_sms_data = click_sms_data.withColumn('product_list_clicked', stringified(click_sms_data['product_list_clicked']))
click_sms_data = click_sms_data.withColumn('sms_all_sent_date', stringified(click_sms_data['sms_all_sent_date']))
click_sms_data = click_sms_data.withColumn('product_list_all', stringified(click_sms_data['product_list_all']))

#click_sms_data.write.mode("overwrite").parquet("Clicked_stream_data_2.parquet")
#click_sms_data.coalesce(1).write.mode("overwrite").option('header', 'true').csv("Clicked_stream_data.csv")

#click_sms_data = spark.read.parquet("Clicked_stream_data_2.parquet")
mapped_product = spark.read.csv("adobe_product_mapping.csv", header = True, inferSchema = True)
click_data_cols = ['Mobile_Number', 'Products', 'Visit_Time', 'Mappings', 'Visit_Duration', 'sms_clicked_sent_date', 
                   'product_list_clicked', 'sms_all_sent_date', 'product_list_all']

click_sms_data = click_sms_data.join(mapped_product, on = ['Products'], how = 'left')[click_data_cols]

recent_visit = udf(get_recent_visit)
click_sms_data = click_sms_data.withColumn('Concordent_Follow_ups', when((click_sms_data['sms_all_sent_date'] != '') & 
                                                                         (click_sms_data['product_list_all'] != ''), 
                                       recent_visit(F.struct('Mappings', 'Visit_Time', 'sms_all_sent_date', 'product_list_all'))))

click_sms_data = click_sms_data.withColumn('Concordence_Percent', when(click_sms_data['Concordent_Follow_ups'].isNotNull() & 
                                                                  (click_sms_data['Concordent_Follow_ups'] != '').cast(BooleanType()), 
                                                F.round((F.size(F.split(click_sms_data['Concordent_Follow_ups'], ';')) / 
                                                         (F.size(F.split(click_sms_data['product_list_all'], '~')) - 1)) * 100, 2)))

click_sms_data = click_sms_data.withColumn('Positive_Clicks_Concordent', when((click_sms_data['sms_all_sent_date'] != '') & 
                                                                         (click_sms_data['product_list_all'] != ''), 
                                       recent_visit(F.struct('Mappings', 'Visit_Time', 'sms_clicked_sent_date', 'product_list_clicked'))))

click_sms_data = click_sms_data.withColumn('Positive_Click_Concordence_Percent', when(click_sms_data['Positive_Clicks_Concordent'].isNotNull() & 
                                                                                      (click_sms_data['Positive_Clicks_Concordent'] != '').cast(BooleanType()), 
                                                F.round((F.size(F.split(click_sms_data['Positive_Clicks_Concordent'], ';')) / 
                                                         (F.size(F.split(click_sms_data['product_list_clicked'], '~')) - 1)) * 100, 2)))

click_sms_data = click_sms_data[click_sms_data['Products'] != '']
#click_sms_data.write.mode("overwrite").parquet("Clicked_concordence_check2.parquet")
#click_sms_data.coalesce(1).write.mode("overwrite").option('header', 'true').csv("Clicked_concordence_check.csv")

#click_sms_data = spark.read.parquet("Clicked_concordence_check2.parquet")

get_recent_visit_all_udf = udf(get_recent_visit_all)
click_sms_data = click_sms_data.withColumn('Nearest_Contact', when((click_sms_data['sms_all_sent_date'] != '') & 
                                                                         (click_sms_data['product_list_all'] != ''), 
                                       get_recent_visit_all_udf(F.struct('Visit_Time', 'sms_all_sent_date'))))
click_sms_data = click_sms_data.fillna({'Nearest_Contact': 'Never Stimulated'})

get_recent_visit_intent_udf = udf(get_recent_visit_intent)
click_sms_data = click_sms_data.withColumn('most_recent_visit_intent', when((click_sms_data['sms_all_sent_date'] != '') & 
                                                                         (click_sms_data['product_list_all'] != ''), 
                                       get_recent_visit_intent_udf(F.struct('Mappings', 'Visit_Time', 'sms_all_sent_date', 'product_list_all'))))
click_sms_data = click_sms_data.fillna({'most_recent_visit_intent': 'Never Stimulated'})

first_concordent_stimulation_udf = udf(first_concordent_stimulation)
click_sms_data = click_sms_data.withColumn('most_recent_concordence_timestamp', when((click_sms_data['sms_all_sent_date'] != '') & 
                                                                         (click_sms_data['product_list_all'] != ''), 
                                                   first_concordent_stimulation_udf(F.struct('Mappings', 'Visit_Time', 'sms_all_sent_date', 'product_list_all'))))
click_sms_data = click_sms_data.fillna({'most_recent_concordence_timestamp':'Never Stimulated'})

click_percent_udf = udf(click_percent)
click_sms_data = click_sms_data.withColumn('Click_Percent', when((click_sms_data['product_list_all'] != ''),
                                                         click_percent_udf(F.struct('product_list_clicked', 'product_list_all'))))

click_sms_data.write.mode("overwrite").parquet("Clicked_concordence_check_3.parquet")