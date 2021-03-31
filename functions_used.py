# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 19:46:56 2020

@author: ketankishore
"""
import plotly.graph_objects as go
import base64
import re
from urllib.parse import unquote
from datetime import datetime


def check_offerid_mobile(url: str):
    
    try:
        
        if (url != None) and (url != ''):
    
            if ('offerId=' in url or 'mob=' in url) and ('?' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('offerId', '') not in ['', ' ', None, 'null'] and params.get('mob', '') not in ['', ' ', None, 'null']:
                    return 'offerId_{}:mob_{}'.format(params.get('offerId', ''), params.get('mob', ''))
                elif params.get('offerId', '') not in ['', ' ', None, 'null']:
                    return 'offerId_{}'.format(params.get('offerId', ''))
                elif params.get('mob', '') not in ['', ' ', None, 'null']:
                    return 'mob_{}'.format(params.get('mob', ''))
                else:
                    return None
                
            elif ('offerId=' in url or 'mob=' in url) and (('?' not in url) & ('&' in url)):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('&')[1:] if len(i.split('=')) > 1}
                if params.get('offerId', '') not in ['', ' ', None, 'null'] and params.get('mob', '') not in ['', ' ', None, 'null']:
                    return 'offerId_{}:mob_{}'.format(params.get('offerId', ''), params.get('mob', ''))
                elif params.get('offerId', '') not in ['', ' ', None, 'null']:
                    return 'offerId_{}'.format(params.get('offerId', ''))
                elif params.get('mob', '') not in ['', ' ', None, 'null']:
                    return 'mob_{}'.format(params.get('mob', ''))
                else:
                    return None
                
            else:
                return None
            
        else:
            return None

    except Exception as e:
        print(str(e), url)
        return None
        #raise Exception
    
    
def get_utm_source(url):
    
    try:
        
        if url != '' and url != None and len(url) > 5:
            
            if ('utm_source=' in url) and ('?' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('utm_source', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_source', '') and (('.in' in params.get('utm_source', '')) or ('.com' in params.get('utm_source', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_source', '')
                else:
                    return 'Not Available'

            elif ('utm_source=' in url) and ('?' not in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('&') if len(i.split('=')) > 1}
                if params.get('utm_source', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_source', '') and (('.in' in params.get('utm_source', '')) or ('.com' in params.get('utm_source', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_source', '')
                else:
                    return 'Not Available'

            elif '/utm_source/' in url:
                index = url.split('/').index('utm_source')
                return url.split('/')[index + 1]
            else:
                return 'Not Available'            
        else: 
            return 'Not Available'

    except Exception as e:
        print(str(e), url)
        return 'In Exception'
    
    
def get_utm_medium(url):
    
    try:
        
        if url != '' and url != None and len(url) > 5:
            
            if ('utm_medium=' in url) and ('?' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('utm_medium', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_medium', '') and (('.in' in params.get('utm_medium', '')) or ('.com' in params.get('utm_medium', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_medium', '')
                else:
                    return 'Not Available'

            elif ('utm_medium=' in url) and ('?' not in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('&') if len(i.split('=')) > 1}
                if params.get('utm_medium', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_medium', '') and (('.in' in params.get('utm_medium', '')) or ('.com' in params.get('utm_medium', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_medium', '')
                else:
                    return 'Not Available'

            elif '/utm_medium/' in url:
                index = url.split('/').index('utm_medium')
                return url.split('/')[index + 1]
            else:
                return 'Not Available'            
        else:
            return 'Not Available'

    except Exception as e:
        print(type(url), url, str(e))
       # print(str(e), url)
        return 'In Exception'
    

def get_utm_campaign(url):
    
    try:
        
        if url != '' and url != None and len(url) > 5:
            
            if ('utm_campaign=' in url) and ('?' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('utm_campaign', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_campaign', '') and (('.in' in params.get('utm_campaign', '')) or ('.com' in params.get('utm_campaign', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_campaign', '')
                else:
                    return 'Not Available'

            elif ('utm_campaign=' in url) and ('?' not in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('&') if len(i.split('=')) > 1}
                if params.get('utm_campaign', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_campaign', '') and (('.in' in params.get('utm_campaign', '')) or ('.com' in params.get('utm_campaign', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_campaign', '')
                else:
                    return 'Not Available'

            elif '/utm_campaign/' in url:
                index = url.split('/').index('utm_campaign')
                return url.split('/')[index + 1]
            else:
                return 'Not Available'            
        else:
            return 'Not Available'

    except Exception as e:
        print(type(url), url, str(e))
       # print(str(e), url)
        return 'In Exception'
    
    
def get_utm_content(url):
    
    try:
        
        if url != '' and url != None and len(url) > 5:
            
            if ('utm_content=' in url) and ('?' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('utm_content', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_content', '') and (('.in' in params.get('utm_content', '')) or ('.com' in params.get('utm_content', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_content', '')
                else:
                    return 'Not Available'

            elif ('utm_content=' in url) and ('?' not in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('&') if len(i.split('=')) > 1}
                if params.get('utm_content', '') not in ['', ' ', None, 'null']:
                    if '@' in params.get('utm_content', '') and (('.in' in params.get('utm_content', '')) or ('.com' in params.get('utm_content', ''))):
                        return 'Personal Email ID'
                    else:
                        return params.get('utm_content', '')
                else:
                    return 'Not Available'

            elif '/utm_content/' in url:
                index = url.split('/').index('utm_content')
                return url.split('/')[index + 1]
            else:
                return 'Not Available'            
        else:
            return 'Not Available'

    except Exception as e:
        print(type(url), url, str(e))
       # print(str(e), url)
        return 'In Exception'
    
    
def parse_url(value):
    
    #print(value.cast(StringType()))
    try:
        if value != '' and 'bajaj' in value and value != None:
            return value.split('/')[3]
        else:
            return None
    except:
        return None

def bar_plotting_function(x_1, y_1, x_axis_title, y_axis_title, title, fig_name, scatter = False, customize_axis = False):
    
    labels_1 = x_1.tolist()
    values_1 = y_1.tolist()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
                x=labels_1, y=values_1,
                text=values_1,
                textposition='auto',
                marker={'color': values_1, 'colorscale': 'Viridis'}
        ))
    if scatter:
        
        fig.add_trace(go.Scatter(
                    x=labels_1, y=values_1,
                    text=values_1,
                    mode = 'lines+markers'
            ))
    
    fig.update_layout(
        title={
            'text': title,
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis_title= x_axis_title,
        yaxis_title= y_axis_title
    )
    
    if customize_axis:
        fig.update_xaxes(tickangle=45, tickfont=dict(family='Rockwell', color='crimson'))
        
    fig.write_html("images1/{}.html".format(fig_name))
    
    return fig


def check_offerid_mobile_2(url: str):
    
    try:
        
        if (url != None) and (url != ''):

            if ('offerid=' in url) and ('?' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('offerid', '') not in ['', ' ', None, 'null']:
                    return 'offerid_{}'.format(params.get('offerid', ''))
                else:
                    pass

            if ('&m=' in url or '?m=' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('m', '') not in ['', ' ', None, 'null']:
                    return 'mob_{}'.format(params.get('m', ''))
                else:
                    pass

            if ('&application_id=' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('application_id', '') not in ['', ' ', None, 'null']:
                    return 'applicationid_{}'.format(params.get('application_id', ''))
                else:
                    pass

            if ('applicationKey=' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('applicationKey', '') not in ['', ' ', None, 'null']:
                    return 'applicationKey_{}'.format(params.get('applicationKey', ''))
                else:
                    pass

            if ('uci=' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('uci', '') not in ['', ' ', None, 'null']:
                    return 'uci_{}'.format(params.get('uci', ''))
                else:
                    pass

            if ('processId=' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('processId', '') not in ['', ' ', None, 'null']:
                    return 'processId_{}'.format(params.get('processId', ''))
                else:
                    pass

            return None
        
        else:
            return None
        
    except Exception as e:
        
        print(str(e))
        print(url)
        return None
        #raise Exception
    
    
def classify_campaign(url: str):
    
    try:
        
        if url not in ['', ' ', None]:
    
            string_to_check = url.split(':')[0]
            if any(word in string_to_check for word in ['AFFILIATE', 'Affiliate', 'affiliate']) and (not(any(word in string_to_check for word in ['bfsd', 'BFSD', 'intcamp']))):
                return 'Affiliate'
    
            if any(word in string_to_check for word in ['Organic', 'ORGANIC', 'organic']) and (not(any(word in string_to_check for word in ['bfsd', 'BFSD', 'intcamp']))):
                return 'Organic'
    
            if any(word in string_to_check for word in ['SMS', 'sms']) and (not(any(word in string_to_check for word in ['bfsd', 'BFSD', 'intcamp']))):
                return 'SMS'
    
            if any(word in string_to_check for word in ['EMAIL', 'email', 'Email']) and (not(any(word in string_to_check for word in ['bfsd', 'BFSD', 'intcamp']))):
                return 'Email'
    
            if '@' in string_to_check and '.com' in string_to_check and 'customer' in string_to_check:
                return 'Customer Channel'
    
            if '@' in string_to_check and ('.com' in string_to_check or '.in' in string_to_check)  and 'employee' in string_to_check:
                return 'Employee Channel'
    
            if any(word in string_to_check for word in ['GOOGLE', 'google', 'Google']):
                return 'Google Channel'
    
            if any(word in string_to_check for word in ['BFSD', 'bfsd', 'Bfsd']):
                return 'BFSD'
    
            if 'intcamp' in string_to_check and (not(any(word in string_to_check for word in ['bfsd', 'BFSD']))):
                return 'Internal Campaign'
    
            if any(word in string_to_check for word in ['Mobile', 'MOBILE', 'mobile']) and (not(any(word in string_to_check for word in ['bfsd', 'BFSD', 'intcamp']))):
                return 'Mobile Campaign'
    
            if any(word in string_to_check for word in ['OAT', 'oat', 'Oat']) and (not(any(word in string_to_check for word in ['bfsd', 'BFSD', 'intcamp']))):
                return 'OAT Campaign'
    
            return 'Others'
    
        else:
            return 'Not Available'
        
    except Exception as e:
        print(str(e), url)
        return 'In Exception'
    
def check_mobile_only(url: str):
    
    try:
        
        if (url != None) and (url != ''):
    
            try:
            
                if ('mob=' in url) and ('?' in url):
                    params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                    if params.get('mob', '') not in ['', ' ', None, 'null']:
                        return '{}'.format(params.get('mob', ''))
            except Exception as e:
                print(str(e), '1', url)
                pass
            
            try:
                
                if ('mob=' in url) and (('?' not in url) & ('&' in url)):
                    params = {i.split('=')[0]: i.split('=')[1] for i in url.split('&')[1:] if len(i.split('=')) > 1}
                    if params.get('mob', '') not in ['', ' ', None, 'null']:
                        return '{}'.format(params.get('mob', ''))
            except Exception as e:
                print(str(e), '2', url)
                pass
            
            try:
                
                if ('&m=' in url or '?m=' in url):
                    params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                    if params.get('m', '') not in ['', ' ', None, 'null']:
                        return '{}'.format(params.get('m', ''))
            except Exception as e:
                print(str(e), '3', url)
                pass
            
            try:
                
                numbers = re.findall(r'[\+]?[789][0-9 .]{8,}[0-9]', url)
                if len(numbers) > 1:
                    for num in numbers:
                        if len(num) == 10:
                            return num
                        else:
                            pass
                        
            except Exception as e:
                print(str(e), '4', url)
                pass
        else:
            return None

    except Exception as e:
        print(str(e), url)
        return None
    
def number_decode(number):
    
    try:
        if number != None:
            decoded_val = base64.b64decode(number).decode('utf-8')
            return decoded_val
    except Exception as e:
        print(str(e), '1', number)
        return None

def check_product_code(url: str):
        
    try:
        
        if (url != None) and (url != ''):    
            
            if ('prodCode=' in url) and ('?' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in url.split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('prodCode', '') not in ['', ' ', None, 'null']:
                    return '{}'.format(params.get('prodCode', ''))
                
        return None
        
    except Exception as e:
        print(str(e), url)
        return None
    
def find_searched_item(url: str):
        
    try:
        
        if (url != None) and (url != ''):    
            
            if ('search' in url) and ('&q=' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in unquote(url).split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('q', '') not in ['', ' ', None, 'null']:
                    return ' {}'.format(params.get('q', '').replace('+', ' '))
                
            if ('search' in url) and ('?q=' in url):
                params = {i.split('=')[0]: i.split('=')[1] for i in unquote(url).split('?')[1].split('&') if len(i.split('=')) > 1}
                if params.get('q', '') not in ['', ' ', None, 'null']:
                    return ' {}'.format(params.get('q', '').replace('+', ' '))
        
        return None
    except Exception as e:
        print(str(e), url)
        return None
    

def is_numeric(text):
    
    try:
        
        if (text != None) and (text != ''):

            if text.isdigit():
                return True

        return False
    
    except Exception as e:
        
        print(str(e), text)
        return False
    
def stringify(list_item):
    
    try:
        
        joined_string = ''
        if list_item != None:
            for text in list_item:
                joined_string = joined_string + text + '~'
        return joined_string
    
    except Exception as e:
        print(str(e), list_item)
        return ''
    
    
def get_recent_visit(data):
    try:
        if data[2] != None and data[3] != None:
            final_result = []
            mappings, visit_time, sms_all_sent, product_list_all = data[0], data[1], data[2].split('~'), data[3].split('~')
            if len(sms_all_sent) > 0 and len(product_list_all) > 0:  
                #mappings, visit_time, sms_all_sent, product_list_all = data[0], data[1], data[2].split('~'), data[3].split('~')
                count = 0
                product_list_all = [product for product in product_list_all if product != '']
                sms_all_sent = [sms for sms in sms_all_sent if sms != '']
                product_count_total = len(product_list_all)
                time_diff_list = []
                visit = datetime.strptime(visit_time, "%Y-%m-%d %H:%M:%S")
                while(mappings in product_list_all):
                    index = product_list_all.index(mappings)
                    sent_date = datetime.strptime(sms_all_sent[index], "%Y-%m-%d %H:%M:%S")
                    if visit > sent_date:
                        time_diff = visit - sent_date
                        time_diff_list.append((time_diff, ' Before'))
                    else:
                        time_diff = sent_date - visit
                        time_diff_list.append((time_diff, ' After'))
                    count += 1
                    product_list_all.pop(index)
                    sms_all_sent.pop(index)
                   # print(visit_time, time_diff, product, count)
                for time in time_diff_list:
                    stamp = str(time[0]) + time[1]
                    final_result.append(stamp)
                return ';'.join([str(delta).replace(',', '') for delta in final_result])
    except Exception as e:
        print(str(e), data)
        return None
    
def get_recent_visit_intent(data):
    try:
        if data[0] != None and data[1] != None and str(data[0]) != 'nan' and str(data[1]) != 'nan' and \
           data[2] != None and data[3] != None and str(data[2]) != 'nan' and str(data[3]) != 'nan':
            final_result = []
            #visit_time, sms_sent = data[0], data[1].split('~')
            mappings, visit_time, sms_sent, product_list_all = data[0], data[1], data[2].split('~'), data[3].split('~')
            sms_sent = [sms for sms in sms_sent if sms != '']
            product_list_all = [product for product in product_list_all if product != '']
            if len(sms_sent) > 0 and len(product_list_all) > 0:  
                sms_all_sent = [sms for sms in sms_sent if sms != '' and sms > visit_time]
                if len(sms_all_sent) > 0:
                    nearest_time_index = sms_sent.index(min(sms_all_sent))
                    nearest_product = product_list_all[nearest_time_index]
                    
                    if (mappings == nearest_product) or (mappings.strip() in nearest_product):
                        return 'Concordent'
                    else:
                        return 'Discoredent'      
        else:
            return 'Never Stimulated'
                
    except Exception as e:
        print(str(e), data)
        return None
    
def get_recent_visit_all(data):
    try:
        if data[0] != None and data[1] != None and str(data[0]) != 'nan' and str(data[1]) != 'nan':
            final_result = []
            visit_time, sms_all_sent = data[0], data[1].split('~')
            if len(sms_all_sent) > 0 and len(visit_time) > 0:  
                #mappings, visit_time, sms_all_sent, product_list_all = data[0], data[1], data[2].split('~'), data[3].split('~')
                backup_sms_data = sms_all_sent
                sms_all_sent = [sms for sms in sms_all_sent if sms != '' and sms > visit_time]
                if len(sms_all_sent) > 0:
                    nearest_time = datetime.strptime(min(sms_all_sent), "%Y-%m-%d %H:%M:%S")
                    visit = datetime.strptime(visit_time, "%Y-%m-%d %H:%M:%S")
                    time_diff = (nearest_time - visit).total_seconds() / 3600

                    if 0 < time_diff <= 24:
                        return 'Stimulated Within 24hrs'
                    elif 24 < time_diff <= 48:
                        return 'Stimulated Within 24-48hrs'
                    elif 48 < time_diff <= 96:
                        return 'Stimulated Within 48-96hrs'
                    elif 96 < time_diff <= 168:
                        return 'Stimulated Within 96-168hrs'
                    else:
                        return 'Stimulated after 168hrs'
                else:
                    if len([sms for sms in backup_sms_data if sms != '' and sms < visit_time]) > 0:
                        return 'Stimulated in Past'
                    return 'Never Stimulated'
                
        else:
            return 'Never Stimulated'
                
    except Exception as e:
        print(str(e), data)
        return None
    
def first_concordent_stimulation(data):
    
    try:
        if data[0] != None and data[1] != None and str(data[0]) != 'nan' and str(data[1]) != 'nan' and \
           data[2] != None and data[3] != None and str(data[2]) != 'nan' and str(data[3]) != 'nan':
            final_result = []
            backup_result = []
            mappings, visit_time, sms_sent, product_list_all = data[0], data[1], data[2].split('~'), data[3].split('~')
            sms_sent = [sms for sms in sms_sent if sms != '']
            product_list_all = [product for product in product_list_all if product != '']
            if len(sms_sent) > 0 and len(product_list_all) > 0:  
                #mappings, visit_time, sms_all_sent, product_list_all = data[0], data[1], data[2].split('~'), data[3].split('~')
                sms_all_sent = [sms for sms in sms_sent if sms != '' and sms > visit_time]
                visit = datetime.strptime(visit_time, "%Y-%m-%d %H:%M:%S")
                if len(sms_all_sent) > 0:
                    while(mappings in product_list_all):
                        index = product_list_all.index(mappings)
                        sent_time = sms_sent[index]
                        #print(product_list_all[index], sent_time, visit_time)
                        if sent_time in sms_all_sent:
                            final_result.append(sent_time)
                        else:
                            backup_result.append(sent_time)
                        product_list_all.pop(index)
                        sms_sent.pop(index)
                    
                    if len(final_result) == 0 and len(backup_result) > 0:
                        return 'Stimulated Previous to Visit'
                    
                    if len(final_result) > 0:
                        nearest_concordent_time = datetime.strptime(min(final_result), "%Y-%m-%d %H:%M:%S")
                        time_diff = (nearest_concordent_time - visit).total_seconds() / 3600
                        #print(time_diff)

                        if 0 < time_diff <= 24:
                            return 'Stimulated Within 24hrs'
                        elif 24 < time_diff <= 48:
                            return 'Stimulated Within 24-48hrs'
                        elif 48 < time_diff <= 96:
                            return 'Stimulated Within 48-96hrs'
                        elif 96 < time_diff <= 168:
                            return 'Stimulated Within 96-168hrs'
                        else:
                            return 'Stimulated after 168hrs'
                else:
                    return 'Never Stimulated'
                
        else:
            return 'Never Stimulated'
                
    except Exception as e:
        print(str(e), data)
        return None
    
def click_percent(data):
    
    try:
        
        if data[1] != None and str(data[1]) != 'nan' and data[1] != '':
            if (data[0] == None or str(data[0]) == 'nan' or data[0] == ''):
                return '0 Percent Clicks'
            product_click, product_sent = data[0].split('~'), data[1].split('~')
            product_click = [product for product in product_click if product != '']
            product_sent =  [product for product in product_sent if product != '']
            if len(product_sent) > 0:
                
                percent = (len(product_click) / len(product_sent)) * 100
            
                if percent < 20:
                    return '1 - 20 Percent'
                elif 20 <= percent < 40:
                    return '20 - 40 Percent'
                elif 40 <= percent < 60:
                    return '40 - 60 Percent'
                elif 60 <= percent < 80:
                    return '60 - 80 Percent'
                elif 80 <= percent:
                    return 'Greater than 80 Percent'
                else:
                    return 'UnDetected'
                
            else:
                return 'No SMS Data'
        else:
            return 'No SMS Data'
        
    except Exception as e:
        
        print(str(e))
        return 'No SMS Data'