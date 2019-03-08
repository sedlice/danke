# -*- coding:utf-8 -*-
import datetime
import MyFunc
import time
import random
import requests
import urllib
from bs4 import BeautifulSoup


def get_html_info(dk_id, dk_url, dk_from, batch_time):
    myfunc = MyFunc.MyFunc()
    time.sleep(random.randint(2,5))
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0'}
    try:
        response_obj = requests.get(dk_url, headers=headers)
        response_obj.encoding = 'utf-8'
        html_str = response_obj.text
        html_obj = BeautifulSoup(html_str, 'html.parser')
        dk_name = html_obj.find('div', class_='room-name')
        try:
            dk_subway = dk_name.em.string
        except Exception:
            dk_subway = '无'
        dk_name = dk_name.h1.string
        dk_img = html_obj.find('div', class_='carousel-inner')
        dk_img_list = dk_img.find_all('img')
        dk_img_url = dk_img_list[0]['src']
        urllib.request.urlretrieve(dk_img_url,'E://dankeImg/%s.jpg' % dk_id)
        if dk_name.find('主卧') != -1:
            dk_room_type = '主卧'
        elif dk_name.find('次卧') != -1:
            dk_room_type = '次卧'
        else:
            dk_room_type = '待定'
        dk_price = html_obj.find('div', class_='room-price-num')
        if dk_price is None and len(dk_price) <= 0:
            dk_price = html_obj.find('div', class_='room-price-sale')
            dk_price = dk_price.string.strip()
        else:
            dk_price = dk_price.string.replace('元/月').strip()
        dk_tags = html_obj.find('div', class_='room-title')
        dk_tags_span_list = dk_tags.find_all('span')
        dk_tags_list = list()
        for tag in dk_tags_span_list:
            dk_tags_list.append(tag.string)
        dk_tags = ','.join(dk_tags_list)
        dk_details = html_obj.find('div', class_='room-list-box')
        dk_detail_list = dk_details.find_all('div', class_='room-list')
        for detail_item in dk_detail_list:
            detail_item = detail_item.label
            detail_str = detail_item.get_text()
            if detail_str.find('建筑面积') != -1:
                dk_area = detail_str.replace('建筑面积：约', '').replace('㎡（以现场勘察为准）', '')
            elif detail_str.find('户型') != -1:
                dk_lease_type = detail_item.b.extract()
                dk_lease_type = dk_lease_type.string
                dk_house_type = detail_item.string
                dk_house_type = dk_house_type.replace('户型：', '')
            elif detail_str.find('朝向') != -1:
                dk_direction = detail_item.label.string
                dk_direction = dk_direction.replace('朝向：', '')
            elif detail_str.find('楼层') != -1:
                dk_floor = detail_item.label.string
                dk_floor = dk_floor.replace('楼层：', '')
            elif detail_str.find('区域') != -1:
                dk_district = detail_item.find('div')
                dk_district_list = dk_district.find_all('a')
                dk_district_big = dk[0].string
                dk_district_small = dk[1].string
                dk_community = dk[2].string
            elif detail_str.find('地铁') != -1:
                dk_subway_info = detail_item.label.string
            else:
                pass
        dk_furnishing = html_obj.find('div', class_='room-info-list')
        dk_furnishing_list = dk_furnishing.find_all('tr')
        dk_furnishing_list = dk_furnishing[1].strings
        dk_furnishing = ','.join(dk_furnishing_list)
        dk_rooms = html_obj.find('div', class_='room-info-firend')
        dk_rooms = dk_rooms.find('tbody')
        dk_rooms_list = dk_rooms.find_all('tr')
        outer_list = list()
        for tr in dk_rooms_list
            inner_list = list()
            tds = tr.find_all('td')
            inner_list.append(tds[0].strong.string)
            if tds[0].a is not None:
                room_url = tds[0].a['href']
                inner_list.append(room_url)
                room_id = room_url.split('/')[4].replace('.html', '')
                myfunc.execute_pgsql_sql("insert into danke_waiting_data(dk_id,dk_url,dk_from,batch_time) values('%s','%s','%s','%s')" % (room_id, room_url, dk_from, batch_time))
            inner_list.append(tds[1].string)
            inner_list.append(tds[2].string)
            inner_list.append(tds[6].string)
            outer_list.append('|'.join(inner_list))
        dk_roommate = ','.join(outer_list)
        add_time = myfunc.now_time_to_string('time')
        data_tuple = (dk_id, dk_name, dk_price, dk_subway, dk_subway_info, dk_tags, dk_area, dk_house_type, dk_lease_type, dk_room_type, dk_direction, 
                    dk_district_big, dk_district_small, dk_community, dk_floor, dk_furnishing, dk_roommate, dk_url, batch_time, add_time)
        data_list = [data_tuple]
        myfunc.insert_into_pgsql('danke_%s_detail' % dk_from, data_list)


def run_spider():
    myfun = MyFunc.MyFunc()
    while True:
        result_set = myfun.select_list_from_pgsql('delete from danke_waiting_data where id in (select id from danke_waiting_data order by id limit 1) returning dk_id,dk_url,dk_from,batch_time')
        if len(result_set) > 0:
            for item in result_set:
                dk_id = item[0]
                dk_url = item[1]
                dk_from = item[2]
                batch_time = item[3]
                get_html_info(dk_id, dk_url, dk_from, batch_time)
        else:
            break

if __name__ == "__main__":
    run_spider()