# -*- coding:utf-8 -*-
import datetime
import MyFunc
import time
import random
import requests
from bs4 import BeautifulSoup


def run_spider():
    '''
    爬取所有房子的列表，存到一个基础数据库中
    '''
    offset = 1
    myfun = MyFunc.MyFunc()
    while True:
        time.sleep(random.randint(2,5))
        base_url = 'https://www.danke.com/room/bj?page=%s' % offset
        headers = {'user-agent': myfun.choice_ua()}
        try:
            response_obj = requests.get(base_url, headers=headers)
            response_obj.encoding = 'utf-8'
            html_str = response_obj.text
            if html_str.find('notext_box') != -1 or html_str.find('r_lbx') == -1:
                break
            else:
                html_obj = BeautifulSoup(html_str, 'html.parser')
                item_list = html_obj.find_all('div', class_='r_lbx')
                data_list = list()
                for item in item_list:
                    item = item.find('div', class_='r_lbx_cen')
                    dkUrl = item.find('div', class_='r_lbx_cena')
                    dkUrl = dkUrl.a.get('href')
                    dkID = dkUrl.split('/')[4].replace('.html', '')
                    result_set = myfun.execute_pgsql_sql("select count(id) from danke_list where dk_id='%s'" % dkID)
                    if result_set[0][0] > 0:
                        myfun.update_pgsql(dkID)
                    else:
                        dkName = item.find('div', class_='r_lbx_cena')
                        dkName = dkName.a.string
                        dkName = dkName.replace('\n', '').strip()
                        add_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        data_tuple = (dkID, dkName, dkUrl, add_time)
                        data_list.append(data_tuple)
                myfun.insert_into_pgsql('danke_bj_list', data_list)
                print('now is page %s' % offset)
                offset += 1
        except Exception as ex:
            myfun.write_to_log(str(ex))


if __name__ == "__main__":
    run_spider()
