# -*- coding:utf-8 -*-
import datetime
import psycopg2
import random
import re
import os

class MyFunc(object):
    def now_time_to_string(self, time_type):
        '''
        根据传入的类型字符串返回时间的字符串
        :param time_type:希望返回的时间字符串的时间类型，如：year,month,day,time，也可简写为首字母
        '''
        time_type = time_type.lower()
        now_time = datetime.datetime.now()
        if time_type == 'year' or time_type == 'y':
            return now_time.strftime('%Y')
        elif time_type == 'month' or time_type == 'm':
            return now_time.strftime('%Y%m')
        elif time_type == 'day' or time_type == 'd':
            return now_time.strftime('%Y%m%d')
        elif time_type == 'time' or time_type == 't':
            return now_time.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return now_time


    def write_to_log(self, error_str):
        '''
        将报错信息写入到log文件中
        :param error_str:错误信息的字符串
        '''
        today_str = datetime.datetime.now().strftime('%Y%m%d')
        folder = os.path.exists('Logs')
        if not folder:
            os.makedirs('Logs')
        with open('Logs/error_log_%s.txt' % today_str, 'a', encoding='utf-8') as f:
            f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            f.write(':%s' % error_str)
            f.write('\n========================\n')

    
    def choice_ua(self, us_type='pc'):
        '''
        随机从以下User-Agent中抽取一个返回
        :param us_type:选择类型，网页端pc或者移动端mb，默认为pc
        '''
        ua_list_pc = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Safari/537.36 Core/1.63.6756.400 QQBrowser/10.3.2473.400',
            'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko Core/1.63.6756.400 QQBrowser/10.3.2473.400',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:64.0) Gecko/20100101 Firefox/64.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:65.0) Gecko/20100101 Firefox/65.0'
        ]
        ua_list_mb = [
            'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Mobile Safari/537.36',
            'Mozilla/5.0 (Linux; Android 8.0; Pixel 2 Build/OPD3.170816.012) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Mobile Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3_1 like Mac OS X) AppleWebKit/603.1.30 (KHTML, like Gecko) Version/10.0 Mobile/14E304 Safari/602.1',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 10_3 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) CriOS/56.0.2924.75 Mobile/14E5239e Safari/602.1',
            'Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1'
        ]
        if us_type == 'pc':
            ua = random.choice(ua_list_pc)
        else:
            ua = random.choice(ua_list_mb)
        return ua


    def execute_pgsql_sql(self, query_sql):
        '''
        查询数据方法，执行传入的查询语句，将取得的结果集返回
        :param query_sql: 查询语句
        :return 查询到的结果集
        '''
        if len(query_sql) > 0:
            jobdata = []
            for t in range(3):
                try:
                    conn1 = psycopg2.connect(database="danke", user="postgres", password="pgadmin", host="127.0.0.1", port="5432")
                    cur1 = conn1.cursor()
                    cur1.execute(query_sql)
                    if query_sql.find('select') != -1 or query_sql.find('delete') != -1:
                        jobdata = cur1.fetchall()
                    conn1.commit()
                    cur1.close()
                    conn1.close()
                    break
                except Exception as ex:
                    self.write_to_log(ex.pgerror)
                    continue
            return jobdata
    

    def select_fields_from_pgsql(self, table_name):
        '''
        查询该表中除掉id以外所有字段拼成的字符串
        :param table_name: 表名称
        '''
        if len(table_name) > 0:
            for t in range(3):
                try:
                    conn2 = psycopg2.connect(database="danke", user="postgres", password="pgadmin", host="127.0.0.1", port="5432")
                    cur2 = conn2.cursor()
                    cur2.execute('select * from %s' % table_name)
                    jobfield_list = cur2.description
                    new_jobfield_list = list()
                    for i in jobfield_list:
                        field_name = i[0]
                        if field_name != 'id':
                            new_jobfield_list.append(field_name)
                    jobfield = ','.join(new_jobfield_list)
                    conn2.commit()
                    cur2.close()
                    conn2.close()
                    break
                except Exception as ex:
                    self.write_to_log(ex.pgerror)
                    continue
            return jobfield


    def insert_into_pgsql(self, table_name, data_list):
        '''
        一次性插入多条数据，并在报错时重复3次，直到插入成功或超过3次后结束
        :param table_name: 表名
        :param data_list: 数据集，每条记录用元祖，再用list包含多个元祖
        :return: 无返回值
        '''
        if len(data_list) > 0:
            fiels = self.select_fields_from_pgsql(table_name)
            param_length = len(data_list[0])
            param_holder = list()
            for i in range(param_length):
                param_holder.append('%s')
            query_sql = "insert into %s(%s) values " % (table_name, fiels)
            out_list = list()
            for i in data_list:
                inner_list = list()
                if len(out_list) > 0:
                    out_list.append(',')
                out_list.append('(')
                for j in i:
                    if len(inner_list) > 0:
                        inner_list.append(',')
                    inner_list.append('\'')
                    inner_list.append(j)
                    inner_list.append('\'')
                out_list.append(''.join(inner_list))
                out_list.append(')')
            param_str = ''.join(out_list)
            query_sql = '%s%s' % (query_sql, param_str)
            for t in range(3):
                try:
                    conn = psycopg2.connect(database="danke", user="postgres", password="pgadmin", host="127.0.0.1", port="5432")
                    cur = conn.cursor()
                    cur.execute(query_sql)
                    conn.commit()
                    cur.close()
                    conn.close()
                    break
                except Exception as ex:
                    self.write_to_log(ex.pgerror)
                    continue


    def update_pgsql(self, dkID, dk_from):
        '''
        更新时间信息，以后可以用于判断哪些房子已经下架
        :param dkID: 房子的id
        :return: 无返回值
        '''
        for t in range(3):
            try:
                conn = psycopg2.connect(database="danke", user="postgres", password="pgadmin", host="127.0.0.1", port="5432")
                cur = conn.cursor()
                now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cur.execute("update danke_%s_list set update_time='%s' where dk_id='%s'" % (dk_from, now_time, dkID))
                conn.commit()
                print(u'data(%s) update success!' % dkID)
                cur.close()
                conn.close()
                break
            except Exception as ex:
                self.write_to_log(ex.pgerror)
                continue

