# -*- coding:utf-8 -*-

import MyFunc
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
plt.rcParams['font.sans-serif'] = ['SimHei']


def show_pie(labels, sizes):
    """
    绘制饼图
    """
    # explode = (0, 0.1, 0, 0)  # 突出显示某一块，都为0则都不突出
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
            shadow=False, startangle=90)
    ax1.axis('equal')
    plt.show()


def show_hist(datas):
    """
    绘制直方图
    """
    plt.figure(1, dpi=50)
    data = datas
    plt.hist(data)
    plt.show()


def show_bar(x_data, y_data):
    x = x_data
    y = y_data
    plt.bar(x, y, align =  'center') 
    plt.title('蛋壳公寓在北京各区可出租房屋数量（昨天为止）') 
    plt.ylabel('数量') 
    plt.xlabel('行政区') 
    plt.show()


def show_plot(x_data, y_data, d_title):
    fig, ax = plt.subplots()
    ax.plot(x_data, y_data)
    formatter = ticker.FormatStrFormatter('￥%1.2f')
    ax.yaxis.set_major_formatter(formatter)
    fig.suptitle(d_title)
    plt.show()



if __name__ == "__main__":
    myfun = MyFunc.MyFunc()
    datas_x = list()
    datas_y = list()
    sql1 = "select dk_district_big,count(id) as counts from danke_bj_detail where dk_district_big<>'' group by dk_district_big"
    sql2 = "select dk_district_big,avg(dk_price) from danke_bj_detail where dk_district_big<>'' and dk_lease_type='合' group by dk_district_big"
    sql3 = "select dk_district_big,avg(dk_price) from danke_bj_detail where dk_district_big<>'' and dk_lease_type='整' group by dk_district_big"
    result_set = myfun.execute_pgsql_sql(sql1)
    for r in result_set:
        datas_x.append(r[0])
        datas_y.append(r[1])
    # show_bar(datas_x, datas_y)
    # show_plot(datas_x, datas_y, '蛋壳公寓各区整租房平均房租')
    show_pie(datas_x, datas_y)
    
