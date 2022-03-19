#!/user/bin/python
# -*- coding:UTF-8 -*-

import requests
import time
import random
from bs4 import BeautifulSoup
import re
import xlwt,xlrd
import totalTickets
from urllib.parse import quote
from fake_useragent import UserAgent

#IP池
proxypool_url = 'http://127.0.0.1:5555/random'

def main(url):
    # 特价机票页面：从西安到全国各地
    url = url
    # 添加请求头信息
    my_headers = [        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0);",
        "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
        "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
        "Mozilla/5.0 (X11; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0"]
    headers = {
         # 'User-Agent': random.choice(my_headers)
        'User-Agent': UserAgent().random
    }

    proxy = requests.get((proxypool_url)).text
    print(proxy)
    proxies = {'http': 'http://'+proxy}

    # 发送请求，获得响应
    r = requests.get(url, headers=headers, proxies=proxies)
    print(r.status_code)
    time.sleep(0.1)
    # 解析页面代码
    soup = BeautifulSoup(r.text, 'html.parser')
    infolist = []
    datalist = []
    getInfo(soup,infolist)
    getData(soup,datalist)

    # if (len(datalist) == 0 and trytimes > 0):
    #     main(url, trytimes-1)

    # 保存数据
    savepath = '../data/leave/西安到全国热门城市.xls'
    writeData(infolist, datalist, savepath)

#起点
findDeparture = re.compile(r'class="header-airline".*>【单程】 (.*?)-.*?</h2>')
#终点
findDestination = re.compile(r'class="header-airline".*>【单程】 .*-(.*?)</h2>')
#日期
findDate = re.compile(r'"header-date".*>去.?(.*?)</span>')
# 航空公司
# <a.*>(.*)</a>
findCompany = re.compile(r'img.*alt="(.*?)".*')
# 飞机机型
findModel = re.compile(r'<p.*>(.*)\s</p>')
# 起飞时间
findLeftTime = re.compile(r'<span.*>(.*)</span>.*"transfer"')
# 起飞机场
findLeftPort = re.compile(r'class="airport depart.*">(.*)</h2>.*"transfer"')
# 飞行方式
findWay = re.compile(r'class="arrow".*class="transfer".*>(.*)</div><.*class="icon"')
# 到达时间
findArriveTime = re.compile(r'class="airport arrive".*class="airport-time".*><span.*>(.{5})</span>.*')
findArriveTime2 = re.compile(r'class="airport arrive".*class="airport-time".*><span.*>(.*)</span><span class="airport-time-days".*>.*')
# 到达机场
findArrivePort = re.compile(r'<h2.*">(.*)</h2>')
# 准点率
findPunctualRate = re.compile(r'准点率<br.*/>(.*?)</div>')
# 最低价格
findLowestPrice = re.compile(r'<span.*>(.*?)</span>起')


def getInfo(soup,infolist):
    # infolist = []
    for item in soup.find_all('div', class_="header"):
        # print(item)
        data = []  # 保存一张机票的所有信息
        item = str(item)

        departures = re.findall(findDeparture, item)
        destinations = re.findall(findDestination, item)
        dates = re.findall(findDate, item)

        if(len(departures) == 0 & len(destinations) == 0 & len(dates) == 0):
            continue

        # print("起点"+str(departures))
        if(len(departures) == 0):
            data.append(' ')
        else:
            departure = departures[0]
            data.append(departure)

        # print("终点"+str(destinations))
        if(len(destinations) == 0):
            data.append(' ')
        else:
            destination = destinations[0]
            data.append(destination)

        # print("日期"+str(dates))
        if(len(dates) == 0):
            data.append(' ')
        else:
            date = dates[0]
            data.append(date)

        infolist.append(data)


# 一个功能对应一个函数
# 爬取网页
def getData(soup,datalist):
    # datalist = []
    for item in soup.find_all('div', class_="prices"):  # 查找符合要求的字符串，形成列表

        # print(item)   #测试：查看机票item全部信息
        data = []  # 保存一张机票的所有信息
        item = str(item)

        # 解析每张机票的数据
        companies = re.findall(findCompany, item)
        if(len(companies) == 0): #正则表达式匹配不成功时，可能为空
            data.append(' ')
        else:
            company = companies[0]
            data.append(company)


        models = re.findall(findModel, item)
        if(len(models) == 0):
            data.append(' ')
        else:
            model = models[0]
            data.append(model)


        leftTimes =  re.findall(findLeftTime, item)
        if(len(leftTimes) == 0):
            data.append(' ')
        else:
            leftTime = leftTimes[0]
            data.append(leftTime)

        leftPorts =  re.findall(findLeftPort, item)
        if(len(leftPorts) == 0):
            data.append(' ')
        else:
            leftPort = leftPorts[0]
            data.append(leftPort)

        Ways = re.findall(findWay, item)
        if(len(Ways) == 0):
            data.append('非直飞')
        else:
            Way = Ways[0]
            data.append(Way)

        arriveTimes =  re.findall(findArriveTime, item)
        if(len(arriveTimes) == 0):
            data.append(' ')
        else:
            if(arriveTimes[0] == '+1天'):
                arriveTimes2 = re.findall(findArriveTime2, item)
                arriveTime = arriveTimes2[0]
                data.append(arriveTime)
            else:
                arriveTime = arriveTimes[0]
                data.append(arriveTime)

        arrivePorts = re.findall(findArrivePort, item)
        if(len(arrivePorts) == 0):
            data.append(' ')
        else:
            arrivePort = arrivePorts[0]
            data.append(arrivePort)

        punctualRates = re.findall(findPunctualRate, item)
        if(len(punctualRates) == 0):
            data.append(' ')
        else:
            punctualRate = punctualRates[0]
            data.append(punctualRate)

        lowestPrices =   re.findall(findLowestPrice, item)
        if(len(lowestPrices) == 0):
            data.append(' ')
        else:
            lowestPrice = lowestPrices[0]
            data.append(lowestPrice)


        datalist.append(data)  # 把处理好的一张机票信息放入datalist




# 保存数据
def writeData(infolist, datalist, savepath):
    open_file = xlrd.open_workbook(savepath)
    table = open_file.sheets()[0]
    print("write....")
    print(infolist)
    if  table.nrows == 1:
        col = ("id", "departure", "destination", "date", "company", "model", "leaveTime", "leavePort", "way", "arriveTime",
               "arrivePort", "punctualRate", "lowestPrice")
        for i in range(0, 13):
            sheet.write(0, i, col[i])  # 列名
    for i in range(0, len(datalist)):
        print("第%d条" % (i + 1))
        data = datalist[i]
        info = infolist[0]
        sheet.write(i + table.nrows, 0, totalTickets.id)
        totalTickets.id = totalTickets.id + 1
        for j in range(0, 3):
            sheet.write(i+table.nrows, j+1, info[j])#i+1
        for j in range(0, 9):
            sheet.write(i+table.nrows, j+4, data[j])  # 数据i+1

    book.save(savepath)  # 保存


if __name__ == "__main__":
    count = 1
    book = xlwt.Workbook(encoding="utf-8", style_compression=0)  # 创建workbook对象
    sheet = book.add_sheet('美团机票', cell_overwrite_ok=True)  # 创建工作表
    f = open('leaveUrl.txt', 'r', encoding='gbk')
    urllist = f.readlines()
    for url in urllist:
        ret = quote(url, safe=';/?:@&=+$,', encoding='utf-8') #含中文的url转化成utf-8格式
        # main(ret, 20)
        main(ret)
        count += 1
        time.sleep(random.randint(1,3))