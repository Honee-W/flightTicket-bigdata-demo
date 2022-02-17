timelist = ['2022-02-12','2022-02-13','2022-02-14','2022-02-15','2022-02-16','2022-02-17','2022-02-17','2022-02-18','2022-02-19','2022-02-20','2022-02-21']
citylist = ['北京','上海','广州','深圳','昆明','成都','重庆','郑州','杭州','三亚','武汉','长沙','乌鲁木齐','贵阳',
            '青岛','南京','海口','厦门','哈尔滨','南宁','济南','天津','大连','兰州','太原','沈阳','长春','福州','丽江']
citynumber = ['pek','sha','can','szx','kmg','ctu','ckg','cgo','hgh','syx','wuh','csx','urc','kwe','tao',
              'nkg','hak','xmn','hrb','nng','tna','tsn','dlc','lhw','tyn','she','cgq','foc','ljg']
listl = []
lista = []
for time in timelist:
    count = 0
    for city in citylist:
        listl.append('https://www.meituan.com/flight/xiy-'+citynumber[count]+'/?departCn=西安&arriveCn='+city+
                        '&forwardDate='+time+'&isFilterWithChild=0&isFilterWithBaby=0')
        count += 1

for time in timelist:
    count = 0
    for city in citylist:
        lista.append('https://www.meituan.com/flight/'+citynumber[count]+'-xiy/?departCn='+city+
                        '&arriveCn=西安&forwardDate='+time+'&isFilterWithChild=0&isFilterWithBaby=0')
        count += 1

# 输出txt文本
pathleave = ('./leaveUrl.txt')
patharrive = ('./arriveUrl.txt')
filel = open(pathleave, 'w')
filea = open(patharrive, 'w')
for url in lista:
    filea.write(url+'\n')
    filea.flush()
filea.close()
for url in listl:
    filel.write((url+'\n'))
    filel.flush()
filel.close()