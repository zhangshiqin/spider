# -*- coding: utf-8 -*-
'''

@Author: Isabella
@Create time: 2018/6/13 14:57

'''
import requests
from bs4 import BeautifulSoup
import pymongo
import time

def save_to_mongo(result):
    try:
        db[MONGO_Collection].insert(result)
        print("存储成功")
    except Exception:
        print("存储失败")

def get_html(i):
    headers = {
        'x-requested-with': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36',
        'cookie':'_ga=GA1.2.1332905533.1528701127; route=6098c46d36cc5825a33759f7c165d408; _gid=GA1.2.1710498787.1528795610; Hm_lvt_17521045a35cb741c321d016e40c7f95=1528701129,1528795610,1528874985; JOSESSIONID=2D1A9F3D1488671C95B8BAD861CAD404-n1; Hm_lpvt_17521045a35cb741c321d016e40c7f95=1528884226'
    }
    rurl = 'https://search.jobmd.cn/s?wd='+ keyword +'&locations=420000&t='+str(i)
    r = requests.get(rurl,headers=headers)
    r.encoding = 'utf-8'
    soup = BeautifulSoup(r.text,'lxml')
    return soup

def get_detail(url):
    rd = requests.get(url)
    rd.encoding = 'utf-8'
    rdsoup = BeautifulSoup(rd.text,'lxml')
    return rdsoup

def get_page_num():
    soup = get_html(i=1)
    sum = soup.find('span', class_='num').text
    return sum

def get_jobs():
    soup = get_html(i)
    form = soup.find('div',class_ = 'list-box mt10 list-box-B')
    try:
        divs = form.select('.list-dl')
        for div in divs:
            # 基础信息
            jobname = div.find('a',class_='item-jobs-name').text
            hosname = div.find('a',class_='item-ent-name').text
            jobloc = div.find('span',class_='job-loc').text
            jobcate = div.find('span', class_='job-cate').text
            jobedu = div.find('span',class_='job-edu').text
            workyear = div.find('span',class_='job-work').text
            jobcontents = div.find('div',class_='job-class-content')
            posttime = jobcontents.find('span', class_='job-class-meta').text

            # 职位详情
            jobhref = div.find('a',class_='show-more')['href']
            rdsoup = get_detail(jobhref)
            items = rdsoup.find('div', class_='box-info_base')
            salary = items.find('span').text.strip().replace(',','')
            try:
                salarys = salary.split('-')
                salary_low = int(salarys[0])
                salary_high = int(salarys[1])
            except:
                salary_low = salary
                salary_high = None
                print('薪资为面议')
            jd = rdsoup.find('div', class_='work-content_box-detail c-box').text.strip()

            # 公司详情
            hosinfo = rdsoup.find('div',class_='work-hospital_info')
            hosinfos = hosinfo.find_all('span')
            industry = hosinfos[0].text
            hoslevel = hosinfos[1].text
            stuffcount = hosinfos[2].text

            # 创建dict。因为mongodb基于js
            dic = {
                '职位':jobname.strip(),
                '薪资最低':salary_low,
                '薪资最高':salary_high,
                '地址':jobloc.strip()[6:],
                '专业要求':jobcate.strip()[6:],
                '学历':jobedu.strip()[5:],
                '工作年限':workyear.strip()[5:-1],
                '招聘开始时间':posttime.replace("\n","")[0:10],
                '招聘截止时间':posttime.replace("\n","")[11:21],
                '职位链接': jobhref.strip(),
                '公司': hosname.strip(),
                '具体行业':industry.strip(),
                '医院类型':hoslevel.strip()[0:4],
                '医院级别':hoslevel.strip()[5:7],
                '公司人数':stuffcount.strip(),
                '职位详情': jd.replace('\n', ' '),
            }
            print(dic)
            save_to_mongo(dic)
    except Exception as err:
        print(err)
        print('可能是因为该岗位无数据')

def txt2list(file):
    with open(file,encoding='utf-8') as f:
        content = [line.strip() for line in f]
        return content

if __name__ == "__main__":
    worktime = time.strftime("%Y%m%d", time.localtime())

    SERVICE_ARGS = ['--load-images=false', '--disk-cache=true']
    client = pymongo.MongoClient('localhost', 27017)
    db = client['dingxiang']

    for keyword in txt2list('list.txt'):
        MONGO_Collection = worktime + '-' + keyword + '-' + '湖北'
        pagenum = int(get_page_num())//20+1
        for i in range(1,pagenum+1):
            get_jobs()

