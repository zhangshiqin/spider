import requests
import random
import time
import datetime
import pymongo
import re
import csv
import math
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import os
import tarfile

today = time.strftime("%Y%m%d", time.localtime())
now_time = datetime.datetime.now()
yes_time = now_time + datetime.timedelta(days=-1)
yesterday = yes_time.strftime('%Y%m%d')

# 将结果插入mongodb
def save_to_mongo(result):
    try:
        db[MONGO_Collection].insert(result)
        print("存储成功")
    except Exception:
        print("存储失败")
        
# 获取拉勾的json文件        
def lagou(city, positionName, pageNo, pageSize):
    cookies = {
        "Cookie": "user_trace_token=20180122190616-4411246f-ff64-11e7-b4a1-525400f775ce; LGUID=20180122190616-44112bfa-ff64-11e7-b4a1-525400f775ce; X_HTTP_TOKEN=2d753a5ac2b3e6e2423fcb4022407518; gate_login_token=197ff91d128acf987e1608ff3bf9333c3c2c1b88eabfedfb; index_location_city=%E6%9D%AD%E5%B7%9E; PRE_UTM=m_cf_cpt_baidu_pc; PRE_HOST=bzclk.baidu.com; PRE_SITE=http%3A%2F%2Fbzclk.baidu.com%2Fadrc.php%3Ft%3D06KL00c00f7Ghk60yUKm0FNkUsKKdyNp00000PW4pNb00000LbFd7H.THL0oUh11x60UWdBmy-bIy9EUyNxTAT0T1Y3nh7bmvcLmH0snj0LryRk0ZRqPjNKwH0LwbN7fH7Awbw7PjKafRDsfbc3PDPKf1I7n1b0mHdL5iuVmv-b5Hnsn1nznjR1njfhTZFEuA-b5HDv0ARqpZwYTZnlQzqLILT8UA7MULR8mvqVQ1qdIAdxTvqdThP-5ydxmvuxmLKYgvF9pywdgLKW0APzm1YzP10LPf%26tpl%3Dtpl_10085_15730_11224%26l%3D1500117464%26attach%3Dlocation%253D%2526linkName%253D%2525E6%2525A0%252587%2525E9%2525A2%252598%2526linkText%253D%2525E3%252580%252590%2525E6%25258B%252589%2525E5%25258B%2525BE%2525E7%2525BD%252591%2525E3%252580%252591%2525E5%2525AE%252598%2525E7%2525BD%252591-%2525E4%2525B8%252593%2525E6%2525B3%2525A8%2525E4%2525BA%252592%2525E8%252581%252594%2525E7%2525BD%252591%2525E8%252581%25258C%2525E4%2525B8%25259A%2525E6%25259C%2525BA%2526xp%253Did%28%252522m6c247d9c%252522%29%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FH2%25255B1%25255D%25252FA%25255B1%25255D%2526linkType%253D%2526checksum%253D220%26ie%3Dutf-8%26f%3D3%26tn%3Dbaiduhome_pg%26wd%3D%25E6%258B%2589%25E9%2592%25A9%25E7%25BD%2591%26oq%3D%2525E6%25258B%252589%2525E5%25258B%2525BE%2525E7%2525BD%252591%2525E7%252588%2525AC%2525E8%252599%2525AB%26rqlang%3Dcn%26prefixsug%3D%2525E6%25258B%252589%2525E9%252592%2525A9%2525E7%2525BD%252591%26rsp%3D1%26inputT%3D277; PRE_LAND=https%3A%2F%2Fwww.lagou.com%2F%3Futm_source%3Dm_cf_cpt_baidu_pc; fromsite=bzclk.baidu.com; utm_source=""; JSESSIONID=ABAAABAAAFDABFG399ECCCCEDB8F54778C63A5054EDD7B0; _putrc=D71643F76AF6F41F; login=true; unick=%E6%9D%A8%E5%87%8C%E9%94%8B; _ga=GA1.2.253347541.1516619174; _gid=GA1.2.2037776006.1516619174; _ga=GA1.3.253347541.1516619174; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1516619174,1516619191,1516623170,1516623176; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1516623224; LGSID=20180122201252-91c897a5-ff6d-11e7-a5bc-5254005c3644; LGRID=20180122201345-b1ca9540-ff6d-11e7-a5bc-5254005c3644"
    }
    useragents = [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 9_2 like Mac OS X) AppleWebKit/601.1 (KHTML, like Gecko) CriOS/47.0.2526.70 Mobile/13C71 Safari/601.1.46",
        "Mozilla/5.0 (Linux; U; Android 4.4.4; Nexus 5 Build/KTU84P) AppleWebkit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0)"
    ]
    req_url = "https://m.lagou.com/search.json?"
    params = {
        "city": city,
        "positionName": positionName,
        "pageNo": pageNo,
        "pageSize": pageSize
    }
    headers = {
        'Host': 'm.lagou.com',
        'Origin': 'https://m.lagou.com/search.html',
        'User-Agent': random.choice(useragents)
    }
    res = requests.get(url=req_url, headers=headers,
                       params=params, cookies=cookies)  # f发送请求
    res_json = res.json()  # 获取json数据
    return res_json  # 返回json数据

# 获取json文件中的招聘数据
def get_result(res_json):
    result = res_json['content']['data']['page']['result']
    return result  # 返回json数据
    
# 获取查询结果的实际页数
def get_page(res_json):
    totalcount = int(res_json['content']['data']['page']['totalCount'])
    page = int(math.ceil(totalcount/15))
    return page
    
# 将mongo中昨日数据导出到csv
def export_to_csv(file):
    projectionFields = {'_id': False, 'positionName': True, 'createTime': True, 'companyName': True, 'city': True,
                        'salary_low': True,'salary_high':True,'salary_avg':True}
    data = db[MONGO_Collection].find({'createTime': re.compile(yesterday)}, projection=projectionFields)

    csvfile = open(file, 'w', newline='', encoding='utf-8')
    writer = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_ALL)
    for line in data:
        res = [line[key] for key in ['positionName', 'createTime', 'companyName', 'city','salary', 'salary_low','salary_high','salary_avg']]
        writer.writerow(res)
    print('已导出到data文件夹下的csv中')

# 打包csv文件
def make_targz(output_filename, source_dir):
    with tarfile.open(output_filename, "w") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def send_mail():
    smtp_server = 'smtp.exmail.qq.com'
    from_mail = 'zhangshiqin@rulertech.com'
    mail_pass = '*********'#第三方授权码
    to_mail = ['zhangshiqin@rulertech.com']
    subject = yesterday+'-spider-jobdata'
    message = MIMEMultipart()
    message['From'] = Header("诗琴", 'utf-8')
    message['To'] = Header("Isabella", 'utf-8')
    message['Subject'] = Header(subject, 'utf-8')
    # 邮件正文内容
    message.attach(MIMEText(yesterday+' '+'job data', 'plain', 'utf-8'))

    # 构造附件
    att1 = MIMEText(open('data/'+attrname, 'rb').read(), 'base64', 'utf-8')
    att1["Content-Type"] = 'application/octet-stream'
    att1["Content-Disposition"] = 'attachment; filename='+attrname
    message.attach(att1)

    s = smtplib.SMTP_SSL()
    s.connect(smtp_server, '465')
    s.login(from_mail, mail_pass)
    s.sendmail(from_mail, to_mail, message.as_string())
    print('邮件已发送')
    s.quit()

def update(data):
    return db[MONGO_Collection].update_one({"_id": data['_id']}, {"$set": data})
    
# 处理时间格式
def clear_time():
    items = db[MONGO_Collection].find({})
    for item in items:
        item['createTime'] = item['createTime'].replace('今天', today)
        item['createTime'] = item['createTime'].replace('昨天', yesterday)
        item['createTime'] = item['createTime'].replace('-', '')
        update(item)
    print('time ok')
    
# 处理薪资格式
def clear_salary():
    items = db[MONGO_Collection].find({})
    for item in items:
        if type(item['salary']) == type({}):
            continue
        salary_list = item['salary'].lower().replace("k", "000").split("-")
        if len(salary_list) != 2:
            print(salary_list)
            continue
        try:
            salary_list = [int(x) for x in salary_list]
        except:
            print(salary_list)
            continue
        item['salary_low'] = salary_list[0]
        item['salary_high'] = salary_list[1]
        item['salary_avg'] = (salary_list[0] + salary_list[1]) / 2
        update(item)
    print('salary ok')

if __name__ == "__main__":
    pageSize = 15
    positions =['产品经理','python工程师']
    path = 'data/' + yesterday + '/'
    #如果没有path路径，则系统生成
    if not os.path.exists(path):
        os.mkdir(path)
    for position in positions:
        SERVICE_ARGS = ['--load-images=false', '--disk-cache=true']
        client = pymongo.MongoClient('localhost',27017)
        db = client['lagou']
        db.authenticate("usr","pwd")
        MONGO_Collection = position + today
        cities = [ '上海','北京','深圳', '广州', ]#'成都', '重庆', '杭州', '武汉','苏州','西安','天津', '南京','郑州','长沙','沈阳','青岛','宁波','东莞','无锡', '厦门']
        for city in cities:
            res_json1 = lagou(city, position, 1, pageSize)
            page = get_page(res_json1)
            for i in range(page):
                res_json = lagou(city, position, i, pageSize)
                result = get_result(res_json)
                save_to_mongo(result)
                time.sleep(1)
        # 清理时间和薪资格式
        clear_time()
        clear_salary()
        #筛选导出昨日职位数据到csv
        filename = position + yesterday
        file = path + filename + '.csv'
        export_to_csv(file)
    #压缩
    sourcedir = 'data/' + yesterday
    output_filename = 'data/'+'Jobdata-'+yesterday+'.tar'
    attrname = 'Jobdata-'+yesterday+'.tar'

    make_targz(output_filename, sourcedir)
    # 发送邮件
    send_mail()
