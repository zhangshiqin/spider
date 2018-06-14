# -*- coding: utf-8 -*-
'''
@Author: isabella
@Create time: 2018/6/14 14:40
'''


import pymongo
import os
import time
import xlwt

def export_2_excel():
    projectionFields = {'_id': False, '职位': True, '薪资最低': True, '薪资最高':True,'地址': True, '专业要求': True,
                        '学历': True, '工作年限': True, '招聘开始时间': True, '招聘截止时间': True, '职位链接': True,
                        '公司': True,'具体行业': True,'医院类型': True,'医院级别': True,'公司人数': True,'职位详情': True}
    data = db[MONGO_Collection].find(projection=projectionFields)
    sheet = book.add_sheet(filename, cell_overwrite_ok=True)
    rowtitle = ['职位', '薪资最低','薪资最高', '地址', '专业要求', '学历', '工作年限', '招聘开始时间','招聘截止时间','职位链接','公司','具体行业',
                '医院类型','医院级别','公司人数','职位详情' ]
    for t in range(len(rowtitle)):
        sheet.write(0,t,rowtitle[t])

    row_excel = 0
    for line in data:
        row_excel += 1
        res = [line[key] for key in ['职位', '薪资最低','薪资最高', '地址', '专业要求', '学历', '工作年限', '招聘开始时间','招聘截止时间',
                                     '职位链接','公司','具体行业','医院类型','医院级别','公司人数','职位详情' ]]
        print(res)
        len_res = len(res)
        col_excel = 0
        for j in range(len_res):
            sheet.write(row_excel, col_excel, res[j])
            col_excel += 1
            book.save(path + worktime +'-丁香人才网-'+ location + '.xls')

def txt2list(file):
    with open(file,encoding='utf-8') as f:
        content = [line.strip() for line in f]
        return content

if __name__ == "__main__":
    worktime = time.strftime("%Y%m%d", time.localtime())
    path = 'data/'
    if not os.path.exists(path):
        os.mkdir(path)
    SERVICE_ARGS = ['--load-images=false', '--disk-cache=true']
    client = pymongo.MongoClient('localhost', 27017)
    db = client['dingxiang']

    book = xlwt.Workbook(encoding='utf-8', style_compression=0)

    for company in txt2list('list.txt'):
        location = '湖北'
        MONGO_Collection = worktime  + '-'+company + '-' + location
        filename = company
        file = path + MONGO_Collection
        export_2_excel()