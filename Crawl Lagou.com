# coding=utf-8
import json
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.setrecursionlimit(1000000)
import MySQLdb
import requests

# username = ''
# password = ''
# host = ''
# dbase = ''


headers = {
    'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36'
}
class lagou_spider:
    def __init__(self):
        self.dbconn = MySQLdb.connect(host=host, user=username, passwd=password, db=dbase, charset="utf8")
        self.cursor = self.dbconn.cursor()
        self.cursor1 = self.dbconn.cursor()
        self.totalPageCount = 0
        self.curpage = 0
        self.curkd = 0
        self.curcity = 0
        self.citylist = [
            'http://www.lagou.com/jobs/positionAjax.json?px=new&city=%E5%8C%97%E4%BA%AC&needAddtionalResult=false',
            'http://www.lagou.com/jobs/positionAjax.json?px=new&city=%E4%B8%8A%E6%B5%B7&needAddtionalResult=false',
            'http://www.lagou.com/jobs/positionAjax.json?px=new&city=%E5%B9%BF%E5%B7%9E&needAddtionalResult=false',
            'http://www.lagou.com/jobs/companyAjax.json?px=new&city=%E6%B7%B1%E5%9C%B3&needAddtionalResult=false',
            'http://www.lagou.com/jobs/positionAjax.json?px=new&city=%E6%9D%AD%E5%B7%9E&needAddtionalResult=false',
            'http://www.lagou.com/jobs/positionAjax.json?px=new&city=%E6%88%90%E9%83%BD&needAddtionalResult=false'
            ]  # 北上广深杭成
        self.myurl = self.citylist[0]
        self.crawlflag = 0  # proxy有效性
        self.address = None

        self.kds = []
        arg = ()
        self.cursor.callproc('lagou_keyword_get', arg)
        temp_data = self.cursor.fetchall()
        for kd_data in temp_data:
            self.kds.append(kd_data[0])
        self.kd = self.kds[0]
        self.cursor.close()

    def get_proxy(self):
        arg = ()
        self.cursor = self.dbconn.cursor()
        self.cursor.callproc('proxy_address_get', arg)
        address = self.cursor.fetchall()[0][0]
        self.cursor.close()
        return address

    def crawl_info(self):
        if self.crawlflag == 0:
            self.address = self.get_proxy()
        proxies = {
            'http': '{}'.format(self.address)
        }
        query = {
            'pn': self.curpage, 'kd': self.kd
        }
        print 'proxy:{},keyword:{},page:{}'.format(self.address,self.kd,self.curpage)
        try:
            requests.adapters.DEFAULT_RETRIES = 6
            resp = requests.post(
                self.myurl,
                data=query,
                headers=headers,
                proxies=proxies,
                timeout=15
            )
            jdict = json.loads(resp.content, encoding='UTF-8')
            jcontent = jdict["content"]
            jposresult = jcontent["positionResult"]
            jresult = jposresult["result"]
            self.totalPageCount = jposresult['totalCount'] / 15 + 1
            if self.totalPageCount > 30:
                self.totalPageCount = 30
            for each in jresult:
                city = each['city']
                companyId = each['companyId']
                companyName = each['companyShortName']
                industryField = each['industryField']
                financeStage = each['financeStage']  # 融资阶段
                companySize = each['companySize']
                companyUrl = 'http://www.lagou.com/gongsi/%s.html' % (str(each['companyId']))
                district = each['district']
                # 市区位置
                if len(str(each['businessZones'])) > 4:
                    businessZones = '-'.join(each['businessZones'])
                else:
                    businessZones = ''
                # 商区位置
                # 工作信息
                positionId = each['positionId']
                positionName = each['positionName']
                positionType = self.kd
                jobNature = each['jobNature']  # 工作类型
                workYear = each['workYear']
                # 把工资字符串（ak-bk）转成最大和最小值(a,b)
                sal = each['salary']
                sal = sal.split('-')
                if len(sal) == 1:
                    salaryMax = int(sal[0][:sal[0].find('k')])
                else:
                    salaryMax = int(sal[1][:sal[1].find('k')])
                salaryMin = int(sal[0][:sal[0].find('k')])
                salaryAvg = (salaryMin + salaryMax) / 2

                jobUrl = 'http://www.lagou.com/jobs/%s.html' % (str(each['positionId']))
                positionAdvantage = each['positionAdvantage']  # 工作福利
                if len(str(each['companyLabelList'])) > 4:
                    companyLabelList = '-'.join(each['companyLabelList'])
                else:
                    companyLabelList = ''
                # 公司福利
                createTime = each['createTime']

                self.cursor = self.dbconn.cursor()

                try:
                    job_arg = (
                        positionId,
                        positionName,
                        positionType,
                        industryField,
                        companyId,
                        companyName,
                        city,
                        district,
                        businessZones,
                        jobNature,
                        workYear,
                        salaryAvg,
                        salaryMin,
                        salaryMax,
                        jobUrl,
                        positionAdvantage,
                        companyLabelList,
                        createTime
                    )
                    print job_arg
                    self.cursor.callproc('lagou_jobinfo_insert', job_arg)
                except Exception:
                    err_arg = ('拉勾', jobUrl, sys.exc_info()[0], sys.exc_info()[1], '工作信息写入')
                    self.cursor.callproc('lagou_error_insert', err_arg)

                try:
                    company_arg = (
                        companyId,
                        companyName,
                        city,
                        district,
                        industryField,
                        companyUrl,
                        companySize,
                        financeStage,
                        businessZones
                    )
                    self.cursor1.callproc('lagou_companyinfo_insert', company_arg)
                except Exception:
                    err_arg = ('拉勾', jobUrl, sys.exc_info()[0], sys.exc_info()[1], '公司信息写入')
                    self.cursor1.callproc('lagou_error_insert', err_arg)

            self.crawlflag = 1
            self.cursor = self.dbconn.cursor()
            arg = (self.address,self.crawlflag)
            self.cursor.callproc('proxy_address_update',arg)
            self.cursor.close()
            return self.start()

        except Exception as err:
            print err
            self.crawlflag = 0
            self.cursor = self.dbconn.cursor()
            arg = (self.address,self.crawlflag)
            self.cursor.callproc('proxy_address_update',arg)
            err_arg = ('拉勾', self.address, sys.exc_info()[0], sys.exc_info()[1], '错误信息写入')
            self.cursor1.callproc('lagou_error_insert', err_arg)
            self.cursor.close()
            self.curpage -= 1
            return self.start()



    def start(self):
        if self.curpage <= self.totalPageCount:
            print self.totalPageCount
            self.curpage += 1
            self.crawl_info()
        elif self.curkd < len(self.kds)-1:
            self.curpage = 1
            self.totalPageCount = 0
            self.curkd += 1
            self.kd = self.kds[self.curkd]
            self.crawl_info()
        elif self.curcity < len(self.citylist)-1:
            print 'Change City'
            self.curpage = 1
            self.totalPageCount = 0
            self.curcity += 1
            self.myurl = self.citylist[self.curcity]

if __name__ == '__main__':
    lagou = lagou_spider()
    lagou.start()
