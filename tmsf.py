# -*- coding: utf-8 -*-

import re
import urllib2
import sqlite3
import random
import threading
from bs4 import BeautifulSoup

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

# Some User Agents
hds = [{'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}, \
       {'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'}, \
       {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'}, \
       {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'}, \
       {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'}, \
       {'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'}, \
       {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'}, \
       {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'}, \
       {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'}, \
       {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'}, \
       {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'}, \
       {'User-Agent': 'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'}, \
       {'User-Agent': 'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]

# 杭州区域列表
#regions = [u"xihu", u"xiacheng", u"jianggan", u"gongshu", u"shangcheng", u"binjiang", u"yuhang", u"xiaoshan"]
regions = [u'binjiang']

dict = {"xihu": "西湖", "xiacheng": "下城", "jianggan": "江干", "gongshu": "拱墅", "shangcheng": "上城",
        "binjiang": "滨江", "yuhang": "余杭", "xiaoshan": "萧山"}

lock = threading.Lock()

class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self, path, command='', *args, **kwargs):
        self.lock = threading.RLock()  # 锁
        self.path = path  # 数据库连接参数

        if command != '':
            conn = self.get_conn()
            cu = conn.cursor()
            cu.execute(command)

    def get_conn(self):
        conn = sqlite3.connect(self.path)  # ,check_same_thread=False)
        conn.text_factory = str
        return conn

    def conn_close(self, conn=None):
        conn.close()

    def conn_trans(func):
        def connection(self, *args, **kwargs):
            self.lock.acquire()
            conn = self.get_conn()
            kwargs['conn'] = conn
            rs = func(self, *args, **kwargs)
            self.conn_close(conn)
            self.lock.release()
            return rs
        return connection

    @conn_trans
    def execute(self, command, method_flag=0, conn=None):
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0], command[1])
            conn.commit()
        except sqlite3.IntegrityError, e:
            # print e
            return -1
        except Exception, e:
            print e
            return -2
        return 0

    @conn_trans
    def fetchall(self, command="select name from xiaoqu", conn=None):
        cu = conn.cursor()
        lists = []
        try:
            cu.execute(command)
            lists = cu.fetchall()
        except Exception, e:
            print e
            pass
        return lists

def gen_ershoufang_insert_command(info_dict):
    """
    生成二手房在售数据库插入命令
    """
    info_list = [u'城区', u'板块', u'小区名称', u'户型', u'价格', u'面积',u'均价', u'朝向', u'装修',
                 u'电梯', u'楼层', u'建造时间', u'关注人数', u'看房次数', u'发布时间', u'链接', u'备注', u'标识' ]
    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append("")
    t = tuple(t)
    command = (r"insert into ershoufang values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", t)
    return command


def ershoufang_spider(db_ershoufang, url_page, region):
    """
    爬取单一页面链接中的成交记录
    """
    try:
        req = urllib2.Request(url_page, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib2.urlopen(req, timeout=10).read()
        plain_text = unicode(source_code)
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('ershoufang_spider', url_page)
        return
    except Exception, e:
        print e
        exception_write('ershoufang_spider', url_page)
        return

    ershoufang_list = soup.findAll("div", {"class": "info clear"})

    for ershoufang in ershoufang_list:
        info_dict = {}

        houseTitle = ershoufang.find("div", {"class": "title"})
        href = houseTitle.a.get("href")

        info_dict.update({u'链接': href})
        info_dict.update({u'城区': dict[region]})

        total_price = ershoufang.find("div", {"class": "totalPrice"}).text
        info_dict.update({u'价格': total_price})

        house_info = ershoufang.find("div", {"class": "houseInfo"}).get_text().strip()
        info = house_info.split("|")
        if info:
            info_dict.update({u'小区名称': info[0]})
            info_dict.update({u'户型': info[1].strip()})
            info_dict.update({u'面积': info[2].strip()})
            info_dict.update({u'朝向': info[3].strip()})
            info_dict.update({u'装修': info[4].strip()})
            info_dict.update({u'电梯': info[-1].strip()})

        position_info = ershoufang.find("div", {"class": "positionInfo"}).text
        info = position_info.strip().split(")")
        floor= info[0].strip()+")"
        buildtime = info[-1].split("-")[0].strip()
        info_dict.update({u'楼层': floor})
        info_dict.update({u'建造时间': buildtime})
        bankuai = info[-1].split("-")[-1]
        info_dict.update({u'板块': bankuai})

        follow_info = ershoufang.find("div", {"class": "followInfo"}).get_text().split("/")
        concerned = follow_info[0].strip()
        visit = follow_info[1].strip()
        date = follow_info[2].strip()
        info_dict.update({u'关注人数': concerned})
        info_dict.update({u'看房次数': visit})
        info_dict.update({u'发布时间': date})

        unitPrice = ershoufang.find("div", {"class": "unitPrice"}).find("span").text
        info_dict.update({u'均价': unitPrice})

        detail = ershoufang.find("div", {"class": "tag"}).text
        if detail:
            info_dict.update({u'备注': detail})

        command = gen_ershoufang_insert_command(info_dict)
        db_ershoufang.execute(command, 1)


def region_ershoufang_spider(db_ershoufang, region):
    """
    爬取一个城区中的所有二手房信息
    """
    url = u"http://hz.lianjia.com/ershoufang/" + region + "/"

    try:
        req = urllib2.Request(url, headers=hds[random.randint(0, len(hds) - 1)])
        source_code = urllib2.urlopen(req, timeout=10).read()
        plain_text = unicode(source_code)
        soup = BeautifulSoup(plain_text, "html.parser")
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write("region_ershoufang_spider", region)
        return
    except Exception, e:
        print e
        exception_write("region_ershoufang_spider", region)
        return

    try:
        page_info= soup.find("div", {"class": "page-box house-lst-page-box"})
    except AttributeError as e:
        page_info = None
    if page_info == None:
        return None
    page_info_str = page_info.get("page-data").split(",")[0].split(":")[1]
    total_pages = int(page_info_str)

    threads = []
    for i in range(total_pages):
        url_page = u"http://hz.lianjia.com/ershoufang/%s/pg%d/" % (region, i + 1)
        #print "在爬第%d页" % (i+1)
        t = threading.Thread(target=ershoufang_spider, args=(db_ershoufang, url_page, region))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def regions_ershoufang_spider(db_ershoufang):
    """
    批量爬取小区成交记录
    """
    for region in regions:
        region_ershoufang_spider(db_ershoufang, region)
        print '已经爬取了%s区在售二手房信息' % dict[region]
    print 'done'


def exception_write(fun_name, url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt', 'a')
    line = "%s %s\n" % (fun_name, url)
    f.write(line)
    f.close()
    lock.release()


if __name__ == "__main__":

    command = "create table if not exists ershoufang (region TEXT primary key UNIQUE, biacircle TEXT, name TEXT, " \
              "style TEXT, price TEXT, area TEXT, unit_price TEXT, orientation TEXT, decoration TEXT, elevator TEXT, " \
              "floor TEXT, build_time TEXT, concerned TEXT, visit TEXT, date TEXT, href TEXT, detail TEXT, flag TEXT )"

    db_ershoufang = SQLiteWraper('lianjia-ershoufang.db', command)

    regions_ershoufang_spider(db_ershoufang)
