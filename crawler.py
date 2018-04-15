#!/home/user/anaconda3/bin/python3.6
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import os
import re
import sys
import json
import requests
import argparse
import time
import codecs
from bs4 import BeautifulSoup
from six import u
import pymysql.cursors
from datetime import datetime as dtime
# connect to the database
connection = pymysql.connect(host='localhost',
                             user='user',
                             password='l0726',
                             db='ptt',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

#sql create table name=x_post
sql_post_create="""
CREATE TABLE `{table}_post` (
`po_id` bigint(100) DEFAULT NULL,
`date` datetime DEFAULT NULL,
`title` text COLLATE utf8_unicode_ci DEFAULT NULL,
`content` text COLLATE utf8_unicode_ci DEFAULT NULL,
`push_tag` smallint(5) DEFAULT NULL,
`boo_tag` smallint(5) DEFAULT NULL,
`neutral_tag` smallint(5) DEFAULT NULL,
`count_tag` smallint(5) DEFAULT NULL,
`all_tag`  smallint(5) DEFAULT NULL,
`author` text COLLATE utf8_unicode_ci DEFAULT NULL,
`ip` text COLLATE utf8_unicode_ci DEFAULT NULL,
`index` int(10) DEFAULT NULL,
`url` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY  (`po_id`),
  UNIQUE(`url`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""

#sql post table maxid
sql_post_maxid="""
SELECT `po_id` FROM `{table}_post` order by `po_id` desc limit 1
"""

#sql post table insert
sql_post_insert="""
INSERT INTO `{table}_post` (`po_id`,`date`,`title`,`content`,`push_tag`,`boo_tag`,`neutral_tag`,`count_tag`,
`all_tag`,`author`,`ip`,`index`,`url`) 
VALUES ('{pid}','{date}','{title}','{content}','0','0','0','0','0','{author}','{ip}','{index}','{url}')
"""

#sql post table update
sql_post_update="""
UPDATE `{table}_post` SET `push_tag`={push_tag},`boo_tag`={boo_tag},`neutral_tag`={neutral_tag},`count_tag`={count_tag},`all_tag`={all_tag} WHERE url="{url}"
"""

#sql create table name=x_push
sql_push_create="""
CREATE TABLE `{table}_push` (
`po_id` bigint(100) DEFAULT NULL,
`pu_id` smallint(5) DEFAULT NULL,
`tag` text COLLATE utf8_unicode_ci DEFAULT NULL,
`content` text COLLATE utf8_unicode_ci DEFAULT NULL,
`userid` text COLLATE utf8_unicode_ci DEFAULT NULL,
`date` text COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY  (`po_id`,`pu_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
"""

#sql push table insert
sql_push_insert="""
INSERT INTO `{table}_push` (`po_id`,`pu_id`,`tag`,`content`,`userid`,`date`)
VALUES ('{po_id}','{pu_id}','{tag}','{content}','{userid}','{date}')
"""

#table關聯 暫時不使用 而且使用了會有bug(一直會重複一次) 不明原因
sql="""
ALTER TABLE PublicServan_post ADD CONSTRAINT test FOREIGN KEY (po_id) REFERENCES PublicServan_push (po_id) ON DELETE CASCADE ON UPDATE CASCADE
"""

#global變數
index=0
table=None

__version__ = '1.0'

# if python 2, disable verify flag in requests.get()
VERIFY = True
if sys.version_info[0] < 3:
    VERIFY = False
    requests.packages.urllib3.disable_warnings()

class PttWebCrawler(object):

    PTT_URL = 'https://www.ptt.cc'

    """docstring for PttWebCrawler"""
    def __init__(self, cmdline=None, as_lib=False):
        parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description='''
            A crawler for the web version of PTT, the largest online community in Taiwan.
            Input: board name and page indices (or articla ID)
            Output: BOARD_NAME-START_INDEX-END_INDEX.json (or BOARD_NAME-ID.json)
        ''')
        parser.add_argument('-b', metavar='BOARD_NAME', help='Board name', required=True)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-i', metavar=('START_INDEX', 'END_INDEX'), type=int, nargs=2, help="Start and end index")
        group.add_argument('-a', metavar='ARTICLE_ID', help="Article ID")
        parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + __version__)

        if not as_lib:
            if cmdline:
                args = parser.parse_args(cmdline)
            else:
                args = parser.parse_args()
            board = args.b

            #執行腳本先建立資料表 存在就略過
            #---------- sql ----------#
            global table
            table=board
            try:
                os.chdir("/home/user/ptt-web-crawler-master/PttWebCrawler/"+table+"/")
            except:
                os.chdir("/home/user/ptt-web-crawler-master/PttWebCrawler/")
                os.mkdir(table)
                os.chdir("/home/user/ptt-web-crawler-master/PttWebCrawler/"+table+"/")
            
            try:                
                cursor=connection.cursor()
                cursor.execute(sql_post_create.format(table=table))
                connection.commit()
                cursor.execute(sql_push_create.format(table=table))
                connection.commit()
                # print("資料表新增成功")
                
            except:
                #至少要執行一行敘述 pass是空指令
                pass
                # print("資料表已存在")
            #---------- sql ----------#

            if args.i:
                start = args.i[0]

                #index在每次執行後都會存到記事本檔 下次執行不論start為多少 會直接用記事本的值當start end每次都使用-1 抓目前最新的index
                #檔案不存在就會建立 存在就讀取
                #---------- file ----------#
                try:
                    start_index = open(table+"_index.txt", "r")
                    start=int(start_index.read())
                    # print("index檔案已存在 讀取index")
                except:
                    start_index = open(table+"_index.txt", "w+")
                    start_index.write(str(start))
                    start_index.close()
                    start_index = open(table+"_index.txt", "r")
                    start=int(start_index.read())
                    # print("index檔案不存在 建立檔案後寫入目前index並讀取")
                start_index.close()
                #---------- file ----------#
                
                if args.i[1] == -1:
                    end = self.getLastPage(board)
                else:
                    end = args.i[1]
                self.parse_articles(start, end, board)
            else:  # args.a
                article_id = args.a
                self.parse_article(article_id, board)
        
    def parse_articles(self, start, end, board, path='.', timeout=3):
            filename = board + '-' + str(start) + '-' + str(end) + '.json'
            filename = os.path.join(path, filename)
            # self.store(filename, u'{"articles": [', 'w')

            for i in range(end-start+1):

                check_time_H=time.strftime("%H", time.localtime())
                check_time_M=time.strftime("%M", time.localtime())
                # print(check_time)
                if(int(check_time_H)%4==0 and int(check_time_M)==0):
                    # print(int(check_time))
                    print("設定的時間到 結束程式")
                    print("time end")
                    print("----------")
                    sys.exit(0)

                #每次換index後就覆蓋
                #---------- file ----------#
                global index
                # index = start + i
                # q86tj/69d g3fu06j;3uf06au04q86xu;3u,4
                index = start-2 + i

                #---------- file ----------#

                time_now=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                time_log = open(table+"_log.txt", "a")
                try:
                    resp = requests.get(
                        url = self.PTT_URL + '/bbs/' + board + '/index' + str(index) + '.html',
                        cookies={'over18': '1'}, verify=VERIFY, timeout=timeout
                    )
                    print('Processing index:', str(index))
                    start_index = open(table+"_index.txt", "w+")
                    start_index.write(str(index))
                    start_index.close()
                    time_log.write(str(time_now)+" run successful,index="+str(index)+'\n')
                    time_log.close()
                except: 
                    time_log.write(str(time_now)+" run error,index="+str(index)+'\n')
                    time_log.close()
                    print("網路異常 程式終止")
                    print("network error")
                    print("----------")
                    sys.exit(0)
                else:
                    
                # resp = requests.get(
                #     url = self.PTT_URL + '/bbs/' + board + '/index' + str(index) + '.html',
                #     cookies={'over18': '1'}, verify=VERIFY, timeout=timeout
                # )
                    if resp.status_code != 200:
                        print('invalid url:', resp.url)
                        continue
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    divs = soup.find_all("div", "r-ent")
                    for div in divs:
                        try:
                            # ex. link would be <a href="/bbs/PublicServan/M.1127742013.A.240.html">Re: [問題] 職等</a>
                            href = div.find('a')['href']
                            link = self.PTT_URL + href
                            article_id = re.sub('\.html', '', href.split('/')[-1])
                            if div == divs[-1] and i == end-start:  # last div of last page
                                # self.store(filename, self.parse(link, article_id, board), 'a')
                                self.parse(link, article_id, board)
                            else:
                                # self.store(filename, self.parse(link, article_id, board) + ',\n', 'a')
                                self.parse(link, article_id, board)
                        except:
                            pass
                    time.sleep(0.1)
            # self.store(filename, u']}', 'a')
            print("已爬完指定頁面資料")
            print("finish")
            print("----------")
            sys.exit(0)
                
                # return filename

    def parse_article(self, article_id, board, path='.'):
        link = self.PTT_URL + '/bbs/' + board + '/' + article_id + '.html'
        filename = board + '-' + article_id + '.json'
        filename = os.path.join(path, filename)
        # self.store(filename, self.parse(link, article_id, board), 'w')
        self.parse(link, article_id, board)
        return filename

    @staticmethod
    def parse(link, article_id, board, timeout=3):
        # print('Processing article:', article_id)
        resp = requests.get(url=link, cookies={'over18': '1'}, verify=VERIFY, timeout=timeout)
        if resp.status_code != 200:
            print('invalid url:', resp.url)
            return json.dumps({"error": "invalid url"}, sort_keys=True, ensure_ascii=False)
        soup = BeautifulSoup(resp.text, 'html.parser')
        main_content = soup.find(id="main-content")
        metas = main_content.select('div.article-metaline')
        author = ''
        title = ''
        date = ''

        if metas:
            author = metas[0].select('span.article-meta-value')[0].string if metas[0].select('span.article-meta-value')[0] else author
            title = metas[1].select('span.article-meta-value')[0].string if metas[1].select('span.article-meta-value')[0] else title
            date = metas[2].select('span.article-meta-value')[0].string if metas[2].select('span.article-meta-value')[0] else date

            # remove meta nodes
            for meta in metas:
                meta.extract()
            for meta in main_content.select('div.article-metaline-right'):
                meta.extract()

        # remove and keep push nodes
        pushes = main_content.find_all('div', class_='push')
        
        for push in pushes:
            push.extract()

        try:
            ip = main_content.find(text=re.compile(u'※ 發信站:'))
            ip = re.search('[0-9]*\.[0-9]*\.[0-9]*\.[0-9]*', ip).group()
        except:
            ip = "None"

        # 移除 '※ 發信站:' (starts with u'\u203b'), '◆ From:' (starts with u'\u25c6'), 空行及多餘空白
        # 保留英數字, 中文及中文標點, 網址, 部分特殊符號
        filtered = [ v for v in main_content.stripped_strings if v[0] not in [u'※', u'◆'] and v[:2] not in [u'--'] ]
        expr = re.compile(u(r'[^\u4e00-\u9fa5\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\s\w:/-_.?~%()]'))
        for i in range(len(filtered)):
            filtered[i] = re.sub(expr, '', filtered[i])

        filtered = [_f for _f in filtered if _f]  # remove empty strings
        filtered = [x for x in filtered if article_id not in x]  # remove last line containing the url of the article
        content = ' '.join(filtered)
        content = re.sub(r'(\s)+', ' ', content)
        # print 'content', content
        # push messages
        p, b, n = 0, 0, 0
        messages = []

        #抓目前post table的po_id的最大值
        #---------- sql ----------#
        date=PttWebCrawler.date_to_numeric(date)
        #print("start")
        #print(date)
        cursor=connection.cursor()
        cursor.execute(sql_post_maxid.format(table=table))
        value = cursor.fetchall()
        # print(value)
        if value!=():
            number1=value[0]['po_id']
        else:
            number1=0
        number1=number1+1
        # print("number="+str(number1))
        #print("end")
        #post table insert
        try:
            cursor=connection.cursor()
            cursor.execute(sql_post_insert.format(table=table,pid=number1,date=date,title=title,content=content,author=author,ip=ip,index=index,url=link))
            connection.commit()
        except:
            # connection.rollback()
            pass
            #print("重複PO文 略過")
        
        #給push table的pu_id使用
        number2=0
        #---------- sql ----------#

        for push in pushes:

            if not push.find('span', 'push-tag'):
                continue
            push_tag = push.find('span', 'push-tag').string.strip(' \t\n\r')
            push_userid = push.find('span', 'push-userid').string.strip(' \t\n\r')
            # if find is None: find().strings -> list -> ' '.join; else the current way
            push_content = push.find('span', 'push-content').strings
            push_content = ' '.join(push_content)[1:].strip(' \t\n\r')  # remove ':'
            push_ipdatetime = push.find('span', 'push-ipdatetime').string.strip(' \t\n\r')
            messages.append( {'push_tag': push_tag, 'push_userid': push_userid, 'push_content': push_content, 'push_ipdatetime': push_ipdatetime} )

            #新增
            #---------- sql ----------#
            number2=number2+1
            # print(number2)
            #push table insert
            check_ip= "."
            search_first_space= " "
            new_date=""
            try:
                push_ipdatetime.index(check_ip)
                new_date=push_ipdatetime[push_ipdatetime.index(search_first_space)+1:len(push_ipdatetime)]
                # print("有ip 需要拆字串")
            except:
                new_date=push_ipdatetime
                # print("沒ip 不用拆")
            # print("原始")
            # print(push_ipdatetime)
            # print("結果")   
            # print(new_date)
            try:
                cursor=connection.cursor()
                cursor.execute(sql_push_insert.format(table=table,po_id=number1,pu_id=number2,tag=push_tag,content=push_content,userid=push_userid,date=new_date))
                connection.commit()
            except:
                # connection.rollback()
                pass
                #print("重複推文 略過")


            #---------- sql ----------#

            if push_tag == u'推':
                p += 1
            elif push_tag == u'噓':
                b += 1
            else:
                n += 1

        # count: 推噓文相抵後的數量; all: 推文總數
        message_count = {'all': p+b+n, 'count': p-b, 'push': p, 'boo': b, "neutral": n}

        #post table第一次insert不會加入tag的值 由這邊update
        #---------- sql ----------#
        cursor=connection.cursor()
        cursor.execute(sql_post_update.format(table=table,push_tag=p,boo_tag=b,neutral_tag=n,count_tag=p-b,all_tag=p+b+n,url=link))
        connection.commit()
        #---------- sql ----------#

        # print 'msgs', messages
        # print 'mscounts', message_count

        # json data
        data = {
            'url': link,
            'board': board,
            'article_id': article_id,
            'article_title': title,
            'author': author,
            'date': date,
            'content': content,
            'ip': ip,
            'message_conut': message_count,
            'messages': messages
        }
        # print 'original:', d
        return json.dumps(data, sort_keys=True, ensure_ascii=False)

    @staticmethod
    def getLastPage(board, timeout=3):
        content = requests.get(
            url= 'https://www.ptt.cc/bbs/' + board + '/index.html',
            cookies={'over18': '1'}, timeout=timeout
        ).content.decode('utf-8')
        first_page = re.search(r'href="/bbs/' + board + '/index(\d+).html">&lsaquo;', content)
        if first_page is None:
            return 1
        return int(first_page.group(1)) + 1

    @staticmethod
    def store(filename, data, mode):
        with codecs.open(filename, mode, encoding='utf-8') as f:
            f.write(data)

    @staticmethod
    def get(filename, mode='r'):
        with codecs.open(filename, mode, encoding='utf-8') as f:
            return json.load(f)

    #2018-3-15
    #date format bug fix
    #question
    #call function,classname.functionname
    #funk
    @staticmethod
    def date_to_numeric(date):
        #date = '07/06/2017 07:46:39'
        #date = 'Wed Nov  4 12:04:28 2011'
        #date = '※ 轉錄者: smallwo (71.212.4.14), 11/30/2016 14:40:18'
        temp = re.split(' ',date)
        if( len( temp[len(temp)-1] )!=4 or date == 'None' ):
            return 0
        if( re.search('※',date) ):
            #print(date)
            # r"[0-9]*/[0-9]*/[0-9]* [[0-9]*:[0-9]*:[0-9]*]*" 為正規化擷取文字
            date = str( re.findall( r"[0-9]*/[0-9]*/[0-9]* [[0-9]*:[0-9]*:[0-9]*]*" , date ) )        
            date = date.replace("['",'')
            date = date.replace("']",'')
            date = dtime.strptime(date,'%m/%d/%Y %H:%M:%S')
        else:
            date = date.replace('  ',' ')
            # 正規化 抓取月分與星期
            regex = re.compile('(?P<week>[a-zA-Z]+)\s+(?P<month>[a-zA-Z]+)')
            m = regex.search(date)
            month = m.group('month')
            week  = m.group('week')
            date = str( re.findall( r" [0-9]* [[0-9]*:[0-9]*:[0-9]* [0-9]*]*" , date ) )
            date = date.replace("['",'')
            date = date.replace("']",'')
            date = week + ' '+ month + date
            if( month == 'July' ):# 月份的簡寫, 在July比較特別, 需要額外處理
                # %a 是星期, %B是月份(非縮寫),  %d 是day, %H是小時, %M是分鐘, %S 是秒, %Y是年
                date = dtime.strptime(date,'%a %B %d %H:%M:%S %Y')
            else:# %b 是月份縮寫
                date = dtime.strptime(date,'%a %b %d %H:%M:%S %Y')
        # change to numeric by seconds
        value = ( date - dtime(1970,1,1) ).total_seconds()# 所有日期轉成秒, 便於比較
        return date
    
if __name__ == '__main__':
    c = PttWebCrawler()