# -*- coding: utf-8 -*-
import time
import requests
from dateutil.parser import parse
from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError

# 爬蟲參考
# http://bhan0507.logdown.com/posts/1291544-python-facebook-api

# 在Facebook Graph API Exploer取得token(自己新建應用程式)，最長只能到2個月
# 參考
# http://blog.csdn.net/mochong/article/details/60872512
fb_token = 'EAAb9jaZBAP58BAAN72VXZAiog2IvZCIIJ634b07RIVxzg6aqE8LUPeJlMOVVwXWVwika2lLrvP2EN6pV8wqB32yMrWZAmPPvBvTkfIUee8ajQVw3RYPw0OOoarMw4YkYhjCPZBkNqkSo3mtEwYyLZB2P6vlQlq3souCwzVudbPeAZDZD'

# developers.line.me/console/取得Channel access token
# 參考
# https://medium.com/@lukehong/%E5%88%9D%E6%AC%A1%E5%98%97%E8%A9%A6-line-bot-sdk-eaa4abbe8d6e
line_bot_api = LineBotApi('9eJEqukDtah4hl8la1VDq3NAAVymZVwQdDe6yGHK7AglA42gcdwgWTR/IJghRopBx0rHLbzPQR8iUlHp3Fi/txa0Fvh+9tjX51GaihlYIYFFB4ZD3eznPRnAo5VLutpVqNBXpCcF4/0mHy0Yph3UFwdB04t89/1O/w1cDnyilFU=')

# 在Facebook Graph API Exploer取得粉絲專頁的id與名稱，並將其包成字典dic
fanspage_dic = {'169635866411766':'原價屋coolpc'}

user_dic = { 'user1': 'Ubea7f50235253321cc11825e391e92dd','user2':'U598174990ef971f4055bbba84db57a3e'}

# 測試用
# keyword_dic={0:'少女'}
keyword_dic={1:'限量搶購',2:'顯示卡',3:'不貼保固貼紙'}

# log記錄腳本執行的時間，改要儲存的路徑
f=open('fb_log.txt','a+')

# 舊文章與新文章
old_str=""
new_str=""

# log function，時間+狀態
def log(log_str):
    f.write(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+log_str+'\n')
    print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+log_str)
    
# 腳本可一直執行，直到主動中斷為止(ctrl+C)
while(True):
    try:
        # 營業時間才爬
        timer=int(time.strftime("%H", time.localtime()))         
        if(timer>=10 and timer<=22):
            # 使用for迴圈依序讀取粉絲頁的資訊，可以一次爬多個粉絲頁
            for fanspage in fanspage_dic:
                # print出res會以json形式回傳，最外層為data
                # format把id、token和爬取的貼文數傳入{}裡，因為只確認是否有新貼文 基本上只爬1篇
                res = requests.get('https://graph.facebook.com/v2.12/{}/posts?limit={}&access_token={}'.format(fanspage, 1, fb_token))                
                for information in res.json()['data']:
                    if 'message' in information:
                        # 判斷PO文日期是否為當天日期，若不判斷可能會爬到昨日的貼文
                        if(str(time.strftime("%Y-%m-%d", time.localtime()))==str(parse(information['created_time']).date())):
                            # 原價屋,PO文內容,PO文時間                
                            # 抓取下來的第一篇文章存入字串，下次重新比對，相同則代表沒有新的PO文
                            new_str=information['message']
                            if(old_str!=new_str):
                                # 字串檢查，是否包含指定字串，沒有返回-1
                                # -----測試-----
                                # if(str.find(information['message'],keyword_dic[0])!=-1):
                                #     line_bot_api.multicast([user_dic['user1']],TextSendMessage(text="出現 {} 字串".format(keyword_dic[0])))
                                #     log(" 出現 {} 字串".format(keyword_dic[0]))
                                # -----測試-----
                                # 判斷多個關鍵字
                                for key in keyword_dic:
                                    if(str.find(information['message'],keyword_dic[key])!=-1):
                                        # 不使用line_bot_api.multicast，比較麻煩，改用迴圈傳給單一使用者
                                        for user in user_dic:
                                            line_bot_api.push_message(user_dic[user],TextSendMessage(text="出現 {} 字串".format(keyword_dic[key])))
                                        log(" 出現 {} 字串".format(keyword_dic[key]))
                                old_str=new_str
                                # print("Yes")
                            else:
                                pass
                                # print("No")
            log(" run")
            # 建議1~2分鐘執行一次
            time.sleep(120)
        else:
            log(" sleep time")
            time.sleep(600)

    except Exception as e:
        log(" "+str(e))