#!~/anaconda3/bin/python
# -*- coding: utf-8 -*-
import time
import requests
#import pandas as pd 
from dateutil.parser import parse
from linebot import LineBotApi
from linebot.models import TextSendMessage
from linebot.exceptions import LineBotApiError

#爬蟲參考
#http://bhan0507.logdown.com/posts/1291544-python-facebook-api

#在Facebook Graph API Exploer取得token(自己新建應用程式)
#參考
#http://blog.csdn.net/mochong/article/details/60872512
fb_token = 'EAAb9jaZBAP58BAAN72VXZAiog2IvZCIIJ634b07RIVxzg6aqE8LUPeJlMOVVwXWVwika2lLrvP2EN6pV8wqB32yMrWZAmPPvBvTkfIUee8ajQVw3RYPw0OOoarMw4YkYhjCPZBkNqkSo3mtEwYyLZB2P6vlQlq3souCwzVudbPeAZDZD'

#developers.line.me/console/取得Channel access token
#參考
#https://medium.com/@lukehong/%E5%88%9D%E6%AC%A1%E5%98%97%E8%A9%A6-line-bot-sdk-eaa4abbe8d6e
line_bot_api = LineBotApi('9eJEqukDtah4hl8la1VDq3NAAVymZVwQdDe6yGHK7AglA42gcdwgWTR/IJghRopBx0rHLbzPQR8iUlHp3Fi/txa0Fvh+9tjX51GaihlYIYFFB4ZD3eznPRnAo5VLutpVqNBXpCcF4/0mHy0Yph3UFwdB04t89/1O/w1cDnyilFU=')

#在Facebook Graph API Exploer取得粉絲專頁的id與名稱，並將其包成字典dic
fanspage_dic = {'169635866411766':'原價屋coolpc'}
user_dic = { 'user1': 'Ubea7f50235253321cc11825e391e92dd','user2':'U598174990ef971f4055bbba84db57a3e'}

#log檔
f=open('/home/user/fb_log.txt','a+')
old_str=""
new_str=""
while(True):
    try:
        #營業時間才爬
        tm_now=int(time.strftime("%H", time.localtime()))         
        if(tm_now>=10 and tm_now<=22):
            information_list = []
            #使用for迴圈依序讀取粉絲頁的資訊 可以一次爬多個粉絲頁
            for fanspage_number in fanspage_dic:
                #print出res會以json形式回傳 最外層為data
                #用format把id與token傳入{}裡
                res = requests.get('https://graph.facebook.com/v2.12/{}/posts?limit=1&access_token={}'.format(fanspage_number, fb_token))                
                for information in res.json()['data']:
                    if 'message' in information:
                        #判斷PO文日期是否為當天日期
                        if(str(time.strftime("%Y-%m-%d", time.localtime()))==str(parse(information['created_time']).date())):
                            #原價屋,PO文內容,PO文時間
                            information_list.append([fanspage_dic[fanspage_number], information['message'], parse(information['created_time']).date()])                           
                            #debug用 抓取下來的第一篇文章存入字串 下次重新比對 相同則代表沒有新的PO文
                            new_str=information['message']
                            if(old_str!=new_str):
                                #字串檢查 是否包含指定字串 沒有返回-1
                                if(str.find(information['message'],'限量搶購')!=-1):
                                    print('出現 "限量" 字串')
                                    line_bot_api.multicast([user_dic['user1'],user_dic['user2']],TextSendMessage(text='出現 "限量" 字串'))
                                    f.write(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' 出現 "限量" 字串\n')
                                if(str.find(information['message'],'顯示卡')!=-1):
                                    print('出現 "顯示卡" 字串')
                                    line_bot_api.multicast([user_dic['user1'],user_dic['user2']],TextSendMessage(text='出現 "顯示卡" 字串'))
                                    f.write(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' 出現 "顯示卡" 字串\n')
                                if(str.find(information['message'],'不貼保固貼紙')!=-1):
                                    print('出現 "不貼保固貼紙" 字串')
                                    line_bot_api.multicast([user_dic['user1'],user_dic['user2']],TextSendMessage(text='出現 "不貼保固貼紙" 字串'))
                                    f.write(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' 出現 "不貼保固貼紙" 字串\n')
                                old_str=new_str
                                #print("Yes")
                            else:
                                pass
                                #print("No")
            f.write(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' run\n')
            print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' run')
            #最後將list轉換成dataframe，並輸出成csv檔
            #information_df = pd.DataFrame(information_list, columns=['粉絲專頁', '發文內容', '發文時間']) 
            #information_df.to_csv('Data Visualization Information.csv', index=False)
            time.sleep(60)
        else:
            f.write(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' sleep time\n')
            print(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' sleep time')
            time.sleep(600)
    except Exception as e:
        print (e)
        f.write(str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))+' exception | '+str(e)+'\n')