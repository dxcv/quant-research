
import requests
from bs4 import BeautifulSoup
import re
import os
import pymongo
from io import StringIO
from dateutil.parser import parse

class News_Page_Get(object):

    def __init__(self, strIO, url):
        self.strIO = strIO
        self.url = url
        self.ERROR = False
        # 处理http://warrant.jrj.com.cn/news/2007-04-11/000002139717.html这类网页和404的网页
        try:
            self.pageReq = requests.get(self.url)  # 如果出现http://warrant...的网页，则出现requests.exceptions.ConnectionError
            self.status_code = self.pageReq.status_code  # 可以连接，但是404
        except requests.exceptions.ConnectionError:
            self.status_code = 4040  # 连接报错，定义为4040

        if re.findall(r'http://www.jrj.com.cn/error.shtml', self.pageReq.text) != [] \
                or re.findall("<script>window.location.href='(.*?)';</script>", self.pageReq.text) != []:  # 跳转的
            self.status_code = 404

    def get_passages_list(self):

        self.page_bs = BeautifulSoup(self.pageReq.text, 'html.parser')

        # 这个网页的标签变动了，有两种情况
        if len(self.page_bs.find_all('div', class_='newsCon')) == 1:
            self.passagesList = self.page_bs.find_all('div', class_='newsCon')[0].find_all('p')
        elif len(self.page_bs.find_all('div', class_='newsCon')) == 2:
            self.passagesList = self.page_bs.find_all('div', class_='newsCon')[0].find_all('p')
        elif len(self.page_bs.find_all('div', class_='newsCon')) == 3:
            self.passagesList = self.page_bs.find_all('div', class_='newsCon')[1].find_all('p')
        elif len(self.page_bs.find_all('div', class_='texttit_m1')) == 1:
            self.passagesList = self.page_bs.find_all('div', class_='texttit_m1')[0].find_all('p')
        elif len(self.page_bs.find_all('div', class_='textmain tmf14 jrj-clear')) == 1:
            self.passagesList = self.page_bs.find_all('div', class_='textmain tmf14 jrj-clear')[0].find_all('p')
        elif len(self.page_bs.find_all('p')) == 1:
            self.passagesList = self.page_bs.find_all('p')[0].text.split('\u3000\u3000')
        elif len(self.page_bs.find_all('div', class_='Detail')) == 1:
            self.passagesList = BeautifulSoup(self.pageReq.text, 'html.parser').find_all('div', class_='Detail')[0].text.split(
                '\u3000\u3000')
        elif len(self.page_bs.find_all('div', class_='fnomal')) == 1:
            self.passagesList = BeautifulSoup(self.pageReq.text, 'html.parser').find_all('div', class_='fnomal')[0].text.split(
                '\u3000\u3000')
        else:
            with open(r'D:\py36 projects\quant-research\tools\web_crawler\fail_record.txt','at') as f:
                f.write(self.url + ', content\n')
            self.ERROR = True
            return None
            # raise IndexError('出问题了！！')

        # 不是放在标签p里面的
        if self.passagesList == []:
            self.passagesList = self.page_bs.find_all(
                'div', class_='newsCon'
            )[0].text.replace('\u3000\u3000', '\n\u3000\u3000').split('\n')
            self.passagesList = [s for s in self.passagesList if s != '']

    def get_news_content(self):
        # 第一个页面布局
        if re.findall(r'<!--jrj_final_source_start-->(.*?)<!--jrj_final_source_end-->', self.pageReq.text, re.S) != []:
            self.src = \
                re.findall(r'<!--jrj_final_source_start-->(.*?)<!--jrj_final_source_end-->',
                           self.pageReq.text,
                           re.S
                           )[0].strip()
        # 第二个来源的识别格式
        elif re.findall(r'<font color=#800080>(.*?)</font>',self.pageReq.text,re.S) != []:
            self.src = re.findall(r'<font color=#800080>(.*?)</font>', self.pageReq.text, re.S)[0]
        # 第三个来源的识别格式
        elif re.findall(r'<span class="font1">(.*?)</span>',self.pageReq.text,re.S) != []:
            self.src = re.findall(r'<span class="font1">(.*?)</span>', self.pageReq.text, re.S)[0]
        # 第四个来源的识别格式
        elif re.findall(r'<em>(.*?)</em>', self.pageReq.text, re.S) != []:
            self.src = re.findall(r'<em>(.*?)</em>', self.pageReq.text, re.S)[0]
        elif re.findall(r'<span class="font1" id="mtly">(.*?)</span>', self.pageReq.text, re.S) != []:
            self.src = re.findall(r'<span class="font1" id="mtly">(.*?)</span>', self.pageReq.text, re.S)[0]
        else:
            with open(r'D:\py36 projects\quant-research\tools\web_crawler\fail_record.txt', 'at') as f:
                f.write(self.url + ', source\n')
            self.ERROR = True
            return None
            # raise ValueError('还有其他页面布局')

        self.get_passages_list()
        for passageI in range(len(self.passagesList)):
            # 将（更多个股业绩查询请点击）这些去掉
            try:
                self.strIO.write(re.sub(r'([（(][查更].+?请点击[）)])', '', self.passagesList[passageI].text) + '\n')
            except:
                self.strIO.write(re.sub(r'([（(][查更].+?请点击[）)])', '', self.passagesList[passageI]) + '\n')

        # 如果具体新闻页面有分页
        if re.findall(r'<!-- 分页节点区 start-->(.+?)<!-- 分页节点区 end-->', self.pageReq.text, re.S) == []:
            pass
        elif re.findall(r'上一页\d+?下一页', self.passagesList[passageI].text) == []:  # 另外一个有下一页的情形
            pass
        else:
            for newsNextPageI in range(1, 10):  # 最多估计就10页
                # newsNextPageI = 2
                self.url = re.sub(r'((?:-\d+?)?\.shtml$)', f'-{newsNextPageI}.shtml', self.url)
                self.pageReq = requests.get(self.url)
                self.status_code = self.pageReq.status_code
                if self.status_code == 404:
                    break

                # class标签的问题
                self.get_passages_list()

                for passageI in range(len(self.passagesList)):
                    # passageI = 5
                    self.strIO.write(re.sub(r'([（(][查更].+?请点击[）)])', '', self.passagesList[passageI].text) + '\n')


if __name__ == '__main__':

    # year = 2011

    for year in range(2011,2019):
        # 当年的日历页面，不容易变化
        year = 2011
        calendarUrl = f'http://stock.jrj.com.cn/xwk/{year}.shtml'
        calendarUrls_bs = BeautifulSoup(requests.get(calendarUrl).text, 'html.parser')
        calendarUrlsBfDiv = calendarUrls_bs.find_all('div', class_='cont')[0]
        dateList = calendarUrlsBfDiv.find_all('a')  # 每日的连接
        datePattern = re.compile(r'/(\d{8})_')

        # 连接数据库
        newsdb = pymongo.MongoClient("localhost", 27017).newsdb

        for dateListI in range(len(dateList))[334:]:
            # dateListI = 0

            # 拿那天的新闻的url
            dateStr = datePattern.findall(dateList[dateListI].get('href'))[0]
            someDateUrl = dateList[dateListI].get('href')
            # someDateUrl = 'http://stock.jrj.com.cn/xwk/200704/20070422_1.shtml'

            # 进入当日多条新闻的页面
            someDateNewsListWeb_bs = BeautifulSoup(requests.get(someDateUrl).text, 'html.parser')
            someDateNewsList = someDateNewsListWeb_bs.find_all('ul', class_='list')[0]
            someDateNewsList = someDateNewsList.find_all('li')
            if someDateNewsList == []:  # 今天没有内容
                print(dateStr+'没有新闻')
                continue
            print(dateStr, '第1页')

            # 爬下当日第一页的每一条新闻
            for someDateNewsI in range(len(someDateNewsList)):
                # someDateNewsI = 0

                # 找到每个新闻链接的list
                try:
                    certainNewsUrl = someDateNewsList[someDateNewsI].find_all('a')[1].get('href')
                except IndexError:
                    continue
                newsTitle = someDateNewsList[someDateNewsI].find_all('a')[1].text
                newsTitle = re.sub(r'^(\s)', r'', newsTitle)  # 去掉标题开头的空格
                newsDatetimeStr = someDateNewsList[someDateNewsI].find_all('span')[0].text
                # title = '收盘：广电电气跌9.98%报7.22元 换手0.79%'

                if newsdb.news_jrj.find({"title": newsTitle, "datetime": parse(newsDatetimeStr)}).count() == 0:
                    pass
                else:
                    print(f'\t{newsDatetimeStr}, {newsTitle}丢弃：已经存在')
                    continue

                # 删去报个股行情的那些条目
                if (not re.search(r'^快讯：.+?停 报于.+?元$', newsTitle) is None) or (
                not re.search(r'^收盘：.+?元 换手.+?%$', newsTitle) is None) \
                        or ('晨会纪要' in newsTitle) \
                        or ('晨会研究' in newsTitle) \
                        or ('大赛' in newsTitle) \
                        or ('日金股' in newsTitle):
                    print(f'\t{newsDatetimeStr}, {newsTitle}丢弃：收盘播报类')
                    continue

                # 具体页面不存在新闻内容
                '''
                测试
                certainNewsUrl = 'http://stock.jrj.com.cn/2010/03/2408087164910.shtml'
                newsStr = StringIO()
                newspage = News_Page_Get(newsStr, certainNewsUrl)
                newspage.get_news_content()
                a = newsStr.getvalue()
                print(a)
                '''
                newsStr = StringIO()
                newspage = News_Page_Get(newsStr, certainNewsUrl)
                if newspage.status_code == 404 or newspage.status_code == 4040:
                    print(f'\t{newsDatetimeStr}, {newsTitle}丢弃：网页不存在')
                    continue

                newspage.get_news_content()

                if newspage.ERROR is True:
                    print(f'\t{newsDatetimeStr}, {newsTitle}格式不对，已经记录')
                    continue


                # 写入mongodb
                newsdb.news_jrj.insert({'title': newsTitle,
                                              'date': parse(dateStr),
                                              'datetime': parse(newsDatetimeStr),
                                              'source': newspage.src,
                                              'tag': '缺省',
                                              'news_content': newsStr.getvalue()})
                print(f'\t{newsDatetimeStr}, {newsTitle}爬取成功')
                del newsStr

            # 新闻列表有下一页的
            page_newslib = someDateNewsListWeb_bs.find_all('p', class_='page_newslib')
            if page_newslib == []:
                pass
            else:
                page_newslib = page_newslib[0]

                page_newslib = page_newslib.find_all('a')
                for nextI in range(len(page_newslib)):
                    # nextI = 2
                    if page_newslib[nextI].text != '上一页' \
                            and page_newslib[nextI].text != '下一页' \
                            and page_newslib[nextI].text != '1':
                        print(dateStr, f'第{nextI}页')

                        someDateUrl = re.sub(f'/{dateStr[:6]}/(.*)$', f'/{dateStr[:6]}/' + page_newslib[nextI].get('href'),
                                             someDateUrl)
                        someDateNewsListWeb_bs = BeautifulSoup(requests.get(someDateUrl).text, 'html.parser')
                        someDateNewsList = someDateNewsListWeb_bs.find_all('ul', class_='list')[0]
                        someDateNewsList = someDateNewsList.find_all('li')
                        for someDateNewsI in range(len(someDateNewsList)):
                            # someDateNewsI = 0

                            # 找到每个新闻链接的list
                            try:
                                certainNewsUrl = someDateNewsList[someDateNewsI].find_all('a')[1].get('href')
                            except IndexError:
                                continue
                            newsTitle = someDateNewsList[someDateNewsI].find_all('a')[1].text
                            newsTitle = re.sub(r'^(\s)', r'', newsTitle)  # 去掉标题开头的空格
                            newsDatetimeStr = someDateNewsList[someDateNewsI].find_all('span')[0].text
                            # title = '收盘：广电电气跌9.98%报7.22元 换手0.79%'

                            if newsdb.news_jrj.find({"title": newsTitle, "datetime": parse(newsDatetimeStr)}).count() == 0:
                                pass
                            else:
                                print(f'\t{newsDatetimeStr}, {newsTitle}丢弃：已经存在')
                                continue

                            # 删去报个股行情的那些条目
                            if (not re.search(r'^快讯：.+?停 报于.+?元$', newsTitle) is None) or (
                                    not re.search(r'^收盘：.+?元 换手.+?%$', newsTitle) is None) \
                                    or ('晨会纪要' in newsTitle) \
                                    or ('晨会研究' in newsTitle) \
                                    or ('大赛' in newsTitle) \
                                    or ('日金股' in newsTitle):
                                print(f'\t{newsDatetimeStr}, {newsTitle}丢弃：收盘播报类')
                                continue

                            # 具体页面不存在新闻内容
                            newsStr = StringIO()
                            newspage = News_Page_Get(newsStr, certainNewsUrl)

                            if newspage.status_code == 404 or newspage.status_code == 4040:
                                print(f'\t{newsDatetimeStr}, {newsTitle}丢弃：网页不存在')
                                continue

                            newspage.get_news_content()

                            if newspage.ERROR is True:
                                print(f'\t{newsDatetimeStr}, {newsTitle}格式不对，已经记录')
                                continue

                            # 写入mongodb
                            newsdb.news_jrj.insert({'title': newsTitle,
                                                          'date': parse(dateStr),
                                                          'datetime': parse(newsDatetimeStr),
                                                          'source': newspage.src,
                                                          'tag': '缺省',
                                                          'news_content': newsStr.getvalue()})
                            print(f'\t{newsDatetimeStr}, {newsTitle}爬取成功')
                            del newsStr

