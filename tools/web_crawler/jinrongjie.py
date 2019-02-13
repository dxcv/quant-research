

if __name__ == '__main__':

    import requests
    from bs4 import BeautifulSoup
    import re
    import os

    year = 2016
    calendarUrl = f'http://stock.jrj.com.cn/xwk/{year}.shtml'

    # 日历页面
    calendarUrls_bs = BeautifulSoup(requests.get(calendarUrl).text)
    calendarUrlsBfDiv = calendarUrls_bs.find_all('div', class_='cont')[0]
    dateList = calendarUrlsBfDiv.find_all('a')  # 每日的连接
    datePattern = re.compile(r'/(\d{8})_')

    for dateListI in range(len(dateList)):
        # aI = 0
        # 进入某一天的页面
        dateStr = datePattern.findall(dateList[dateListI].get('href'))[0]
        someDateUrl = dateList[dateListI].get('href')
        someDateNewsListWeb_bs = BeautifulSoup(requests.get(someDateUrl).text)
        someDateNewsList = someDateNewsListWeb_bs.find_all('ul', class_='list')[0]
        someDateNewsList = someDateNewsList.find_all('li')
        print(dateStr, f'第{dateListI+1}页')
        # 创造路径
        if os.path.exists(f'D:\\scrawler_data\\{dateStr[:6]}'):
            pass
        else:
            os.makedirs(f'D:\\scrawler_data\\{dateStr[:6]}')

        for someDateNewsI in range(len(someDateNewsList)):
            # newsI = 1
            # 找到每个新闻链接的list
            try:
                certainNewsUrl = someDateNewsList[someDateNewsI].find_all('a')[1].get('href')
            except IndexError:
                continue
            newsTitle = someDateNewsList[someDateNewsI].find_all('a')[1].text
            newsTitle = re.sub(r'^(\s)', r'', newsTitle)
            newsDatetimeStr = someDateNewsList[someDateNewsI].find_all('span')[0].text

            # 具体新闻页面
            certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)
            passagesList = certainNews_bs.find_all('div', class_='texttit_m1')[0].find_all('p')


            # 写入文件
            with open(f'D:\\scrawler_data\\{dateStr[:6]}\\'
                      f'{re.sub(r" ", "_", re.sub(r"[:-]", "",newsDatetimeStr))} {title}.txt',
                      encoding='utf-8',
                      mode='w') as f:
                f.write(newsTitle + '\n')
                for passageI in range(len(passagesList)):
                    f.writelines(passagesList[passageI].text + '\n')

        # 有下一页的
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
                    print(f'第{2}页')
                    someDateUrl = re.sub(f'/{dateStr[:6]}/(.*)$', f'/{dateStr[:6]}/' + page_newslib[nextI].get('href'), someDateUrl)
                    someDateNewsListWeb_bs = BeautifulSoup(requests.get(someDateUrl).text)
                    someDateNewsList = someDateNewsListWeb_bs.find_all('ul', class_='list')[0]
                    someDateNewsList = someDateNewsList.find_all('li')
                    for someDateNewsI in range(len(someDateNewsList)):
                        # newsI = 1
                        # 找到每个新闻链接的list
                        certainNewsUrl = someDateNewsList[someDateNewsI].find_all('a')[1].get('href')
                        newsTitle = someDateNewsList[someDateNewsI].find_all('a')[1].text
                        newsTitle = re.sub(r'^(\s)', r'', newsTitle)
                        newsDatetimeStr = someDateNewsList[someDateNewsI].find_all('span')[0].text

                        # 具体新闻页面
                        certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)
                        passagesList = certainNews_bs.find_all('div', class_='texttit_m1')[0].find_all('p')

                        # 写入文件
                        with open(f'D:\\scrawler_data\\{dateStr[:6]}\\'
                                  f'{re.sub(r" ", "_", re.sub(r"[:-]", "",datetimeStr))} {title}.txt',
                                  encoding='utf-8',
                                  mode='w') as f:
                            f.write(newsTitle + '\n')
                            for passageI in range(len(passagesList)):
                                f.writelines(passagesList[passageI].text + '\n')


# 存到mongodb中
if __name__ == '__main__':

    import requests
    from bs4 import BeautifulSoup
    import re
    import os
    import pymongo
    import io

    year = 2007

    # 当年的日历页面
    calendarUrl = f'http://stock.jrj.com.cn/xwk/{year}.shtml'
    calendarUrls_bs = BeautifulSoup(requests.get(calendarUrl).text)
    calendarUrlsBfDiv = calendarUrls_bs.find_all('div', class_='cont')[0]
    dateList = calendarUrlsBfDiv.find_all('a')  # 每日的连接
    datePattern = re.compile(r'/(\d{8})_')

    # 连接数据库
    newsdb = pymongo.MongoClient("localhost", 27017).newsdb

    for dateListI in range(len(dateList)):
        # dateListI = 0

        # 进入某一天的页面
        dateStr = datePattern.findall(dateList[dateListI].get('href'))[0]
        someDateUrl = dateList[dateListI].get('href')
        # someDateUrl = 'http://stock.jrj.com.cn/xwk/200802/20080209_1.shtml'

        # 进入当日多条新闻的页面
        someDateNewsListWeb_bs = BeautifulSoup(requests.get(someDateUrl).text)
        someDateNewsList = someDateNewsListWeb_bs.find_all('ul', class_='list')[0]
        someDateNewsList = someDateNewsList.find_all('li')
        if someDateNewsList == []:  # 今天没有内容
            continue
        print(dateStr, '第1页')

        # 爬下当日第一页的新闻
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
            print(f'正在爬取{newsDatetimeStr}, {newsTitle}')
            # title = '收盘：广电电气跌9.98%报7.22元 换手0.79%'

            # 删去报个股行情的那些条目
            if (not re.search(r'^快讯：.+?停 报于.+?元$', newsTitle) is None) or (not re.search(r'^收盘：.+?元 换手.+?%$', newsTitle) is None):
                continue

            # 具体新闻页面
            # certainNewsUrl = 'http://warrant.jrj.com.cn/news/2007-04-11/000002139717.html'


            # 具体页面不存在新闻内容
            if requests.get(certainNewsUrl).status_code == 404:
                continue

            # class标签的问题
            try:
                certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)
                passagesList = certainNews_bs.find_all('div', class_='newsCon')[0].find_all('p')
            except IndexError:  # class不是newsCon的情况
                try:
                    passagesList = certainNews_bs.find_all('div', class_='texttit_m1')[0].find_all('p') # class是texttit_m1的情况
                except IndexError:  #
                    raise IndexError('出问题了！！')
            except requests.exceptions.ConnectionError:
                print('网页链接出错')
                continue

            src = re.findall(r'<!--jrj_final_source_start-->[\x00-\xff]*([^\x00-\xff]+)[\x00-\xff]*<!--jrj_final_source_end-->',
                             requests.get(certainNewsUrl).text,
                             re.S
                             )[0]

            newsStr = io.StringIO()
            for passageI in range(len(passagesList)):
                # passageI = 5

                newsStr.write(re.sub(r'([（(][查更].+?请点击[）)])', '', passagesList[passageI].text) + '\n')  # 将（更多个股业绩查询请点击）这些去掉

            # 如果具体新闻页面有分页
            fenyejiedian = re.findall(r'<!-- 分页节点区 start-->(.+?)<!-- 分页节点区 end-->', requests.get(certainNewsUrl).text, re.S)
            if fenyejiedian == []:
                pass
            else:
                for newsNextPageI in range(1, 10):  # 最多估计就10页
                    # newsNextPageI = 2
                    certainNewsUrl = re.sub(r'((?:-\d+?)?\.shtml$)', f'-{newsNextPageI}.shtml',certainNewsUrl)
                    if requests.get(certainNewsUrl).status_code == 404:
                        break
                    certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)

                    # class标签的问题
                    try:
                        certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)
                        passagesList = certainNews_bs.find_all('div', class_='newsCon')[0].find_all('p')
                    except IndexError:  # class不是newsCon的情况
                        try:
                            passagesList = certainNews_bs.find_all('div', class_='texttit_m1')[0].find_all(
                                'p')  # class是texttit_m1的情况
                        except IndexError:  #
                            raise IndexError('出问题了！！')
                    except requests.exceptions.ConnectionError:
                        print('网页链接出错')
                        continue

                    for passageI in range(len(passagesList)):
                        # passageI = 5
                        newsStr.write(re.sub(r'([（(][查更].+?请点击[）)])', '',
                                             passagesList[passageI].text) + '\n')  # 将（更多个股业绩查询请点击）这些去掉

            # 写入mongodb
            newsdb.newscollection.insert({'title':newsTitle,
                                          'datetime':newsDatetimeStr,
                                          'source':src,
                                          'tag':'缺省',
                                          'news_content':newsStr.getvalue()})


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
                    someDateNewsListWeb_bs = BeautifulSoup(requests.get(someDateUrl).text)
                    someDateNewsList = someDateNewsListWeb_bs.find_all('ul', class_='list')[0]
                    someDateNewsList = someDateNewsList.find_all('li')
                    for someDateNewsI in range(len(someDateNewsList)):
                        # newsI = 1
                        # 找到每个新闻链接的list
                        try:
                            certainNewsUrl = someDateNewsList[someDateNewsI].find_all('a')[1].get('href')
                        except IndexError:
                            continue
                        newsTitle = someDateNewsList[someDateNewsI].find_all('a')[1].text
                        newsTitle = re.sub(r'^(\s)', r'', newsTitle)  # 去掉标题开头的空格
                        newsDatetimeStr = someDateNewsList[someDateNewsI].find_all('span')[0].text

                        # 删去报个股行情的那些条目
                        if (not re.search(r'^快讯：.+?停 报于.+?元$', newsTitle) is None) or (
                        not re.search(r'^收盘：.+?元 换手.+?%$', newsTitle) is None):
                            continue

                        # 具体新闻页面
                        # certainNewsUrl = 'http://stock.jrj.com.cn/2008-02-13/000003280121.shtml'
                        certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)

                        # 具体页面不存在新闻内容
                        if requests.get(certainNewsUrl).status_code == 404:
                            continue

                        # class标签的问题
                        try:
                            certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)
                            passagesList = certainNews_bs.find_all('div', class_='newsCon')[0].find_all('p')
                        except IndexError:  # class不是newsCon的情况
                            try:
                                passagesList = certainNews_bs.find_all('div', class_='texttit_m1')[0].find_all(
                                    'p')  # class是texttit_m1的情况
                            except IndexError:  #
                                raise IndexError('出问题了！！')
                        except requests.exceptions.ConnectionError:
                            print('网页链接出错')
                            continue

                        src = re.findall(
                            r'<!--jrj_final_source_start-->[\x00-\xff]*([^\x00-\xff]+)[\x00-\xff]*<!--jrj_final_source_end-->',
                            requests.get(certainNewsUrl).text,
                            re.S
                            )[0]

                        newsStr = io.StringIO()
                        for passageI in range(len(passagesList)):
                            # passageI = 5

                            newsStr.write(re.sub(r'([（(][查更].+?请点击[）)])', '',
                                                 passagesList[passageI].text) + '\n')  # 将（更多个股业绩查询请点击）这些去掉

                        # 如果具体新闻页面有分页
                        fenyejiedian = re.findall(r'<!-- 分页节点区 start-->(.+?)<!-- 分页节点区 end-->',
                                                  requests.get(certainNewsUrl).text, re.S)
                        if fenyejiedian == []:
                            pass
                        else:
                            for newsNextPageI in range(1, 10):  # 最多估计就10页
                                # newsNextPageI = 2
                                certainNewsUrl = re.sub(r'((?:-\d+?)?\.shtml$)', f'-{newsNextPageI}.shtml',
                                                        certainNewsUrl)
                                if requests.get(certainNewsUrl).status_code == 404:
                                    break
                                certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)

                                # class标签的问题
                                try:
                                    certainNews_bs = BeautifulSoup(requests.get(certainNewsUrl).text)
                                    passagesList = certainNews_bs.find_all('div', class_='newsCon')[0].find_all('p')
                                except IndexError:  # class不是newsCon的情况
                                    try:
                                        passagesList = certainNews_bs.find_all('div', class_='texttit_m1')[0].find_all(
                                            'p')  # class是texttit_m1的情况
                                    except IndexError:  #
                                        raise IndexError('出问题了！！')
                                except requests.exceptions.ConnectionError:
                                    print('网页链接出错')
                                    continue

                                for passageI in range(len(passagesList)):
                                    # passageI = 5
                                    newsStr.write(re.sub(r'([（(][查更].+?请点击[）)])', '',
                                                         passagesList[passageI].text) + '\n')  # 将（更多个股业绩查询请点击）这些去掉

                        # 写入mongodb
                        newsdb.newscollection.insert({'title': newsTitle,
                                                      'datetime': newsDatetimeStr,
                                                      'source': src,
                                                      'tag': '缺省',
                                                      'news_content': newsStr.getvalue()})