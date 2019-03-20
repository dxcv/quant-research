
# jieba的简单使用
if __name__ == '__main__':

    '''
    jieba分词的三种模式 
         精确模式、全模式、搜索引擎模式 
         - 精确模式：把文本精确的切分开，不存在冗余单词
         - 全模式：把文本中所有可能的词语都扫描出来，有冗余
         - 搜索引擎模式：在精确模式基础上，对长词再次切分
         
    采用算法：
        基于前缀词典实现高效的词图扫描，生成句子中汉字所有可能成词情况所构成的有向无环图 (DAG)
        采用了动态规划查找最大概率路径, 找出基于词频的最大切分组合
        对于未登录词，采用了基于汉字成词能力的 HMM (隐马尔科夫)模型，使用了 Viterbi 算法
    '''

    import jieba

    with open('策略/f_分词/oneline.txt','r', encoding='utf-8') as f:
        text = f.readline()

    # 精确模式————把文本精确的切分开，不存在冗余单词
    seg_list = jieba.lcut(text)

    # 全模式：把文本中所有可能的词语都扫描出来，有冗余
    seg_list_all = jieba.lcut(text, cut_all=True)

    # 搜索引擎模式：在精确模式基础上，对长词再次切分
    seg_list_search = jieba.lcut_for_search(text)

    # 向词典中增加新词
    jieba.add_word('经济增长')

    # 同样的可以返回iter的
    seg_list_iter = jieba.cut(text)
    seg_list_all_iter = jieba.cut(text, cut_all=True)
    seg_list_search_iter = jieba.cut_for_search(text)

# 关键词提取
if __name__ == '__main__':
    # 根据
    import jieba
    import jieba.analyse

    with open('策略/f_分词/oneline.txt','r', encoding='utf-8') as f:
        text = f.readline()


    tags = jieba.analyse.extract_tags(text, topK=5)


# 词性分析
if __name__ == '__main__':
    import jieba.posseg as pseg
    with open('策略/f_分词/oneline.txt','r', encoding='utf-8') as f:
        text = f.readline()
    words = pseg.cut(text)
    for w in words:
        print(w.word, w.flag)

# 设定自己的词典
if __name__ == '__main__':
    '''
    jieba采用延迟加载，"import jieba"不会立即触发词典的加载，一旦有必要才开始加载词典构建trie。
    如果想手工初始jieba，也可以手动初始化。
    
    词典格式和dict.txt一样，
    一个词占一行；每一行分三部分，一部分为词语，另一部分为词频，最后为词性（可省略），用空格隔开 
    '''
    import jieba
    filename = '自定义词典路径'
    # 有了延迟加载机制后，你可以改变主词典的路径:
    jieba.set_dictionary(filename)
    jieba.load_userdict(filename)  # file_name为自定义词典的路径
    # 手动初始化（可选） 加载字典
    jieba.initialize()


# Tokenize：返回词语在原文的起始位置
if __name__ == '__main__':
    import jieba
    result = jieba.tokenize('永和服装饰品有限公司')
    for tk in result:
        print("word %s\t\t start: %d \t\t end:%d" % (tk[0], tk[1], tk[2]) )


