
import time

if __name__ == '__main__':

    # 极易分词错误的文本
    testCases=["结婚的和尚未结婚的确实在干扰分词啊",
               "汽水不如果汁好喝 ",
               "小白痴痴地在门前等小黑回来" ,
              "本月4日晚被人持刀捅伤,犯罪嫌疑人随身携带凶器。 伤口在胳膊上,神经受损,住院15天。 目前犯罪嫌疑人仍旧逍遥法外,请问当地派出所是否失职?",
              "改判被告人死刑立即执行",
              "宣判后，王小军提出上诉。江西省高级人民法院经依法开庭审理，于2013年3月14日以（2012）赣刑一终字第131号刑事裁定，驳回上诉，维持原判，并依法报请本院核准。本院依法组成合议庭，对本案进行了复核，并依法讯问了被告人。现已复核终结。",
              "王小军持一把螺丝刀朝陈某某的胸部戳刺两下，陈某某受伤后逃跑。王小军从旁边卖肉店砧板上拿起一把菜刀追赶未及，将菜刀扔向陈某某，未击中，后逃离现场。"
               ]

    print("_____________jieba___________")
    import  jieba

    t1 =time.time()
    # 精确模式
    for sentence in testCases:
        seg_list = jieba.cut(sentence)
        print("/ ".join(seg_list)) # 精确模式
    t2 = time.time()
    print("jieba time",t2-t1)

    print("_____________thulac___________")
    import thulac

    t1 = time.time()
    thu1 = thulac.thulac()  # 默认模式
    for sentence in testCases:
        text = thu1.cut(sentence, text=True)  # 进行一句话分词
        print(text)
    t2 = time.time()
    print("thulac time", t2 - t1)

    print("_____________fool___________")
    import fool

    t1 = time.time()
    for sentence in testCases:
        print(fool.cut(sentence))
    t2 = time.time()
    print("fool time", t2 - t1)

    print("_____________HanLP___________")
    from pyhanlp import *

    t1 = time.time()
    for sentence in testCases:
        print(HanLP.segment(sentence))
    t2 = time.time()
    print("HanLP_ time", t2 - t1)

    print("_____________中科院nlpir___________")
    import pynlpir  # 引入依赖包

    pynlpir.open()  # 打开分词器
    t1 = time.time()
    for sentence in testCases:
        print(pynlpir.segment(sentence, pos_tagging=False))  # 使用pos_tagging来关闭词性标注
    t2 = time.time()
    print("中科院nlpir time", t2 - t1)
    # 使用结束后释放内存：
    pynlpir.close()

    print("_____________哈工大ltp___________")
    from pyltp import Segmentor
    import os
    LTP_DATA_DIR = 'E:\MyLTP\ltp_data'  # ltp模型目录的路径、
    cws_model_path = os.path.join(LTP_DATA_DIR, 'cws.model')  # 分词模型路径，模型名称为`cws.model`
    segmentor = Segmentor()  # 初始化实例
    segmentor.load(cws_model_path)  # 加载模型
    t1 = time.time()
    for sentence in testCases:
        words = segmentor.segment(sentence)  # 分词结果,pyltp.VectorOfString object
        # s_fenci = list(words)
        # print('分词结果',list(words))
        s_fenci_str = '\t'.join(words)  # str
        print("哈工大", s_fenci_str)
    t2 = time.time()
    segmentor.release()  # 释放模型
