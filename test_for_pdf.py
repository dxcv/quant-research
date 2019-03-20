
from pdfminer.pdfparser import PDFParser,PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal,LAParams
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed

filepath = r'C:\研究报告\20190219-农银国际-农银国际《粤港澳大湾区发展规划纲要》的宏观及行业分析（2019-02-19）.pdf'
fp = open(filepath, 'rb') # 以二进制读模式打开
#用文件对象来创建一个pdf文档分析器
praser = PDFParser(fp)
# 创建一个PDF文档
doc = PDFDocument()
# 连接分析器 与文档对象
praser.set_document(doc)
doc.set_parser(praser)

# 提供初始化密码
# 如果没有密码 就创建一个空的字符串
doc.initialize()

# 创建PDf 资源管理器 来管理共享资源
rsrcmgr = PDFResourceManager()
# 创建一个PDF设备对象
laparams = LAParams()
device = PDFPageAggregator(rsrcmgr, laparams=laparams)
# 创建一个PDF解释器对象
interpreter = PDFPageInterpreter(rsrcmgr, device)

# 循环遍历列表，每次处理一个page的内容
for page in doc.get_pages(): # doc.get_pages() 获取page列表
    interpreter.process_page(page)
    # 接受该页面的LTPage对象
    layout = device.get_result()
    # 这里layout是一个LTPage对象 里面存放着 这个page解析出的各种对象 一般包括LTTextBox, LTFigure, LTImage, LTTextBoxHorizontal 等等 想要获取文本就获得对象的text属性，
    for x in layout:
        if (isinstance(x, LTTextBoxHorizontal)):
            with open(r'C:\研究报告\笔记\2.txt', 'a') as f:
                results = x.get_text()
                print(results)
                f.write(results + '\n')
        else:
            pass
