
# thulac的简单用法
if __name__ == '__main__':
    import thulac
    thu1 = thulac.thulac(seg_only=True)
    text = thu1.cut('在北京大学生活区喝进口红酒', text=True)
    print(text)

    

