from WebPagesParsing import Parse
import json
import pandas as pd

'''
此脚本用于去重，即以电影名称为主码，包含字段 此电影商品的所有版本asinDirector 、
Run time 、Release date、Date First Available 、Actors、Studio 、Producers 、Writers
'''


class UnionFind(object):
    """并查集类"""

    def __init__(self, asinUrl, pageUrl):
        with open(asinUrl, 'r') as f:
            self.asins = json.loads(f.read())

        """长度为n的并查集"""
        self.uf = [-1 for i in range(len(self.asins))]      # 列表0位置空出
        self.sets_count = len(self.asins)                       # 判断并查集里共有几个集合, 初始化默认互相独立

        for i in self.asins:
            if self.find(i) == -10000000 or self.asins[self.find(i)] != i:
                continue
            asinRe = Parse(pageUrl + i + ".html")
            asinRe.parseAsinRelative()
            for j in asinRe.asinRelative:
                self.union(i, j)


    def find(self, asin):
        try:
            pos = self.asins.index(asin)
        except ValueError:
            return -10000000
        else:
            return self.findNum(pos)

    def findNum(self, p):
        """尾递归"""
        if self.uf[p] < 0:
            return p
        self.uf[p] = self.findNum(self.uf[p])
        return self.uf[p]

    def union(self, p, q):
        """连通p,q 让q指向p"""
        proot = self.find(p)
        qroot = self.find(q)
        if proot == -10000000 or qroot == -10000000:
            # print(q)
            # print(qroot)
            return
        if proot == qroot:
            return
        elif self.uf[proot] > self.uf[qroot]:  # 负数比较, 左边规模更小
            self.uf[qroot] += self.uf[proot]
            self.uf[proot] = qroot
        else:
            self.uf[proot] += self.uf[qroot]  # 规模相加
            self.uf[qroot] = proot
            # print(self.uf)
        self.sets_count -= 1  # 连通后集合总数减一

    def is_connected(self, p, q):
        """判断pq是否已经连通"""
        return self.find(p) == self.find(q)  # 即判断两个结点是否是属于同一个祖先


if __name__ == '__main__':
    #第一个指向asin.json文件地址，第二个指向网页的文件夹地址
    uf=UnionFind("./data/asinTest.json", "./data/webPages/")
    dict={'ufValue':uf.uf, 'asin':uf.asins}

    #存并查集的地址
    pd.DataFrame(dict).to_csv('./data/ufTest.csv')

