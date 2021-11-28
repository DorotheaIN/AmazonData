from UnionFind import UnionFind
from WebPagesParsing import Parse
import pandas as pd

class dE(object):
    def __init__(self, asinUrl, pageUrl, outputUrl):
        self.info = []
        self._asin = dict()
        self.uf = UnionFind(asinUrl, pageUrl)

        self.printOutUf()
        self.fillInfo(pageUrl)

        pd.DataFrame(self.info).to_csv(outputUrl)

    def printOutUf(self):
        # for i in range(len(self.uf.uf)):
        #     # print(i)
        #     # print(len(self.uf.uf))
        #     if self.uf.uf[i] < 0:
        #         self._asin[self.uf.asins[i]] = []
        #     else:
        #         self._asin[self.uf.asins[self.uf.findNum(i)]].append(self.uf.asins[i])

        for (i, j, k) in zip(self.uf.uf, self.uf.asins, range(len(self.uf.asins))):
            if i < 0:
                if self._asin.get(k, '1') == '1':
                    self._asin[k] = []
                self._asin[k].append(j)
            else:
                if self._asin.get(i, '1') == '1':
                    self._asin[i] = []
                self._asin[i].append(j)
        # re = filter(lambda i:ufValue[i] < 0,self.asins)
        self._asin = {k: v for (k, v) in self._asin.items() if self.uf.uf[k] < 0}

    def fillInfo(self, pageUrl):
        for (key, value) in self._asin.items():
            temp = {}
            parse = Parse(pageUrl+value[0]+".html")
            # print(parse.asin)
            parse.parseMovieInfo()
            parse.parseName()
            parse.parseRating()
            parse.parseImdbRating()
            parse.parseLabel()
            parse.parseMainActors()
            parse.parseAsinRelative()
            temp['productName'] = parse.name
            temp['videoName']=parse.videoName
            temp['asinRelative'] = value
            # temp['asinRelative'].append(key)
            temp['director'] = parse.director
            temp["runTime"] = parse.runTime
            temp["releaseDate"] = parse.releaseDate
            temp['dateFirstAvailable'] = parse.dateFirstAvailable
            temp['actor'] = parse.actor
            temp["mainActor"] = parse.mainActors
            temp['studio'] = parse.studio
            temp['producers'] = parse.producers
            temp["writers"] = parse.writers
            temp["label"] = parse.label
            temp["labelMovie"] = parse.labelMovie
            temp["rating"] = parse.rating
            temp["imdbRating"] = parse.imdbRating
            temp["ratingNum"] = parse.ratingNum
            self.info.append(temp)

if __name__ == '__main__':
    # 三个参数分别为asin.json文件地址，网页所在文件夹地址（只到文件夹，不用到文件，最后需要加/），输出csv文件地址
    dE("./data/test.json", "./data/", './data/test1.csv')




