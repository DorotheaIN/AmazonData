from bs4 import BeautifulSoup

'''
此脚本将网页上的信息提取出来，即从html文件中提取出asin、此电影商品的其他版本asin、电影名称、Director 、
Run time 、Release date、Date First Available 、Actors、Studio 、Producers 、Writers的信息
'''


class Parse(object):
    def __init__(self, url):
        self.asin = url[-15:-5]         # asin
        self.asinRelative = []          # 此电影商品的其他版本asin
        self.name = ''                  # 电影名称
        self.videoName = ''               # 电影名称
        self.label = ''                 # 书页标签
        self.labelMovie = 0             # 标签中是否含movie字样
        self.rating = ''                # 评分
        self.imdbRating = ''            # imdb评分
        self.ratingNum = ''             # 评分人数
        self.director = []              # 导演
        self.runTime = ''               # 时长
        self.releaseDate = ''           # 发行时间
        self.dateFirstAvailable = ''    #
        self.actor = []                 # 演员
        self.mainActors = []            # 主演
        self.studio = ''                # 工作室
        self.producers = []             #
        self.writers = []               #

        self._url = url                 # 文件目录
        with open(url, 'r') as f:
            page = f.read()
            self.soup = BeautifulSoup(page, 'lxml')
            # self._praseName()
            # self._parseAsinRelative()
            # self._parseMovieInfo()

    def __repr__(self):
        print("asin:")
        print(self.asin)
        print("asinRelative:")
        print(self.asinRelative)
        print("name:")
        print(self.name)
        print("videoName:")
        print(self.videoName)
        print("label:")
        print(self.label)
        print("labelMovie:")
        print(self.labelMovie)
        print("rating")
        print(self.rating)
        print("imdbRating")
        print(self.imdbRating)
        print("rating:")
        print(self.ratingNum)
        print("director:")
        print(self.director)
        print("runTime：")
        print(self.runTime)
        print("release：")
        print(self.releaseDate)
        print("dateFirstAvailable:")
        print(self.dateFirstAvailable)
        print("actor:")
        print(self.actor)
        print("studio:")
        print(self.studio)
        print("producers")
        print(self.producers)
        print("writers:")
        print(self.writers)
        print("mainActors:")
        print(self.mainActors)

    def parseName(self):
        span = self.soup.find('span', id='productTitle')
        if not span:
            return
        self.name = span.get_text().strip()
        # print(self.name)

    def parseAsinRelative(self):
        # print(self.asin)
        div = self.soup.find('div', id='MediaMatrix')
        if not div:
            return
        div = div.find_all(name="div", attrs={"class": "top-level unselected-row"})
        if not div:
            return
        for i in div:
            try:
                a = i.span.table.tr.find('td').find('a', attrs={"class": "title-text"})
                if not a:
                    continue
                url = a.attrs["href"]
            except AttributeError:
                continue
            else:
                self.asinRelative.append(url.split('/')[3])
        # print(self.asinRelative)
        try:
            div = self.soup.find(name="table", attrs={"class": "a-normal a-spacing-none a-spacing-top-extra-large title"}).next_sibling.next_sibling.next_sibling.next_sibling
            self.videoName = div.find(name="span", attrs={"class":"a-size-small a-color-base"}).get_text()
        except AttributeError:
            return

    def parseMainActors(self):
        div = self.soup.find('div', id='bylineInfo')
        if not div:
            return
        spans = div.find_all(name="span", attrs={"class": "author notFaded"})
        if not spans:
            return
        for i in spans:
            try:
                if "Actor" in i.find('span', attrs={"class": "a-color-secondary"}).get_text():
                    self.mainActors.append(i.a.get_text())
                    try:
                        self.actor.index(i.a.get_text())
                    except ValueError:
                        self.actor.append(i.a.get_text())
                    else:
                        pass
                if "Director" in i.find('span', attrs={"class": "a-color-secondary"}).get_text():
                    try:
                        self.director.index(i.a.get_text())
                    except ValueError:
                        self.director.append(i.a.get_text())
                    else:
                        pass
                if "Producer" in i.find('span', attrs={"class": "a-color-secondary"}).get_text():
                    try:
                        self.producers.index(i.a.get_text())
                    except ValueError:
                        self.producers.append(i.a.get_text())
                    else:
                        pass
                if "Writer" in i.find('span', attrs={"class": "a-color-secondary"}).get_text():
                    try:
                        self.writers.index(i.a.get_text())
                    except ValueError:
                        self.writers.append(i.a.get_text())
                    else:
                        pass
            except AttributeError:
                continue


    def parseRating(self):
        div = self.soup.find('div', id='averageCustomerReviews')
        if not div:
            return
        try:
            self.rating = div.find('span', id="acrPopover").attrs["title"]
            self.ratingNum = div.find('span', id="acrCustomerReviewText").get_text()
        except AttributeError:
            return

    def parseImdbRating(self):
        div = self.soup.find('div', id='imdbInfo_feature_div')
        if not div:
            return
        span = div.find('span', attrs={"class": "imdb-rating"})
        if not span:
            return
        self.imdbRating = span.get_text()[4:]

    def parseLabel(self):
        div = self.soup.find('div', id='wayfinding-breadcrumbs_feature_div')
        if not div:
            return
        a = div.find_all('a', attrs={"class": "a-link-normal a-color-tertiary"})
        if not a:
            return
        x = 0
        for i in a:
            if x == 0:
                x += 1
                continue
            if x == len(a)-1:
                try:
                    self.label = i.get_text().strip()
                except AttributeError:
                    continue
            if self.labelMovie == 0:
                try:
                    if 'movie' in i.get_text() or 'Movie' in i.get_text():
                        self.labelMovie = 1
                    elif 'films' in i.get_text() or 'Films' in i.get_text():
                        self.labelMovie = 1
                except AttributeError:
                    continue
            x += 1


    def parseMovieInfo(self):
        div = self.soup.find('div', id='detailBulletsWrapper_feature_div')
        if not div:
            return
        li = div.div.find_all("li")
        if not li:
            return
        for i in li:
            info = i.span.find_all("span")
            if not info:
                continue
            try:
                if "Director" in info[0].get_text():
                    self.director = info[1].get_text().split(", ")
                elif "Run time" in info[0].get_text():
                    self.runTime = info[1].get_text()
                elif "Release date" in info[0].get_text():
                    self.releaseDate = info[1].get_text()
                elif "Date First Available" in info[0].get_text():
                    self.dateFirstAvailable = info[1].get_text()
                elif "Actors" in info[0].get_text():
                    self.actor = info[1].get_text().split(", ")
                elif "Studio" in info[0].get_text():
                    self.studio = info[1].get_text()
                elif "Producers" in info[0].get_text():
                    self.producers = info[1].get_text().split(", ")
                elif "Writers" in info[0].get_text():
                    self.writers = info[1].get_text().split(", ")
            except AttributeError:
                continue

    def parse(self):
        self.parseMovieInfo()
        self.parseRating()
        self.parseName()
        self.parseAsinRelative()
        self.parseImdbRating()
        self.parseLabel()
        self.parseMainActors()


if __name__ == '__main__':
    a=Parse('./data/B00536QGXG.html')
    a.parse()
    print(a)




