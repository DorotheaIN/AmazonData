from typing_extensions import runtime
import pandas as pd
import time 
import numpy as np
import re
import json
# 文件路径
imdb_movies_filepath = 'E:\\yze\\third\\courses\\DataWare\\moviesFinal.tsv'
amazon_productInfo_filepath = 'C:\\Users\\Dorothea\\Documents\\Tencent Files\\2577672771\\FileRecv\\test1.csv'

pd.options.display.max_rows = 10
start_time = time.time()
movies = pd.read_csv(imdb_movies_filepath,sep='\t',header=0,low_memory=False,index_col=0)
amazondata = pd.read_csv(amazon_productInfo_filepath,header=0,low_memory=False,encoding='ISO-8859-1')

failList = []
movieList = []
movieAsins = []
DEBUG = False

amazondata.fillna('NULL',inplace=True)
movies.fillna('NULL',inplace=True)


# 删除冗余信息
def dealBracket(title):
    surpuls = [" DVD"," VHS"] # 商品名称常见冗余
    for i in surpuls:
        title = re.sub(i," ",title)
    title = re.sub(u"\\(.*?\\)|\\{.*?}|\\[.*?]","",title)
    title = re.sub(r"\s+", " ", title) # 合并空格
    title = title.strip() # 去除头尾空格
    return title

# 标题拆分
def dealCut(name,item):
    # names = re.split(r'[-:/]',name)
    years = re.findall(r'\b\d{4}\b', name)
    for i in years:
        if(isYear(i)):
            name = name.replace(i,'-')
            if(item["releaseDate"]=="NULL"):
                item["releaseDate"] = i
    names = set(re.split(r'[-:/]',name))
    changes = []
    for i in names:
        changes.append(i.title())
    changes = set(changes)
    return names.union(changes)

# 判断是否是合法年份
def isYear(x):
    x = int(x)
    if(x>1894 & x<2022):
        return True
    else:
        return False

# 对比导演
def compareDirector(temp,dir):
    if((temp.iloc[0,4] == "NULL") | (dir == "") | (dir == "*")):
        return 0
    dirsIMDB = set(temp.iloc[0,4].split(','))
    dir = dir.replace("'",",")
    dirsAmazon = set(dir.split(','))
    if(DEBUG):
        print(dirsIMDB)
        print(dirsAmazon)
    if((dirsIMDB & dirsAmazon)!=set()):# dir有交集
        return 2
    else:
        return -2
    

# 对比发布年份
def compareYear(temp,date):
    if((temp.iloc[0,2] == "\\N") | (date == "NULL")):
        return 0
    date = date[-4:]# 提取年份
    if(abs(int(temp.iloc[0,2])-int(date))<2):
        return 1
    else:
        return -1

# 对比imdb某电影和Amazon某产品对应的电影
def compareRow(temp,dir,date):
    return compareDirector(temp,dir) + compareYear(temp,date)

# 在IMDB电影数据中查找Amazon标题
def searchName(name):
    temp = movies.loc[(movies["primaryTitle"]==name) | (movies["originalTitle"]==name)]
    return temp
    
# 添加电影asin到list
def addAsinList(asins):
    asins = asins.replace("'"," ")
    asins = asins.split(",")
    for a in asins:
        a = a.strip()
        movieAsins.append(a)



# start
def start(item):
    name = item["productName"]
    videoName = item["videoName"]
    directors = item["director"][1:-1] 
    isMovie = item["labelMovie"] # 一定是电影 大概率找不到，尝试找
    # imdbRating = item["imdbRating"] # 不是5 stars 则说明title不需要处理就能找到，找不到就不是movie
    date = item["releaseDate"]
    if(name=="NULL"):
        print(item["asinRelative"])
        return
    if(videoName != "NULL"):
        samename = searchName(videoName)
        if(samename.shape[0]==0):
            samename = searchName(name)
    else:
        samename = searchName(name)
    if(samename.shape[0]==0):# 没找到
        samename = searchName(dealBracket(name))
        if(samename.shape[0]==0):# 第一次处理没找到
            tempNames = dealCut(dealBracket(name),item)
            for i in tempNames:
                i = dealBracket(i)
                samename = searchName(i)
                if(samename.shape[0]!=0):
                    break
            if(samename.shape[0] == 0):# 第二次处理还没找到
                if(isMovie == 1):# 是电影但是IMDB好像没有
                    if(DEBUG):
                        print("[Movie & Can't find] "+name)
                    failList.append(name)
                    asin = item["asinRelative"][1:-1]
                    addAsinList(asin)
                    movieList.append({
                        "asinRelative":item["asinRelative"],
                        "genres":""
                    })
                    if(videoName == "NULL"):
                        item["videoName"] = name
                    return
                else: # 认为不是电影
                    if(DEBUG):
                        print("[Can't find] "+ name)
                    return
    rows = samename.shape[0] # 获得行数
    if(rows == 0):
        print("???????")
    maxres = -1
    maxindex = -1
    for index in range(0,rows):
        temp = samename[index:index+1]
        result = compareRow(temp,directors,date)
        if(DEBUG):
            print(temp)
            print(result)
        if(result>maxres):
            maxindex = index
            maxres = result
    if(maxindex == -1):
        if(isMovie == 1):
            if(DEBUG):
                print("[Movie & Find But Not Sure The One] " + name)
            failList.append(name)
            asin = item["asinRelative"][1:-1]
            addAsinList(asin)
            movieList.append({
                "asinRelative":item["asinRelative"],
                "genres":""
            })
            if(videoName == "NULL"):
                item["videoName"] = name
            return
        else:
            if(DEBUG):
                print("[Find But Not Movie] " + name)
    else:
        asin = item["asinRelative"][1:-1]
        addAsinList(asin)
        movieList.append({
            "asinRelative":item["asinRelative"],
            "genres":samename.iloc[maxindex,3]
        })
        if(videoName == "NULL"):
            item["videoName"] = samename.iloc[maxindex,0]
        if(DEBUG):
            index = maxindex
            print(samename[index:index+1])




def saveResult():
    form = pd.DataFrame(movieList)
    if(DEBUG):
        print(movieList)
        print(amazondata)
        print(form)

    form = pd.merge(amazondata,form,how="inner",on="asinRelative")
    form.to_csv('E:\\yze\\third\\courses\\DataWare\\allmovies.csv')
    with open('E:\\yze\\third\\courses\\DataWare\\movies_asins.json','w') as file_obj:
        result = json.dumps(movieAsins,sort_keys=True, indent = 4)
        file_obj.write(result)
    print(form)
    print(movieAsins)
    print(len(failList))


temp = amazondata.to_dict(orient = 'records')
DEBUG = False
for item in temp:
    start(item)
print(movieList)
saveResult()
end_time = time.time()
print(end_time-start_time)









