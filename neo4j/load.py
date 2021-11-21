from py2neo import Graph, Node, Relationship
import csv
import pandas as pd
from py2neo.matching import NodeMatcher
#连接数据库
graph=Graph("http://localhost:7474",auth=("neo4j","hcy1658339245"))
#清除原有所有节点等信息
graph.delete_all()

#电影标签：name[2] asinRelative[3] runTime[5] releaseDate[6] dateFirstAvailable[7] studio[9] label[12] labelMovie[13] rating[14] imdbRating[15] ratingNum[16] genres[17]
#演员标签：name[8]
#导演标签：name[4]
#编剧标签：name[11]
#制片人标签：name[10]

#读取电影数据
with open('E:\\yze\\third\\courses\\DataWare\\allmovies.csv','r',encoding='utf-8') as f:
    reader=csv.reader(f)
    data=list(reader)
#为了去重采用匹配
matcher=NodeMatcher(graph)
for i in range(1,len(data)):
    #电影是唯一的，可直接创建
    MovieNode=Node('Movie',name=data[i][2],asinRelative=data[i][3],runTime=data[i][5],releaseDate=data[i][6],dateFirstAvailable=data[i][7],studio=data[i][9],label=data[i][12],labelmovie=data[i][13],rating=data[i][14],imdbRating=data[i][15],ratingNum=data[i][16],genres=data[i][17])
    graph.create(MovieNode)
    #演员
    if(data[i][8]!='[]'):
        #形成list形式
        ActorNameList=data[i][8].replace('[','').replace(']','').replace('\'','').split(',')
        for j in range(0,len(ActorNameList)):
            #匹配此刻数据库中是否有该节点，如果有直接创建关系，如果没有先创建节点
            m=matcher.match('Actor',name=ActorNameList[j]).first()
            if m is None:
                ActorNode=Node('Actor',name=ActorNameList[j])
                graph.create(ActorNode)
                act=Relationship(ActorNode,'act',MovieNode)
                graph.create(act)
            else:
                act=Relationship(m,'act',MovieNode)
                graph.create(act)
    if(data[i][4]!='[]'):
        DirectorNameList=data[i][4].replace('[','').replace(']','').replace('\'','').split(',')
        for k in range(0,len(DirectorNameList)):
            m=matcher.match('Director',name=DirectorNameList[k]).first()
            if m is None:
                DirectorNode=Node('Director',name=DirectorNameList[k])
                graph.create(DirectorNode)
                direct=Relationship(DirectorNode,'direct',MovieNode)
                graph.create(direct)
            else:
                direct=Relationship(m,'direct',MovieNode)
                graph.create(direct)
    if(data[i][11]!='[]'):
        ScreenwriterNameList=data[i][11].replace('[','').replace(']','').replace('\'','').split(',')
        for a in range(0,len(ScreenwriterNameList)):
            m=matcher.match('Screenwriter',name=ScreenwriterNameList[a]).first()
            if m is None:
                ScreenwriterNode=Node('Screenwriter',name=ScreenwriterNameList[a])
                graph.create(ScreenwriterNode)
                write=Relationship(ScreenwriterNode,'write',MovieNode)
                graph.create(write)
            else:
                write=Relationship(m,'write',MovieNode)
                graph.create(write)
    if(data[i][10]!='[]'):
        ProducerNameList=data[i][10].replace('[','').replace(']','').replace('\'','').split(',')
        for n in range(0,len(ProducerNameList)):
            m=matcher.match('Producer',name=ProducerNameList[n]).first()
            if m is None:
                ProducerNode=Node('Producer',name=ProducerNameList[n])
                graph.create(ProducerNode)
                produce=Relationship(ProducerNode,'produce',MovieNode)
                graph.create(produce)
            else:
                produce=Relationship(m,'produce',MovieNode)
                graph.create(produce)




