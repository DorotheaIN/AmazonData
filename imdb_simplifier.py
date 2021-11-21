from os import sep
from typing_extensions import runtime
import pandas as pd
import time 
import re
import numpy

imdb_titles_path = 'C:\\Users\\Dorothea\\Downloads\\title.basics.tsv\\data.tsv'
imdb_movies_path = 'E:\\yze\\third\\courses\\DataWare\\movies.tsv'
imdb_crews_path = 'C:\\Users\\Dorothea\\Downloads\\title.crew.tsv\\data.tsv'
imdb_directoredRelation_path = 'E:\\yze\\third\\courses\\DataWare\\crews.tsv'
imdb_moviesDetail_path = 'E:\\yze\\third\\courses\\DataWare\\moviesdetails.tsv'
pd.options.display.max_rows = 10
start_time = time.time()


def concat_func(x):
    return pd.Series({
        'primaryName':','.join(x['primaryName'].unique()),
    }
  )

def fetchMovies():
    
    df = pd.read_csv(imdb_titles_path,sep='\t',header=0,low_memory=False,index_col=0)
    print(df)
    del df['isAdult']
    del df['endYear']
    del df['runtimeMinutes']
    result = df.loc[df['titleType']=='movie']
    del result['titleType']
    print(result)
    result.to_csv(imdb_movies_path,sep='\t')

def fetchDirectedRelation():
    df = pd.read_csv(imdb_crews_path,sep='\t',header=0,low_memory=False,index_col=0)
    del df['writers']
    print(df)
    df.to_csv(imdb_directoredRelation_path,sep='\t')

def merge():
    # 合并电影&导演
    movies = pd.read_csv(imdb_movies_path,sep='\t',header=0,low_memory=False,index_col=0)
    crews = pd.read_csv(imdb_directoredRelation_path,sep='\t',header=0,low_memory=False,index_col=0)
    result = pd.merge(movies,crews,how="inner",on='tconst')
    print(result)
    result.to_csv(imdb_moviesDetail_path,sep='\t')

def fetchDirectorNames():
    imdb_names_path = 'E:\\yze\\third\\courses\\DataWare\\names.tsv'
    movies = pd.read_csv(imdb_moviesDetail_path,sep='\t',header=0,low_memory=False,index_col=0)
    names = pd.read_csv(imdb_names_path,sep='\t',header=0,low_memory=False,index_col=0)
    # 拆分directors列，以便之后用名字替换序列
    temp = movies["directors"].str.split(',',expand=True)
    temp = temp.stack()
    temp = temp.reset_index(level = 1,drop = True)
    # 转series为dataform
    temp = pd.DataFrame({'tconst':temp.index,'nconst':temp.values})
    # 内连接，生成有tconst,nconst,primaryName三列表示电影和导演关系的临时表
    temp = pd.merge(temp,names,how="inner",on='nconst')
    # 删除临时表中的nconst
    del temp['nconst']
    # 删除IMDB电影详情表中的directors
    del movies['directors']
    # 根据电影序列聚合
    temp =temp.groupby(temp['tconst']).apply(concat_func).reset_index()
    result = pd.merge(movies,temp,how="left",on='tconst')
    result.set_index(['tconst'],inplace=True)
    result.to_csv('E:\\yze\\third\\courses\\DataWare\\moviesFinal.tsv',sep='\t')
    print(result)


end_time = time.time()
print(end_time-start_time)