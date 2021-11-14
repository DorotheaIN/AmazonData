from os import sep
from typing_extensions import runtime
import pandas as pd
import time 

pd.options.display.max_rows = 10
start_time = time.time()

df = pd.read_csv('C:\\Users\\Dorothea\\Downloads\\title.basics.tsv\\data.tsv',sep='\t',header=0,low_memory=False)
del df['tconst']
del df['originalTitle']
del df['isAdult']
del df['endYear']
del df['runtimeMinutes']
result = df.loc[df['titleType']=='movie'].reset_index(drop=True)
print(result)
result.to_csv('C:\\Users\\Dorothea\\Downloads\\title.basics.tsv\\movies.tsv',sep='\t')

# df = pd.read_csv('C:\\Users\\Dorothea\\Downloads\\title.basics.tsv\\movies.csv',header=0,low_memory=False)
# print(df)

end_time = time.time()
print(end_time-start_time)