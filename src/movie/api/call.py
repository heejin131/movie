import os
import requests
import pandas as pd 
from pandas.api.types import is_numeric_dtype

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY=os.getenv("MOVIE_KEY")

def gen_url(dt="20120101", url_param={}):
    "호출 URL 생성, url_param 이 입력되면 multiMovieYn, repNationCd 처리"
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    
    for k, v in url_param.items():
        url = url + f"&{k}={v}"
        
    return url

def call_api(dt="20120101", url_param={}):
    url = gen_url(dt, url_param)
    data = requests.get(url)
    j = data.json()
    return j['boxOfficeResult']['dailyBoxOfficeList']

# {"multiMovieYn":"Y"}
def list2df(data: list, dt:str, url_params={}):
    df = pd.DataFrame(data)
    df['dt'] = dt 
    # df['multiMovieYn'] = 'Y'
    for k, v in url_params.items():
        df[k] = v

    num_cols = ['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
                'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten',
                'salesChange', 'audiInten', 'audiChange']
    #for col_name in num_cols:
        #df[col_name] = pd.to_numeric(df[col_name])
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    return df

def save_df(df, base_path, partitions=['dt']):
    df.to_parquet(base_path, partition_cols=partitions)
    save_path = base_path
    for p in partitions:
        save_path = save_path + f"/{p}={df[p][0]}"
    return save_path

# 칼럼의 값이 열값이면 동일값을 가진 행의 값을 가져옴 

def fill_na_with_column(origin_df, c_name):
    df = origin_df.copy()
    for i, row in df.iterrows():
        if pd.isna(row[c_name]):
            same_movie_df = df[df["movieCd"] == row["movieCd"]]
            notna_idx = same_movie_df[c_name].dropna().first_valid_index()
            if notna_idx is not None:
                df.at[i, c_name] = df.at[notna_idx, c_name]
    return df

def gen_unique(df: pd.DataFrame, drop_columns: list) -> pd.DataFrame:
    df_drop = df.drop(columns=drop_columns)
    unique_df = df_drop.drop_duplicates()
    return unique_df

def re_ranking(df: pd.DataFrame, dt:str) -> pd.DataFrame:
    df["rnum"] = df["audiCnt"].rank(method="dense", ascending=False).astype(int)
    df["rank"] = df["audiCnt"].rank(method="min", ascending=False).astype(int)
    df["dt"] = dt
    return df

def merge_save(ds_nodash, read_base='/home/gmlwls5168/data/movies/dailyboxoffice', save_base='/home/gmlwls5168/data/movies/merge/dailyboxoffice'):
    base_path = f"{read_base}/dt={ds_nodash}"
    # save_path = f"/home/gmlwls5168/data/movies/merge/dailyboxoffice/dt={ds_nodash}"
    df = pd.read_parquet(base_path)
    df1 = fill_na_with_column(df, 'multiMovieYn')
    df2 = fill_na_with_column(df1, 'repNationCd')
    
    df3 = gen_unique(df2, drop_columns=['rnum', 'rank', 'rankInten', 'salesShare'])
  
    df4 = re_ranking(df3, ds_nodash)
    save_path = save_df(df4, save_base, partitions=['dt'])
    return save_path
