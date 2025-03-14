import os

BASE_URL = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY=os.getenv("MOVIE_KEY")

def gen_url(dt="20120101", url_param={}):
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    return url

