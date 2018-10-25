#coding=utf-8
import requests
import time

urls = [
    "https://yiyibooks.cn/xx/python_352/library/index.html",
    "https://qq.com",
    "https://jd.com",
    "https://kandaovr.com",
    "https://aliyun.com",
    "https://www.wikipedia.org/",
    "https://www.mi.com/index.html",
    "https://www.python.org/"
]



def profile(func):
    def wrapper(*args,**kwargs):
        t1 = time.time()
        func(*args,**kwargs)
        t2 = time.time()
        print(t2 - t1)
    return wrapper



@profile
def no_thread():
    for url in urls:
        try:
            r = requests.get(url)
        except:
            continue


if __name__ == "__main__":
    no_thread()