#coding=utf-8
import requests
import time
from threading import Thread
from queue import Queue,Empty
from multiprocessing import Process,Pool,Pipe

urls = [
    "https://yiyibooks.cn/xx/python_352/library/index.html",
    "https://www.qq.com/?fromdefault",
    "https://jd.com",
    "http://youku.com/",
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


def fib(n):
    if n < 2:
        return 1
    return fib(n-1) + fib(n-2)


@profile
def no_thread():
    for i in range(5):
       fib(35)

@profile
def use_threading():
    for i in range(5):
        t = Thread(target=fib,args=(35,))
        t.start()
        t.join() #阻塞至该线程执行完才能执行其他的线程
        #非阻塞，


# TODO ThreadPool
class Worker1(Thread):
    """消费者线程，
    用list实现任务队列和结果队列,
    但是使用list，可能线程不安全，要加锁，这个有时间再去实现
    """
    def __init__(self,worklist,resultlist,*args,**kwargs):
        """
        Args:
             worklist:待消费的任务队列
             resultlist:结果存储队列
        """
        super(Worker1,self).__init__(*args,**kwargs)
        self.workqueue = worklist #可以用Queue，也可以用list,两种方法都实现下
        self.resultqueue = resultlist


    def run(self):
        """
        定义线程执行的任务或者说是活动的方式
        """
        while True:
            try:
                task = self.workqueue[0]
            except: #任务队列空了
                break


class Worker(Thread):
    """
    定义一个能够处理任务的线程类，属于自定义线程类，自定义线程类就需要定义run()函数
    """

    def __init__(self,workqueue,resultqueue,**kwargs):
        super(Worker,self).__init__(**kwargs)
        self.workqueue = workqueue#存放任务的队列,任务一般都是函数
        self.resultqueue = resultqueue#存放结果的队列

    def run(self):
        while True:
            try:
                #从任务队列中取出一个任务，block设置为False表示如果队列空了，就会抛出异常
                task,args,kwargs = self.workqueue.get()
                print(self.name)
                res = task(*args,**kwargs)
                self.resultqueue.put(res)#将任务的结果存放到结果队列中
            except Exception as e:#抛出空队列异常
                print(e)
            self.workqueue.task_done() #如果该方法被调用的次数多于被放入队列中的任务的个数，ValueError异常会被抛出



class ThreadPool(object):
    """线程池类"""
    def __init__(self,num =10):
        self.workqueue = Queue()
        self.resultqueue = Queue()
        self.threadpool = []
        self._createpool(num)

    def _createpool(self,num):
        self.threadpool = [Worker(self.workqueue,self.resultqueue) for i in range(num)]


    def start(self):
        """
        启动任务

        """
        for thread in self.threadpool:
            thread.start()

    def wait_for_complete(self):
        """等待队列里任务完成"""
        self.workqueue.join()

    def add_task(self,task,*args,**kwargs):
        self.workqueue.put((task,(35,),kwargs))


@profile
def use_multiprocess():
    task = [Process(target=fib,args=(35,)) for i in range(5)]

    for p in task:
        p.start()
    for p in task:
        p.join()


@profile
def use_processpool():
    """
    看多进程/进程池的源代码，发现，还是那句话：进程是资源分配和调度的独立单位，不负责任务的执行；而线程才是程序执行的最小单位，
    是CPU调度和分派的基本单位
    进程里的任务还是有进程里的协程来处理
    """
    pool= Pool(5)
    pool.map(fib,[35] * 5) #该函数是阻塞的，阻塞值进程上任务执行完成



#TODO -------管道测试 ，类似于golang的channel ------------
#  x  ===> x + x   ====> x *x
# 需要三个进程，每个进程负责一块,
def task1(pipe,num):
    pipe.send(num + num) #发送数据

def task2(pipe1,pipe2):
    num = pipe1.recv()
    pipe2.send(num * num)

def task3(pipe):
    num = pipe.recv()
    print(num)
    time.sleep(1)


if __name__ == "__main__":
    pipe1_left,pipe1_right = Pipe()
    pipe2_left,pipe2_right = Pipe()
    process1 = Process(target=task1,args=(pipe1_left,3))
    process2 = Process(target=task2,args=(pipe1_right,pipe2_left))
    process3 = Process(target=task3,args=(pipe2_right,))
    process1.start()
    process2.start()
    process3.start()
    process1.join()
    process2.join()
    process3.join()


# TODO 对比golang的channel，还是很好理解的，只是golang的goroutine是很轻量级的




