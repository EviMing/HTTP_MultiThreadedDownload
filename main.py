#请求库
import requests
#多线程库
from concurrent.futures import ThreadPoolExecutor,as_completed
#证书库
import certifi
#系统交互库
import os
#事件处理库
import msvcrt

#[定义函数] 单线程下载
def downloadRange_Internal(URL, session_get, start, end, Headers, file_path):

    #定义请求头，指定下载范围
    Headers = {**Headers, "Range":f"bytes={start}-{end}"}

    #发送请求，使用流式传输
    Result = session_get.get(url=URL, headers=Headers, stream=True, verify=True)

    #如果状态码非2开头，则抛出异常
    Result.raise_for_status()

    #若状态码非206，则主动抛出异常
    if Result.status_code != 206:
        raise Exception('[error] 目标URL不允许多线程下载')

    with open(file_path,"r+b") as k:

        k.seek(start)

        for chunk in Result.iter_content(chunk_size=1*1024*1024):
            if chunk:
                k.write(chunk)

#[定义函数] 多线程执行
def MultiThreadedDownload_Internal(session_get, Headers, size, NumberOfThreads, Path):

    #生成任务区间列表
    """ranges格式：[(0, 2097151), (2097152, 4194303),···]"""
    #列表_任务区间大小
    ranges = []
    #单个线程负责的块大小，必须【大小=文件大小+线程数-1//线程数】，不然会有多余的字节没有线程下载
    chunkSize = (size + NumberOfThreads - 1) // NumberOfThreads

    #预先创建稀疏文件，记录空洞索引位置，写入时写入空洞位置。防止后续爆内存。空洞占位为Windows专用
    with open(Path,"wb") as h:
        #Windows：创建稀疏文件，立即只占 1 B
        msvcrt.setmode(h.fileno(), os.O_BINARY)
        #移动文件指针到文件末尾
        h.seek(size - 1)
        h.write(b"\0")

    for start in range(0, size, chunkSize):
        end = min(start+chunkSize-1, size-1)
        ranges.append((start,end))

    #线程池并发下载
    with ThreadPoolExecutor(max_workers=NumberOfThreads) as executor:

        future_list = [
            executor.submit(downloadRange_Internal, session_get, s, e, Headers, Path)
            for s,e in ranges
        ]

        for future in as_completed(future_list):
            future.result()

#定义函数_执行下载
def run(URL, NumberOfThreads, Path):

    #创建会话池，后续用会话池发送请求
    session_get = requests.Session()
    #使用 certifi 提供的根证书
    session_get.verify = certifi.where()

    #使用会话池获取元数据
    head = session_get.head(url=URL, headers=Headers)

    #从元数据中获取文件大小
    size = int(head.headers["Content-Length"])

    #请求头
    Headers = {
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 QuarkPC/7.0.0.907"
    }

    MultiThreadedDownload_Internal(
        session_get,
        Headers,
        size,
        NumberOfThreads,
        Path
    )
