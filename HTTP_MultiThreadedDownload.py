#请求库
import requests
#多线程库
from concurrent.futures import ThreadPoolExecutor,as_completed
#证书库
import certifi
#系统交互库
import os
#控制台输入输出和键盘事件处理库
import msvcrt
#正则库
import re
#时间库
import time
#键盘库
import keyboard
#解析与处理URL库
import urllib.parse
from urllib.parse import urlparse

#定义函数_多线程下载
def MultiThreadedDownload(URL,NumberOfThreads,Path):

    #请求头
    Headers = {"User-Agent":"range-downloader/1.0"}
    #创建会话池，后续用会话池发送请求
    session_get = requests.Session()
    #使用 certifi 提供的根证书
    session_get.verify = certifi.where()

    #使用会话池获取元数据
    head = session_get.head(url=URL,headers=Headers)

    #从元数据中获取文件大小
    size = int(head.headers["Content-Length"])

    #定义函数_单线程下载
    def downloadRange_Internal(ConversationPool,start,end,req_headers,file_path):

        #定义请求头，指定下载范围
        Headers = {**req_headers,"Range":f"bytes={start}-{end}"}

        #发送请求，使用流式传输
        Result = ConversationPool.get(url=URL,headers=Headers,stream=True,verify=True)

        #如果状态码非2开头，则抛出异常
        Result.raise_for_status()

        #若状态码非206，则主动抛出异常
        if Result.status_code != 206:
            raise RuntimeError('[目标URL不允许多线程下载]Server does not support HTTP range requests')

        #声明块大小的初始值，用于后续累加计算块大小
        bytes_size = 0

        with open(file_path,"r+b") as k:

            #移动文件指针到当前请求的开始位置写入，防止乱序。
            #例如有个请求从11字节下载到20字节，把指针移动到文件的第11字节位置开始写入
            #在这个程序中，开始范围为字典推导式传入的s参数，即开始下载范围
            k.seek(start)
            #分流写入；
            #定义内存占用峰值，单位字节
            PeakMemory = 1024*1024

            #分流写入
            #存储请求结果的变量.iter_content(chunk_size=字节大小)。将响应内容分成固定大小的块，遍历写入
            for chunk in Result.iter_content(chunk_size=PeakMemory):
                if chunk:
                    k.write(chunk)
                    bytes_size+=len(chunk)

        """历史遗留-返回(当次请求的开始值和响应内容)，用于后续添加进列表
        return start,Result.content
        """

        #返回当次响应的开始位置和相应内容的长度
        return start,bytes_size

    #定义函数_多线程下载
    def MultiThreadedDownload_Internal(NumberOfThreads,Path):

        #生成任务区间列表
        """ranges格式：[(0, 2097151), (2097152, 4194303),···]"""
        #列表_任务区间大小
        ranges = []
        #单个线程负责的块大小，必须【大小=文件大小+线程数-1//线程数】，不然会有多余的字节没有线程下载
        chunkSize = (size + NumberOfThreads - 1) // NumberOfThreads

        #文件信息
        print(f"[提示:] 文件准备开始下载，保存路径：{Path}")
        print(f"[声明:] 目标文件大小：{size} 字节 = {size/1024/1024:.2f} MB = {size/1024/1024/1024:.1f} GB")
        print(f"[声明:] 线程数：{NumberOfThreads}")
        print(f"[声明:] 单个负责线程块大小约为：{chunkSize} 字节 = {chunkSize/1024/1024:.2f} MB = {chunkSize/1024/1024/1024:.1f} GB")

        a = time.time()

        print("[声明:] 正在预创建稀疏文件(空洞占位)")
        #预先创建稀疏文件，记录空洞索引位置，写入时写入空洞位置。防止后续爆内存。空洞占位为Windows专用
        with open(Path,"wb") as h:
            # Windows：创建稀疏文件，立即只占 1 B
            msvcrt.setmode(h.fileno(),os.O_BINARY)
            #移动文件指针到文件末尾
            h.seek(size - 1)
            h.write(b"\0")

        """历史遗留-预先创建文件，提前占据文件大小对应的磁盘空间。防止后续爆内存
        with open(Path,"wb") as h:
            h.truncate(size)
            """

        b = time.time()
        c = b - a
        print(f"[声明:] 预创建稀疏文件完成，耗时：{c:.0f} 秒 =  {c/60:.1f} 分 = {c/60/60:.1f} 时")

        print("[声明:] 正在计算每个线程的下载区间")

        for start in range(0,size,chunkSize):
            #计算结束位置，取最小值。开始值+步长-1=当前区间结束位置，总大小-1，表示最后一个字节
            #取最小值是避免越界，若结束位置>总大小，则结束位置为总大小，防止请求范围超出总大小
            end = min(start+chunkSize-1,size-1)
            ranges.append((start,end))

        """历史遗留-提前用列表占位，用于后续找索引位置写入响应内容
        #添加固定数量的空值，提前占位，写入时按照顺序写入
        #防止因为多个线程的完成顺序不一样导致写入时乱序，因而写入的内容不对
        #len函数获取有多少个元组，即有多少个任务
        parts = [None] * len(ranges)
        """

        print("[提示:] 开始下载···")
        print("[声明:] 正在与服务器建立传输连接，连接成功后开始传输，传输过程不可见")

        start_time = time.time()

        #线程池并发下载
        with ThreadPoolExecutor(max_workers=NumberOfThreads) as executor:

            #使用字典推导式生成字典
            #键表达式每执行一次，返回一个Future对象(任务句柄)，用Future对象做字典键
            #enumerate获取每个元素的值和索引值，索引值做字典值
            #索引值在这里代表执行的第几个任务，即线程数
            #将任务句柄和索引值，作为键值对添加到字典中
            future_to_idx = {executor.submit(downloadRange_Internal,session_get,s,e,Headers,Path):
                             idx for idx,(s,e) in enumerate(ranges)}

            #字典推导式每有一个任务完成，将Future丢给对象
            for future in as_completed(future_to_idx):
                #用Future对象做键获取字典的值，即获取线程数
                idx = future_to_idx[future]
                #获取函数返回值
                start,data = future.result()

                """历史遗留-根据任务句柄的线程数部分将响应的内容写入对应索引，用于后续合并
                #找到任务句柄的线程数部分对应的索引值，将响应内容写入
                parts[idx] = data
                """

                print(f"[信息:] 线程: {idx:<3} 已完成 , 字节数: {data} bytes = {data/1024/1024:.2f} MB = {data/1024/1024/1024:.1f} GB")

        """历史遗留-合并列表内容并写入文件
        CompleteData = b"".join(parts)
        with open(Path, "wb") as s:
            s.write(CompleteData)
        """

        end_time = time.time()
        TimeDifference = end_time - start_time

        print(f"[提示:] 文件下载完成，保存路径：{Path} \\n")
        print(f"文件大小：{size} 字节 = {size/1024/1024:.2f} MB = {size/1024/1024/1024:.1f} GB \\n")
        print(f"耗时： {TimeDifference:.0f} 秒 =  {TimeDifference/60:.1f} 分 = {TimeDifference/60/60:.1f} 时")

    MultiThreadedDownload_Internal(NumberOfThreads,Path)

#块大小，单位字节，#1*1024表1KB，后续每*1024字节，单位增加一个级。b→kB→MB→GB
try:

    #声明参数；
    #下载链接
    URL = input("下载链接：")
    #线程数
    NumberOfThreads = int(input("建立线程数："))
    #保存路径
    SaveDirectory = input("文件保存路径：")

    #判断URL是否包裹引号
    if URL.startswith('"') and URL.endswith('"'):
        pass
    elif URL.startswith("'") and URL.endswith("'"):
        pass
    else:
        URL = f"{URL}"

    #获取服务器提供的默认文件名；
    resp = requests.head(URL, allow_redirects=True)
    disp = resp.headers.get("Content-Disposition", "")
    #提取 filename
    m = re.findall(r'filename\*?=(?:UTF-8\'\')?(.+)', disp, re.I)
    name = urllib.parse.unquote(m[0].strip('"')) if m else None
    #若仍为空，用 URL 最后一段
    if not name:
        name = urlparse(URL).path.split("/")[-1] or "download"

    #拼接保存目录和文件名为完整路径
    SplicingPath = os.path.join(SaveDirectory,name)

    print("[提示:] 每个窗口只能下载一次文件]")
    print("[提示:] 如果下载过程中，提示信息卡住不动，按下F5刷新页面")

    #开始下载文件
    MultiThreadedDownload(URL,NumberOfThreads,SplicingPath)

except requests.exceptions.ConnectionError:
    print("[报错:] requests.exceptions.ConnectionError ： 原因 : 网络连接失败,DNS解析失败,服务器不可达")

except requests.exceptions.Timeout:
    print("[报错:] requests.exceptions.Timeout ： 原因 : 连接或读取超时")

except requests.exceptions.SSLError:
    print("[报错:] requests.exceptions.SSLError ： 原因 : SSL验证失败")

except requests.exceptions.HTTPError:
    print("[报错:] requests.exceptions.HTTPError ： 原因 : HTTP状态码4xx或5xx错误")

except requests.exceptions.MissingSchema:
    print("[报错:] requests.exceptions.MissingSchema ： 原因 : 缺少URL的协议")

except requests.exceptions.InvalidSchema:
    print("[报错:] requests.exceptions.InvalidSchema ： 原因 : URL的协议无效")

except requests.exceptions.InvalidURL:
    print("[报错:] requests.exceptions.InvalidURL ： 原因 : URL无效")

except KeyError:
    print("[报错:] KeyError ： 原因 : 服务器响应头缺少Content-Length字段")

except ValueError:
    print("[报错:] ValueError ： 原因 : 输入的线程数不为整数")

except RuntimeError:
    print("[报错:] RuntimeError ： 原因 : 服务器不支持HTTP Range请求(状态码非206)")

except FileNotFoundError:
    print("[报错:] FileNotFoundError ： 原因 : 本地目录不存在")

except PermissionError:
    print("[报错:] PermissionError ： 原因 : 无文件写入权限")

except OSError:
    print("[报错:] OSError ： 原因 : 磁盘空间不足或文件系统错误")

except IOError:
    print("[报错:] IOError ： 原因 : 文件写入I/O错误")

except WindowsError:
    print("[报错:] WindowsError ： 原因 : Windows特有文件系统错误")

except ModuleNotFoundError:
    print("[报错:] ModuleNotFoundError ： 原因 : 缺少依赖库")

except ImportError:
    print("[报错:] ImportError ： 原因 : 无法导入指定模块")

except AttributeError:
    print("[报错:] AttributeError ： 原因 : 库版本不兼容导致方法缺失")

except IndexError:
    print("[报错:] IndexError ： 原因 : 正则表达式匹配结果为空时访问索引")

except UnicodeDecodeError:
    print("[报错:] UnicodeDecodeError ： 原因 : 文件包含无法解码的字符")

except MemoryError:
    print("[报错:] concurrent.futures.TimeoutError ： 原因 : 系统内存不足")

except ZeroDivisionError:
    print("[报错:] ZeroDivisionError ： 原因 : 目标文件大小为0时计算单个线程负责数据量")

except KeyboardInterrupt:
        print("[报错:] KeyboardInterrupt ： 原因 : 用户提前退出")

finally:
    print("按下Q键关闭窗口···")
    while keyboard.read_key().upper() != "Q":
        pass
