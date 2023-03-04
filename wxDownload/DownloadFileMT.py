import os
import json
import hashlib
import datetime
import httpx
import traceback
from tqdm import tqdm
from threading import Thread, Lock
from tqdmlite import tqdmlite
from utils import log

"""
高效率多线程下载文件
TODO 
1、获取新任务时，首先从False分片开始。
2、获取新任务时，如果是活动分片，从剩余的2/3开始。
3、网络错误，一直重试，不必设置重试次数。
"""
class DownloadFileMT():

    # 辅助文件扩展名
    AUX_EXT = "dfmt"

    # 临时文件扩展名
    DL_EXT = "downloading"

    # 最小块
    # BLOCK_SIZE_MIN = 65536
    BLOCK_SIZE_MIN = 1048576

    def __init__(
        self, 
        download_url:str, 
        data_folder:str, 
        filename:str, 
        thread_num:int=10, 
        retry:int=100, 
        # retry:int=10, # TODO 调试
        init_temp:bool=True, 
        lite:bool=True,
        etag:bool=True
    ):
        """
        高效率多线程下载文件

        参数
        ----------
        download_url: str
            文件下载连接
        data_folder: str
            文件存储目录
        filename: str
            文件名
        thread_num: int
            开辟线程数量
        retry: int
            每个线程错误重试次数
        init_temp: bool
            是否删除已经下载的临时文件。如果断点续传，选择False。
        lite: bool
            是否使用lite显示下载进度，适用于输出到日志文件。
        etag: bool
            是否使用etag验证文件是否变更。
        """
        self.RETRY = retry
        self.download_url = download_url
        self.data_folder = data_folder
        self.thread_num = thread_num
        self.file_size = None
        self.etag = ""
        self.cut_size = None
        self.tqdm_obj = None
        self.thread_list = []
        self.lite = lite
        self.verify_etag = etag
        # self.file_path = os.path.join(self.data_folder, download_url.split('/')[-1])
        self.file_path = os.path.join(self.data_folder, filename)

        self.__WRLock = Lock()  # 数据独写互斥锁
        self.__aux_data = {}    # 分块数据

        self.__finished = False # 下载完成标志，用于各线程退出。
        
        # 删除以前下载的文件。删除后不能断点续传
        if init_temp:
            # 删除目标文件
            try:
                os.remove(self.file_path)
            except:
                pass

            try:
                os.remove("%s.%s" % (self.file_path, self.AUX_EXT))
            except:
                pass
            try:
                os.remove("%s.%s" % (self.file_path, self.DL_EXT))
            except:
                pass
    
    def start(self) -> bool:
        """
        开始执行下载操作

        返回
        ----------
        bool
            是否下载成功
        """

        # 创建输出目录
        if not os.path.exists(self.data_folder):
            try:
                os.mkdir(self.data_folder)
            except Exception as ex:
                log("创建输出目录失败：%s" % str(ex))
                return False
            
        # 创建任务
        tasks = self.__createTasks()
        for _ in range(0, self.thread_num - len(tasks)):
            tasks.append(
                self.__getNextTask()
            )
        # self.__aux_data = {
        #     "total": self.file_size,
        #     "data": tasks
        # }
        # self.__saveAuxData()

        # 创建下载进度条
        if self.lite:
            self.tqdm_obj = tqdmlite(total=self.file_size, 
                                unit_scale=True, 
                                desc=self.file_path.split(os.sep)[-1],
                                unit_divisor=1024,
                                unit="B",
                                ncols=200)
        else:
            self.tqdm_obj = tqdm(total=self.file_size, 
                                unit_scale=True, 
                                desc=self.file_path.split(os.sep)[-1],
                                unit_divisor=1024,
                                unit="B",
                                ncols=200)

        # 开始多线程下载
        log("启动下载线程，线程数量：%d" % self.thread_num)
        for thread_index in range(1, self.thread_num + 1):
            thread = Thread(
                target=self.downloader,
                args=(
                    thread_index, 
                    # tasks[thread_index-1][0], 
                    # tasks[thread_index-1][0] + tasks[thread_index-1][1] - 1
                    tasks[thread_index-1]
                )
            )
            thread.setName('Thread-{}'.format(thread_index))
            thread.setDaemon(True)
            thread.start()
            self.thread_list.append(thread)

        # 等待所有的线程执行完毕
        # 有可能有些线程阻塞，要想办法处理退出。
        for thread in self.thread_list:
            thread.join()

        self.__fout.close()
        # 检查辅助文件是否下载完成
        if (len(self.__aux_data['data']) == 1) and (self.__aux_data['data'][0][1] == self.__aux_data['total']):
            # 删除辅助文件
            try:
                os.remove("%s.%s" % (self.file_path, self.AUX_EXT))
            except Exception as ex:
                log("删除辅助文件失败：%s.%s" % (self.file_path, self.AUX_EXT))
            # 修改输出文件名
            try:
                
                os.rename("%s.%s" % (self.file_path, self.DL_EXT), self.file_path)
                return True
            except Exception as ex:
                log("修改输出文件名失败：%s.%s" % (self.file_path, self.DL_EXT))
            return False
        else:
            return False

    # def downloader(self, thread_index, start_index, stop_index):
    def downloader(self, thread_index, task):
        """
        下载任务线程函数

        参数
        ----------
        thread_index
            线程号，用于标识
        start_index
            数据起始位置，从0开始。
        stop_index
            数据结束位置，包括stop_index位置的数据。

        """

        # # TODO 调试
        # if thread_index == 1:
        #     return 

        sc = 0
        # task = [start_index, stop_index + 1 - start_index]
        while True:
            if self.__finished:
                return 
            
            if task is None :
                log("[Thread=%d] 下载任务线程结束退出！" % thread_index)
                return 
            
            total_download = 0
            succ = False
            for i in range(0, self.RETRY):
                if self.__finished:
                    return 
                
                start_index = task[0] + total_download  # 跳过已经下载的数据
                stop_index = task[0] + task[1] - 1
                if self.etag is None:
                    headers = {
                        'Range': 'bytes={}-{}'.format(start_index, stop_index),
                        # 'ETag': self.etag, 
                        # 'if-Range': self.etag,
                    }
                else:
                    headers = {
                        'Range': 'bytes={}-{}'.format(start_index, stop_index),
                        'ETag': self.etag, 
                        'if-Range': self.etag,
                    }
                ilength = stop_index + 1 - start_index
                try:
                    download_count = 0
                    with httpx.stream("GET", self.download_url, headers=headers, timeout=httpx.Timeout(1)) as response:
                        # 判断Content-Length长度是否匹配
                        if int(response.headers['Content-Length']) != ilength:
                            log("响应数据长度错误！thread_idx=%d, ilength=%d, Content-Length=%d" % (thread_index, ilength, int(response.headers['Content-Length'])))
                            raise Exception("响应数据长度错误！")
                        # num_bytes_downloaded = response.num_bytes_downloaded
                        c = datetime.datetime.now()
                        bsd = 0
                        # for chunk in response.iter_bytes(chunk_size=1048576):
                        for chunk in response.iter_bytes(chunk_size=524288):
                            if self.__finished:
                                return 
                            # 跟上一个chunk时间比较，大于多少直接抛异常。
                            nc = datetime.datetime.now()
                            if chunk:
                                bsd, total = self.__saveData(task[0] + total_download,chunk)
                                chunk_size = len(chunk)
                                total_download += chunk_size
                                download_count += chunk_size
                                # self.tqdm_obj.update(response.num_bytes_downloaded - num_bytes_downloaded)
                                # self.tqdm_obj.update(chunk_size)
                                self.tqdm_obj.updateTotal(total)
                                # num_bytes_downloaded = response.num_bytes_downloaded
                                if bsd == 2:
                                    succ = True
                                    break
                            # log("nc-c = %d" % (nc-c).microseconds)
                            if (download_count < ilength) and ((nc-c).microseconds > 600000):
                                raise Exception("下载速度慢，重新链接！")
                            c = nc
                        # 判断实际下载长度是否匹配
                        if (bsd != 2) and (download_count != ilength):
                            log("数据下载错误！thread=%d total_download=%d  ilength=%d  header=%s" % (thread_index, download_count, ilength, str(headers)))
                            raise Exception("数据下载错误！")
                    succ = True
                    break
                except Exception as e:
                    # log("Thread-{}:请求超时,尝试重连\n报错信息:{}".format(thread_index, e))
                    # self.downloader(etag, thread_index, start_index, stop_index)
                    # log("[thread=%s] 分片下载错误：%s" % (thread_index,str(e)))
                    if i == self.RETRY-1:
                        log("超出重试次数。文件分片下载失败")
                        # 辅助文件写入失败标志
                        self.__saveDataFailure(task[0] + total_download)
            if not succ:
                log("文件分片下载失败。index=%d" % thread_index)
                # 连续3次nexttask没完成，退出线程。
                sc += 1
                if sc >= 3:
                    return 
            else:
                sc = 0
            
            # 获取下一个任务
            task = self.__getNextTask()

    def __getFileSize(self):
        """
        获取需要下载的文件大小
        
        返回
        ----------
        int
            文件大小。如果失败返回-1。
        str
            ETag，用于分块下载。
        """
        log("开始获取文件大小")
        etag = ""
        for _ in range(self.RETRY):
            try:
                with httpx.stream("GET", self.download_url, timeout=httpx.Timeout(2)) as response2:
                    file_size = int(response2.headers["Content-Length"])
                    if self.verify_etag:
                        if 'ETag' in response2.headers:
                            etag = response2.headers['ETag']
                        else:
                            etag = None
                        break
                    else:
                        etag = None
            except Exception as ex:
                log("获取文件大小失败，重试！%s" % str(ex))
        log("文件大小：%d, ETag=%s" % (file_size, etag))
        return file_size, etag

    def __saveAuxData(self):
        """
        保存辅助文件数据
        """
        try:
            with open("%s.%s" % (self.file_path, self.AUX_EXT), "w") as f:
                f.write(json.dumps(self.__aux_data))
        except Exception as ex:
            traceback.print_exc()
            log("辅助文件写入失败(%s.%s): %s" % (self.file_path, self.AUX_EXT, str(ex)))

    def __saveFileData(self,offset:int, data:bytearray):
        """
        保存文件数据
        """
        try:
            self.__fout.seek(offset)
            self.__fout.write(data)
            self.__fout.flush()
        except Exception as ex:
            log("文件写入失败：%s" % str(ex))

    def __createTasks(self) -> list:
        """
        初始任务分配。对于一个下载任务只需调用一次。

        返回
        ----------
        list
            分块任务列表，每一项为任务的起始偏移量和数据长度[offset, length]
        """
        # 新下载和续传下载，分配算法不同。
        tasks = []
        try:
            # 读取并解析辅助文件
            if os.path.exists("%s.%s" % (self.file_path, self.AUX_EXT)) and os.path.exists("%s.%s" % (self.file_path, self.DL_EXT)):
                with open("%s.%s" % (self.file_path, self.AUX_EXT), "r") as af:
                    self.__aux_data = json.load(af)
                if os.path.getsize("%s.%s" % (self.file_path, self.DL_EXT)) != self.__aux_data['total']:
                    raise Exception("invalid downloading file")
                # 获取文件大小，进行比较验证
                self.file_size, self.etag = self.__getFileSize()
                if self.file_size != self.__aux_data['total']:
                    raise Exception("invalid downloading file size")
                j = 0
                for i in range(len(self.__aux_data['data'])):
                    if i == len(self.__aux_data['data']) - 1:
                        if self.__aux_data['total'] - (self.__aux_data['data'][i][0] + self.__aux_data['data'][i][1]) > 0:
                            tasks.append([
                                self.__aux_data['data'][i][0] + self.__aux_data['data'][i][1],
                                self.__aux_data['total'] - (self.__aux_data['data'][i][0] + self.__aux_data['data'][i][1])
                            ])
                    else:
                        tasks.append([
                            self.__aux_data['data'][i][0] + self.__aux_data['data'][i][1],
                            self.__aux_data['data'][i+1][0] - (self.__aux_data['data'][i][0] + self.__aux_data['data'][i][1])
                        ])
                    j+=1
                    if j >= self.thread_num:
                        break
                # 打开文件
                self.__fout = open("%s.%s" % (self.file_path, self.DL_EXT), "rb+")
            else:
                raise Exception("no old file")
        except:
            # 删除以前的下载文件
            try:
                os.remove("%s.%s" % (self.file_path, self.AUX_EXT))
            except:
                pass
            try:
                os.remove("%s.%s" % (self.file_path, self.DL_EXT))
            except:
                pass

            # 创建全新的任务
            self.file_size, self.etag = self.__getFileSize()
            if self.file_size <= 0:
                log("文件大小获取失败，退出下载！")
                return None
            
            self.__aux_data = {
                "total": self.file_size,
                "data": []
            }

            block_size = self.file_size // self.thread_num
            for i in range(0, self.thread_num):
                self.__aux_data['data'].append([block_size*i, 0, True])
                if i == self.thread_num - 1:
                    tasks.append([
                        block_size * i,
                        block_size + (self.file_size % self.thread_num),
                        True
                    ])
                else:
                    tasks.append([
                        block_size * i,
                        block_size,
                        True
                    ])
            self.__fout = open("%s.%s" % (self.file_path, self.DL_EXT), "wb+")
            self.__fout.truncate(self.file_size)
            self.__fout.flush()

            self.__saveAuxData()

        return tasks
    
    
    def __getNextTask(self):
        """
        获取后续任务块，包括开始和结束
        """
        self.__WRLock.acquire()
        try:
            # 遍历所有区间，找最大区间。不活跃的区间取整段，活跃区间取中间，直接保存起始偏移量
            data:list = self.__aux_data["data"] 
            task = None
            for i in range(len(data)):
                if i==0:
                    if data[i][0] != 0:
                        task = [0, data[i][0]]

                if i==len(data)-1:
                    if data[i][2] == True:
                        newsize = ((self.file_size - data[i][0] - data[i][1]) + 1) // 2
                        if newsize > (task[1] if task is not None else 0):
                            task = [data[i][0] + data[i][1] + newsize, newsize]
                    else:
                        newsize = self.file_size - data[i][0] - data[i][1]
                        if newsize > (task[1] if task is not None else 0):
                            task = [data[i][0] + data[i][1], newsize]
                else:
                    if data[i][2] == True:
                        newsize = ((data[i+1][0] - data[i][0] - data[i][1]) + 1) // 2
                        if newsize > (task[1] if task is not None else 0):
                            task = [data[i][0] + data[i][1] + newsize, newsize]
                    else:
                        newsize = data[i+1][0] - data[i][0] - data[i][1]
                        if newsize > (task[1] if task is not None else 0):
                            task = [data[i][0] + data[i][1], newsize]

            if task is None:
                log("没有新任务！")
                self.__finished = True
                return None
            # 判断是否小于最小尺寸
            if task[1] < self.BLOCK_SIZE_MIN:
                task = None
                # 是否有不活跃块。取最大不活跃块
                for i in range(len(data)):
                    if data[i][2] == False:
                        if i==len(data)-1:
                            newsize = self.file_size - data[i][0] - data[i][1]
                            if newsize > (task[1] if task is not None else 0):
                                task = [data[i][0] + data[i][1], newsize]
                        else:
                            newsize = data[i+1][0] - data[i][0] - data[i][1]
                            if newsize > (task[1] if task is not None else 0):
                                task = [data[i][0] + data[i][1], newsize]

                # 取最大活跃块全部
                if task is None:
                    for i in range(len(data)):
                        if data[i][2] == True:
                            if i==len(data)-1:
                                newsize = self.file_size - data[i][0] - data[i][1]
                                if newsize > (task[1] if task is not None else 0):
                                    task = [data[i][0] + data[i][1], newsize]
                            else:
                                newsize = data[i+1][0] - data[i][0] - data[i][1]
                                if newsize > (task[1] if task is not None else 0):
                                    task = [data[i][0] + data[i][1], newsize]

            # 写入辅助数据
            if task is not None:
                for i in range(len(data)):
                    if i==0:
                        if task[0] < data[i][0]:
                            data.insert(0, [task[0], 0, True])
                            break
                    if i==len(data)-1:
                        if task[0] > (data[i][0] + data[i][1]):
                            data.append([task[0], 0, True])
                            break
                    if task[0] == (data[i][0] + data[i][1]):
                        data[i][2] = True
                        break
                    elif (task[0] > (data[i][0] + data[i][1])) and (task[0] < data[i+1][0]):
                        data.insert(i+1, [task[0], 0, True])
                        break
                self.__saveAuxData()
            else:
                self.__finished = True
        except Exception as ex:
            traceback.print_exc
            log("获取任务失败：%s" % str(ex))
        finally:
            self.__WRLock.release()
        return task

    def __saveDataFailure(self, offset:int):
        """
        找到指定位置结束的任务块，写入False标志。
        """
        self.__WRLock.acquire()
        try:
            for dt in self.__aux_data['data']:
                if dt[1] == offset:
                    dt[2] = False
                    # log("saveDataFailure: %s" % str(dt))
                    break
            self.__saveAuxData()
        except:
            pass
        finally:
            self.__WRLock.release()

    def __saveData(self, offset:int, data: bytearray):
        """
        写入下载的数据，并更新辅助数据，记录已经下载的数据块。

        参数
        ----------
        offset:
            数据起始偏移量
        data:
            数据

        返回
        ----------
        int
            0 - 失败
            1 - 成功
            2 - 成功，当前数据块下载完成。

        异常
        ----------

        """
        self.__WRLock.acquire()
        try:
            total = 0

            # 数据写入文件
            self.__saveFileData(offset, data)

            # 更新辅助数据
            datalen = len(data)
            intervals:list = self.__aux_data['data']
            intervals.append([offset, datalen, True])
            intervals.sort(key=lambda x:x[0])
            merged = []
            current_interval = intervals[0]
            for interval in intervals[1:]:
                if current_interval[0] + current_interval[1] >= interval[0]:
                    current_interval = [
                        current_interval[0], 
                        max(current_interval[0] + current_interval[1], interval[0] + interval[1]) - current_interval[0],
                        current_interval[2] if current_interval[0] + current_interval[1] > interval[0] + interval[1] else (interval[2] if interval[0] + interval[1] > current_interval[0] + current_interval[1] else (current_interval[2] or interval[2]))
                    ]
                # 如果没有重叠部分，则将当前区间加入结果列表，并更新当前区间
                else:
                    merged.append(current_interval)
                    total += current_interval[1]
                    current_interval = interval                    
            # 将最后一个区间加入结果列表
            merged.append(current_interval)
            total += current_interval[1]
            self.__aux_data['data'] = merged
            self.__saveAuxData()
            
            # 计算已经下载总量


            # 判断结尾是否落在数据区间内，或与下一区间已经没有空隙，即是否要返回2。
            for i in range(len(merged)):
                if (offset+datalen>=merged[i][0]) and (offset+datalen<(merged[i][0] + merged[i][1])):
                    return 2, total
            return 1, total
        except Exception as ex:
            traceback.print_exc()
            log("文件写入失败：%s" % str(ex))
            return 0, total
        finally:
            self.__WRLock.release()

    def __HashVerify(self, hash:bytearray) -> bool:
        """
        对已经下载的数据进行哈希校验
        """
        # TODO
        pass

# 计算文件的 SHA256 哈希值
def calculate_file_sha256(file_path):
    sha256_obj = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(4096)
            if not data:
                break
            sha256_obj.update(data)
    return sha256_obj.hexdigest()

# 校验文件的 SHA256 哈希值
def verify_file_sha256(file_path, expected_sha256):
    actual_sha256 = calculate_file_sha256(file_path)
    return actual_sha256 == expected_sha256

if __name__ == "__main__":
    # download_url = "https://mirrors.ustc.edu.cn/centos/8-stream/isos/x86_64/CentOS-Stream-8-x86_64-latest-dvd1.iso"
    # download_url = "https://mirrors.ustc.edu.cn/centos/8-stream/isos/x86_64/CentOS-Stream-8-x86_64-latest-boot.iso"
    # download_url = "http://mirrors.aliyun.com/centos/8-stream/isos/x86_64/CentOS-Stream-8-x86_64-20230209-boot.iso?spm=a2c6h.25603864.0.0.261e3584mHf4II"
    
    downloader = DownloadFileMT(
        # download_url = "https://mirrors.ustc.edu.cn/centos/8-stream/isos/x86_64/CentOS-Stream-8-x86_64-latest-dvd1.iso",
        # filename = "CentOS-Stream-8-x86_64-latest-dvd1.iso",

        # download_url = "https://storage.raspberrypi.com/rptl-magazines-blobs/pa0pfqv9td3af1icqnpcgy928pss?response-content-disposition=attachment%3B%20filename%3D%22Handbook-2023.pdf%22%3B%20filename%2A%3DUTF-8%27%27Handbook-2023.pdf&response-content-type=application%2Fpdf&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=magazines-production%2F20230304%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Date=20230304T090111Z&X-Amz-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature=ff895cc1b910f9afd1ae6ba89bae6f817c1dbdb615f4fc15446c7207d1af9c19",
        # filename = "Handbook-2023.pdf", 

        download_url = "https://downloads.raspberrypi.org/raspios_full_armhf/images/raspios_full_armhf-2023-02-22/2023-02-21-raspios-bullseye-armhf-full.img.xz",
        filename = "2023-02-21-raspios-bullseye-armhf-full.img.xz",
        data_folder = "D:\\", 
        thread_num = 10,
        init_temp=False
    )
    downloader.start()

    # print(verify_file_sha256(
    #     "d:\\software\\CentOS-Stream-8-x86_64-latest-dvd1.iso", 
    #     "493bb291f4757c926f9bf20c306167257f6f7dafa9e409b40008bc3d388772e8"
    # ))
    # print(verify_file_sha256(
    #     "d:\\CentOS-Stream-8-x86_64-latest-dvd1.iso", 
    #     "b4bb35e2c074b4b9710419a9baa4283ce4a02f27d5b81bb8a714b576e5c2df7a"
    # ))
    print(verify_file_sha256(
        "d:\\2023-02-21-raspios-bullseye-armhf-full.img.xz", 
        "3beb87e294b771840cabb289e5e9952cd3ad66a3207bcdf1c4a6243138c9c028"
    ))
    
    