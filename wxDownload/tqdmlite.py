import time
import threading

class tqdmlite:
    def __init__(
        self,
        total, 
        unit_scale,
        desc,
        unit_divisor,
        unit,
        ncols
    ):
        self.total = total
        self.unit_scale = unit_scale
        self.desc = desc
        self.unit_divisor = unit_divisor
        self.unit = unit
        self.ncols = ncols

        self.last_time = time.time()
        self.start_time = self.last_time
        self.count = 0.0

        self.__Lock = threading.Lock()

    @staticmethod
    def format_interval(t):
        """
        Formats a number of seconds as a clock time, [H:]MM:SS

        Parameters
        ----------
        t  : int
            Number of seconds.

        Returns
        -------
        out  : str
            [H:]MM:SS
        """
        mins, s = divmod(int(t), 60)
        h, m = divmod(mins, 60)
        if h:
            return '{0:d}:{1:02d}:{2:02d}'.format(h, m, s)
        else:
            return '{0:02d}:{1:02d}'.format(m, s)

    @staticmethod
    def format_sizeof(num, suffix='', divisor=1000):
        """
        Formats a number (greater than unity) with SI Order of Magnitude
        prefixes.

        Parameters
        ----------
        num  : float
            Number ( >= 1) to format.
        suffix  : str, optional
            Post-postfix [default: ''].
        divisor  : float, optional
            Divisor between prefixes [default: 1000].

        Returns
        -------
        out  : str
            Number with Order of Magnitude SI unit postfix.
        """
        for unit in ['', 'k', 'M', 'G', 'T', 'P', 'E', 'Z']:
            if abs(num) < 999.5:
                if abs(num) < 99.95:
                    if abs(num) < 9.995:
                        return '{0:1.2f}'.format(num) + unit + suffix
                    return '{0:2.1f}'.format(num) + unit + suffix
                return '{0:3.0f}'.format(num) + unit + suffix
            num /= divisor
        return '{0:3.1f}Y'.format(num) + suffix
    
    def update(self, n=1):
        self.__Lock.acquire()
        try:
            current_time = time.time()
            self.count += n
            perc = self.count * 100 / self.total
            bar = int(perc) * "+"
            bar += (100-int(perc)) * "-"
            duration = self.format_interval(current_time-self.start_time)
            # 瞬时速度
            speed = self.format_sizeof(n/(current_time-self.last_time), divisor=1024)
            # 平均速度
            # speed = self.format_sizeof(self.count / (current_time - self.start_time), divisor=1024)
            if self.total - self.count > 0:
                left_time = self.format_interval((self.total-self.count) / self.count * (current_time-self.start_time))
            else:
                left_time = self.format_interval(0)
            print("%s: %3d%% |%s| %s/%s [%s<%s, %s%s/s]" % (
                self.desc, 
                self.count * 100 / self.total, 
                bar, 
                self.format_sizeof(self.count, divisor=1024),
                self.format_sizeof(self.total, divisor=1024),
                duration,
                left_time,
                speed,
                self.unit
                ))
            self.last_time = current_time
        except:
            pass
        finally:
            self.__Lock.release()

    def updateTotal(self, total=1):
        self.__Lock.acquire()
        n = total - self.count
        try:
            current_time = time.time()
            self.count += n
            perc = self.count * 100 / self.total
            bar = int(perc) * "+"
            bar += (100-int(perc)) * "-"
            duration = self.format_interval(current_time-self.start_time)
            # 瞬时速度
            speed = self.format_sizeof(n/(current_time-self.last_time), divisor=1024)
            # 平均速度
            # speed = self.format_sizeof(self.count / (current_time - self.start_time), divisor=1024)
            if self.total - self.count > 0:
                left_time = self.format_interval((self.total-self.count) / self.count * (current_time-self.start_time))
            else:
                left_time = self.format_interval(0)
            print("%s: %3d%% |%s| %s/%s [%s<%s, %s%s/s]" % (
                self.desc, 
                self.count * 100 / self.total, 
                bar, 
                self.format_sizeof(self.count, divisor=1024),
                self.format_sizeof(self.total, divisor=1024),
                duration,
                left_time,
                speed,
                self.unit
                ))
            self.last_time = current_time
        except:
            pass
        finally:
            self.__Lock.release()
