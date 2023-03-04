import sys
import datetime

def log(*content):
    # print("[" + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "] " + content)
    module_name = sys._getframe(1).f_code.co_filename.split("/")[-1]
    module_name = module_name.split("\\")[-1]
    line_no = str(sys._getframe(1).f_lineno)
    dt = datetime.datetime.now()
    print("[" + dt.strftime("%Y-%m-%d %H:%M:%S.%f") + "]", "\033[1;33m[" + module_name + " (" + line_no + ")]\033[0m", *content, flush=True)
