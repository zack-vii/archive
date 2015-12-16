import MDSplus
import threading as _th
a = 0
def pytest ( arg ):
    global a
    a = a+1
    try:    arg = arg.data()
    except: pass
    try:    arg = int(arg)
    except: arg = 0
    if arg>0:
        print(a,arg)
        threads = [_th.Thread(target=MDSplus.TdiExecute, args=('pytest($-1)',(arg,))) for i in range(1)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        print(a,arg)
if __name__=='__main__':
    x = pytest(5)