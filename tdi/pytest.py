import time
import MDSplus
from threading import Thread
a = 0
worker = None
def pytest ( arg ):
    global a
    a = a+1
    try:    arg = arg.data()
    except: pass
    try:    arg = int(arg)
    except: arg = 0
    if arg==5:
        MDSplus.TdiExecute('pytest(-1)')
    if arg>0:
        print(a,arg)
        threads = [Thread(target=MDSplus.TdiExecute, args=('pytest($-1)',(arg,))) for i in range(1)]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        print(a,arg)
    elif arg==0:
        MDSplus.TdiExecute('pytest(-2)')
    elif arg==-1:
        start()
    else:
        stop()
if __name__=='__main__':
    x = pytest(5)


def start():
    global worker
    worker = Asynch()
    worker.configure()
    worker.start()

def stop():
    global worker
    worker.stop()
    worker.join()


class Asynch(Thread):
    def configure(self):
        self.stopReq = False
        self.daemon = True

    def run(self):
        print('start')
        while not self.stopReq:
            time.sleep(1)
        print('stop')

    def stop(self):
        self.stopReq = True
