import multiprocessing as _mp
from multiprocessing import cpu_count  # analysis:ignore
import time as _time
import threading as _th
from . import support as _sup
from . import version as _ver

_workers = {}

def process(on,task,res):
    _sup.debug('started')
    while on.value or task.poll():
      if task.poll():
        try:
            target,args,kwargs = task.recv()
            try:
                _sup.debug('task resv')
                result = target(*args,**kwargs)
                _sup.debug('task done')
            except KeyboardInterrupt as ki: raise ki
            except Exception as result:
                _sup.debug(str(result))
            try:
                res.send(_sup.requeststr(result))
            except _ver.pickle.PicklingError:
                res.send(str(result))
        except KeyboardInterrupt as ki: raise ki
        except:
            _sup.error()
      else:
          _time.sleep(.1)
    _sup.debug('done!')
    task.close()

if not _ver.iswin:
     from multiprocessing.pool import Pool
     from multiprocessing.process import Process
else:
    def Pool(processes=None, initializer=None, initargs=(), maxtasksperchild=None):
        def process(slf,*args,**kwarg):
            return Process(*args,**kwarg)
        from multiprocessing.pool import Pool as pool
        pool.Process = process
        return pool(processes, initializer, initargs, maxtasksperchild)

    def Process(*args,**kwarg):
        def Popen__init__(sel, process_obj):
            # create pipe for communication with child
            rfd, wfd = _mp.forking.os.pipe()
            # get handle for read end of the pipe and make it inheritable
            rhandle = _mp.forking.duplicate(_mp.forking.msvcrt.get_osfhandle(rfd), inheritable=True)
            _mp.forking.os.close(rfd)
            # start process
            cmd = _mp.forking.get_command_line() + [rhandle]
            cmd = ' '.join('"%s"' % x for x in cmd)
            hp, ht, pid, tid = _mp.forking._subprocess.CreateProcess(
               _mp.forking. _python_exe, cmd, None, None, 1, 0, None, None, None
                )
            ht.Close()
            _mp.forking.close(rhandle)
            # set attributes of self
            sel.pid = pid
            sel.returncode = None
            sel._handle = hp
            # send information to child
            prep_data = _mp.forking.get_preparation_data(process_obj._name)
            if 'main_path' in prep_data.keys():
                del(prep_data['main_path'])
            prep_data['sys_argv']=['']
            to_child = _mp.forking.os.fdopen(wfd, 'wb')
            _mp.forking.Popen._tls.process_handle = int(hp)
            try:
                _mp.forking.dump(prep_data, to_child, _mp.forking.HIGHEST_PROTOCOL)
                _mp.forking.dump(process_obj, to_child, _mp.forking.HIGHEST_PROTOCOL)
            finally:
                del _mp.forking.Popen._tls.process_handle
                to_child.close()
        from multiprocessing.process import Process as process
        process = process(*args,**kwarg)
        from multiprocessing.forking import Popen as popen
        popen.__init__ = Popen__init__
        process.Popen = popen
        return process

class Worker(_th.Thread):
    def __new__(cls,name=None):
        if name is None: name='default'
        if name in _workers.keys():
            return _workers[name]
        return super(Worker,cls).__new__(cls)

    def __init__(self,name=None):
        if name is None: name='default'
        if name in _workers.keys():
            return
        _workers[name] = self
        super(Worker,self).__init__()
        self.daemon = True
        self.name = name
        self._queue = _ver.queue.Queue(1)
        self.last_exception = None
        self._pon = _mp.Value('b',True)
        tsk,self.task = _mp.Pipe(False)
        self.out,res = _mp.Pipe(False)
        self.process = Process(target=process,args=(self._pon,tsk,res),name=name)
        self.process.start()
        self._on = True
        self.start()
        _time.sleep(1)

    def run(self):
        _sup.debug('%s started' % (str(self.name),))
        while self._on or not self._queue.empty():
            try:
                result,target,args,kwargs = self._queue.get(True,.1)
                _sup.debug('%s: %s-task received' % (str(self.name),target.__name__))
                self.task.send((target,args,kwargs))
                res = self.out.recv()
                del(result[self.name])
                _sup.debug(res)
                result[target.__name__] = res
                _sup.debug('%s: %s-task done' % (str(self.name),target.__name__))
                self._queue.task_done()
            except _ver.queue.Empty: continue
            except KeyboardInterrupt as ki: raise ki
            except Exception as exc:
                _sup.debug('%s: %s' % (str(self.name),str(exc)),0)
                if result is not None:
                    result[self.name] = exc
                self.last_exception = exc
        _sup.debug('%s: done!' % (str(self.name),))
        self._pon.value = False
        del(_workers[self.name])

    def join(self):
        self._on = False
        self._queue.join()
        super(Worker,self).join()
        self._pon.value = False
        self.process.join()

    def put(self,target,*args,**kwargs):
        result = {self.name:target.__name__}
        self._queue.put((result,target,args,kwargs))
        _time.sleep(.1)
        return result

def join(name=None):
    if name is None: name='default'
    if not name in _workers.keys():
        return
    return _workers[name].join()

def postprocess(waitlist,method,*va,**kv):
    for it in waitlist:
        if isinstance(it,int):
            while not 'result' in va[it].keys():
                _time.sleep(.1)
            va[it] = va[it]['result']
        else:
            while not 'result' in kv[it].keys():
                _time.sleep(.1)
            kv[it] = kv[it]['result']
    return method(*va,**kv)

def withresult(pipe,target,*args,**kwargs):
    try:
        pipe.send(target(*args,**kwargs))
    except KeyboardInterrupt as ki: raise ki
    except Exception as exc:
        pipe.send(exc)
    pipe.close()
