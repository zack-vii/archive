import MDSplus
def pytest ( arg ):
    try:    arg = arg.data()
    except: pass
    try:    arg = int(arg)
    except: arg = 0
    if arg>0:
        x = MDSplus.TdiExecute('pytest($-1)',(int(arg),))
        print(type(x),x)
        return [arg]+list(x)
    else:
        return [arg]
if __name__=='__main__':
    print(pytest(5))
