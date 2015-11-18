/*
helper fuction converts various inputs to second since Epoch
TimeToSec(value)
KIND(value) is QUADWORD QUADWORD_UNSIGNED: treat value as ns since Epoch
KIND(value) is STRING:                     treat value as TimeString e.g. "2015-11-18 15:48:15.123456789"
otherwise:                                 treat value as seconds since Epoch
*/
fun public TimeToSec(in _in, optional inout _now)
{
    fun now()
    {
        IF( EXTRACT(0,1,GETENV("os"))=="W" )
        {
            /* get current time windows */
            _var = 0X0QU;
            kernel32->GetSystemTimeAsFileTime(ref(_var));
            /* convert 100ns since 1601 to s since 1970 */
            RETURN(DBLE(( _var - 0X19db1ded53e8000QU )/1D7));
        }
        ELSE {
            RETURN(cvttime());  }
    }

    fun convStr( in _in)
    {
        _months = [ "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" ];
        _len = LEN(_in);
        _str = _in//EXTRACT(_len,31-_len,"1970/01/01T00:00:00.000000000D0");
        _YYYY = EXTRACT(0,4,_str);
        _MMM = _months[COMPILE(EXTRACT(5,2,_str))-1];
        _DD = EXTRACT(8,2,_str);
        _HH = EXTRACT(11,2,_str);
        _MM = EXTRACT(14,2,_str);
        _SS = EXTRACT(17,2,_str);
        _rest = COMPILE("0."//EXTRACT(20,11,_str));
        /*'12-JAN-2015 01:23:45'*/
        _str= _DD//"-"//_MMM//"-"//_YYYY//" "//_HH//":"//_MM//":"//_SS;
        _var = 0X0QU;
        IF(MdsShr->LibConvertDateString(_str,ref(_var)))
            /* convert 100ns since 'Modified Julian Date' 17-11-1958 to s since 1970 */
            RETURN((_var - 0X7c95674beb4000QU)/1D7+_rest);
    }

    IF( KIND(_in)==14 )/*STRING*/
    {
        IF(UPCASE(_in)=='NOW')
            RETURN(PRESENT(_now) ? (_now>0D0 ? _now : _now=now()) : now());
        ELSE
            RETURN(convStr(_in));
    }
    ELSE{IF( KIND(_in)==5 )/*QU*/
        RETURN(D_FLOAT(_in/1D9));
    ELSE{IF( KIND(_in)==9 )/*Q*/
        RETURN(D_FLOAT(_in/1D9));
    ELSE
        RETURN(D_FLOAT(_in));
    }}
}
