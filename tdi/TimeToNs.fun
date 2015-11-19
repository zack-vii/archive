/*
helper fuction converts various inputs to second since Epoch
TimeToSec(value)
KIND(value) is QUADWORD QUADWORD_UNSIGNED: treat value as ns since Epoch
KIND(value) is STRING:                     treat value as TimeString e.g. "2015-11-18 15:48:15.123456789"
otherwise:                                 treat value as seconds since Epoch
*/
fun public TimeToNs(in _in, optional inout _now)
{
    fun now()
    {
        IF( EXTRACT(0,1,GETENV("os"))=="W" )
        {
            /* get current time windows */
            _var = 0X0Q;
            kernel32->GetSystemTimeAsFileTime(ref(_var));
            /* convert 100ns since 1601 to s since 1970 */
            RETURN(QUADWORD(( _var - 0X19db1ded53e8000Q )*100Q));
        }
        ELSE
            RETURN(QUADWORD(cvttime()*1000000000Q));
    }

    fun convStr( in _in)
    {
        _months = [ "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC" ];
        _len = LEN(_in);
        _str = _in//EXTRACT(_len,30-_len,"1970/01/01T00:00:00.000000000Q");
        _YYYY = EXTRACT(0,4,_str);
        _MMM = _months[COMPILE(EXTRACT(5,2,_str))-1];
        _DD = EXTRACT(8,2,_str);
        _HH = EXTRACT(11,2,_str);
        _MM = EXTRACT(14,2,_str);
        _SS = EXTRACT(17,2,_str);
        _rest = COMPILE(EXTRACT(20,10,_str));
        /*'12-JAN-2015 01:23:45'*/
        _str= _DD//"-"//_MMM//"-"//_YYYY//" "//_HH//":"//_MM//":"//_SS;
        _var = 0X0Q;
        IF(MdsShr->LibConvertDateString(_str,ref(_var)))
            /* convert 100ns since 'Modified Julian Date' 17-11-1958 to ns since 1970 */
            RETURN((_var - 0X7c95674beb4000Q)*100Q+_rest);
    }

    IF( KIND(_in)==14 )/*STRING*/
    {
        IF(UPCASE(_in)=='NOW')
            RETURN(PRESENT(_now) ? (_now>0Q ? _now : _now=now()) : now());
        ELSE
            RETURN(convStr(_in));
    }
    IF( KIND(_in)==5 )/*QU*/
        RETURN(QUADWORD(_in));
    IF( KIND(_in)==9 )/*Q*/
        RETURN(_in);
    RETURN(QUADWORD(_in*1000000000Q));
}
