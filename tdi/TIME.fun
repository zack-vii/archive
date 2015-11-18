/*
helper fuction that set the _time variable or unsets it.
TIME(from,upto,orig)
from = 0: clear _time

parameters are normalised by the TimeToSec function.

from : defines from parameter     defaults to: 3600
upto : defines upto parameter     defaults to: 'now'
orig : defines the origin t=0     defaults to: 0 or base of relative

from < 1e9s : from as time before upto orig defaults to upto
upto < 1e9s : upto as time after from  orig defaults to from
*/
fun public TIME( optional _from , optional _upto , optional _orig )
{
    IF( IF_ERROR( _in1==0 , 0 ) )
    {
        PUBLIC _time=*;
        DEALLOCATE("_time");
        RETURN(*);
    }
    _now= 0D0;
    _t0 = 0D0;
    _t1 = PRESENT( _in1 ) ? TimeToSec( _in1, _now) : 3600D0;
    _t2 = PRESENT( _in2 ) ? TimeToSec( _in2, _now) : TimeToSec( "NOW" );
    IF(_t1<=1D9 ? _t2>1D9 : 0) _t1 = (_t0=_t2)-_t1; 
    IF(_t2<=1D9 ? _t1>1D9 : 0) _t2 = (_t0=_t1)+_t2;
    _t3 = PRESENT( _orig ) ? TimeToSec( _orig, _now) : _t0;
    RETURN(PUBLIC _time = [ _t1 , _t2 , _t3 ]);
}
