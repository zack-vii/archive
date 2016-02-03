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
fun public TIMEf( optional in _from , optional in _upto , optional in _orig )
{
    IF(PRESENT("_from"))
    {
        IF(!PRESENT("_upto"))
        {
            IF(KIND(_from)==0)
            {
                PUBLIC _time = *;
                DEALLOCATE("_time");
                RETURN(*);
            }
            ELSE
            {
                TREEOPEN("W7X",_from);
                PUBLIC _time = DATA(COMPILE("TIMING"));
                TREECLOSE("W7X",_from);
                RETURN(IF_ERROR(TEXT(_time/1D9,20),"shot not found"));
            }
        }
        ELSE
        {
            _now= 0Q;
            _t0 = 0Q;
            _t1 = PRESENT("_from") ? TimeToNs( _from, _now) : 3600000000000Q;
            _t2 = PRESENT("_upto") ? TimeToNs( _upto, _now) : TimeToNs( "NOW" );
            IF(_t1<=1E18 ? _t2>1E18 : 0) _t1 = (_t0=_t2)-_t1;
            IF(_t2<=1E18 ? _t1>1E18 : 0) _t2 = (_t0=_t1)+_t2;
            _t3 = PRESENT( _orig ) ? TimeToNs( _orig, _now) : _t0;
            RETURN(PUBLIC _time = [ _t1 , _t2 , _t3 ]);
        }
    }
    ELSE
    {
        RETURN(IF_ERROR(TEXT(PUBLIC("_time")/1D9,20),"undefined"));
    }
}
