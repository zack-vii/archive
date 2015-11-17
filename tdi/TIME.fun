/*
helper fuction that set the _time variable or unsets it with TIME(0)
*/
fun public TIME( optional as_is _in1 , optional _in2 , optional _in3 )
{
    IF( PRESENT( _in1 ) ? _in1==0 : 0)
    {
        PUBLIC _time=*;
        DEALLOCATE('_time');
    }
    ELSE
    {
       IF( PRESENT( _in3 ) )
          PUBLIC _time = [D_FLOAT( _in1 ), D_FLOAT( _in2 ), D_FLOAT( _in3 ) ];
       ELSE
       {
           IF( PRESENT( _in2 ) )
               PUBLIC _time = [D_FLOAT( _in1 ), D_FLOAT( _in2 ), D_FLOAT(_in1)];
           ELSE
           {
               IF( EXTRACT(0,1,GETENV("os"))=="W" )
               {
                   /* get current time windows */
                   _t = QUADWORD_UNSIGNED(0);
                   kernel32->GetSystemTimeAsFileTime(ref(_t));
                   /* conver 100ns since 1601 to s since 1970 */
                   _t = ( _t - 0X19db1ded53e8000QU )/1D7;
               }
               ELSE
                   _in2 = cvttime();
               IF( NOT PRESENT( _in1 ) )
                   _in1 = 3600;
               PUBLIC _time = [_t - D_FLOAT(_in1), _t, _t ];
           }
       }
       RETURN(_time);
    }
}