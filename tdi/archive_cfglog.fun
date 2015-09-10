/*
Calls the python mds_cfglog with the url and time from the tree
*/
fun public archive_cfglog ( as_is _node , optional _time)
{
    _url  = EXECUTE( GETNCI( _node, "MINPATH" ) // ":$URL"  );
    PRESENT(_time) ? IF(KIND(_time)==*,_time=EVALUATE(\TIME)) : _time=EVALUATE(\TIME);
    return(pyfun('mds_cfglog', 'archive', _url, _time));
}