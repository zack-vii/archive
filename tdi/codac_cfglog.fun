/*
Calls the python mds_cfglog with the url and time from the tree
*/
fun public codac_cfglog ( as_is _node )
{
    _root = GETNCI(_node, "PARENT");
    _url  = EXECUTE( GETNCI( _root, "MINPATH" ) // ":$URL"  );
    _time = EVALUATE(\TIME);
    return(pyfun('mds_cfglog', 'codac', _url, _time));
}