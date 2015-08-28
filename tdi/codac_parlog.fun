/*
Calls the python mds_parlog with the url and time from the tree
*/
fun public codac_parlog ( as_is _node )
{
    _root = GETNCI(_node, "PARENT");
    _url  = EXECUTE( GETNCI( _root, "MINPATH" ) // ":$URL"  );
    _time = EVALUATE(\TIME);
    return(pyfun('mds_parlog', 'codac', _url, _time));
}