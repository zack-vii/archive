/*
returns the url determined recursively by the name and parent url
if $IDX exists the leaf if a Channel and we add the _DATASTREAM suffix and index
*/
fun public codac_url ( as_is _node )
{
/*    _node = GETNCI(_node, "MINPATH");*/
    _prnt = GETNCI(_node, "PARENT");
    _url  =           EXECUTE( GETNCI( _prnt, "MINPATH" ) // ":$URL"  );
    _name =           EXECUTE( GETNCI( _node, "MINPATH" ) // ":$NAME" );
    _idx  = IF_ERROR( EXECUTE( GETNCI( _node, "MINPATH" ) // ":$IDX"  ), -1 );
    IF (GE(_idx,0)) {
        return(_url // "_DATASTREAM/" // STR(_idx) // "/" // _name);
    } ELSE {
        return( _url // "/" // _name );
    }
}