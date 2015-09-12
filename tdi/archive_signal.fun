fun public archive_signal ( as_is _node, optional _timein )
{
    _time = (PRESENT(_timein) ? KIND(_timein)==* : 1) ? DATA(\TIME) : _timein;
    _path= GETNCI( _node ,"MINPATH");
    _url = EXECUTE( _path //":$URL");
    _help= IF_ERROR(EVALUATE( _path //":$DESCRIPTION"), EVALUATE( _path //":$NAME"), *);
    return(pyfun('mds_signal' , 'archive' , _url , _time , _help ));
}
