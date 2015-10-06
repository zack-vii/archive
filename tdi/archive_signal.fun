fun public archive_signal (as_is _node, optional _timein)
{
    IF( $EXPT=="ARCHIVE" )
        _time = (PRESENT(_timein) ? KIND(_timein)==* : 1) ? [-1800., 0] : _timein;
    ELSE
        _time = DATA(\TIME);
    _path= GETNCI(_node, "MINPATH");
    IF_ERROR(_idx = [EXECUTE(_path // ":$IDX")], _idx = []);
    _urlpar = (SHAPE(_idx) == [0]) ? _path : GETNCI(GETNCI(_path,"PARENT"),"MINPATH");
    _url = EXECUTE( _urlpar //":$URL");
    _help= IF_ERROR(EXECUTE(_path // ":HELP"),
                    EXECUTE(_path // ":DESCRIPTION"),
                    EXECUTE(_path // ":$NAME"),
                    *);
    return(pyfun('mds_signal', 'archive', _url, _time, _help, _idx));
}
