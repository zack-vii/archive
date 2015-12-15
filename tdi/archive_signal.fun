/*
Calls the python mds_signl with the url and time from the tree to generate the signal
*/
fun public archive_signal (as_is _node, optional _timein, optional _cachein)
{
    _notree = IF_ERROR( $EXPT=="" , 1BU );
    IF(_notree)
    {
       TREEOPEN("ARCHIVE",-1);
       _path = _node;
       _node = COMPILE(_path);
    }
    ELSE
    {
       _path = GETNCI(_node, "MINPATH");
    }
    _notashot = IF_ERROR( $SHOT==-1, 1 );
    IF ( _notashot )
        _time = (PRESENT(_timein) ? KIND(_timein)==* : 1) ? [-1800., 0, 0] : _timein;
    ELSE
        _time = DATA(COMPILE("\\TIME"));
    _idx = IF_ERROR(EXECUTE(_path // ":$IDX"), * );
    _value = IF_ERROR(EXECUTE(_path // ":$VALUE"), * );
    _urlpar = (KIND(_idx) == *) ? _path // ":$URL" : GETNCI(GETNCI(_path,"PARENT"),"MINPATH") // ":$URL";
    _url = EXECUTE( _urlpar );
    _help= IF_ERROR(EXECUTE(_path // ":HELP"),
                    EXECUTE(_path // ":DESCRIPTION"),
                    EXECUTE(_path // ":$NAME"),
                    _path);
    _cache = PRESENT(_cachein) ? _cachein : *;
    _signal = pyfun('mds_signal', 'archive', _url, _time, _help, _idx, _value, _cache);
    IF(_notree)
       TREECLOSE();
    return(_signal);
}
