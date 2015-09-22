fun public funtest( in _arg )
{
    return(IF_ERROR(TEXT(_arg),
                    EVALUATE(_arg),
                   ));
}