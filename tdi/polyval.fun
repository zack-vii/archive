fun public polyval( in _vin , in _coeff )
{
   _J = SHAPE(_coeff)[0]-1;
   _vout = _vin * _coeff[_J];
   WHILE ( _J>1 ) _vout = (_vout + _coeff[--_J]) * _vin;
   return(_vout + _coeff[0]);
}
