public fun Notify(in _mode, in _adr, in _nid)
{
  WRITE(*,_mode,_adr,_nid);
  return(1);
  switch(_mode)
  {
    case (0) Write(*,"Open tree: ",_tree," ",_shot); break;
    case (1) Write(*,"Open tree edit: ",_tree," ",_shot); break;
    case (2) Write(*,"Retrieve tree: ",_tree," ",_shot); break;
    case (3) Write(*,"Write tree: ",_tree," ",_shot); break;
    case (4) Write(*,"Close tree: ",_tree," ",_shot); break;
    case (5) Write(*,"Open NCI file for write:",_tree," ",_shot); break;
    case (6) Write(*,"Open DataFile for write:",_tree," ",_shot); break;
    case (7)  _node = getnci(_nid,"FULLPATH"); Write(*,"Get data for node:",_node," ",_tree," ",_shot);break;
    case (8)  _node = getnci(_nid,"FULLPATH"); Write(*,"Get characteristics for node:",_node," ",_tree," ",_shot);break;
    case (9)  _node = getnci(_nid,"FULLPATH"); Write(*,"Put data for node:",_node," ",_tree," ",_shot);break;
    case (10) _node = getnci(_nid,"FULLPATH"); Write(*,"put characteristics for node:",_node," ",_tree," ",_shot);break;
  }
  return(1);
}
