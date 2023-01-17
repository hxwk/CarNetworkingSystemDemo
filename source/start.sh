#!/bin/bash
echo
  hive -e "select * from t_user u \
  join t_category c on u.uid=c.cid"
