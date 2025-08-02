SELECT tm.*
FROM table_game_user_map tm 
LEFT JOIN playing_tables pt ON pt.table_id = tm.table_id 
WHERE pt.lt_id in ('2','8') 
  AND tm.created_at >= NOW() - INTERVAL 16 HOUR;