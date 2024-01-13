import sqlite3
import pandas as pd

db = sqlite3.connect('issues.db')
cursor = db.cursor()

"""status_begin"""

"""Варинт перевода ЮИКС даты в удобоваримый вид"""
# cursor.execute("SELECT DATETIME(status_begin / 1000, 'unixepoch') FROM history LIMIT 0, 10")

'''1669363867000'''

# cursor.execute("SELECT issue_key, status FROM history WHERE status_begin > 1670000000000 LIMIT 0, 10")

# cursor.execute("""SELECT issue_key, GROUP_CONCAT(status) as all_statues
#                     FROM history
#
#                     GROUP BY issue_key
#                     ORDER BY status_begin
#                     LIMIT 0, 10""")

# cursor.execute("""SELECT issue_key, status_begin, split_part(all_statues, "," -1)
#                     FROM (
#                         SELECT issue_key, GROUP_CONCAT(status) as all_statues, status_begin
#                         FROM history
#                         WHERE status_begin < 1670000000000
#                         GROUP BY issue_key)
#                     WHERE all_statues NOT LIKE '%Resolved' and all_statues NOT LIKE '%Closed'
#                     LIMIT 0, 10""")



"""Попробуем скомпоновать запросы с помощью функуции JOIN"""
T = '2020-04-14T09:16:32'
cursor.execute("""with ranked as (
                      select *,
                        row_number() over (partition by issue_key order by status desc) as rn
                      from history
                    )
                    select 
                      issue_key, 
                      status, 
                      DATETIME(status_begin / 1000, 'unixepoch')
                    from ranked
                    where rn = 1 
                    and status NOT LIKE 'Resolved' 
                    and status NOT LIKE 'Closed' 
                    and status_begin < strftime('%s', '2020-04-14T09:16:32') * 1000
                    LIMIT 0, 10;""")

print(*cursor.fetchall(), sep='\n')

# cursor.execute("SELECT status, GROUP_CONCAT(issue_key) FROM history GROUP BY status LIMIT 0, 20")
# print(*cursor.fetchall(), sep='\n')

# """Балуюсь со временем
# 1) выделяю в листе+кортеже нужный индекс
# 2) обрезаю 3 последних знака, пандас не любит милисикунды
# 3) вывожу что было + преобразование"""
# time = cursor.fetchall()[1][4]
# no_ms_time = int(str(time)[:10])
# print(no_ms_time)
# print(time)
# print(pd.to_datetime(no_ms_time, unit='s'))


db.close()