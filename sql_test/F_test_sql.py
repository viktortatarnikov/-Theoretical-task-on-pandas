import sqlite3

db = sqlite3.connect('issues.db')
cursor = db.cursor()

"""Попробуем скомпоновать запросы с помощью функуции JOIN"""
T = '2020-04-14T09:16:32'

zapros = """with ranked as (
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
                    LIMIT 0, 10;"""

cursor.execute(zapros)

# cursor.execute("""with ranked as (
#                       select *,
#                         row_number() over (partition by issue_key order by status desc) as rn
#                       from history
#                     )
#                     select
#                       issue_key,
#                       status,
#                       DATETIME(status_begin / 1000, 'unixepoch')
#                     from ranked
#                     where rn = 1
#                     and status NOT LIKE 'Resolved'
#                     and status NOT LIKE 'Closed'
#                     and status_begin < strftime('%s', '2020-04-14T09:16:32') * 1000
#                     LIMIT 0, 10;""")
#
print(*cursor.fetchall(), sep='\n')
#

db.close()


# import numpy as np
# print(np.arange(30).reshape(3, 10))
# print([[(5 * x) + ind for ind in range(5)] for x in range(5)])

def find_max_uniqe(string):
    max_arr = ''
    start = 0
    indexes = {}
    uniq = set([])
    for i, simbol in enumerate(string):
        if simbol in uniq:
            max_arr = max(max_arr, string[start:i], key=lambda x: len(x))
            start = indexes[simbol] + 1


        indexes[simbol] = i
        uniq.add(simbol)
    max_arr = max(max_arr, string[start:], key=lambda x: len(x))

    return max_arr

tests = ['bbb', 'a', 'ab', '', 'aaaaaaaaa', 'abcdef', 'abcddef', 'aba']

for test1 in tests:
    print(test1, find_max_uniqe(test1))