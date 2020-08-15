import psycopg2
from collections import Counter
from datetime  import datetime


conn = psycopg2.connect(
    dbname='san',
    user='postgres',
)
cur = conn.cursor()
'''
cur.execute("SELECT DISTINCT * FROM lessons \
INNER JOIN participants as par \
ON lessons.event_id=par.event_id \
 INNER JOIN users \
  ON par.user_id=users.id \
 INNER JOIN quality \
  ON quality.lesson_id=lessons.id \
WHERE users.role='tutor' \
AND \
lessons.subject='phys';")

rows = cur.fetchall()

counter = 0
for r in rows:
    counter += 1
    print(r)

print('DONE')
print(f"Count:{counter}")
print(len(rows))
''' 
k=14
cur.execute("SELECT lessons.scheduled_time, users.id, lessons.id,  quality.tech_quality, users.role FROM quality\
                INNER JOIN lessons\
                    ON quality.lesson_id=lessons.id\
                INNER JOIN participants\
                    ON participants.event_id=lessons.event_id\
                INNER JOIN users\
                    ON users.id=participants.user_id\
            WHERE lessons.subject='phys'\
            AND quality.tech_quality!=0\
            AND users.role!='wtutor'\
            AND lessons.scheduled_time='2020-01-20'\
            GROUP BY lessons.scheduled_time, lessons.id, users.id, quality.tech_quality, users.role;")


rows = cur.fetchall()

res = []
for r in rows:
    res.append(r[1])
print(Counter(res))

counter = 0
for r in rows:
    counter += 1
    print(r)


print('DONE')
print(f"Count:{counter}")

cur.close()
conn.close()