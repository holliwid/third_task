import psycopg2

conn = psycopg2.connect(
    dbname='san',
    user='postgres',
)
cur = conn.cursor()


def users(filepath='users.txt', cursor=cur):
    cur.execute("TRUNCATE users")
    with open(filepath) as file:
        line = file.readline()
        while line:
            data = list(line.split())
            cur.execute("INSERT INTO users (id, role) values (%s, %s);", (data[0], data[2]))
            conn.commit()
            line = file.readline()      


def lessons(filepath='lessons.txt', cursor=cur):
    cur.execute("TRUNCATE lessons")
    with open(filepath) as file:
        line = file.readline()
        while line:
            data = list(line.split())
            cur.execute("INSERT INTO lessons (id, event_id, subject, scheduled_time) values (%s, %s, %s, %s);", (data[0], data[2], data[4], data[6]))
            conn.commit()
            line = file.readline()   


def participants(filepath='participants.txt', cursor=cur):
    cur.execute("TRUNCATE participants")
    with open(filepath) as file:
        line = file.readline()
        while line:
            data = list(line.split())
            cur.execute("INSERT INTO participants (event_id, user_id) values (%s, %s);", (data[0], data[2]))
            conn.commit()
            line = file.readline() 


def quality(filepath='quality.txt', cursor=cur):
    cur.execute("TRUNCATE quality")
    with open(filepath) as file:
        line = file.readline()
        while line:
            data = list(line.split())
            print(data)
            if len(data) == 2:
                data.append(0)
            cur.execute("INSERT INTO quality (lesson_id, tech_quality) values (%s, %s);", (data[0], data[2]))
            conn.commit()
            line = file.readline() 

if __name__ == "__main__":
    users()
    lessons()
    participants()
    quality()
    cur.close()
    conn.close()
    print("DONE!")