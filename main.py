import psycopg2

conn = psycopg2.connect(database='clients', user='postgres', password='padzzzer18flot18vk')

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clients (
        client_id SERIAL PRIMARY KEY,
        name VARCHAR(40) NOT NULL,
        surname VARCHAR(40) NOT NULL,
        e_mail VARCHAR(40) NOT NULL
        );
                
        CREATE TABLE IF NOT EXISTS phone_numbers (
        phone_numbers_id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES clients(client_ID),
        phone_numbers DECIMAL (11)
        );
        """)
    conn.commit()
    return print('Таблицы созданы')
    pass

def add_new_client(conn, name, surname, e_mail=None):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO clients (name, surname, e_mail)
        VALUES (%s, %s, %s)
        RETURNING (client_id, name, surname, e_mail);
        """, (name, surname, e_mail))
        conn.commit()
        return cur.fetchone()
    pass

def add_phone_numbers(conn, client_id, phone_numbers):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phone_numbers (client_id, phone_numbers)
        VALUES (%s, %s)
        RETURNING (client_id, phone_numbers);
        """, (client_id, phone_numbers))
        conn.commit()
        return cur.fetchone()
    pass

def change_client(conn, client_id, name=None, surname=None, e_mail=None):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE clients
        SET name=%s, surname=%s, e_mail=%s
        WHERE client_id=%s
        RETURNING (name, surname, e_mail, client_id);
        """, (name, surname, e_mail, client_id))
        conn.commit()
    pass

def change_phone(conn, client_id, phone_numbers=None):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE phone_numbers
        SET phone_numbers=%s
        WHERE client_id=%s
        RETURNING (client_id, phone_numbers);
        """, (phone_numbers, client_id))
        conn.commit()
    pass

def delete_phone (conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM phone_numbers
        WHERE client_id=%s;
        """, (client_id,))
        conn.commit()
    pass

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM clients
        WHERE client_id=%s;
        """, (client_id,))
        conn.commit()
    pass

def find_client (conn, name=None, surname=None, e_mail=None, phone_numbers=None):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT c.name, c.surname, c.e_mail, pn.phone_numbers From clients c
        LEFT JOIN phone_numbers pn ON c.client_id = pn.client_id
        WHERE c.name=%s OR c.surname=%s OR c.e_mail=%s OR pn.phone_numbers=%s;
        """, (name, surname, e_mail, phone_numbers,))
        return print(cur.fetchone())
    pass

# create_db()

# add_new_client('Вася', 'Пупкин', 'vp@mail.ru')
# add_new_client('Петя', 'Иванов', 'pi@mail.ru')
# add_new_client('Илья', 'Макаров', 'im@mail.ru')

# add_phone_numbers('1', '88005553535')
# add_phone_numbers('2', '89651154422')
# add_phone_numbers('3', '85553331122')

# change_client(conn, '1', 'Антон', 'Антонов', 'aa@mail.ru')

# change_phone(conn, '1', '55555555555')

# delete_phone(conn, '1')

# delete_client(conn, '1')

# find_client(conn, 'Петя')

conn.close()