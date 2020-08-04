import psycopg2
from src.data.config import config

def fetch_all_person_rows():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = 'SELECT * FROM person;'
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def describe_column():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = "SELECT column_name FROM information_schema.columns WHERE table_name = 'person';"
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def get_person():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        q2 = "SELECT * FROM person"
        cursor.execute(q2)
        row2 = cursor.fetchall()
        description = cursor.description
        print(description)
        print(row2)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def get_certificates():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        q2 = "SELECT * FROM certificates"
        cursor.execute(q2)
        row2 = cursor.fetchall()
        description = cursor.description
        print(description)
        print(row2)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def aws_holders():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = '''
        SELECT person.name,certificates.name 
        FROM person,certificates 
        WHERE certificates.name = 'AWS Certified Cloud Associate ' 
        AND certificates.person_id=person.id;
        '''
        cursor.execute(SQL)
        row = cursor.fetchall()
        print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def insert_certificate_row(nimi,personiidee):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute('''
            INSERT INTO certificates(name,person_id) 
            VALUES (%s, %s);
            ''', (nimi,personiidee))
        con.commit()
        # row = cursor.fetchall()
        # print(row)
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def insert_person_row(nimi,ikä,student=False):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute('''
            INSERT INTO person(name,age,student) 
            VALUES (%s, %s, %s);
            ''', (nimi,ikä,student))
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()


def update_person(iidee,name,age,student=False):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute('''
            UPDATE person SET (name,age,student) = (%s, %s, %s)
            WHERE id = %s;
            ''', (name,age,student,iidee))
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def update_certificate(iidee,name,personid):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute('''
            UPDATE certificates SET (name,person_id) = (%s, %s)
            WHERE id = %s;
            ''', (name,personid,iidee))
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def delete_person(iidee):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute('''
            DELETE FROM person WHERE id = %s;
            ''', (iidee,))
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def delete_certificate(iidee):
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute('''
            DELETE FROM certificates WHERE id = %s;
            ''', (iidee,))
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def create_table():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        cursor.execute('''
            CREATE TABLE pets (
            id SERIAL PRIMARY KEY, 
            name varchar(255) NOT NULL,
            animal varchar(255) NOT NULL,
            owner_id int,
            CONSTRAINT fk_owner
                FOREIGN KEY(owner_id)
                    REFERENCES person(id)
            ) ;
            ''')
        con.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

