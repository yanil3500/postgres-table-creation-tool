# cSpell:ignore psycopg2, zorba, sname, bname
from psycopg2 import connect as connect_to, DatabaseError

table_names = ['drugs', 'isoforms', 'isoforms_responds_drugs', 'lit_discovers_di_response', 'lit_discovers_dm_response', 'lit_discovers_drugs', 'lit_discovers_isoforms', 'lit_discovers_mutations', 'literature', 'mutations', 'mutations_responds_drugs', 'reserves', 'sailors']                

create_table_commands = [
    """CREATE TABLE Drugs ( D_Name varchar, D_Nucleotide_Sequence varchar, Manufacturer varchar, PRIMARY KEY(D_Name))""",
    """CREATE TABLE Isoforms ( Isoform_ID varchar,I_Nucleotide_Sequence varchar,PRIMARY KEY(Isoform_ID))""",
    """CREATE TABLE Mutations ( Amino_Acid_Sequence varchar,Isoform_ID varchar,M_Nucleotide_Sequence varchar,PRIMARY KEY (Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (Isoform_ID) REFERENCES Isoforms ON DELETE CASCADE)""",
    """CREATE TABLE Isoforms_Responds_Drugs ( D_Name varchar,Isoform_ID varchar,Sensitivity INTEGER,Resistance_Mechanism varchar,PRIMARY KEY (D_Name, Isoform_ID),FOREIGN KEY (D_Name) REFERENCES Drugs, FOREIGN KEY (Isoform_ID) REFERENCES Isoforms)""",
    """CREATE TABLE Mutations_Responds_Drugs ( D_Name varchar, Amino_Acid_Sequence varchar,Isoform_ID varchar, Sensitivity INTEGER,Resistance_Mechanism varchar,PRIMARY KEY (D_Name, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (D_Name) REFERENCES Drugs,FOREIGN KEY (Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations)""",
    """CREATE TABLE Literature ( DOI varchar,Date DATE,PRIMARY KEY (DOI))""",
    """CREATE TABLE Lit_Discovers_Drugs ( DOI varchar,D_Name varchar,PRIMARY KEY (DOI, D_Name),FOREIGN KEY (DOI) REFERENCES Literature, FOREIGN KEY (D_Name) REFERENCES Drugs)""",
    """CREATE TABLE Lit_Discovers_Isoforms ( DOI varchar,Isoform_ID varchar,PRIMARY KEY (DOI, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (Isoform_ID) REFERENCES Isoforms)""",
    """CREATE TABLE Lit_Discovers_Mutations (DOI varchar,Amino_Acid_Sequence varchar,Isoform_ID varchar,PRIMARY KEY (DOI, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations)""",
    """CREATE TABLE Lit_Discovers_DI_Response (DOI varchar,D_Name varchar,Isoform_ID varchar,PRIMARY KEY (DOI, D_Name, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (D_Name, Isoform_ID) REFERENCES Isoforms_Responds_Drugs)""",
    """CREATE TABLE Lit_Discovers_DM_Response (DOI varchar,D_Name varchar,Amino_Acid_Sequence varchar,Isoform_ID varchar,PRIMARY KEY (DOI, D_Name, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (D_Name, Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations_Responds_Drugs)""",
]


def config(filename='database.ini', section='postgresql'):
    from configparser import ConfigParser
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))
    return db


def connect():
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgresSQL server
        conn = connect_to(**params)

        # create cursor
        cur = conn.cursor()

        # drop all tables
        cur.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')


        # create tables
        for command in create_table_commands:
            cur.execute(command)

        # commit the changes to the database
        conn.commit()

        # close communication with the database
        conn.close()
    except (Exception, DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection is closed.')


def is_table_in_db(db_conn, table_name):
    db_cur = db_conn.cursor()
    db_cur.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = '{}'
    """.format(table_name)
    )
    request = db_cur.fetchone()
    if request[0] == 1:
        db_cur.close()
        return True

    db_cur.close()
    return False


if __name__ == '__main__':
    connect()
