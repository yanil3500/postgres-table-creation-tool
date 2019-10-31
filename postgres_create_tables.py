# cSpell:ignore psycopg2, zorba, sname, bname
from psycopg2 import connect as connect_to, DatabaseError
import csv
import sys
__version__ = '1.0.0'

table_names = ['drugs', 'isoforms', 'isoforms_responds_drugs', 'lit_discovers_di_response', 'lit_discovers_dm_response', 'lit_discovers_drugs', 'lit_discovers_isoforms', 'lit_discovers_mutations', 'literature', 'mutations', 'mutations_responds_drugs']                

create_table_commands = [
    """CREATE TABLE Drugs ( D_Name varchar, D_Nucleotide_Sequence varchar, Manufacturer varchar, PRIMARY KEY(D_Name))""",
    """CREATE TABLE Isoforms ( Isoform_ID varchar,I_Nucleotide_Sequence varchar,PRIMARY KEY(Isoform_ID))""",
    """CREATE TABLE Mutations ( Amino_Acid_Sequence varchar,Isoform_ID varchar,M_Nucleotide_Sequence varchar,PRIMARY KEY (Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (Isoform_ID) REFERENCES Isoforms ON DELETE CASCADE)""",
    """CREATE TABLE Isoforms_Responds_Drugs ( D_Name varchar,Isoform_ID varchar,Sensitivity INTEGER,Resistance_Mechanism varchar,PRIMARY KEY (D_Name, Isoform_ID),FOREIGN KEY (D_Name) REFERENCES Drugs, FOREIGN KEY (Isoform_ID) REFERENCES Isoforms)""",
    """CREATE TABLE Mutations_Responds_Drugs ( D_Name varchar, Amino_Acid_Sequence varchar,Isoform_ID varchar, Sensitivity INTEGER,Resistance_Mechanism varchar,PRIMARY KEY (D_Name, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (D_Name) REFERENCES Drugs,FOREIGN KEY (Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations)""",
    """CREATE TABLE Literature ( DOI varchar,Date DATE,PRIMARY KEY (DOI))""",
    """CREATE TABLE Lit_Discovers_Drugs ( DOI varchar,D_Name varchar,PRIMARY KEY (DOI, D_Name),FOREIGN KEY (DOI) REFERENCES Literature, FOREIGN KEY (D_Name) REFERENCES Drugs)""",
    """CREATE TABLE Lit_Discovers_Isoforms ( DOI varchar,Isoform_ID varchar,PRIMARY KEY (DOI, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (Isoform_ID) REFERENCES Isoforms)""",
    """CREATE TABLE Lit_Discovers_Mutations ( DOI varchar,Amino_Acid_Sequence varchar,Isoform_ID varchar,PRIMARY KEY (DOI, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations)""",
    """CREATE TABLE Lit_Discovers_DI_Response ( DOI varchar,D_Name varchar,Isoform_ID varchar,PRIMARY KEY (DOI, D_Name, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (D_Name, Isoform_ID) REFERENCES Isoforms_Responds_Drugs)""",
    """CREATE TABLE Lit_Discovers_DM_Response ( DOI varchar,D_Name varchar,Amino_Acid_Sequence varchar,Isoform_ID varchar,PRIMARY KEY (DOI, D_Name, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (D_Name, Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations_Responds_Drugs)""",
]


insert_into_commands = {
'drugs': {'query': """INSERT INTO Drugs (c1,c2,c3) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'isoforms': {'query': """INSERT INTO Isoforms(c1,c2) VALUES(%s,%s)""", 'number_of_values': 2},
'mutations': {'query': """INSERT INTO Mutations(c1,c2,c3) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'isoforms_responds_drugs': {'query': """INSERT INTO Isoforms_Responds_Drugs(c1,c2,c3,c4) VALUES(%s,%s,%s,%s)""", 'number_of_values': 4},
'mutations_responds_drugs': {'query': """INSERT INTO Mutations_Responds_Drugs(c1,c2,c3,c4,c5) VALUES(%s,%s,%s,%s,%s)""", 'number_of_values': 5},
'literature': {'query': """INSERT INTO Literature(c1,c2) VALUES(%s, %s)""", 'number_of_values': 2},
'lit_discovers_drugs': {'query': """INSERT INTO Lit_Discovers_Drugs(c1,c2) VALUES(%s, %s)""", 'number_of_values': 2},
'lit_discovers_isoforms': {'query': """INSERT INTO Lit_Discovers_Isoforms(c1,c2) VALUES(%s, %s)""", 'number_of_values': 2},
'lit_discovers_mutations': {'query': """INSERT INTO Lit_Discovers_Mutations(c1,c2,c3) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'lit_discovers_di_response': {'query': """INSERT INTO Lit_Discovers_DI_Response(c1,c2,c3) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'lit_discovers_dm_response': {'query': """INSERT INTO Lit_Discovers_DM_Response(c1,c2,c3,c4) VALUES(%s,%s,%s,%s)""", 'number_of_values': 4} 
    }


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


def connect(database_configuration_path):
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # read connection parameters
        params = config(filename=database_configuration_path)

        # connect to the PostgresSQL server
        conn = connect_to(**params)

        return conn
    except (Exception, DatabaseError) as error:
        print(error)

def create_tables(connection):
    # drop all tables
    cur = connection.cursor()
    cur.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')

    # create tables
    for command in create_table_commands:
        cur.execute(command)

    # commit the changes to the database
    connection.commit()

    if connection is not None:
        connection.close()
        print('Database connection is closed.')

def insert_data(connection, insert_statements):
    cursor = connection.cursor()
    for statement, values in insert_statements:
        cursor.execute(statement, values)
    connection.commit()
    cursor.close()
    connection.close()


def prepare_insert_statements(csv_filename):
    insert_statements = []
    with open(csv_filename) as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        for row in reader:
            insert_statement = insert_into_commands[row[0]]['query']
            num_of_expected_values = insert_into_commands[row[0]]['number_of_values']
            values = row[1:]
            if len(values) < num_of_expected_values:
                raise Exception("Number of provided values is not equal to the number of expected values")
            insert_statements.append((insert_statement, values))
    return insert_statements


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
    prepare_insert_statements(sys.argv[1])
    conn = connect('database.ini')
