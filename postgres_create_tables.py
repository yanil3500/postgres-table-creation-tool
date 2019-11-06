# cSpell:ignore psycopg2, zorba, sname, bname
from psycopg2 import connect as connect_to, DatabaseError
import csv
import sys
import fire
__version__ = '1.0.0'

table_names = ['drugs', 'isoforms', 'isoforms_responds_drugs', 'lit_discovers_di_response', 'lit_discovers_dm_response', 'lit_discovers_drugs', 'lit_discovers_isoforms', 'lit_discovers_mutations', 'literature', 'mutations', 'mutations_responds_drugs']                

create_table_commands = [
    """CREATE TABLE Drugs ( D_Name varchar, D_Nucleotide_Sequence varchar, Manufacturer varchar, PRIMARY KEY(D_Name))""",
    """CREATE TABLE Isoforms ( Isoform_ID varchar,I_Nucleotide_Sequence varchar,PRIMARY KEY(Isoform_ID))""",
    """CREATE TABLE Mutations ( Amino_Acid_Sequence varchar,Isoform_ID varchar,M_Nucleotide_Sequence varchar,PRIMARY KEY (Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (Isoform_ID) REFERENCES Isoforms ON DELETE CASCADE)""",
    """CREATE TABLE Isoforms_Responds_Drugs ( D_Name varchar,Isoform_ID varchar,Sensitivity INTEGER,Resistance_Mechanism varchar,PRIMARY KEY (D_Name, Isoform_ID),FOREIGN KEY (D_Name) REFERENCES Drugs, FOREIGN KEY (Isoform_ID) REFERENCES Isoforms)""",
    """CREATE TABLE Mutations_Responds_Drugs ( D_Name varchar, Amino_Acid_Sequence varchar,Isoform_ID varchar, Sensitivity INTEGER,Resistance_Mechanism varchar,PRIMARY KEY (D_Name, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (D_Name) REFERENCES Drugs,FOREIGN KEY (Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations)""",
    """CREATE TABLE Literature (Author varchar, Title varchar, Publication_Title varchar, Publication_Year integer, DOI varchar, Issue varchar, Volume varchar, Last_Updated DATE, PRIMARY KEY (DOI))""",
    """CREATE TABLE Lit_Discovers_Drugs ( DOI varchar,D_Name varchar,PRIMARY KEY (DOI, D_Name),FOREIGN KEY (DOI) REFERENCES Literature, FOREIGN KEY (D_Name) REFERENCES Drugs)""",
    """CREATE TABLE Lit_Discovers_Isoforms ( DOI varchar,Isoform_ID varchar,PRIMARY KEY (DOI, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (Isoform_ID) REFERENCES Isoforms)""",
    """CREATE TABLE Lit_Discovers_Mutations ( DOI varchar,Amino_Acid_Sequence varchar,Isoform_ID varchar,PRIMARY KEY (DOI, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations)""",
    """CREATE TABLE Lit_Discovers_DI_Response ( DOI varchar,D_Name varchar,Isoform_ID varchar,PRIMARY KEY (DOI, D_Name, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (D_Name, Isoform_ID) REFERENCES Isoforms_Responds_Drugs)""",
    """CREATE TABLE Lit_Discovers_DM_Response ( DOI varchar,D_Name varchar,Amino_Acid_Sequence varchar,Isoform_ID varchar,PRIMARY KEY (DOI, D_Name, Amino_Acid_Sequence, Isoform_ID),FOREIGN KEY (DOI) REFERENCES Literature,FOREIGN KEY (D_Name, Amino_Acid_Sequence, Isoform_ID) REFERENCES Mutations_Responds_Drugs)""",
]


insert_into_commands = {
'drugs': {'query': """INSERT INTO Drugs (d_name,d_nucleotide_sequence,manufacturer) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'isoforms': {'query': """INSERT INTO Isoforms(isoform_id, i_nucleotide_sequence) VALUES(%s,%s)""", 'number_of_values': 2},
'mutations': {'query': """INSERT INTO Mutations(amino_acid_sequence,isoform_id,m_nucleotide_sequence) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'isoforms_responds_drugs': {'query': """INSERT INTO Isoforms_Responds_Drugs(d_name,isoform_id,sensitivity,resistance_mechanism) VALUES(%s,%s,%s,%s)""", 'number_of_values': 4},
'mutations_responds_drugs': {'query': """INSERT INTO Mutations_Responds_Drugs(d_name,amino_acid_sequence,isoform_id,sensitivity,resistance_mechanism) VALUES(%s,%s,%s,%s,%s)""", 'number_of_values': 5},
'literature': {'query': """INSERT INTO Literature(author, title, publication_title, publication_year, doi, issue, volume, last_updated) VALUES(%s, %s, %s, %s, %s, %s, %s, now())""", 'number_of_values': 7},
'lit_discovers_drugs': {'query': """INSERT INTO Lit_Discovers_Drugs(doi,date) VALUES(%s, %s)""", 'number_of_values': 2},
'lit_discovers_isoforms': {'query': """INSERT INTO Lit_Discovers_Isoforms(doi,isoform_id) VALUES(%s, %s)""", 'number_of_values': 2},
'lit_discovers_mutations': {'query': """INSERT INTO Lit_Discovers_Mutations(doi,amino_acid_sequence,isoform_id) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'lit_discovers_di_response': {'query': """INSERT INTO Lit_Discovers_DI_Response(doi,d_name,isoform_id) VALUES(%s,%s,%s)""", 'number_of_values': 3},
'lit_discovers_dm_response': {'query': """INSERT INTO Lit_Discovers_DM_Response(doi,d_name,amino_acid_sequence,isoform_id) VALUES(%s,%s,%s,%s)""", 'number_of_values': 4} 
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


def connect():
    """Connect to the PostgreSQL database server"""
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgresSQL server
        conn = connect_to(**params)

        return conn
    except (Exception, DatabaseError) as error:
        print(error)

def create_tables(debug=True):
    # drop all tables
    connection = connect()
    cur = connection.cursor()
    if debug:
        cur.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')

    # create tables
    for command in create_table_commands:
        cur.execute(command)

    # commit the changes to the database
    connection.commit()

    if connection is not None:
        connection.close()
        print('Database connection is closed.')

def insert_data(csv_filename):
    connection = connect()
    cursor = connection.cursor()
    print('csv_filename: ' + csv_filename)
    insert_statements = prepare_insert_statements(csv_filename)
    for statement, values in insert_statements:
        cursor.execute(statement, values)
    connection.commit()
    cursor.close()
    connection.close()


def prepare_insert_statements(csv_filename):
    insert_statements = []
    duplicates = set()
    with open(csv_filename, mode='r', encoding='utf-8-sig') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        for row in reader:
            if row[0] == 'Table':
                continue
            insert_statement = insert_into_commands[row[0]]['query']
            num_of_expected_values = insert_into_commands[row[0]]['number_of_values']
            values = update_empty_values_with_none(row[1:])
            joined_vals = ''.join(row[1:])
            if joined_vals not in duplicates:
                duplicates.add(joined_vals)
            else:
                continue
            if len(values) < num_of_expected_values:
                print('values: {}'.format(values))
                raise Exception("Number of provided values is not equal to the number of expected values")
            insert_statements.append((insert_statement, values))
    return insert_statements

def update_empty_values_with_none(values):
    vals = []
    for val in values:
        if val:
            vals.append(val)
        else:
            vals.append(None)
    return vals


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

def print_this(el):
    for e in el:
        print(e)


if __name__ == '__main__':
    fire.Fire({
        'create_tables': create_tables,
        'insert_data': insert_data
    })
