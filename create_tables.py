from create_tables_tool.postgres_create_tables import connect as create_tables

def main():
    print('creating tables.')
    create_tables()
    print('done.')

if __name__ == "__main__":
    main()