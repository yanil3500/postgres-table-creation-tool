## How To:
1. Run `python3 -m venv ENV` to create a python virtual environment (this is necessary in order to run the needed python packages)
2. Activate the environment by running `source ENV/bin/activate`
3. Run `pip3 install .` to install the necessary packages
4. To create the tables, run `python3 postgres_create_tables.py create_tables`
5. To fill the database, run `python3 postgres_create_tables.py insert_data <data.csv>`
   
## Note on `insert data`
1. It expects a csv file that follows the following format:
   1. `name of the relation`, followed by the values to be inserted
      1. For example, if I wanted to insert data to the drugs tables, the csv line would like this:
         1. drugs,benztropine,Dacelo-ovaeguineae,"Upsher-Smith Laboratories, Inc."