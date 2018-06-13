# Importing libraries
import sqlalchemy as sql
import pandas as pd


# Create SQL engine, connection string in brackets
def create_engine(connection_string):
    engine = sql.create_engine(connection_string)
    return engine


# Connected using context manager, turned result set into a DataFrame
def dataset_import():
    with engine.connect() as con:
        rs = con.execute('SELECT * FROM actor')
        df = pd.DataFrame(rs.fetchall())
        df.columns = rs.keys()
        return df


# Returns a generator object along with the length of lfiltermore as a tuple
# Used to filter the dataset based on what the first name starts with and additionally what the second name starts with
# repr used in the generator to surround data in quotes so it can be used in the SQL statement
def filter_list(df):
    filter_list = []
    for index, value in df.iterrows():
        if (value.first_name.startswith('A') or
            value.first_name.startswith('E') or
            value.first_name.startswith('I') or
            value.first_name.startswith('O') or
            value.first_name.startswith('U')):
                filter_list.append([value.actor_id, value.first_name, value.last_name, value.last_update])
    l_filtermore = [x for x in filter_list if x[2].startswith('M') or x[2].startswith('G')]
    sql_gen = (repr(column) for lists in l_filtermore for column in lists)
    return sql_gen, len(l_filtermore)
    

# *args used to unpack the tuple passed from filterList containing the sqlgen generator object and the length of the list.
# sql_gen is used to retrieve the data to import
# no_of_records is used as the number of rows that are being imported
def sqlUpdate(*args):
    sql_gen, no_of_records = args[0][0], args[0][1]
    for total_records in range(no_of_records):
        with engine.connect() as con:
            con.execute("INSERT INTO actor_updated VALUES (" 
                                                        + next(sql_gen) 
                                                        + ', ' 
                                                        + next(sql_gen) 
                                                        + ', ' 
                                                        + next(sql_gen)
                                                        + ', ' 
                                                        + next(sql_gen) 
                                                        + ')')


# Creating the engine by passing the connection string to create_engine
engine = create_engine('mysql://root:James123@localhost/sakila')


# Calling the sqlUpdate function with the filterList function that takes datasetImport it's argument
#This imports the dataset, filters the dataset and finally inserts the results into sql
sqlUpdate(filter_list(dataset_import()))
