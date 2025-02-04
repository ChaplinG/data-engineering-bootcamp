#imports the libraries and tools needed to configure a DAG (Directed Acyclic Graph) in Airflow and interact with databases
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

#the dictionary will contain the parameters that will be used in the DAG
default_args = {
    'owner' : 'airflow', #Airflow user that was created; DAG owner
    'depends_on_past' : False, #if some of the past code didn't execute, it won't depend on the succesful execution of the previous task
    'start_date' : datetime(2024,1,1),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0, #if a task fails, retries define how many times it will try to run it again
    'retry_delay' : timedelta(minutes=5), #the time it will wait until it tries to run again
} 

#creates the definition of the DAG
@dag(
    dag_id = 'postgres_to_snowflake', #as it will show up in the Airflow interface
    default_args = default_args,
    description = 'Load data incrementally from Postgres to Snowflake',
    schedule_interval = timedelta(days=1), #how often the DAG will be executed
    catchup = False, #if the execution fails, "catchup" defines whether it will execute all the missed executions
)

#the decorator above is about the following function:
def prostgres_to_snowflake_etl():
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']
    
    #creates a loop to go through the list created above
    for table_name in table_names:
        #creates the tasks
        @task(task_id = f'get_max_id_{table_name}') #get_max_id checks the last ID in the destination, the highest primary key
        def get_max_primary_key(table_name: str): #takes the primary key of the current table
            with SnowflakeHook(snowflake_conn_id = 'snowflake').get_conn() as conn: #the hook is like a connector of Snowflake; Airflow has a space to register connections, which is made by/with an ID. For this course, it gets the names "airflow" and "snowflake"
                with conn.cursor() as cursor: #after the connection, a cursor is open
                    cursor.execute(f'SELECT MAX(ID_{table_name}) FROM {table_name}') #cursor.execute will execute a SQL code
                    max_id = cursor.fetchone()[0] #SELECT return only one record/one value
                    return max_id if max_id is not None else 0 #if there is no record, it will return 0 

        #opening a connection with Postgres
        @task(task_id = f'load_data_{table_name}')
        def load_incremental_data(table_name:str, max_id: int): #load_incremental_data in the incremental load process, it loads starting from the last key
            with PostgresHook(postgres_conn_id = 'postgres').get_conn() as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    primary_key = f'ID_{table_name}' #creation of the primary_key name

                    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'") #get columns name
                    columns = [row[0] for row in pg_cursor.fetchall()] #use fetchall so we have the name of all the columns
                    columns_list_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))  #creation of placeholders list; there will have an insert like "veiculos, vendas, estados, etc" + and then "(%, %, %)" which are the placeholders that will be filled with insert query on the next second part of the code

                    pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id}") #select only the columns higher than the last ID identified with get_max_id
                    rows = pg_cursor.fetchall()

                    with SnowflakeHook(snowflake_conn_id = 'snowflake').get_conn() as sf_conn:
                        with sf_conn.cursor() as sf_cursor:
                            insert_query = f"INSERT INTO {table_name} ({columns_list_str})VALUES ({placeholders})"
                            for row in rows:
                                sf_cursor.execute(insert_query, row)

        max_id = get_max_primary_key(table_name)
        load_incremental_data(table_name, max_id)

#now the code runs the function created
prostgres_to_snowflake_etl_dag = prostgres_to_snowflake_etl()