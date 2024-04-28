import sqlalchemy as sa
import pandas as pd
import pymssql
from dotenv import dotenv_values
from dagster import (
    get_dagster_logger,
)

import rld_to_mssql_tools.prj_parameters as prj


# ____________ Keywords to define primary keys on MSSQL tables
DATA_TABLE_PK = "Timestamp"
LOGGER_TABLE_PK = "Site Number"
SENSOR_TABLE_FK = "Channel"

# ____________ Data frame column name
# To know wich rld file the data is coming from, its append to the dataframe
DF_RLD_FILE_COL = "rld_file_name"
DF_TXT_FILE_COL = "txt_file_name"

# ____________ Table names where loggers, sensors and data will be uploaded
LOGGER_TABLE_NAME = "NRG_Loggers"
SENSOR_TABLE_NAME = "NRG_Sensors"
MSSQL_DATA_TABLE_NAME = "NRG_Tower_SN"

# ____________ Constants for DuckDB paths and database name, and upload user
DUCKDB_PATH = "duckdb_data"
DUCKDB_NAME = "rld_to_mmsql.db"
UPLD_USER = "darespinozas_dagster"

def create_mssql_engine():
    """ Creates a SQLAlchemy engine using the parameters from get_mssql_params function

    Returns:
        sa.engine: SQLAlchemy engine created with specified parameters
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Use environment variables for database connection
        db_params = prj.get_mssql_params()
        
        # Create SQLalchemy engine
        connection_str = f"mssql+pyodbc://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        engine = sa.create_engine(connection_str)
        
        return (engine)
    except sa.exc.SQLAlchemyError as db_e:
        logger.error(f"Error de SQLAlchemy, al crear engine:\n{str(db_e)}")
    except Exception as e:
        logger.error(f"Error al crear SQLAlchemy engine:\n{str(e)}")


def create_mssql_conn ():
    """ Creates a connection to MSSQL using Pymssql with parameters from get_mssql_params function

    Returns:
        pymssql.connect: Connection to MSSQL using Pymssql
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Use environment variables for database connection
        db_params = prj.get_mssql_params()
        
        # Create Pymssql connection
        conn = pymssql.connect(server=db_params['host'],
            user=db_params['user'],
            password=db_params['password'],
            database=db_params['database'],
            port=db_params['port'],
            as_dict=True)
        
        return (conn)
    except Exception as e:
        logger.error(f"Error al crear conexión Pymssql:\n{str(e)}")


def create_alter_mssql_table(table_name, data_to_insert, pk_column="", pk_flag=True, text_file_name=""):
    """ Given a Dataframe, uses a SQLAlchemy engine to get metadata from MSSQL database and query to its information schema to know if a specified table exists.
        If table doesnt exist, creates it. If it exists check if columns from table and dataframe are the same.
        If Dataframe has more columns, ALTER the table adding them.
        
        If data_to_insert is empty or not passed as argument, only checks if table_name exists on MSSQL
        
        Returns true false if there's an error.
    Args:
        table_name (String): MSSQL table name to seach for
        data_to_insert (pd.Dataframe): Says by itself
        pk_column (String): String that will be primary key of the table
        pk_flag (Bool): Flag that says if primary key should be created on table, if True fk_column must be declared
        text_file_name (String): Text file name where data is

    Returns:
        bool: False if theres an error creating or altering the table, True if not
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Use environment variables for database connection
        db_params = prj.get_mssql_params()
        
        # Check if specified table exists
        engine = create_mssql_engine()
        insp = sa.inspect(engine)
        tbl_exist = insp.has_table(table_name, schema=db_params['schema'])
        engine.dispose()
        
        # If Dataframe is empty, only return table existence
        # If its not empty use it to create or alter the table
        if (not data_to_insert.empty):
            # If table doesnt exist, create it
            if (not tbl_exist):
                # Prepare engine and metadata for table creation
                engine = create_mssql_engine()
                metadata = sa.MetaData()
                
                # Get list of columns for MSSQL creation with sqlalchemy
                mssql_cols = []
                for col_name, col_type in data_to_insert.dtypes.items():
                    if col_type == 'int64':
                        mssql_cols.append(sa.Column(col_name, sa.Integer))
                    elif col_type == 'object':
                        mssql_cols.append(sa.Column(col_name, sa.String))
                    elif col_type == 'datetime64[ns]':
                        mssql_cols.append(sa.Column(col_name, sa.DateTime))
                    elif col_type == 'float64':
                        mssql_cols.append(sa.Column(col_name, sa.Float))
                    else:
                        mssql_cols.append(sa.Column(col_name, sa.String))
                
                # Check if Dataframe has primary key column
                if pk_column in data_to_insert.columns and pk_flag:
                    # Set Timestamp column as Primary Key
                    # SQLAlchemy column objects
                    for col in mssql_cols:
                        if col.name == pk_column:
                            col.primary_key = True
                    
                    # Create table with Primary key
                    table = sa.Table(table_name, metadata, *mssql_cols)
                    metadata.create_all(engine)
                    logger.info(f"Creada tabla {table_name}. Llave primaria {pk_column}.")
                    
                    # Close engine used for table creation
                    engine.dispose()
                    return (True)
                
                elif pk_column in data_to_insert.columns and not pk_flag:
                    # Create table without Primary key
                    table = sa.Table(table_name, metadata, *mssql_cols)
                    metadata.create_all(engine)
                    logger.info(f"Creada tabla {table_name} sin llave primaria")
                    
                    # Close engine used for table creation
                    engine.dispose()
                    return (True)
                else:
                    logger.error(f"No se encontró columna {pk_column} en Dataframe. No se puede cargar data a MSSQL.")
                    # Close engine used for table creation
                    engine.dispose()
                    return (False)
            else:
                # Get column names and datatypes from Dataframe
                df_columns = pd.DataFrame({'col_name': data_to_insert.columns, 'col_type': data_to_insert.dtypes.values})
                # Create a column with the column name in Uppercase, to compare names
                # Because MSSQL is not case sensitive
                df_columns['col_name_vs'] = df_columns['col_name'].str.upper()
                
                # Get metadata from InformationSchema of MSSQL
                engine = create_mssql_engine()
                metadata = sa.MetaData()
                metadata.reflect(bind=engine)
                
                # Get MSSQL specified table column names
                table_exs = metadata.tables[table_name]
                tbl_columns = table_exs.columns.keys()
                df_tbl_columns = pd.DataFrame(tbl_columns, columns=['col_name'])
                # Create a column with the column name in Uppercase, to compare names
                # Because MSSQL is not case sensitive 
                df_tbl_columns['col_name_vs'] = df_tbl_columns['col_name'].str.upper()
                
                # Make a left join with the column names in Upper case of dataframe and column names from MSSQL table
                lj_col_names = df_columns.merge(df_tbl_columns, on='col_name_vs', how='left', indicator=True)
                lj_col_names = lj_col_names.loc[lj_col_names._merge=='left_only', lj_col_names.columns!='_merge']
                
                # Create new columns on table, if they exist
                if len(lj_col_names) > 0:
                    # If text file name was provided
                    if text_file_name != "":
                        logger.warning(f"Se identificó {len(lj_col_names)} nueva/s columnas en data proveniente de del archivo {text_file_name}.")
                    else:
                        logger.warning(f"Se identificó {len(lj_col_names)} nueva/s columnas.")
                    
                    # Get MSSQL connection to execute alter statements
                    conn = create_mssql_conn()
                    cursor = conn.cursor()
                    
                    # Iterate with new columns name and type
                    for idx, col_now in lj_col_names.iterrows():
                        # Get type for MSSQL
                        if col_now['col_type'] == 'int64':
                            new_col_type = "SMALLINT"
                        elif col_now['col_type'] == 'object':
                            new_col_type = "VARCHAR(512)"
                        elif col_now['col_type'] == 'datetime64[ns]':
                            new_col_type = "DATETIME"
                        elif col_now['col_type'] == 'float64':
                            new_col_type = "FLOAT"
                        else:
                            new_col_type = "VARCHAR(512)"
                        
                        # Create new column on table, executing ALTER statement
                        alter_sql = f"ALTER TABLE {db_params['database']}.{db_params['schema']}.[{table_name}] ADD [{col_now['col_name_x']}] {new_col_type};"
                        cursor.execute(alter_sql)
                        conn.commit()
                        logger.info(f"Creada columna '{col_now['col_name_x']}' tipo {new_col_type} en tabla {table_name}.")
                        
                    # Close cursor and connection
                    cursor.close()
                    conn.close()
                
                return (True)
        else:
            # If text file name was provided
            if text_file_name != "":
                logger.warning(f"Sin data del archivo '{text_file_name}' para ingresar ")
            else:
                logger.warning(f"Sin data para ingresar")
            return (False)
    except sa.exc.SQLAlchemyError as db_e:
        logger.error(f"Error de SQLAlchemy, al crear/alterar tabla {table_name}:\n{str(db_e)}")
        return (False)
    except Exception as e:
        logger.error(f"Error al crear/alterar tabla {table_name}:\n{str(e)}")
        return (False)


def insert_data_msssql(table_name, data_to_insert, pk_column, pk_value="", fk_column=""):
    """ Given a MSSQL table name makes a query to get all timestamps, wich are primary keys.
        After that, compares the result of the query with timestamps contained on the dataframe to get
        another dataframe of the rows that arent on the MSSQL table. Finally it uploads the last dataframe
        to the MSSQL table.
    
    Args:
        table_name (String): MSSQL table name to insert data
        data_to_insert (Pd.Dataframe): Dataframe containing data to insert
        pk_column
        fk_column
    Returns:
        int: Number of rows inserted in table, returns -1 if rows were not inserted
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Use environment variables for database connection
        db_params = prj.get_mssql_params()
        
        # Check if specified table exists
        engine = create_mssql_engine()
        
        # Get current Timestamps on table
        # If fk_column was declared SQL sentence will change to retrieve primary and foreing keys
        if fk_column == "":
            existing_sql = f"""SELECT [{pk_column}]
                        FROM {db_params['database']}.{db_params['schema']}.[{table_name}];"""
            existing_tms = pd.read_sql(existing_sql, con=engine)[pk_column]
        else:
            existing_sql = f"""SELECT [{fk_column}]
                        FROM {db_params['database']}.{db_params['schema']}.[{table_name}]
                        WHERE [{pk_column}] = {pk_value};"""
            existing_tms = pd.read_sql(existing_sql, con=engine)[fk_column]
        
        # Get new records to insert
        # If fk_column was declared it will be used instead of the pk_column to filter duplicated records
        if fk_column == "":
            # Make a left join with Dataframes on primary key
            merge_df = pd.merge(data_to_insert, existing_tms, on=pk_column, how='left', indicator=True)
        else:
            # Make a left join with Dataframes on foreign key
            merge_df = pd.merge(data_to_insert, existing_tms, on=fk_column, how='left', indicator=True)
        
        # Keep data only from the left and data_to_insert columns only
        merge_df = merge_df.loc[merge_df._merge=='left_only', merge_df.columns!='_merge']
        new_records = merge_df[data_to_insert.columns]
        
        if len(new_records) > 0:
            # Upload data from Dataframe
            num_ins = new_records.to_sql(name=table_name,
                            con=engine, 
                            if_exists='append', 
                            index=False, 
                            schema=db_params['schema'])
            # Close engine
            engine.dispose()
            return (num_ins)
        else:
            # Close engine
            engine.dispose()
            return (-1)
        
    except sa.exc.SQLAlchemyError as db_e:
        logger.error(f"Error de SQLAlchemy, al insertar data en tabla {table_name}:\n{str(db_e)}")
    except Exception as e:
        logger.error(f"Error al insertar data en tabla MSSQL {table_name}:\n{str(e)}")