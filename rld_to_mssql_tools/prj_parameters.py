from dotenv import dotenv_values
from dagster import (
    get_dagster_logger,
)


# ____________ Set RLD directory, TXT directory locations and max atempts of RLD conversions
RLD_DIR = "RLD_INPUT"
TXT_DIR = "TXT_OUTPUT"

def get_project_parms():
    """ Reads .env file an returns project related parameters

    Returns:
        dict: a dictionary with parameters
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Read .env file
        prj_params = dotenv_values(".env")
        
        # Return as a dictionary
        return (dict(symphonie_raw_dir=prj_params['SYMPH_RAW_DIR'],
                    conv_max_att=int(prj_params['CONV_ATTEMP'])))
    except Exception as e:
        logger.error(f"Ocurrió un error al establecer parámetros del proyecto:\n{str(e)}")


def get_mssql_params():
    """ Load database parameters from .env file using dotenv package

    Returns:
        dict: A dictionary that contains parameters from .env file 
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Load environment variables from .env file
        db_params = dotenv_values(".env")
        
        # Return MSSQL parameters as a dict
        return (dict(host=db_params['MSSQL_DB_HOST'],
                    port=db_params['MSSQL_DB_PORT'],
                    database=db_params['MSSQL_DB_DATABASE'],
                    schema=db_params['MSSQL_DB_SCHEMA'],
                    user=db_params['MSSQL_DB_USER'],
                    password=db_params['MSSQL_DB_PASSWORD']))
    except Exception as e:
        logger.error(f"Error al obtener parametros de conexión MSSQL:\n{str(e)}")