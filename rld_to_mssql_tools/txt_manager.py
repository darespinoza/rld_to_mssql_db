import os
import re
import pandas as pd
from dagster import (
    get_dagster_logger,
)

import rld_to_mssql_tools.prj_parameters as prj
import rld_to_mssql_tools.db_manager as pdb

# ____________ Constants keywords where data starts
DATA_START_WORD = "Timestamp"
# ____________ Constants keywords where channel sensors starts and ends
PARAM_CHANNEL = "Channel"
PARAM_CHANNEL_STOP = "Data"

# ____________ Constants for parameters located in decrypted text files
LOGGER_SITE_NUM = "Site Number:"
LOGGER_MODEL = "Model"
LOGGER_SERIAL_NUM = "Serial Number"
PARAM_EXP_PAR = "Export Parameters"
PARAM_SIT_PRP = "Site Properties"
PARAM_LOG_HIS = "Logger History"
PARAM_SENS_HIST = "Sensor History"


def search_param_txt(txt_file_path, seached_param):
    """Reads the specified plain text file and searchs for parameter. If found returns the results in a dictionary.

    Args:
        txt_file_path (_type_): Path to plain text file to search on
        seached_param (_type_): String to be searched on text file

    Returns:
        dict: Retunrs the row number where searched_param is located and its content removing the specified string. 
            If not found it will return -1 for row number and an empty string as param value.
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Read specified file
        with open(txt_file_path, 'r') as my_file:
            for row_num, row_content in enumerate(my_file, 1):
                if seached_param in row_content:
                    # Remove searched param from row and replace white spaces with underscores
                    param_value = row_content.replace(seached_param, "")
                    param_value = param_value.strip()
                    param_value = param_value.replace(" ", "_")
                    # Return row number where searched parameter is located and its value
                    return (dict(row_num = row_num, param_value = param_value))
            
            # Word not found in file
            return (dict(row_num = -1, param_value = ""))
    except Exception as e:
        logger.error(f"Ocurrió un error al buscar parametro '{seached_param}' en archivo {txt_file_path}")


def get_params_from_txt(txt_file, header, param_line=0, skip_rows=0):
    """ Given a text file and a header to search in, reads the file and when found reads the next lines
        wich are expected to be parameters separated with a colon ":" character.
        The search stops when find a empty file is found, and the result 

    Args:
        txt_file (String): Text file name
        header (String): Header looking for
        param_line (int, optional): If set, is used instead of header to search. Defaults to 0.
        skip_rows (int, optional): Rows to skip in case parameters start above header. Defaults to 0.

    Returns:
        pd.Dataframe: A Dataframe with columns of parameters and a row of their values
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Check if header exists in text file
        txt_path = os.path.join(prj.TXT_DIR, txt_file)
        
        # If param_line is set, dont search for parameter
        row_number = 0
        if param_line == 0:
            found_row = search_param_txt(txt_path, header)
            row_number = found_row['row_num'] 
        else:
            row_number = param_line
        
        # If header was found, return its parameters
        if (row_number > 0 and skip_rows >= 0):
            skip_count = 0
            dict_keys = []
            dict_vals = []
            
            # Read text file
            with open(txt_path, 'r') as my_file:
                # Skip lines to header line number
                for _ in range(row_number - 1):
                    next(my_file)
                
                for line in my_file:
                    # Skip rows from header, in case header is not taken as a parameter
                    if skip_count == skip_rows:
                        # Check for empty line as it ends parameters list, when its end return a Dataframe
                        if line.strip() == "":
                            dynamic_dict = dict(zip(dict_keys, dict_vals))
                            df = pd.DataFrame(dynamic_dict, index=[0])
                            return (df)
                        else:
                            # Get parameter
                            param_split = line.strip().split(':')
                            # Remove parameter from line to get its value
                            # This method is used because value could have ":" on it, wich is used to split the string
                            param_value = line.replace(param_split[0] + ":", "")
                            param_value = param_value.strip()
                            # Remove tab, line feed and carriage return
                            param_value = re.sub(r'[\n\t\r]', '', param_value)
                            # Add params and values to arrays
                            dict_keys.append(param_split[0].strip())
                            dict_vals.append(param_value.strip())
                    else:
                        skip_count += 1
        
        else:
            logger.warning(f"No se encontró cabecera '{header}' en archivo '{txt_file}'")
            return (pd.DataFrame())
        
    except Exception as e:
        logger.error(f"Ocurrió un error al obtener parámetros '{header}' de archivo '{txt_file}':\n{str(e)}")


def get_dataframe_txt(txt_file):
    """ Reads the specified text file and search for key word specified on above global variable to know where data is located, 
        after that it gets a Dataframe using the first row as column names and the remaining rows as its content. If key word 
        is not found it will return an empty Dataframe.

    Args:
        txt_file (_type_): File name of plain text file where data is searched

    Returns:
        pd.Dataframw: Dataframe with data contained on specified text file. Empty dataframe if data is not found. 
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Create file path
        file_path = os.path.join(prj.TXT_DIR, txt_file)
        
        # Search for DATA_START_WORD
        found_row = search_param_txt(file_path, DATA_START_WORD)
        
        # Get a Dataframe if data was found
        if (found_row['row_num'] > 0):
            logger.info(f"Archivo {txt_file} contiene data en linea {found_row['row_num']}")
            # Get Dataframe
            nrg_df = pd.read_csv(file_path, delimiter='\t', 
                                skiprows= found_row["row_num"]-1, 
                                header= 0)
            
            # If Timestamp column is in Dataframe, convert it to datetime
            if pdb.DATA_TABLE_PK in nrg_df.columns:
                nrg_df[pdb.DATA_TABLE_PK] = pd.to_datetime(nrg_df[pdb.DATA_TABLE_PK])
                return (nrg_df)
            else:
                return (pd.DataFrame())
        else:
            logger.warning(f"No se encontró la palabra clave '{DATA_START_WORD}' en el archivo {file_path}")
            return (pd.DataFrame())
    except Exception as e:
        logger.error(f"Error al obtener Dataframe de archivo de texto {file_path}:\n{str(e)}")


def get_sensor_channels(txt_file, header):
    """ Read a text file and search for the header, when found search for each Channel line wich indicate
        where a Sensor Channel is. It appends the line number to a list and returns it

    Args:
        txt_file (String): Text file name
        header (String): Header to search for, it should be related to Channels

    Returns:
        List : list with line numbers where Channels were found
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Check if header exists in text file
        txt_path = os.path.join(prj.TXT_DIR, txt_file)
        h_row = search_param_txt(txt_path, header)
        
        # If header was found, return its parameters
        if (h_row['row_num'] > 0):
            channel_lines = []
            
            # Read text file
            with open(txt_path, 'r') as my_file:
                # Skip lines to header line number
                for _ in range(h_row['row_num']):
                    next(my_file)
                
                # Search for sensor channels in lines
                for row_num, row_content in enumerate(my_file, h_row['row_num'] + 1):
                    # Check if current line has channel
                    if PARAM_CHANNEL in row_content and not PARAM_CHANNEL_STOP in row_content:
                        channel_lines.append(row_num)
                    
            return channel_lines
        else:
            logger.warning(f"No se encontró cabecera para Channels '{header}' en archivo '{txt_file}'")
            return (pd.DataFrame())
        
    except Exception as e:
        logger.error(f"Ocurrió un error al obtener'{header}' de archivo '{txt_file}':\n{str(e)}")