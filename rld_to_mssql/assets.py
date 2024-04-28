import os
import nrgpy
import duckdb
import shutil
import pandas as pd

from datetime import datetime
from typing import Dict, List
from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
    get_dagster_logger,
)

import rld_to_mssql_tools.prj_parameters as prj
import rld_to_mssql_tools.db_manager as pdb
import rld_to_mssql_tools.dir_manager as pdir
import rld_to_mssql_tools.txt_manager as ptxt


@asset(
    group_name="rld_files_search",
    io_manager_key="fs_io_manager",
)
def raw_dir_files(context: AssetExecutionContext) -> List:
    """ Returns a list of rld files found on Symphonie's raw directory. It will not remove, copy or perform any other actions on this directory.
        Symphonie's raw directory is specified on environment variable and this Dagster project should have access permission to it.
        
        This asset also requires access to know the type of files.
    Args:
        context (AssetExecutionContext): For metadata

    Returns:
        List: List of rld files found on Symphonie's raw directory
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Get only RLD files from Symphonie's raw directory
        prj_params = prj.get_project_parms()
        raw_dir_files = os.listdir(prj_params['symphonie_raw_dir'])
        rld_files = [now_file for now_file in raw_dir_files if now_file.endswith(".rld")]
        
        # Check if rld files were found
        if len(rld_files) > 0:
            context.add_output_metadata(
                metadata={
                    "num_raw_files": len(rld_files),
                    # Show tail with 5 last items of list
                    "preview": rld_files[len(rld_files)-5:],
                }
            )
        else:
            logger.error(f"No se encontraron archivos .rld en {prj_params['symphonie_raw_dir']}")
        
        # Return found rld file names
        return (list(rld_files))
    except PermissionError:
        logger.error(f"Sin permisos para acceder al directorio {prj_params['symphonie_raw_dir']}")
    except Exception as e:
        logger.error(f"Ocurrió un error al obtener archivos .rld de {prj_params['symphonie_raw_dir']}:\n{str(e)}")


@asset(
    group_name="rld_files_search",
    io_manager_key="fs_io_manager",
)
def uploaded_to_mssql_files(context: AssetExecutionContext) -> pd.DataFrame:
    """ Performs a query to DuckDB to get about rld files record, to know wich are already uploaded to MSSQL. 
        If DuckDB database is not created this asset will create it using specified global variables, 
        and will create a table for rld uploaded files record too.

    Args:
        context (AssetExecutionContext): For metadata

    Returns:
        pd.DataFrame: Dataframe with records of uploaded to MSSQL files
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Check if DuckDB database path exists, create it if not
        if not os.path.exists(pdb.DUCKDB_PATH):
            logger.info(f"Creando directorio {pdb.DUCKDB_PATH} para {pdb.DUCKDB_NAME}")
            os.makedirs(pdb.DUCKDB_PATH)
        
        # Verify if database exist, if not create it
        duckdb_name_path = f"{pdb.DUCKDB_PATH}/{pdb.DUCKDB_NAME}"
        
        # Connect to database
        ddb_con = duckdb.connect(database=duckdb_name_path, read_only=False)
        
        # If DuckDB table doesnt exist, create it
        ddb_con.sql("CREATE TABLE IF NOT EXISTS decrypted_n_uploaded (file_name VARCHAR, upload_timestamp TIMESTAMP, uploaded_by VARCHAR)")
        
        # Select file_name from upload register
        result = ddb_con.sql("SELECT file_name FROM decrypted_n_uploaded")
        df = result.fetchdf()
        
        # Add metadata
        context.add_output_metadata(
            metadata={
                "num_duckdb_files": len(df),
                "preview": MetadataValue.md(df.head().to_markdown()),
            }
        )
        
        # Close connection
        ddb_con.close()
        
        # Return dataframe
        return (df)
    except Exception as e:
        logger.error(f"Error al obtener registro de archivos cargados de DuckDB:\n{str(e)}")


@asset(
    group_name="rld_files_search",
    io_manager_key="fs_io_manager",
)
def not_uploaded_files(context: AssetExecutionContext,
                    raw_dir_files: List,
                    uploaded_to_mssql_files: pd.DataFrame) -> pd.DataFrame:
    """ Indicates wich files from Symphonie raw directory are not uploaded to MSSQL yet, 
        performing a left join on files coming from Symphonie's raw directory and DuckDB uploaded files record

    Args:
        context (AssetExecutionContext): For metadata
        raw_dir_files (List): List of rld files in Symphonie raw directory
        uploaded_to_mssql_files (pd.DataFrame): A dataframe with the name of rld files that are uploaded to MSSQLs

    Returns:
        pd.DataFrame: Result of a left join of rld files wich came from Symphonie's raw directory and DuckDB uploaded files record
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Convert raw_dir_files list to Dataframe
        df_list = pd.DataFrame(raw_dir_files, columns=['file_name'])
        
        # Make a left join with the list and Dataframe
        lj_file_names = pd.merge(df_list, uploaded_to_mssql_files, on='file_name', how='left', indicator=True)
        # Keep data only from the left
        lj_file_names = lj_file_names.loc[lj_file_names._merge=='left_only', lj_file_names.columns!='_merge']
        
        # Add metadata
        context.add_output_metadata(
            metadata={
                "num_not_uploaded": len(lj_file_names),
                "preview": MetadataValue.md(lj_file_names.head().to_markdown()),
            }
        )
        
        return (lj_file_names)
    except Exception as e:
        logger.error(f"Error al generar listado de archivos no cargados a MSSQL:\n{str(e)}")


@asset(
    group_name="rld_files_search",
    io_manager_key="fs_io_manager",
)
def copy_to_decrypt_dir(context: AssetExecutionContext,
                    not_uploaded_files: pd.DataFrame) -> None:
    """ Copies not uploaded to MSSQL rld files from Symphonie's raw directory into a directory located into this project's directory, 
        its name is specified on above global variable.

    Args:
        context (AssetExecutionContext): For metadata
        not_uploaded_files (pd.DataFrame): A dataframe with names of not uploaded to MSSQL rld files
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Check if RLD_INPUT path exists, create it if not
        if not os.path.exists(prj.RLD_DIR):
            logger.info(f"Creando directorio {prj.RLD_DIR}")
            os.makedirs(prj.RLD_DIR)
        
        # Delete all files before copying RLD files
        pdir.delete_dir_files(prj.RLD_DIR)
        
        # Iterate with RLD file paths
        for index, row in not_uploaded_files.iterrows():
            file_now = row['file_name']
            prj_params = prj.get_project_parms()
            origen = os.path.join(prj_params['symphonie_raw_dir'], file_now)
            destino = os.path.join(prj.RLD_DIR, file_now)
            shutil.copy(origen, destino)
            logger.info(f"Se ha copiado archivo {file_now} en {prj.RLD_DIR}.")

    except FileNotFoundError:
        logger.error("El archivo/directorio especificado no existe.")
    except Exception as e:
        logger.error(f"Error al copiar archivos hacia {prj.RLD_DIR}:\n{str(e)}")



@asset(
    deps=[copy_to_decrypt_dir],
    group_name="rld2txt_conversion",
    io_manager_key="fs_io_manager",
)
def convert_rld_to_txt(context: AssetExecutionContext, ) -> None:
    """ Uses a nrgpy Converter to decrypt rld files into plain text format. Decrypted files are placed on project's specified TXT directory.
        If conversion fails it attempts to do it again, with a maximum of attemtps specified on environment variable.
        
    Args:
        context (AssetExecutionContext): For metadata
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Check if TXT destination exists, if not then make
        if not os.path.exists(prj.TXT_DIR):
            logger.info(f"Creando directorio {prj.TXT_DIR}")
            os.makedirs(prj.TXT_DIR)
        
        # Delete all files before conversion
        pdir.delete_dir_files(prj.TXT_DIR)
        
        # Get RLD files fron RLD_INPUT directory
        prj.RLD_DIR_files = os.listdir(prj.RLD_DIR)
        rld_files = [now_file for now_file in prj.RLD_DIR_files if now_file.endswith(".rld")]
        
        # If rld files are found convert them to txt
        if len(rld_files) > 0:
            # Use specified max attempts
            prj_params = prj.get_project_parms()
            conv_max_att = prj_params['conv_max_att']
            for attempt in range(1, conv_max_att + 1):
                try:
                    # Attempt to convert RLDs to TXTs
                    converter = nrgpy.local_rld(rld_dir=prj.RLD_DIR, out_dir=prj.TXT_DIR)
                    converter.convert()
                except Exception as nrg_e:
                    # Print error messages for attempt
                    logger.warning(f"Intento {attempt}: Ocurrió una excepción durante la conversión RLD a TXT:\n{nrg_e}")
                    if attempt < conv_max_att:
                        logger.warning("Reintentando...")
                    else:
                        logger.warning(f"Se alcanzó el máximo número de intentos de conversion ({conv_max_att}).")
                else:
                    logger.info("Conversión RLD a TXT exitosa")
                    break
        else:
            logger.warning(f"No se encontraron archivos .rld en {prj.RLD_DIR} para conversión")
    except Exception as e:
        logger.error(f"Error fatal al convertir RLD a TXT:\n{str(e)}")


@asset(
    deps=[convert_rld_to_txt],
    group_name="mssql_upload",
    io_manager_key="fs_io_manager",
)
def nrg_files_n_tables(context: AssetExecutionContext, ) -> pd.DataFrame:
    """ A Dataframe with RLD file names, TEXT file names and table names. Table names are created with site number that came from 
        the decrypted RLD file in text format, and a generic Tower name specified on a global variable . 
        This parameter should exist on the text file, if not table name is not created.
        
        Depends on convert_rld_to_txt asset, as this asset gets information from the decrypted text files
    Args:
        context (AssetExecutionContext): For metadata on logs
    Returns:
        pd.DataFrame: A Dataframe created with RLD, TXT file names and created table names.
    """
    
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Get TXT files
        prj.TXT_DIR_files = os.listdir(prj.TXT_DIR) 
        txt_files = [nowt_file for nowt_file in prj.TXT_DIR_files if nowt_file.endswith(".txt")]
        
        # Get table names from each decrypted rld files
        files_tables_dict = []
        for idx, txtf_now  in enumerate(txt_files):
            # Set rld file name
            rld_file_now = txtf_now.replace("_meas.txt", ".rld")
            # Open current file and search parameters on it
            txt_path = os.path.join(prj.TXT_DIR, txtf_now)
            p_sitenum = ptxt.search_param_txt(txt_path, ptxt.LOGGER_SITE_NUM)
            # Create table name
            tbl_name = f"{pdb.MSSQL_DATA_TABLE_NAME}_{p_sitenum['param_value']}"
            
            # If parameters are found add to the list of table names
            if (p_sitenum['param_value'] != ""):
                logger.info(f"Datos de rchivo {rld_file_now} serán cargados en tabla MSSQL {tbl_name}")
                files_tables_dict.append(dict(rld_fname=rld_file_now, txt_fname=txt_files[idx], table_name=tbl_name))
            else:
                logger.warning(f"Uno o más  parámetros faltantes en archivo {rld_file_now}.\nRequeridos: Site number, Project y Location.\nNombre incompleto para tabla: {tbl_name}")
        
        # Add metadata
        df_files_tables = pd.DataFrame(files_tables_dict)
        context.add_output_metadata(
            metadata={
                "num_files_tables": len(df_files_tables),
                "preview": MetadataValue.md(df_files_tables.head().to_markdown()),
            }
        )
        
        return (df_files_tables)
    except Exception as e:
        logger.error(f"Error al obtener nombres de archivos y tablas para carga a MSSQL:\n{str(e)}")


@asset(
    group_name="mssql_upload",
    io_manager_key="fs_io_manager",
)
def upload_data_to_mmsql(context: AssetExecutionContext, 
                        nrg_files_n_tables: pd.DataFrame) -> pd.DataFrame:
    """ Given the dataframe with RLD, TXT and table names, check if tables exist on MSSQL and creates if not making sure that columns match. 
        After that upload the data to MSSQL if any exists.
    Args:
        context (AssetExecutionContext): For metadata
        nrg_files_n_tables (pd.DataFrame): A Dataframe with rld files, text files and table names
    Returns:
        pd.Dataframe: A Dataframe with the name of the rld files that were succesfully uploaded to MSSQL or did not have data.
                    So they are not passed to the pipeline again
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Iterate with each row of Dataframe
        files_duckdb = []
        for idx, frow in nrg_files_n_tables.iterrows():
            # Get data from text decrypted file
            data_to_insert = ptxt.get_dataframe_txt(frow['txt_fname'])
            
            # Check if Dataframe has data, set a flag to keep record of files that doesnt contain data
            if not data_to_insert.empty:
                # Add txt and rld file name, where the data came from
                data_to_insert[pdb.DF_RLD_FILE_COL] = frow['rld_fname']
                data_to_insert[pdb.DF_TXT_FILE_COL] = frow['txt_fname']
                
                # Try to create or alter table
                ct_result = pdb.create_alter_mssql_table(frow['table_name'], data_to_insert, pk_column=pdb.DATA_TABLE_PK, pk_flag=True, text_file_name=frow['txt_fname'])
                
                # Table exists or was altered
                if (ct_result):
                    rows_ins = pdb.insert_data_msssql(frow['table_name'], data_to_insert, pdb.DATA_TABLE_PK)
                
                # Save rld file name if it was uploaded to MSSQL
                if rows_ins > 0:
                    logger.info(f"Se insertaron {rows_ins} filas en tabla {frow['table_name']}.")
                
            else:
                logger.warning(f"No se encontró data en el archivo {frow['txt_fname']}.")
            
            # Save file name to upload register on DuckDB
            files_duckdb.append(dict(file_name=frow['rld_fname'],
                                    upload_timestamp=datetime.now(),
                                    uploaded_by=pdb.UPLD_USER))
        
        # Save metadata of uploaded rld files
        df_files_duckdb = pd.DataFrame(files_duckdb)
        context.add_output_metadata(
            metadata={
                "upld_rld_num": len(df_files_duckdb),
                "preview": MetadataValue.md(df_files_duckdb.head().to_markdown()),
            }
        )
        
        # Return Dataframe with uploaded rld files names
        return (df_files_duckdb)
    except Exception as e:
        logger.error(f"Error al cargar data en MSSQL:\n{str(e)}")


@asset(
    deps=[upload_data_to_mmsql],
    group_name="mssql_upload",
    io_manager_key="fs_io_manager",
)
def logger_n_sensor_register(context: AssetExecutionContext,
                        nrg_files_n_tables: pd.DataFrame) -> None:
    """ Having a Dataframe with rld file names, text file names, and MSSQL table names, this asset search for loggers and
        sensors info to then upload them on MSSQL tables for Loggers and Sensors

    Args:
        context (AssetExecutionContext): For metatada and logs
        nrg_files_n_tables (pd.DataFrame): Contains rld file names, text file names, and MSSQL table names
    """
    try:
        # Get Dagster logger
        logger = get_dagster_logger()
        
        # If nrg_files_n_tables Dataframe is not emtpy
        if not nrg_files_n_tables.empty:
            # Get logger and sensors parameters from each text file and upload to database
            for index, row in nrg_files_n_tables.iterrows():
                df_export = ptxt.get_params_from_txt(row["txt_fname"], ptxt.PARAM_EXP_PAR, skip_rows=1)
                df_site = ptxt.get_params_from_txt(row["txt_fname"], ptxt.PARAM_SIT_PRP, skip_rows=1)
                df_loggerh = ptxt.get_params_from_txt(row["txt_fname"], ptxt.PARAM_LOG_HIS, skip_rows=1)
                
                # Check if dfs are not empty and with required cplumns
                if not df_site.empty and not df_export.empty and not df_loggerh.empty:
                    # Check required columns
                    if (pdb.LOGGER_TABLE_PK in df_export.columns and ptxt.LOGGER_MODEL in df_loggerh.columns and ptxt.LOGGER_SERIAL_NUM in df_loggerh.columns):
                        # Add columns from aux dataframes
                        df_site[pdb.LOGGER_TABLE_PK] = df_export[pdb.LOGGER_TABLE_PK]
                        df_site[ptxt.LOGGER_MODEL] = df_loggerh[ptxt.LOGGER_MODEL]
                        df_site[ptxt.LOGGER_SERIAL_NUM] = df_loggerh [ptxt.LOGGER_SERIAL_NUM]
                        
                        # Create or alter table with logger data
                        logger_table_res = pdb.create_alter_mssql_table(pdb.LOGGER_TABLE_NAME, df_site, pk_column=pdb.LOGGER_TABLE_PK, pk_flag=False)
                        # If table exists or was created add logger data
                        if logger_table_res:
                            # Check if logger is already in table, if not insert
                            rows_ins = pdb.insert_data_msssql(pdb.LOGGER_TABLE_NAME, df_site, pdb.LOGGER_TABLE_PK)
                            
                            # New loggers registered
                            ins_site = df_site.at[0, pdb.LOGGER_TABLE_PK]
                            if rows_ins > 0:
                                logger.info(f"Se insertó {rows_ins} logger/s. Site number {ins_site} en {pdb.LOGGER_TABLE_NAME}")
                            
                            # Search for Sensor Channels
                            sens_channels = ptxt.get_sensor_channels(row["txt_fname"], ptxt.PARAM_SENS_HIST)
                            
                            # Get parameters for each sensor in channel
                            for chann in sens_channels:
                                # Get sensor parameters
                                df_chann = ptxt.get_params_from_txt(row["txt_fname"], ptxt.PARAM_CHANNEL, param_line=int(chann), skip_rows=0)
                                
                                # If result df is not empty, add logger primary key
                                if not df_chann.empty:
                                    # Add Site number to Sensor data
                                    df_chann[pdb.LOGGER_TABLE_PK] = df_export[pdb.LOGGER_TABLE_PK]
                                    
                                    # Create or alter table for sensors data
                                    sensor_table_res = pdb.create_alter_mssql_table(pdb.SENSOR_TABLE_NAME, df_chann, pk_column=pdb.SENSOR_TABLE_FK, pk_flag=False)
                                    
                                    # If table exists or was created add sensor data
                                    if sensor_table_res:
                                        # Check if logger is already in table, if not insert
                                        pk_val = df_chann.at[0, pdb.LOGGER_TABLE_PK]
                                        rows_ins = pdb.insert_data_msssql(pdb.SENSOR_TABLE_NAME, df_chann, pdb.LOGGER_TABLE_PK, pk_value=pk_val, fk_column=pdb.SENSOR_TABLE_FK)
                                        
                                        # New sensors registered
                                        ins_chan = df_chann.at[0, pdb.SENSOR_TABLE_FK]
                                        if rows_ins > 0:
                                            logger.info(f"Se insertó {rows_ins} sensor/es. Channel {ins_chan}, Site Number {ins_site} en {pdb.SENSOR_TABLE_NAME}")
                    else:
                        logger.warning(f'No se encontraron los valores requeridos {pdb.LOGGER_TABLE_PK}, {ptxt.LOGGER_MODEL} y {ptxt.LOGGER_SERIAL_NUM} para guardar logger NRG')
                else:
                    logger.warning(f'No se encontraron los parámetros {ptxt.PARAM_EXP_PAR}, {ptxt.PARAM_SIT_PRP} y {ptxt.PARAM_LOG_HIS} requeridos para guardar logger NRG')
        else:
            logger.info(f"Sin archivos rld desencriptados para procesar")
    except Exception as e:
        logger.error(f"Error al registrar data en sensores/dataloggers MSSQL:\n{str(e)}")


@asset(
    group_name="mssql_upload",
    io_manager_key="fs_io_manager",
)
def save_rld_files(context: AssetExecutionContext, 
                    upload_data_to_mmsql: pd.DataFrame) -> None:
    """ Update the uploaded rld files record using the Dataframe that come with rld files names, timestamp and upload user.

    Args:
        context (AssetExecutionContext): For metadata
        upload_data_to_mmsql (pd.DataFrame): A Dataframe with the name of the rld files that were upload to MSSQL, also a timestamp when it was uploaded and a user.
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # DuckDB database path
        duckdb_name_path = f"{pdb.DUCKDB_PATH}/{pdb.DUCKDB_NAME}"
        
        # Connect to database and create a cursor
        ddb_con = duckdb.connect(database = duckdb_name_path, read_only = False)
        cursor = ddb_con.cursor()
        
        # Insert rld file names and timestamps on DuckDB table 
        for idx, rld_files in upload_data_to_mmsql.iterrows():
            # Insert using try: except:
            logger.info(f"Registrado archivo {rld_files['file_name']} en DuckDB")
            cursor.execute("INSERT INTO decrypted_n_uploaded VALUES (?, ?, ?)", (rld_files['file_name'], rld_files['upload_timestamp'], rld_files['uploaded_by']))
        
        # Close connection
        ddb_con.close()
    except Exception as e:
        logger.error(f"Error al registrar archivos rld cargados, en DuckDB:\n{str(e)}")