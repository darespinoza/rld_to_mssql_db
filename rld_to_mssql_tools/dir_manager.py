import os
from dagster import (
    get_dagster_logger,
)


def delete_dir_files(delete_path):
    """ Deletes all files on a specified directory

    Args:
        delete_path (_type_): Absolute or relative path where files are gonna be deleted
    """
    try:
        # Get Dagster Logger
        logger = get_dagster_logger()
        
        # Get files in directory
        dir_files = os.listdir(delete_path)
        
        # If files were found, delete them
        if len(dir_files) > 0:
            # Delete each file in directory
            for curr_file in dir_files:
                file_path = os.path.join(delete_path, curr_file)
                os.remove(file_path)
            
            logger.info(f"Archivos de {delete_path} eliminados correctamente.")
    except Exception as e:
        logger.error(f"Ocurrió un error al eliminar archivos:\n{str(e)}")