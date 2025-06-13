import pandas as pd 
from typing import Optional, Union
import os
import logging
from datetime import datetime

# Configure logging
def setup_logger():
    """
    Set up logging configuration with timestamp and log level
    """
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'{timestamp}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logger()

def read_csv(file_path: str) -> Optional[pd.DataFrame]:
    """
    Read a CSV file and return a pandas DataFrame.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        Optional[pd.DataFrame]: DataFrame containing the CSV data if successful,
                              None if there was an error reading the file
    """
    try:
        logger.info(f"Attempting to read CSV file: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Successfully read CSV file: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file {file_path}: {str(e)}")
        return None
    
def merge(first_csv: Union[str, pd.DataFrame], 
          second_csv: Union[str, pd.DataFrame],
          on: Optional[str] = None,
          how: str = 'inner') -> Optional[pd.DataFrame]:
    """
    Merge two CSV files or DataFrames based on a common column.
    
    Args:
        first_csv (Union[str, pd.DataFrame]): First CSV file path or DataFrame
        second_csv (Union[str, pd.DataFrame]): Second CSV file path or DataFrame
        on (Optional[str]): Column name to merge on. If None, will merge on common columns
        how (str): Type of merge to perform ('inner', 'outer', 'left', 'right')
                   Default is 'inner'
    
    Returns:
        Optional[pd.DataFrame]: Merged DataFrame if successful, None if there was an error
        
    Raises:
        ValueError: If both inputs are strings and 'on' parameter is not provided
    """
    try:
        logger.info("Starting merge operation")
        # Convert string paths to DataFrames if needed
        df1 = pd.read_csv(first_csv) if isinstance(first_csv, str) else first_csv
        df2 = pd.read_csv(second_csv) if isinstance(second_csv, str) else second_csv
        
        logger.info(f"Merging DataFrames on column: {on} with method: {how}")
        # Merge the DataFrames
        merged_df = pd.merge(df1, df2, on=on, how=how)
        logger.info("Successfully merged DataFrames")
        return merged_df
        
    except Exception as e:
        logger.error(f"Error merging DataFrames: {str(e)}")
        return None
    

def main():
    logger.info("Starting data ingestion process")
    
    # Create data/raw directory if it doesn't exist
    os.makedirs('data/raw', exist_ok=True)
    logger.info("Created/verified data/raw directory")
    
    # Read the movies and ratings data
    logger.info("Reading movies and ratings data")
    movies_df = read_csv('movies.csv')
    ratings_df = read_csv('ratings.csv')
    
    if movies_df is not None and ratings_df is not None:
        # Merge movies and ratings data
        logger.info("Merging movies and ratings data")
        merged_df = merge(movies_df, ratings_df, on='movieId')
        
        if merged_df is not None:
            # Save the merged data
            output_path = 'data/raw/merged_data.csv'
            merged_df.to_csv(output_path, index=False)
            logger.info(f"Successfully saved merged data to {output_path}")
        else:
            logger.error("Failed to merge the datasets")
    else:
        logger.error("Failed to read one or both input files")
    
    logger.info("Data ingestion process completed")

if __name__ == "__main__":
    main()