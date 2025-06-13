from data_ingestion import logger
from typing import Optional, Union
import pandas as pd 
import os
import yaml

# Load parameters from yaml file
with open('params.yaml', 'r') as file:
    params = yaml.safe_load(file)

def load_data(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load data from a CSV file and return a pandas DataFrame.
    
    Args:
        file_path (str): Path to the CSV file to load
        
    Returns:
        Optional[pd.DataFrame]: DataFrame containing the loaded data if successful,
                              None if there was an error loading the file
    """
    try:
        logger.info(f"Loading data from: {file_path}")
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded data from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {str(e)}")
        return None
    
def clean_data(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Clean the merged dataset by removing 'genres' and 'timestamp' columns.
    
    Args:
        df (pd.DataFrame): Input DataFrame containing the merged data
        
    Returns:
        Optional[pd.DataFrame]: Cleaned DataFrame if successful, None if there was an error
    """
    try:
        logger.info("Starting data cleaning process")
        # Remove specified columns
        columns_to_drop = ['genres', 'timestamp']
        cleaned_df = df.drop(columns=columns_to_drop, errors='ignore')
        logger.info("Successfully cleaned data")
        return cleaned_df
    except Exception as e:
        logger.error(f"Error cleaning data: {str(e)}")
        return None
    
def create_pivot_table(df: pd.DataFrame, 
                      index: str,
                      columns: str,
                      values: str) -> Optional[pd.DataFrame]:
    """
    Create a pivot table from the input DataFrame.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        index (str): Column to use as index
        columns (str): Column to use as columns
        values (str): Column to aggregate
        aggfunc (str): Aggregation function to use (default: 'mean')
        
    Returns:
        Optional[pd.DataFrame]: Pivot table if successful, None if there was an error
    """
    try:
        logger.info(f"Creating pivot table with index={index}, columns={columns}, values={values}")
        pivot_df = pd.pivot_table(df, 
                                index=index,
                                columns=columns,
                                values=values)
        logger.info("Successfully created pivot table")
        return pivot_df
    except Exception as e:
        logger.error(f"Error creating pivot table: {str(e)}")
        return None
    
def fillna(df: pd.DataFrame, thresh: int, strategy: Union[str, int]) -> Optional[pd.DataFrame]:
    """
    Fill missing values in the DataFrame based on threshold and strategy.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        thresh (int): Minimum number of non-NA values required to keep a column
        strategy (Union[str, int]): Value to fill NA with (can be string or numeric)
        
    Returns:
        Optional[pd.DataFrame]: DataFrame with filled values if successful, None if there was an error
    """
    try:
        logger.info(f"Filling missing values with threshold={thresh} and strategy={strategy}")
        user_ratings = df.dropna(thresh=thresh, axis=1).fillna(strategy)
        logger.info("Successfully filled missing values")
        return user_ratings
    except Exception as e:
        logger.error(f"Error filling missing values: {str(e)}")
        return None
    

def main():
    logger.info("Starting data preprocessing process")
    
    # Read the merged data
    input_path = 'data/raw/merged_data.csv'
    df = load_data(input_path)
    
    if df is not None:
        # Create user-movie rating matrix
        logger.info("Creating user-movie rating matrix")
        pivot_df = create_pivot_table(df, 
                                    index='userId',
                                    columns='title',
                                    values='rating')
        
        if pivot_df is not None:
            # Fill missing values using parameters from yaml
            logger.info("Filling missing values in the rating matrix")
            filled_df = fillna(
                pivot_df, 
                thresh=params['data_preprocessing']['thresh'],
                strategy=params['data_preprocessing']['strategy']
            )
            
            if filled_df is not None:
                # Save the processed data
                os.makedirs('data/processed', exist_ok=True)
                output_path = 'data/processed/user_movie_matrix.csv'
                filled_df.to_csv(output_path)
                logger.info(f"Successfully saved processed data to {output_path}")
            else:
                logger.error("Failed to fill missing values")
        else:
            logger.error("Failed to create pivot table")
    else:
        logger.error("Failed to read the merged data file")
    
    logger.info("Data preprocessing process completed")

if __name__ == "__main__":
    main()