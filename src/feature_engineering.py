import pandas as pd 
from data_preprocessing import load_data, logger
import os
import yaml

# Load parameters from yaml file
with open('params.yaml', 'r') as file:
    params = yaml.safe_load(file)


def get_correlation(df: pd.DataFrame, method: str) -> pd.DataFrame:
    """
    Calculate Pearson correlation between movies based on user ratings.
    
    Args:
        df (pd.DataFrame): User-movie rating matrix where rows are users and columns are movies
        
    Returns:
        pd.DataFrame: Correlation matrix between movies
    """
    try:
        logger.info("Calculating Pearson correlation between movies")
        correlation_matrix = df.corr(method)
        logger.info("Successfully calculated correlation matrix")
        return correlation_matrix
    except Exception as e:
        logger.error(f"Error calculating correlation: {str(e)}")
        return None
    
def main():
    logger.info("Starting feature engineering process")
    
    # Load the processed data
    input_path = 'data/processed/user_movie_matrix.csv'
    df = load_data(input_path)
    
    if df is not None:
        # Calculate movie correlations
        logger.info("Calculating movie correlations")
        correlation_matrix = get_correlation(df, method= params['feature_engineering']['method'])
        
        if correlation_matrix is not None:
            # Save the correlation matrix
            os.makedirs('data/features', exist_ok=True)
            output_path = 'data/features/movie_correlations.csv'
            correlation_matrix.to_csv(output_path)
            logger.info(f"Successfully saved correlation matrix to {output_path}")
        else:
            logger.error("Failed to calculate correlation matrix")
    else:
        logger.error("Failed to load the processed data")
    
    logger.info("Feature engineering process completed")

if __name__ == "__main__":
    main()