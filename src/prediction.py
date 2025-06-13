import pandas as pd 
import os
from data_ingestion import logger
from dvclive import Live
import yaml

def get_recommendations(df: pd.DataFrame, movie_name: str, movie_rating: int) -> pd.Series:
    """
    Get movie recommendations based on a user's rating for a specific movie.
    
    Args:
        df (pd.DataFrame): Correlation matrix between movies
        movie_name (str): Name of the movie the user rated
        movie_rating (int): User's rating for the movie (1-5)
        
    Returns:
        pd.Series: Series of similar movies sorted by similarity score
    """
    try:
        logger.info(f"Getting recommendations for movie: {movie_name} with rating: {movie_rating}")
        
        # Calculate similarity scores based on correlation and user rating
        similar_score = df[movie_name] * (movie_rating - 2.5)
        
        # Sort movies by similarity score in descending order
        similar_score = similar_score.sort_values(ascending=False)
        
        # Get top 10 recommendations
        top_recommendations = similar_score.head(10)
        
        logger.info(f"Successfully generated {len(top_recommendations)} recommendations")
        return top_recommendations
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {str(e)}")
        return pd.Series()

def save_recommendations(recommendations: pd.Series, movie_name: str):
    """
    Save movie recommendations to a CSV file.
    
    Args:
        recommendations (pd.Series): Series of movie recommendations
        movie_name (str): Name of the movie recommendations are based on
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs('data/output', exist_ok=True)
        
        # Create filename based on movie name
        filename = f"data/output/recommendations_{movie_name.replace(' ', '_').replace('(', '').replace(')', '')}.csv"
        
        # Save recommendations to CSV
        recommendations.to_csv(filename)
        logger.info(f"Successfully saved recommendations to {filename}")
        
        # Log the output file using dvclive
        with Live() as live:
            live.log_artifact(filename, type="output")
        
    except Exception as e:
        logger.error(f"Error saving recommendations: {str(e)}")

if __name__ == "__main__":
    # Load parameters
    with open("params.yaml", "r") as f:
        params = yaml.safe_load(f)
    
    # Log parameters using dvclive
    with Live(save_dvc_exp= True) as live:
        live.log_params(params)
    
    # Load the correlation matrix
    correlation_df = pd.read_csv("data/features/movie_correlations.csv", index_col=0)
    
    # Test the recommendations for The Matrix
    movie_name = "Shrek (2001)"
    recommendations = get_recommendations(
        df=correlation_df,
        movie_name=movie_name,
        movie_rating=5  # Assuming a high rating since it's a classic
    )
    
    # Save recommendations to file
    save_recommendations(recommendations, movie_name)
    
    print("\nTop 10 Movie Recommendations for The Matrix:")
    print(recommendations)
