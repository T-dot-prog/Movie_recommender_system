import pandas as pd 
from .data_ingestion import logger



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


# Test the function
if __name__ == "__main__":
    # Load the correlation matrix
    correlation_df = pd.read_csv("D:/cloned_website/data/features/movie_correlations.csv", index_col=0)
    
    # Test the recommendations for The Matrix
    recommendations = get_recommendations(
        df=correlation_df,
        movie_name="Matrix, The (1999)",
        movie_rating=5  # Assuming a high rating since it's a classic
    )
    
    print("\nTop 10 Movie Recommendations for The Matrix:")
    print(recommendations)


