from fastapi import FastAPI
from src.prediction import get_recommendations
import pandas as pd 

app = FastAPI()

@app.post("/predict/{movie_name}/{rating}")
async def predict(movie_name: str, rating: int):
    try:
        # Load the correlation matrix
        correlation_df = pd.read_csv("D:/cloned_website/data/features/movie_correlations.csv", index_col=0)
        
        # Get recommendations
        recommendations = get_recommendations(
            df=correlation_df,
            movie_name=movie_name,
            movie_rating=rating
        )
        
        # Convert recommendations to dictionary format
        recommendations_dict = recommendations.to_dict()
        
        return {
            "status": "success",
            "movie_name": movie_name,
            "rating": rating,
            "recommendations": recommendations_dict
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


