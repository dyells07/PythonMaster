import numpy as np
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import SpectralClustering

users_df = pd.read_excel('users.xlsx', sheet_name='Sheet1')
movies_df = pd.read_excel('movies.xlsx', sheet_name='Sheet2')
ratings_df = pd.read_excel('ratings.xlsx', sheet_name='Sheet1')

user_features = users_df[['age', 'gender', 'occupation', 'zipcode']].values

user_movie_ratings = ratings_df.pivot(index='user_id', columns='movie_id', values='rating').fillna(0)

num_clusters = 4

spectral_clustering = SpectralClustering(n_clusters=num_clusters, random_state=42)
user_labels = spectral_clustering.fit_predict(user_movie_ratings)


def recommend_movies_for_new_user(new_user_features, user_features, user_labels, user_movie_ratings, movies_df, num_recommendations=10):
   
    similarity_scores = cosine_similarity([new_user_features], user_features)
    
    most_similar_user_label = user_labels[np.argmax(similarity_scores)]
    
    cluster_users = np.where(user_labels == most_similar_user_label)[0]

    cluster_movie_ratings = user_movie_ratings.iloc[cluster_users]
    average_ratings = cluster_movie_ratings.mean(axis=0)
    
    recommended_movie_ids = average_ratings.sort_values(ascending=False).index[:num_recommendations]
    
    recommended_movies = movies_df[movies_df['movie_id'].isin(recommended_movie_ids)]['name'].tolist()
    
    return recommended_movies

new_user_data = {"age": 25, "gender": "F", "occupation": "Artist", "zipcode": "56789"}


new_user_features = np.array([new_user_data["age"], 0 if new_user_data["gender"] == "F" else 1, 0, int(new_user_data["zipcode"])])

recommended_movies = recommend_movies_for_new_user(new_user_features, user_features, user_labels, user_movie_ratings, movies_df)

print("Recommended Movie Names:", recommended_movies)
