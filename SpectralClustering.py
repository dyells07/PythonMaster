import numpy as np
from sklearn.cluster import SpectralClustering, KMeans
from sklearn.metrics.pairwise import cosine_similarity


# new_user_data = {"gender": 0, "age": 26, "occupation": 0, "zipcode": "12345"}

users = [
    {"id": 1, "gender": 1, "age": 25, "occupation": 0, "zipcode": "12345"},
    {"id": 2, "gender": 0, "age": 35, "occupation": 2, "zipcode": "56789"},
    {"id": 3, "gender": 0, "age": 30, "occupation": 1, "zipcode": "34567"},
    {"id": 4, "gender": 0, "age": 28, "occupation": 1, "zipcode": "34567"},
    {"id": 5, "gender": 0, "age": 30, "occupation": 1, "zipcode": "45679"},
    {"id": 6,"gender": 0, "age": 26, "occupation": 0, "zipcode": "12345"},
    {"id": 7,"gender": 0, "age": 26, "occupation": 0, "zipcode": "12345"},
    {"id": 8,"gender": 0, "age": 26, "occupation": 0, "zipcode": "12345"},
]




movies = [
    {"id": 1, "name": "The Shawshank Redemption", "category": "Drama"},
     {"id": 2, "name": "The Godfather", "category": "Crime"},
    {"id": 3, "name": "Pulp Fiction", "category": "Crime"},
     {"id": 4, "name": "The Dark Knight", "category": "Action"},
    {"id": 5, "name": "Fight Club", "category": "Drama"},
     {"id": 6, "name": "Forrest Gump", "category": "Drama"},
]



ratings = [
    {"user_id": 1, "movie_id": 1, "rating": 1},
     {"user_id": 2, "movie_id": 2, "rating": 1},
     {"user_id": 3, "movie_id": 3, "rating": 1},
    {"user_id": 4, "movie_id": 4, "rating": 1},
    {"user_id": 5, "movie_id": 5, "rating":1},
        {"user_id":6 , "movie_id": 6, "rating": 1},
      

]
user_ids = [user["id"] for user in users]
num_users = len(user_ids)
user_id_to_index = {user_id: index for index, user_id in enumerate(user_ids)}


num_movies = len(movies)
user_ratings_matrix = np.zeros((num_users, num_movies))
for rating in ratings:
    user_index = user_id_to_index.get(rating["user_id"])
    if user_index is not None:
        movie_index = rating["movie_id"] - 1
        user_ratings_matrix[user_index, movie_index] = rating["rating"]


num_clusters = 1

user_similarity = cosine_similarity(user_ratings_matrix)
spectral_clustering = SpectralClustering(n_clusters=num_clusters, affinity='precomputed', random_state=42)
user_labels = spectral_clustering.fit_predict(user_similarity)

def find_nearest_cluster(new_user_features, cluster_centers):
    distances = np.linalg.norm(cluster_centers - new_user_features.reshape(1, -1), axis=1)
    nearest_cluster = np.argmin(distances)
    return nearest_cluster

def recommend_movies_for_user(new_user_features, user_labels, user_ratings_matrix, movie_data, num_recommendations=1):
    nearest_cluster = find_nearest_cluster(new_user_features, cluster_centers)
    cluster_users = np.where(user_labels == nearest_cluster)[0]
    cluster_user_ratings = user_ratings_matrix[cluster_users]
    rated_movies_mask = cluster_user_ratings.sum(axis=0) > 0
    
    cluster_user_ratings = cluster_user_ratings[:, rated_movies_mask]
    movie_data = [movie_data[idx] for idx, is_rated in enumerate(rated_movies_mask) if is_rated]
    
    if not any(rated_movies_mask):
        return []  
    
    average_ratings = cluster_user_ratings.mean(axis=0)
    top_movie_indices = np.argsort(average_ratings)[::-1][:num_recommendations]
    recommended_movies = [movie_data[idx]["name"] for idx in top_movie_indices]
    return recommended_movies

new_user_data = {"gender": 0, "age": 35, "occupation": 2, "zipcode": "56789"}

num_features = len(user_ids) 
new_user_features = np.zeros(num_features)
new_user_features[:4] = [new_user_data["age"], new_user_data["gender"], new_user_data["occupation"], int(new_user_data["zipcode"])]

kmeans = KMeans(n_clusters=num_clusters, random_state=42)
kmeans.fit(user_similarity)
cluster_centers = kmeans.cluster_centers_


recommended_movie_names = recommend_movies_for_user(new_user_features, user_labels, user_ratings_matrix, movies)

print("Recommended Movie Names:", recommended_movie_names)