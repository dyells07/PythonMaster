import numpy as np
from sklearn.cluster import SpectralClustering, KMeans
from sklearn.metrics.pairwise import cosine_similarity


# new_user_data = {"gender": 0, "age": 26, "occupation": 0, "zipcode": "12345"}

users = [
    {"id": 1, "gender": 1, "age": 25, "occupation": 0, "zipcode": "12345"},
    {"id": 2, "gender": 0, "age": 35, "occupation": 2, "zipcode": "56789"},
    {"id": 3, "gender": 0, "age": 30, "occupation": 1, "zipcode": "34567"},
    {"id": 4, "gender": 0, "age": 28, "occupation": 1, "zipcode": "34567"},
    {"id": 5, "gender": 0, "age": 30, "occupation": 1, "zipcode": "45679"}
]




movies = [
    {"id": 1, "name": "The Shawshank Redemption", "category": "Drama"},
     {"id": 2, "name": "The Godfather", "category": "Crime"},
    {"id": 3, "name": "Pulp Fiction", "category": "Crime"},
     {"id": 4, "name": "The Dark Knight", "category": "Action"},
    {"id": 5, "name": "Fight Club", "category": "Drama"},
    # {"id": 6, "name": "Forrest Gump", "category": "Drama"},
    # {"id": 7, "name": "Inception", "category": "Sci-Fi"},
    {"id": 8, "name": "The Matrix", "category": "Sci-Fi"},
    {"id": 9, "name": "The Lord of the Rings: The Fellowship of the Ring", "category": "Adventure"},
    {"id": 10, "name": "The Avengers", "category": "Action"},
    {"id": 11, "name": "The Lion King", "category": "Animation"},
    {"id": 12, "name": "The Social Network", "category": "Drama"},
    {"id": 13, "name": "The Departed", "category": "Crime"},
    {"id": 14, "name": "Gladiator", "category": "Action"},
    {"id": 15, "name": "The Silence of the Lambs", "category": "Thriller"},
    {"id": 16, "name": "The Prestige", "category": "Mystery"},
    {"id": 17, "name": "The Dark Knight Rises", "category": "Action"},
    {"id": 18, "name": "The Godfather: Part II", "category": "Crime"},
    {"id": 19, "name": "Whiplash", "category": "Drama"},
    {"id": 20, "name": "The Green Mile", "category": "Drama"},
    {"id": 21, "name": "The Departed", "category": "Crime"},
    {"id": 22, "name": "The Grand Budapest Hotel", "category": "Comedy"},
    {"id": 23, "name": "The Revenant", "category": "Adventure"},
    {"id": 24, "name": "The Wolf of Wall Street", "category": "Biography"},
    {"id": 25, "name": "The Shining", "category": "Horror"},
    {"id": 26, "name": "The Great Gatsby", "category": "Drama"},
    {"id": 27, "name": "The Princess Bride", "category": "Adventure"},
    {"id": 28, "name": "The Notebook", "category": "Romance"},
    {"id": 29, "name": "The Exorcist", "category": "Horror"},
    {"id": 30, "name": "The Bourne Identity", "category": "Action"},
    {"id": 31, "name": "The Pursuit of Happyness", "category": "Biography"},
    {"id": 32, "name": "The Conjuring", "category": "Horror"},
    {"id": 33, "name": "The Hangover", "category": "Comedy"},
    {"id": 34, "name": "The Social Dilemma", "category": "Documentary"},
    {"id": 35, "name": "The Big Lebowski", "category": "Comedy"},
    {"id": 36, "name": "The Fault in Our Stars", "category": "Romance"},
    {"id": 37, "name": "The Maze Runner", "category": "Action"},
    {"id": 38, "name": "The Conjuring 2", "category": "Horror"},
    {"id": 39, "name": "The Breakfast Club", "category": "Comedy"},
    {"id": 40, "name": "The Perks of Being a Wallflower", "category": "Drama"},
    {"id": 41, "name": "The Devil Wears Prada", "category": "Comedy"},
    {"id": 42, "name": "The Incredibles", "category": "Animation"},
    {"id": 43, "name": "The Blair Witch Project", "category": "Horror"},
    {"id": 44, "name": "The Avengers: Infinity War", "category": "Action"},
    {"id": 44, "name": "The Avengers: Infinity War", "category": "Action"},
    {"id": 45, "name": "The Martian", "category": "Sci-Fi"},
    {"id": 46, "name": "The Hangover", "category": "Comedy"},
    {"id": 47, "name": "The Social Network", "category": "Drama"},
    {"id": 48, "name": "The Dark Knight Rises", "category": "Action"},
    {"id": 49, "name": "The Revenant", "category": "Adventure"},
    {"id": 50, "name": "The Shape of Water", "category": "Fantasy"},
    {"id": 51, "name": "The Silence of the Lambs", "category": "Thriller"},
    {"id": 52, "name": "The Princess Bride", "category": "Adventure"},
    {"id": 53, "name": "The Notebook", "category": "Romance"},
    {"id": 54, "name": "The Godfather: Part II", "category": "Crime"},
    {"id": 55, "name": "The Bourne Identity", "category": "Action"},
    {"id": 56, "name": "The Pursuit of Happyness", "category": "Biography"},
    {"id": 57, "name": "The Conjuring", "category": "Horror"},
    {"id": 58, "name": "The Hangover", "category": "Comedy"},
    {"id": 59, "name": "The Social Dilemma", "category": "Documentary"},
    {"id": 60, "name": "The Fault in Our Stars", "category": "Romance"},
    {"id": 61, "name": "The Maze Runner", "category": "Action"},
    {"id": 62, "name": "The Conjuring 2", "category": "Horror"},
    {"id": 63, "name": "The Breakfast Club", "category": "Comedy"},
    {"id": 64, "name": "The Perks of Being a Wallflower", "category": "Drama"},
    {"id": 65, "name": "The Devil Wears Prada", "category": "Comedy"},
    {"id": 66, "name": "The Incredibles", "category": "Animation"},
    {"id": 67, "name": "The Blair Witch Project", "category": "Horror"},
    {"id": 68, "name": "The Avengers: Infinity War", "category": "Action"},
    {"id": 69, "name": "The Martian", "category": "Sci-Fi"},
    {"id": 70, "name": "The Hangover", "category": "Comedy"},
    {"id": 71, "name": "The Social Network", "category": "Drama"},
    {"id": 72, "name": "The Dark Knight Rises", "category": "Action"},
    {"id": 73, "name": "The Revenant", "category": "Adventure"},
    {"id": 74, "name": "The Shape of Water", "category": "Fantasy"},
    {"id": 75, "name": "The Silence of the Lambs", "category": "Thriller"},
    {"id": 76, "name": "The Princess Bride", "category": "Adventure"},
    {"id": 77, "name": "The Notebook", "category": "Romance"},
    {"id": 78, "name": "The Godfather: Part II", "category": "Crime"},
    {"id": 79, "name": "The Bourne Identity", "category": "Action"},
    {"id": 80, "name": "The Pursuit of Happyness", "category": "Biography"},
    {"id": 81, "name": "The Conjuring", "category": "Horror"},
    {"id": 82, "name": "The Hangover", "category": "Comedy"},
    {"id": 83, "name": "The Social Dilemma", "category": "Documentary"},
    {"id": 84, "name": "The Fault in Our Stars", "category": "Romance"},
    {"id": 85, "name": "The Maze Runner", "category": "Action"},
    {"id": 86, "name": "The Conjuring 2", "category": "Horror"},
    {"id": 87, "name": "The Breakfast Club", "category": "Comedy"},
]



ratings = [
    {"user_id": 1, "movie_id": 1, "rating": 1},
    {"user_id": 2, "movie_id": 2, "rating": 3},
     {"user_id": 4, "movie_id": 1, "rating": 3},
       
     {"user_id": 5, "movie_id": 1, "rating": 3},
        
     {"user_id": 5, "movie_id": 2, "rating": 5},
    {"user_id": 2, "movie_id": 3, "rating": 5},
     {"user_id": 2, "movie_id": 3, "rating": 5},
        {"user_id": 2, "movie_id": 4, "rating": 5},
        {"user_id": 2, "movie_id": 3, "rating": 5},
           {"user_id": 1, "movie_id": 3, "rating": 5},
    {"user_id": 3, "movie_id": 4, "rating": 5},
       {"user_id": 1, "movie_id": 1, "rating": 1},
    {"user_id": 2, "movie_id": 2, "rating": 3},
     {"user_id": 4, "movie_id": 1, "rating": 3},
       
     {"user_id": 5, "movie_id": 1, "rating": 3},
        
     {"user_id": 5, "movie_id": 2, "rating": 5},
    {"user_id": 2, "movie_id": 3, "rating": 5},
     {"user_id": 2, "movie_id": 3, "rating": 5},
        {"user_id": 2, "movie_id": 4, "rating": 5},
        {"user_id": 2, "movie_id": 3, "rating": 5},
           {"user_id": 1, "movie_id": 3, "rating": 5},
    {"user_id": 3, "movie_id": 4, "rating": 5},

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

new_user_data = {"gender": 0, "age": 26, "occupation": 0, "zipcode": "12345"}

num_features = 5  
new_user_features = np.zeros(num_features)
new_user_features[:4] = [new_user_data["age"], new_user_data["gender"], new_user_data["occupation"], int(new_user_data["zipcode"])]

kmeans = KMeans(n_clusters=num_clusters, random_state=42)
kmeans.fit(user_similarity)
cluster_centers = kmeans.cluster_centers_


recommended_movie_names = recommend_movies_for_user(new_user_features, user_labels, user_ratings_matrix, movies)

print("Recommended Movie Names:", recommended_movie_names)