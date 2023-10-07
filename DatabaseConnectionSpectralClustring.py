
Import pyodbc
Import numpy as np
From sklearn.cluster import KMeans
From sklearn.metrics.pairwise import cosine_similarity
From sklearn.cluster import SpectralClustering

# Connect to the SQL Server database
Conn = pyodbc.connect(‘DRIVER={SQL Server};’
                      ‘SERVER=DESKTOP-2ILB58A;’
                      ‘DATABASE=your_database;’
                      ‘UID=your_username;’
                      ‘PWD=your_password’)

# Fetch users from the database
Cursor = conn.cursor()
Cursor.execute(‘SELECT * FROM users’)
Users = cursor.fetchall()

# Fetch movies from the database
Cursor.execute(‘SELECT * FROM movies’)
Movies = cursor.fetchall()

# Fetch ratings from the database
Cursor.execute(‘SELECT * FROM ratings’)
Ratings = cursor.fetchall()

# Create user_ids, user_id_to_index, and user_ratings_matrix
User_ids = [user.id for user in users]
Num_users = len(user_ids)
User_id_to_index = {user.id: index for index, user in enumerate(users)}

Num_movies = len(movies)
User_ratings_matrix = np.zeros((num_users, num_movies))
For rating in ratings:
    User_index = user_id_to_index.get(rating.user_id)
    If user_index is not None:
        Movie_index = rating.movie_id – 1
        User_ratings_matrix[user_index, movie_index] = rating.rating

# Perform Spectral Clustering
Num_clusters = 1
User_similarity = cosine_similarity(user_ratings_matrix)
Spectral_clustering = SpectralClustering(n_clusters=num_clusters, affinity=’precomputed’, random_state=42)
User_labels = spectral_clustering.fit_predict(user_similarity)

# Perform KMeans clustering to find cluster centers
Kmeans = KMeans(n_clusters=num_clusters, random_state=42)
Kmeans.fit(user_similarity)
Cluster_centers = kmeans.cluster_centers_

# Function to recommend movies for the new user
Def recommend_movies_for_user(new_user_features, user_labels, user_ratings_matrix, movie_data, num_recommendations=1):
    # ... (same as before) ...

# Example new user data from the database
New_user_data = {“gender”: 0, “age”: 26, “occupation”: 0, “zipcode”: “12345”}
Num_features = 5
New_user_features = np.zeros(num_features)
New_user_features[:4] = [new_user_data[“age”], new_user_data[“gender”], new_user_data[“occupation”], int(new_user_data[“zipcode”])]

# Get recommended movies for the new user
Recommended_movie_names = recommend_movies_for_user(new_user_features, user_labels, user_ratings_matrix, movies)

Print(“Recommended Movie Names:”, recommended_movie_names)


