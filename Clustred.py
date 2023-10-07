import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity
import turtle

np.random.seed(42)

users = [
    {"id": i, "gender": np.random.choice(["M", "F"]), "age": np.random.randint(18, 65),
     "occupation": np.random.choice(["Engineer", "Artist", "Doctor", "Teacher", "Student", "Lawyer", "Nurse",
                                     "Chef", "Scientist", "Writer"]),
     "zipcode": str(np.random.randint(10000, 99999))} for i in range(1, 101)
]

movies = [{"id": i, "name": f"Movie {chr(65 + i)}", "category": np.random.choice(["Action", "Drama", "Comedy"])}
          for i in range(1, 21)]

ratings = [{"user_id": user["id"], "movie_id": np.random.randint(1, 21), "rating": np.random.randint(1, 6)}
           for user in users for _ in range(10)]



def get_valid_gender_input(prompt, x, y):
    while True:
        input_box = turtle.Screen().textinput(prompt, "")
        if input_box in ["M", "F"]:
            return input_box
        else:
            print("Invalid input. Please enter 'M' for Male or 'F' for Female.")

def get_valid_age_input(prompt, x, y):
    while True:
        input_box = turtle.Screen().textinput(prompt, "")
        if input_box.isdigit() and 18 <= int(input_box) <= 65:
            return int(input_box)
        else:
            print("Invalid input. Please enter a valid age between 18 and 65.")

def get_valid_occupation_input(prompt, x, y):
    valid_occupations = ["Engineer", "Artist", "Doctor", "Teacher", "Student", "Lawyer", "Nurse", "Chef", "Scientist", "Writer"]
    while True:
        input_box = turtle.Screen().textinput(prompt, "")
        if input_box in valid_occupations:
            return input_box
        else:
            print("Invalid input. Please choose from the provided occupations.")

def get_valid_zipcode_input(prompt, x, y):
    while True:
        input_box = turtle.Screen().textinput(prompt, "")
        if input_box.isdigit() and len(input_box) == 5:
            return input_box
        else:
            print("Invalid input. Please enter a valid 5-digit zipcode.")


user_ids = [user["id"] for user in users]
num_users = len(user_ids)
user_id_to_index = {user_id: index for index, user_id in enumerate(user_ids)}

user_movie_ratings = np.zeros((num_users, len(movies)))
for rating in ratings:
    user_index = user_id_to_index[rating["user_id"]]
    movie_index = rating["movie_id"] - 1
    user_movie_ratings[user_index, movie_index] = rating["rating"]

num_clusters = 3
kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init=10)
user_labels = kmeans.fit_predict(user_movie_ratings)

turtle.bgcolor("lightblue")

def get_user_input(prompt, x, y):
    input_box = turtle.Screen().textinput(prompt, "")
    while input_box is None:
        input_box = turtle.Screen().textinput(prompt, "")
    return input_box

def display_text(text, x, y):
    turtle.penup()
    turtle.goto(x, y)
    turtle.write(text, align="left", font=("Arial", 14, "normal"))


new_user_data = {}
display_text("Enter User Information:", -200, 200)
new_user_data["gender"] = get_valid_gender_input("Gender (M/F): ", -200, 150)
new_user_data["age"] = get_valid_age_input("Age: ", -200, 100)
new_user_data["occupation"] = get_valid_occupation_input("Occupation: ", -200, 50)
new_user_data["zipcode"] = get_valid_zipcode_input("Zipcode: ", -200, 0)

def preprocess_user_data(user_data):
    user_features = [user_data["age"]]
    gender_mapping = {"M": 1, "F": 0}
    user_features.append(gender_mapping.get(user_data["gender"], 0))
    occupation_mapping = {
        "Engineer": 0,
        "Artist": 1,
        "Doctor": 2
    }
    user_features.append(occupation_mapping.get(user_data["occupation"], 0))
    user_features.append(int(user_data["zipcode"]))
    return user_features

new_user_features = np.array(preprocess_user_data(new_user_data)) 

cluster_centers = kmeans.cluster_centers_

cluster_centers_reshaped = cluster_centers[:, :4]

cosine_similarities = cosine_similarity(new_user_features.reshape(1, -1), cluster_centers_reshaped)

nearest_cluster = np.argmax(cosine_similarities)

def display_movies(recommended_movies):
    turtle.clear()
    display_text("Recommended Movies:", -200, 200)
    y_pos = 150
    for movie in recommended_movies:
        display_text(movie, -200, y_pos)
        y_pos -= 50

def recommend_movies(nearest_cluster, user_labels, user_movie_ratings, movie_data, num_recommendations=4):
    cluster_users = np.where(user_labels == nearest_cluster)[0]
    cluster_user_ratings = user_movie_ratings[cluster_users]
    average_ratings = cluster_user_ratings.mean(axis=0)
    top_movie_indices = np.argsort(average_ratings)[::-1][:num_recommendations]
    recommended_movies = [movies[idx]["name"] for idx in top_movie_indices]
    return recommended_movies

recommended_movie_names = recommend_movies(nearest_cluster, user_labels, user_movie_ratings, movies)

display_movies(recommended_movie_names)
turtle.done()
