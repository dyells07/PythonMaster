# Import Part
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from sklearn.metrics.pairwise import cosine_similarity
from ast import literal_eval

# Code Part

# Read the full CSV into 'df'
df = pd.read_csv("tmdb_5000_movies.csv")

# Define the selected columns for 'movies_df'
selected_columns_movies = [
    'budget', 'genres', 'homepage', 'id', 'keywords', 'original_language',
    'original_title', 'overview', 'popularity', 'production_companies',
    'production_countries', 'release_date', 'revenue', 'runtime',
    'spoken_languages', 'status', 'tagline', 'title', 'vote_average',
    'vote_count'
]

# Read the 'movies_df' DataFrame with selected columns
movies_df = df[selected_columns_movies]

# Define the selected columns for 'credits_df'
selected_columns_credits = ['id', 'original_title', 'cast', 'crew']

# Read 'credits_df' with selected columns
credits_df = df[selected_columns_credits]

# Now you can merge 'movies_df' and 'credits_df' on a common column, for example, 'id'
merged_df = movies_df.merge(credits_df, on="id")
# Demographic Filtering
C = movies_df["vote_average"].mean()
m = movies_df["vote_count"].quantile(0.9)
new_movies_df = movies_df.copy().loc[movies_df["vote_count"] >= m]

def weighted_rating(x, C=C, m=m):
    v = x["vote_count"]
    R = x["vote_average"]
    return (v / (v + m) * R) + (m / (v + m) * C)

new_movies_df["score"] = new_movies_df.apply(weighted_rating, axis=1)
new_movies_df = new_movies_df.sort_values('score', ascending=False)

def plot():
    popularity = movies_df.sort_values("popularity", ascending=False)
    plt.figure(figsize=(12, 6))
    plt.barh(popularity["title"].head(10), popularity["popularity"].head(10), align="center", color="skyblue")
    plt.gca().invert_yaxis()
    plt.title("Top 10 movies")
    plt.xlabel("Popularity")
    plt.show()

plot()

# Content-based Filtering
tfidf = TfidfVectorizer(stop_words="english")
movies_df["overview"] = movies_df["overview"].fillna("")
tfidf_matrix = tfidf.fit_transform(movies_df["overview"])

# Compute similarity
cosine_sim = linear_kernel(tfidf_matrix, tfidf_matrix)
indices = pd.Series(movies_df.index, index=movies_df["title"]).drop_duplicates()

def get_recommendations(title, cosine_sim=cosine_sim):
    idx = indices[title]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:11]
    movies_indices = [ind[0] for ind in sim_scores]
    movies = movies_df["title"].iloc[movies_indices]
    return movies

print("Recommendations for The Dark Knight Rises:")
print(get_recommendations("The Dark Knight Rises"))
print()
print("Recommendations for Avengers:")
print(get_recommendations("The Avengers"))

# Additional Data Preprocessing
features = ["cast", "crew", "keywords", "genres"]

for feature in features:
    movies_df[feature] = movies_df[feature].apply(literal_eval)

def get_director(x):
    for i in x:
        if i["job"] == "Director":
            return i["name"]
    return np.nan

def get_list(x):
    if isinstance(x, list):
        names = [i["name"] for i in x]
        if len(names) > 3:
            names = names[:3]
        return names
    return []

movies_df["director"] = movies_df["crew"].apply(get_director)
features = ["cast", "keywords", "genres"]

for feature in features:
    movies_df[feature] = movies_df[feature].apply(get_list)

def clean_data(x):
    if isinstance(x, list):
        return [str.lower(i.replace(" ", "")) for i in x]
    else:
        if isinstance(x, str):
            return str.lower(x.replace(" ", ""))
        else:
            return ""

features = ['cast', 'keywords', 'director', 'genres']

for feature in features:
    movies_df[feature] = movies_df[feature].apply(clean_data)

def create_soup(x):
    return ' '.join(x['keywords']) + ' ' + ' '.join(x['cast']) + ' ' + x['director'] + ' ' + ' '.join(x['genres'])

movies_df["soup"] = movies_df.apply(create_soup, axis=1)

count_vectorizer = CountVectorizer(stop_words="english")
count_matrix = count_vectorizer.fit_transform(movies_df["soup"])
cosine_sim2 = cosine_similarity(count_matrix, count_matrix)

movies_df = movies_df.reset_index()
indices = pd.Series(movies_df.index, index=movies_df['title'])

print("# Content-Based System - metadata #")
print("Recommendations for The Dark Knight Rises:")
print(get_recommendations("The Dark Knight Rises", cosine_sim2))
print()
print("Recommendations for Avengers:")
print(get_recommendations("The Avengers", cosine_sim2))
