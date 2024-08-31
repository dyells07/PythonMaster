import numpy as np
from sklearn.cluster import KMeans  # Import KMeans
from sklearn.datasets import load_digits
from scipy.spatial.distance import cdist
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
from sklearn.preprocessing import scale
from sklearn.model_selection import ShuffleSplit, cross_val_score  # Import cross-validation tools
from sklearn.metrics import adjusted_rand_score  # Import scoring method

# Load and scale the digits dataset
digits = load_digits()
data = scale(digits.data)
n_samples, n_features = data.shape
labels = digits.target

# Perform PCA to reduce dimensionality to 2 components
pca = PCA(n_components=2)
reduced_data = pca.fit_transform(data)

# Range of k values to test
K = range(1, 20)
explained_variance = []

# Calculate the explained variance for each value of k
for k in K:
    kmeans = KMeans(init='k-means++', n_clusters=k, n_init=10)
    kmeans.fit(reduced_data)
    # Compute the sum of the minimum distances to cluster centers
    explained_variance.append(sum(np.min(cdist(reduced_data, kmeans.cluster_centers_, 'euclidean'), axis=1)) / reduced_data.shape[0])

# Plot the elbow graph
plt.figure(figsize=(8, 6))
plt.plot(K, explained_variance, 'bx-')
plt.xlabel('Number of clusters (k)')
plt.ylabel('Explained variance')
plt.title('Elbow Method For Optimal k')
plt.show()

# Define the KMeans model with the optimal number of clusters (e.g., from the elbow method)
optimal_k = 10  # Replace with the optimal k determined from the elbow method
kmeans = KMeans(init='k-means++', n_clusters=optimal_k, n_init=10)

# Define cross-validation parameters
cv = ShuffleSplit(n_splits=10, test_size=0.4, random_state=0)

# Perform cross-validation using the Adjusted Rand Index as the scoring method
scores = cross_val_score(kmeans, data, labels, cv=cv, scoring='adjusted_rand_score')

# Print individual fold scores and the average score
print("Cross-Validation Scores: ", scores)
print("Average Score: ", np.mean(scores))
