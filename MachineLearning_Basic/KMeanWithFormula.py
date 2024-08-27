import numpy as np
import matplotlib.pyplot as plt

def euclidean_distance(a, b):
    return np.sqrt(np.sum((a - b) ** 2))

def kmeans(X, K, max_iters=100):
    np.random.seed(42)
    centroids = X[np.random.choice(X.shape[0], K, replace=False)]
    
    for _ in range(max_iters):
        clusters = [[] for _ in range(K)]
        for x in X:
            distances = [euclidean_distance(x, centroid) for centroid in centroids]
            cluster_index = np.argmin(distances)
            clusters[cluster_index].append(x)
        
        clusters = [np.array(cluster) for cluster in clusters]
        
        new_centroids = np.array([cluster.mean(axis=0) for cluster in clusters])
        
        if np.all(centroids == new_centroids):
            break
        
        centroids = new_centroids

    return centroids, clusters

np.random.seed(42)
X = np.vstack((np.random.randn(150, 2) * 0.75 + np.array([1, 0]),
               np.random.randn(150, 2) * 0.5 - np.array([1, 0]),
               np.random.randn(150, 2) * 0.6 + np.array([0, 1])))

K = 3
centroids, clusters = kmeans(X, K)

colors = ['r', 'g', 'b']
for cluster, color in zip(clusters, colors):
    plt.scatter(cluster[:, 0], cluster[:, 1], c=color)

plt.scatter(centroids[:, 0], centroids[:, 1], c='yellow', s=200, marker='x', label='Centroids')
plt.title('K-Means Clustering from Scratch')
plt.xlabel('X')
plt.ylabel('Y')
plt.legend()
plt.show()
