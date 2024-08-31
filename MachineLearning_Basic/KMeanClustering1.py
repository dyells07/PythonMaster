from time import time
import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import load_digits
from sklearn.preprocessing import scale
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn import metrics

# Load and scale the digits dataset
digits = load_digits()
data = scale(digits.data)
n_samples, n_features = data.shape
n_digits = len(np.unique(digits.target))
labels = digits.target
sample_size = 300

print("n_digits: %d, \t n_samples: %d, \t n_features: %d" % (n_digits, n_samples, n_features))
print(79 * '_')
print('% 9s' % 'init' + '    time   inertia    homo   compl   v-meas     ARI     AMI  silhouette')

# Function to benchmark k-means clustering
def bench_k_means(estimator, name, data):
    t0 = time()
    estimator.fit(data)
    print('% 9s    %.2fs    %i    %.3f    %.3f    %.3f    %.3f    %.3f    %.3f'
          % (name, (time() - t0), estimator.inertia_,
             metrics.homogeneity_score(labels, estimator.labels_),
             metrics.completeness_score(labels, estimator.labels_),
             metrics.v_measure_score(labels, estimator.labels_),
             metrics.adjusted_rand_score(labels, estimator.labels_),
             metrics.adjusted_mutual_info_score(labels, estimator.labels_),
             metrics.silhouette_score(data, estimator.labels_,
                                      metric='euclidean',
                                      sample_size=sample_size)))

# Benchmark the original k-means clustering
bench_k_means(KMeans(init='k-means++', n_clusters=n_digits, n_init=10), name="k-means++", data=data)

# Apply PCA to reduce the dimensionality
pca = PCA(n_components=n_digits).fit(data)
data_pca = pca.transform(data)

# Benchmark k-means clustering after PCA
bench_k_means(KMeans(init='k-means++', n_clusters=n_digits, n_init=10), name="PCA-based", data=data_pca)

# Visualize the clustering result
plt.figure(figsize=(8, 6))
reduced_data = PCA(n_components=2).fit_transform(data)
kmeans = KMeans(init='k-means++', n_clusters=n_digits, n_init=10)
kmeans.fit(reduced_data)
h = 0.02

x_min, x_max = reduced_data[:, 0].min() - 1, reduced_data[:, 0].max() + 1
y_min, y_max = reduced_data[:, 1].min() - 1, reduced_data[:, 1].max() + 1
xx, yy = np.meshgrid(np.arange(x_min, x_max, h), np.arange(y_min, y_max, h))

Z = kmeans.predict(np.c_[xx.ravel(), yy.ravel()])
Z = Z.reshape(xx.shape)
plt.imshow(Z, interpolation='nearest', extent=(xx.min(), xx.max(), yy.min(), yy.max()), 
           cmap=plt.cm.Paired, aspect='auto', origin='lower')

plt.scatter(reduced_data[:, 0], reduced_data[:, 1], c=labels, edgecolor='k', s=20)
plt.title('K-means clustering on the digits dataset (PCA-reduced data)')
plt.xlim(x_min, x_max)
plt.ylim(y_min, y_max)
plt.xticks(())
plt.yticks(())
plt.show()
