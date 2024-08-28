import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from sklearn.datasets import make_classification
from sklearn.neighbors import KNeighborsClassifier

def plot_decision_boundary(X_train, y_train, model, title):
    x_min, x_max = X_train[:, 0].min() - 1, X_train[:, 0].max() + 1
    y_min, y_max = X_train[:, 1].min() - 1, X_train[:, 1].max() + 1
    xx, yy = np.meshgrid(np.arange(x_min, x_max, 0.1),
                         np.arange(y_min, y_max, 0.1))
    
    Z = model.predict(np.c_[xx.ravel(), yy.ravel()])
    Z = Z.reshape(xx.shape)
    
    plt.contourf(xx, yy, Z, alpha=0.3, cmap=ListedColormap(('red', 'green')))
    plt.scatter(X_train[:, 0], X_train[:, 1], c=y_train, s=30, edgecolor='k')
    plt.title(title)
    plt.show()

X_train, y_train = make_classification(n_samples=100, n_features=2, n_informative=2, n_redundant=0,
                                       n_clusters_per_class=1, random_state=42)

plt.scatter(X_train[:, 0], X_train[:, 1], c=y_train, cmap=ListedColormap(('red', 'green')), edgecolor='k')
plt.title("Training Data")
plt.show()

knn = KNeighborsClassifier(n_neighbors=5)
knn.fit(X_train, y_train)
plot_decision_boundary(X_train, y_train, knn, "K-Nearest Neighbors (K=5)")

class WeightedKNN(KNeighborsClassifier):
    def predict(self, X):
        neighbors = super().kneighbors(X, return_distance=True)
        distances, indices = neighbors
        weights = np.exp(-distances)
        weighted_votes = []
        for i in range(len(X)):
            class_counts = np.bincount(y_train[indices[i]], weights=weights[i])
            weighted_votes.append(np.argmax(class_counts))
        return np.array(weighted_votes)

weighted_knn = WeightedKNN(n_neighbors=5)
weighted_knn.fit(X_train, y_train)
plot_decision_boundary(X_train, y_train, weighted_knn, "Weighted K-Nearest Neighbors")

class EpsilonBallNN:
    def __init__(self, epsilon=0.5):
        self.epsilon = epsilon
    
    def fit(self, X_train, y_train):
        self.X_train = X_train
        self.y_train = y_train
    
    def predict(self, X):
        predictions = []
        for test_point in X:
            distances = np.linalg.norm(self.X_train - test_point, axis=1)
            indices = np.where(distances <= self.epsilon)[0]
            if len(indices) == 0:
                predictions.append(-1) 
            else:
                class_counts = np.bincount(self.y_train[indices])
                predictions.append(np.argmax(class_counts))
        return np.array(predictions)

epsilon_ball_nn = EpsilonBallNN(epsilon=0.5)
epsilon_ball_nn.fit(X_train, y_train)
plot_decision_boundary(X_train, y_train, epsilon_ball_nn, "Epsilon-Ball Nearest Neighbors (epsilon=0.5)")
