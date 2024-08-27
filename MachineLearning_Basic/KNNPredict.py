import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_classification
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from matplotlib.colors import ListedColormap

# Generate synthetic data
X, y = make_classification(n_samples=100, n_features=2, n_informative=2, n_redundant=0, n_clusters_per_class=1, random_state=42)
y = np.where(y == 0, -1, 1)  # Change labels from 0/1 to -1/1 for consistency

# Define a function to plot decision boundaries
def plot_decision_boundaries(X, y, model, title):
    x_min, x_max = X[:, 0].min() - 1, X[:, 0].max() + 1
    y_min, y_max = X[:, 1].min() - 1, X[:, 1].max() + 1
    xx, yy = np.meshgrid(np.arange(x_min, x_max, 0.01),
                         np.arange(y_min, y_max, 0.01))
    Z = model.predict(np.c_[xx.ravel(), yy.ravel()])
    Z = Z.reshape(xx.shape)
    
    plt.contourf(xx, yy, Z, alpha=0.8, cmap=ListedColormap(('red', 'blue')))
    plt.scatter(X[:, 0], X[:, 1], c=y, edgecolor='k', s=20, cmap=ListedColormap(('darkred', 'darkblue')))
    plt.title(title)
    plt.show()

# K-Nearest Neighbors with K=1
knn = KNeighborsClassifier(n_neighbors=1)
knn.fit(X, y)
plot_decision_boundaries(X, y, knn, "KNN (K=1) Decision Boundary")

# K-Nearest Neighbors with K=5
knn5 = KNeighborsClassifier(n_neighbors=5)
knn5.fit(X, y)
plot_decision_boundaries(X, y, knn5, "KNN (K=5) Decision Boundary")

# Decision Tree
tree = DecisionTreeClassifier()
tree.fit(X, y)
plot_decision_boundaries(X, y, tree, "Decision Tree Decision Boundary")
