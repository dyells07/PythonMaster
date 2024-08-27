import numpy as np
import matplotlib.pyplot as plt
from sklearn.datasets import make_classification
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

def plot_decision_boundary(model, X, y):
    x_min, x_max = X[:, 0].min() - 1, X[:, 0].max() + 1
    y_min, y_max = X[:, 1].min() - 1, X[:, 1].max() + 1
    xx, yy = np.meshgrid(np.arange(x_min, x_max, 0.1),
                         np.arange(y_min, y_max, 0.1))
    Z = model.predict(np.c_[xx.ravel(), yy.ravel()])
    Z = Z.reshape(xx.shape)
    plt.contourf(xx, yy, Z, alpha=0.4)
    plt.scatter(X[:, 0], X[:, 1], c=y, edgecolors='k', marker='o')

X, y = make_classification(
    n_samples=100, n_features=2, n_informative=2, n_redundant=0, n_clusters_per_class=1, random_state=42
)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

empty_tree = DecisionTreeClassifier(max_depth=1)  
empty_tree.fit(X_train, y_train)
y_pred_empty = empty_tree.predict(X_test)
empty_tree_accuracy = accuracy_score(y_test, y_pred_empty)

full_tree = DecisionTreeClassifier(max_depth=None) 
full_tree.fit(X_train, y_train)
y_pred_full = full_tree.predict(X_test)
full_tree_accuracy = accuracy_score(y_test, y_pred_full)

print(f"Accuracy of underfitting (empty) tree: {empty_tree_accuracy * 100:.2f}%")
print(f"Accuracy of overfitting (full) tree: {full_tree_accuracy * 100:.2f}%")

plt.figure(figsize=(12, 6))

plt.subplot(1, 2, 1)
plt.title('Underfitting (Empty) Tree')
plot_decision_boundary(empty_tree, X, y)
plt.scatter(X[:, 0], X[:, 1], c=y, edgecolors='k', marker='o')
plt.xlabel('Feature 1')
plt.ylabel('Feature 2')

plt.subplot(1, 2, 2)
plt.title('Overfitting (Full) Tree')
plot_decision_boundary(full_tree, X, y)
plt.scatter(X[:, 0], X[:, 1], c=y, edgecolors='k', marker='o')
plt.xlabel('Feature 1')
plt.ylabel('Feature 2')

plt.tight_layout()
plt.show()
