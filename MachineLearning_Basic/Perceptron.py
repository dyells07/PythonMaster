import numpy as np
import matplotlib.pyplot as plt

class Perceptron:
    def __init__(self, input_size, learning_rate=0.01, epochs=1000):
        self.weights = np.zeros(input_size + 1)
        self.learning_rate = learning_rate
        self.epochs = epochs

    def activation_function(self, x):
        return np.where(x >= 0, 1, 0)

    def predict(self, x):
        z = np.dot(x, self.weights[1:]) + self.weights[0]
        return self.activation_function(z)

    def train(self, X, y):
        for _ in range(self.epochs):
            for i in range(len(X)):
                prediction = self.predict(X[i])
                self.weights[1:] += self.learning_rate * (y[i] - prediction) * X[i]
                self.weights[0] += self.learning_rate * (y[i] - prediction)  # Bias update

    def decision_boundary(self, X):
        x_min, x_max = X[:, 0].min() - 1, X[:, 0].max() + 1
        y_min, y_max = X[:, 1].min() - 1, X[:, 1].max() + 1
        xx, yy = np.meshgrid(np.arange(x_min, x_max, 0.1),
                             np.arange(y_min, y_max, 0.1))
        grid = np.c_[xx.ravel(), yy.ravel()]
        predictions = self.predict(grid)
        zz = predictions.reshape(xx.shape)
        return xx, yy, zz

from sklearn.datasets import make_classification

X, y = make_classification(n_samples=100, n_features=2, n_informative=2, n_redundant=0,
                           n_clusters_per_class=1, flip_y=0, random_state=1)

y = np.where(y == 0, -1, 1)

perceptron = Perceptron(input_size=2)
perceptron.train(X, y)

xx, yy, zz = perceptron.decision_boundary(X)

plt.contourf(xx, yy, zz, alpha=0.3)
plt.scatter(X[:, 0], X[:, 1], c=y, cmap=plt.cm.coolwarm)
plt.title("Perceptron Decision Boundary")
plt.show()
