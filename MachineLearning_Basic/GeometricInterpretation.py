import numpy as np
import matplotlib.pyplot as plt

class Perceptron:
    def __init__(self, num_features, learning_rate=0.01, max_iter=1000):
        self.weights = np.zeros(num_features)
        self.bias = 0
        self.learning_rate = learning_rate
        self.max_iter = max_iter

    def predict(self, x):
        activation = np.dot(self.weights, x) + self.bias
        return 1 if activation > 0 else -1

    def fit(self, X, y):
        num_examples = X.shape[0]
        
        for _ in range(self.max_iter):
            for xi, target in zip(X, y):
                prediction = self.predict(xi)
                if target * (np.dot(self.weights, xi) + self.bias) <= 0:
                    self.weights += self.learning_rate * target * xi
                    self.bias += self.learning_rate * target

    def plot_decision_boundary(self, X, y):
        plt.scatter(X[:, 0], X[:, 1], c=y, cmap='bwr', marker='o')
        

        x_values = np.linspace(min(X[:, 0]), max(X[:, 0]), 100)
        y_values = -(self.weights[0] * x_values + self.bias) / self.weights[1]
        plt.plot(x_values, y_values, color='black')
        
        plt.quiver(0, 0, self.weights[0], self.weights[1], angles='xy', scale_units='xy', scale=1, color='blue')
        
        plt.xlim(min(X[:, 0]) - 1, max(X[:, 0]) + 1)
        plt.ylim(min(X[:, 1]) - 1, max(X[:, 1]) + 1)
        plt.xlabel('Feature 1')
        plt.ylabel('Feature 2')
        plt.title('Decision Boundary and Weight Vector')
        plt.grid(True)
        plt.show()

if __name__ == "__main__":
    X = np.array([[2, 3], [1, 1], [2, 1], [3, 4]])
    y = np.array([1, -1, -1, 1]) 

    perceptron = Perceptron(num_features=2)

    perceptron.fit(X, y)
    perceptron.plot_decision_boundary(X, y)
