import numpy as np

class Perceptron:
    def __init__(self, num_features, learning_rate=0.01, epochs=1000):
        self.weights = np.zeros(num_features)
        self.bias = 0
        self.learning_rate = learning_rate
        self.epochs = epochs

    def predict(self, x):
        activation = np.dot(self.weights, x) + self.bias
        return 1 if activation > 0 else -1

    def fit(self, X, y):
        for _ in range(self.epochs):
            for xi, target in zip(X, y):
                prediction = self.predict(xi)
                if target != prediction:
                    self.weights += self.learning_rate * (target - prediction) * xi
                    self.bias += self.learning_rate * (target - prediction)

if __name__ == "__main__":
    X = np.array([[2, 3], [1, 1], [2, 1], [3, 4]])
    y = np.array([1, -1, -1, 1]) 

    perceptron = Perceptron(num_features=2)

    perceptron.fit(X, y)

    test_sample = np.array([2, 2])
    prediction = perceptron.predict(test_sample)
    print(f"Prediction for {test_sample}: {prediction}")
