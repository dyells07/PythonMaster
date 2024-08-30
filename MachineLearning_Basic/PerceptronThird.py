import numpy as np
import matplotlib.pyplot as plt

np.random.seed(0)
num_samples = 100
X = np.random.randn(num_samples, 2)  
y = np.where(X[:, 0] + X[:, 1] > 0, 1, -1)  

w = np.zeros(2)
b = 0
learning_rate = 0.1
epochs = 10

for _ in range(epochs):
    for i in range(num_samples):
        activation = np.dot(X[i], w) + b
        if y[i] * activation <= 0:  
            w += learning_rate * y[i] * X[i]
            b += learning_rate * y[i]

x1 = np.linspace(-2, 2, 100)
x2 = -(w[0]/w[1]) * x1 - (b/w[1])

plt.figure(figsize=(8, 6))
plt.scatter(X[:, 0], X[:, 1], c=y, cmap='bwr', marker='o')
plt.plot(x1, x2, '-k', label=f'Decision boundary: {w[0]:.2f}x1 + {w[1]:.2f}x2 + {b:.2f} = 0')
plt.xlabel('x1')
plt.ylabel('x2')
plt.legend()
plt.title('Perceptron Decision Boundary')
plt.grid(True)
plt.show()
