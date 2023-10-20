import matplotlib.pyplot as plt
from sklearn.datasets import make_moons

X, y = make_moons(n_samples=1000, noise=0.4, random_state=217)

plt.scatter(X[:, 0], X[:, 1], c=y)
plt.show()
