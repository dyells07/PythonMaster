import numpy as np
from sklearn.tree import DecisionTreeRegressor
rng = np.random.RandomState(1)
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))
regr = DecisionTreeRegressor(max_depth=2)
regr.fit(X, y)
X_test = np.arange(0.0, 5.0, 1)[:, np.newaxis]
result = regr.predict(X_test)
print(result)
