import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor

# Generate random data
rng = np.random.RandomState(1)
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))

# Create and train the Decision Tree Regressor
regr = DecisionTreeRegressor(max_depth=2)
regr.fit(X, y)

# Generate test data for prediction
X_test = np.arange(0.0, 5.0, 0.01)[:, np.newaxis]
y_pred = regr.predict(X_test)

# Plot the data and the regression predictions
plt.figure(figsize=(8, 6))
plt.scatter(X, y, s=20, edgecolor="black", c="darkorange", label="data")
plt.plot(X_test, y_pred, color="cornflowerblue", label="prediction")
plt.xlabel("Data")
plt.ylabel("Target")
plt.title("Decision Tree Regressor")
plt.legend()
plt.show()
