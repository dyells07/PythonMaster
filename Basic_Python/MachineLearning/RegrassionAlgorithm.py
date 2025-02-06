import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor

# Set random seed for reproducibility
rng = np.random.RandomState(1)

# Generate synthetic dataset
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))  # Adding noise

# Train Decision Tree Regressor
regr = DecisionTreeRegressor(max_depth=3, random_state=0)
regr.fit(X, y)

# Generate test data for predictions
X_test = np.linspace(0, 5, 500).reshape(-1, 1)
y_pred = regr.predict(X_test)

# Plot dataset and predictions
plt.figure(figsize=(8, 6))
plt.scatter(X, y, s=40, edgecolors="black", c="darkorange", label="Training Data", alpha=0.8)
plt.plot(X_test, y_pred, color="royalblue", linewidth=2, label="Decision Tree Prediction")
plt.xlabel("Input Feature")
plt.ylabel("Target Output")
plt.title("Decision Tree Regression (max_depth=3)")
plt.legend()
plt.grid(True, linestyle="--", alpha=0.6)
plt.show()
