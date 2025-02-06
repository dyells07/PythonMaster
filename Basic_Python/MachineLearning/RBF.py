import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.decomposition import KernelPCA
from sklearn.datasets import make_regression

# Set random seed for reproducibility
rng = np.random.RandomState(1)

# --- Linear Regression ---
print("Linear Regression Example:")
X_linear = np.sort(5 * rng.rand(80, 1), axis=0)
y_linear = np.sin(X_linear).ravel()
y_linear[::5] += 3 * (0.5 - rng.rand(16))
linear_reg = LinearRegression().fit(X_linear, y_linear)
linear_prediction = linear_reg.predict(X_linear)
print(linear_prediction)

# --- Decision Tree Regressor ---
print("\nDecision Tree Regressor Example:")
X_tree = np.sort(5 * rng.rand(80, 1), axis=0)
y_tree = np.sin(X_tree).ravel()
y_tree[::5] += 3 * (0.5 - rng.rand(16))
tree_regressor = DecisionTreeRegressor(max_depth=2).fit(X_tree, y_tree)
tree_prediction = tree_regressor.predict(X_tree)
print(tree_prediction)

# --- Support Vector Regressor ---
print("\nSupport Vector Regressor Example:")
X_svr = np.sort(5 * rng.rand(80, 1), axis=0)
y_svr = np.sin(X_svr).ravel()
y_svr[::5] += 3 * (0.5 - rng.rand(16))
svr = SVR().fit(X_svr, y_svr)
svr_prediction = svr.predict(X_svr)
print(svr_prediction)

# --- Random Forest Regressor ---
print("\nRandom Forest Regressor Example:")
X_rf, y_rf = make_regression(n_features=4, n_informative=2, random_state=0, shuffle=False)
random_forest_regressor = RandomForestRegressor(n_estimators=100, max_depth=3, random_state=0)
random_forest_regressor.fit(X_rf, y_rf)
rf_prediction = random_forest_regressor.predict([[0, 1, 0, 1]])
print(rf_prediction)

# --- Kernel PCA ---
print("\nKernel PCA Example:")
X_kpca, y_kpca = make_regression(n_features=4, n_informative=2, random_state=0, shuffle=False)
kpca = KernelPCA(kernel='rbf', gamma=15).fit(X_kpca)
X_kpca_transformed = kpca.transform(X_kpca)

# Plot Kernel PCA results
plt.figure(figsize=(6, 5))
plt.scatter(X_kpca_transformed[:, 0], X_kpca_transformed[:, 1], c=y_kpca, cmap='viridis', edgecolors='k')
plt.colorbar(label="Target Value")
plt.xlabel("Principal Component 1")
plt.ylabel("Principal Component 2")
plt.title("Kernel PCA Visualization")
plt.grid(True, linestyle="--", alpha=0.6)
plt.show()
