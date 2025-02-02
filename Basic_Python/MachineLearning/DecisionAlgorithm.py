import numpy as np
import matplotlib.pyplot as plt
from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error

rng = np.random.RandomState(1)
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))

svr = SVR(kernel='rbf', C=10, epsilon=0.1).fit(X, y)

X_test = np.linspace(0, 5, 500)[:, np.newaxis]
predicted_values = svr.predict(X_test)

mse = mean_squared_error(y, svr.predict(X))

plt.figure(figsize=(8, 6))
plt.scatter(X, y, color='darkorange', edgecolors='k', alpha=0.7, label='Noisy Data')
plt.plot(X_test, predicted_values, color='navy', linewidth=2, label='SVR Prediction')

plt.xlabel('Data', fontsize=12)
plt.ylabel('Target', fontsize=12)
plt.title(f'Support Vector Regressor (MSE: {mse:.4f})', fontsize=14)
plt.legend()
plt.grid(True, linestyle='--', alpha=0.6)
plt.show()
