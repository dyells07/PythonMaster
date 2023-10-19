from sklearn.svm import SVR
import numpy as np
import matplotlib.pyplot as plt

# Generate random data
rng = np.random.RandomState(1)
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))

# Create and train the Support Vector Regressor
svr = SVR().fit(X, y)

# Generate test data for prediction
X_test = np.arange(0.0, 5.0, 0.01)[:, np.newaxis]

# Predict using SVR
predicted_values = svr.predict(X_test)

# Plot the data and the SVR predictions
plt.figure(figsize=(8, 6))
plt.scatter(X, y, color='darkorange', label='data')
plt.plot(X_test, predicted_values, color='navy', label='prediction')
plt.xlabel('Data')
plt.ylabel('Target')
plt.title('Support Vector Regressor')
plt.legend()
plt.show()
