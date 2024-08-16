from sklearn import linear_model
import numpy as np
import matplotlib.pyplot as plt

rng = np.random.RandomState(1)
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))

lassoReg = linear_model.Lasso(alpha=0.1)
lassoReg.fit(X, y)

X_test = np.arange(0.0, 5.0, 1)[:, np.newaxis]
predicted_y = lassoReg.predict(X_test)

plt.figure(figsize=(8, 6))
plt.scatter(X, y, color='darkorange', label='data')
plt.plot(X_test, predicted_y, color='navy', label='prediction')
plt.xlabel('Data')
plt.ylabel('Target')
plt.title('Lasso Regression')
plt.legend()
plt.show()
