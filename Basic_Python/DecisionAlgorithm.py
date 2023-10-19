from sklearn.svm import SVR
import numpy as np
rng = np.random.RandomState(1)
X = np.sort(5 * rng.rand(80, 1), axis=0)
y = np.sin(X).ravel()
y[::5] += 3 * (0.5 - rng.rand(16))
svr = SVR().fit(X, y)
X_test = np.arange(0.0, 5.0, 1)[:, np.newaxis]
svr.predict(X_test)