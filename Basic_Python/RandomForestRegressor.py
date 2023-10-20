from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_regression
import numpy as np
import matplotlib.pyplot as plt

# Generate random data for demonstration
X, y = make_regression(n_features=4, n_informative=2, random_state=0, shuffle=False)

# Create a RandomForestRegressor and train the model
rfr = RandomForestRegressor(max_depth=3)
rfr.fit(X, y)

# Predict using the trained model
prediction = rfr.predict([[0, 1, 0, 1]])
print("Prediction:", prediction)

# Visualize the importance of features (optional)
feature_importance = rfr.feature_importances_
plt.bar(range(len(feature_importance)), feature_importance, tick_label=['Feature 1', 'Feature 2', 'Feature 3', 'Feature 4'])
plt.xlabel('Features')
plt.ylabel('Importance')
plt.title('Feature Importance')
plt.show()
