import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestRegressor
from sklearn.datasets import make_regression

# Generate synthetic regression data
X, y = make_regression(n_features=4, n_informative=2, random_state=0, shuffle=False)

# Initialize and train RandomForestRegressor
rfr = RandomForestRegressor(n_estimators=100, max_depth=3, random_state=0)
rfr.fit(X, y)

# Make a prediction
sample_input = np.array([[0, 1, 0, 1]])
prediction = rfr.predict(sample_input)
print("Prediction:", prediction[0])

# Feature importance visualization
feature_importance = rfr.feature_importances_
feature_labels = [f'Feature {i+1}' for i in range(len(feature_importance))]

plt.figure(figsize=(6, 4))
plt.bar(feature_labels, feature_importance, color='skyblue', edgecolor='black')
plt.xlabel('Features')
plt.ylabel('Importance')
plt.title('Feature Importance in RandomForest')
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()
