import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from sklearn.datasets import load_digits
from sklearn.decomposition import PCA

# Load the digits dataset
digits = load_digits()
data = digits.data
labels = digits.target

# Apply PCA to reduce the dimensionality
pca = PCA(n_components=10)
data_r = pca.fit_transform(data)

# Create a vector of colors for plotting
colors = cm.rainbow(np.linspace(0, 1, len(np.unique(labels))))

# Create the scatter plot
plt.figure(figsize=(8, 6))
for c, i in zip(colors, np.unique(labels)):
    plt.scatter(data_r[labels == i, 0], data_r[labels == i, 1], c=[c], label=f'Digit {i}', alpha=0.4)

plt.legend()
plt.title('Scatterplot of Points plotted in first 10 Principal Components')
plt.xlabel('Principal Component 1')
plt.ylabel('Principal Component 2')
plt.show()
