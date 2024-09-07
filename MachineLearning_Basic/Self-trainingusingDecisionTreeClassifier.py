import numpy as np
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score

# Load the Iris dataset
iris = load_iris()
X = iris.data
y = iris.target

# Introduce some unlabeled data (-1) into the dataset
# Let's say we only know 20% of the labels, and the rest are -1
labeled_ratio = 0.2
rng = np.random.default_rng(seed=42)
random_unlabeled_points = rng.choice(len(y), size=int((1 - labeled_ratio) * len(y)), replace=False)
y[random_unlabeled_points] = -1

# Split the dataset into a training set (with labeled and unlabeled) and a test set
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Define a SelfLearningModel class that applies self-training
class SelfLearningModel:
    def __init__(self, base_model, max_iter=100, prob_threshold=0.8):
        self.model = base_model
        self.max_iter = max_iter
        self.prob_threshold = prob_threshold
    
    def fit(self, X, y):
        labeled_idx = np.where(y != -1)[0]
        unlabeled_idx = np.where(y == -1)[0]
        i = 0

        while i < self.max_iter and len(unlabeled_idx) > 0:
            self.model.fit(X[labeled_idx], y[labeled_idx])
            
            # Predict the labels and probabilities for unlabeled data
            predicted_labels = self.model.predict(X[unlabeled_idx])
            predicted_probs = np.max(self.model.predict_proba(X[unlabeled_idx]), axis=1)
            
            # Select high-confidence predictions
            confident_idx = unlabeled_idx[predicted_probs > self.prob_threshold]
            
            if len(confident_idx) == 0:
                break

            # Update the labels for confident predictions
            y[confident_idx] = predicted_labels[predicted_probs > self.prob_threshold]
            labeled_idx = np.where(y != -1)[0]
            unlabeled_idx = np.where(y == -1)[0]
            
            i += 1
        
        # Fit on the final labeled dataset
        self.model.fit(X[labeled_idx], y[labeled_idx])
    
    def predict(self, X):
        return self.model.predict(X)

# Initialize the base model (Decision Tree Classifier)
base_model = DecisionTreeClassifier()

# Initialize and train the SelfLearningModel
self_training_model = SelfLearningModel(base_model)
self_training_model.fit(X_train, y_train)

# Predict on the test data
y_pred = self_training_model.predict(X_test)

# Calculate the accuracy of the model on test data
test_accuracy = accuracy_score(y_test, y_pred)
print(f"Test Accuracy after Self-Training: {test_accuracy:.2f}")
