import numpy as np
import sklearn.metrics
import scipy.stats
import nlopt
import random
from sklearn.base import BaseEstimator
from sklearn.linear_model import SGDClassifier
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Define a simple SelfLearningModel for comparison
class SelfLearningModel(BaseEstimator):
    def __init__(self, basemodel, threshold=0.5, max_iter=10, verbose=0):
        self.model = basemodel
        self.threshold = threshold
        self.max_iter = max_iter
        self.verbose = verbose
    
    def fit(self, X, y):
        self.model.fit(X[y != -1], y[y != -1])
        
        for iteration in range(self.max_iter):
            probs = self.model.predict_proba(X[y == -1])[:,1]
            pseudo_labels = (probs >= self.threshold).astype(int)
            X_combined = np.vstack((X[y != -1], X[y == -1]))
            y_combined = np.hstack((y[y != -1], pseudo_labels))
            self.model.fit(X_combined, y_combined)
            if self.verbose:
                acc = self.model.score(X, y)
                print(f"Iteration {iteration+1}, Accuracy: {acc:.4f}")
        return self
    
    def predict(self, X):
        return self.model.predict(X)
    
    def score(self, X, y):
        return accuracy_score(y, self.predict(X))

# Define the CPLELearningModel adjusted for Python 3
class CPLELearningModel(BaseEstimator):
    def __init__(self, basemodel, pessimistic=True, predict_from_probabilities=False,
                 use_sample_weighting=True, max_iter=3000, verbose=1):
        self.model = basemodel
        self.pessimistic = pessimistic
        self.predict_from_probabilities = predict_from_probabilities
        self.use_sample_weighting = use_sample_weighting
        self.max_iter = max_iter
        self.verbose = verbose
        self.bestdl = np.inf
        self.bestlbls = []
        self.it = 0
        self.noimprovementsince = 0
        self.maxnoimprovementsince = 3
        self.buffersize = 200
        self.lastdls = [0] * self.buffersize
    
    def discriminative_likelihood(self, model, labeledData, labeledy=None,
                                  unlabeledData=None, unlabeledWeights=None,
                                  unlabeledlambda=1, gradient=[], alpha=0.01):
        unlabeledy = (unlabeledWeights[:, 0] < 0.5).astype(int)
        uweights = np.copy(unlabeledWeights[:, 0])
        uweights[unlabeledy == 1] = 1 - uweights[unlabeledy == 1]
        weights = np.hstack((np.ones(len(labeledy)), uweights))
        labels = np.hstack((labeledy, unlabeledy))
        
        if self.use_sample_weighting:
            model.fit(np.vstack((labeledData, unlabeledData)), labels, sample_weight=weights)
        else:
            model.fit(np.vstack((labeledData, unlabeledData)), labels)
        
        P = model.predict_proba(labeledData)
        labeledDL = -sklearn.metrics.log_loss(labeledy, P, labels=[0,1])
        
        unlabeledP = model.predict_proba(unlabeledData)
        eps = 1e-15
        unlabeledP = np.clip(unlabeledP, eps, 1 - eps)
        unlabeledDL = np.average((unlabeledWeights[:,0] * (1 - unlabeledy) * np.log(unlabeledP[:,0]) +
                                  unlabeledWeights[:,0] * unlabeledy * np.log(unlabeledP[:,1])))
        
        return unlabeledDL + labeledDL  # Return a single scalar value
    
    def fit(self, labeledX, labeledy, unlabeledX):
        soft_labels = np.random.random(len(unlabeledX))
        
        def f(softlabels, grad):
            softlabels_np = np.array(softlabels)
            unlabeledWeights = np.vstack((softlabels_np, 1 - softlabels_np)).T
            dl_total = self.discriminative_likelihood(self.model, labeledX, labeledy, 
                                                      unlabeledX, unlabeledWeights)
            return dl_total
        
        opt = nlopt.opt(nlopt.GN_DIRECT_L_RAND, len(unlabeledX))
        opt.set_lower_bounds([0.0] * len(unlabeledX))
        opt.set_upper_bounds([1.0] * len(unlabeledX))
        opt.set_min_objective(f)
        opt.set_maxeval(self.max_iter)
        
        # Ensure the correct size for soft_labels
        print(f"soft_labels size: {len(soft_labels)}, unlabeledX size: {len(unlabeledX)}")
        
        self.bestsoftlbl = opt.optimize(soft_labels)
        
        unlabeledy = (self.bestsoftlbl < 0.5).astype(int)
        uweights = np.copy(self.bestsoftlbl)
        uweights[unlabeledy == 1] = 1 - uweights[unlabeledy == 1]
        weights = np.hstack((np.ones(len(labeledy)), uweights))
        labels = np.hstack((labeledy, unlabeledy))
        
        if self.use_sample_weighting:
            self.model.fit(np.vstack((labeledX, unlabeledX)), labels, sample_weight=weights)
        else:
            self.model.fit(np.vstack((labeledX, unlabeledX)), labels)
        
        return self
    
    def predict(self, X):
        return self.model.predict(X)
    
    def score(self, X, y):
        return accuracy_score(y, self.predict(X))

# Main function to demonstrate the CPLE implementation
def main():
    np.random.seed(42)
    random.seed(42)
    
    X, y = make_classification(n_samples=10000, n_features=20, n_classes=2, 
                               n_informative=15, n_redundant=5, random_state=42)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    labeled_N = 100
    labeled_indices = []
    for class_label in np.unique(y_train):
        class_indices = np.where(y_train == class_label)[0]
        labeled_indices += list(random.sample(list(class_indices), labeled_N // 2))
    
    unlabeled_indices = np.setdiff1d(np.arange(len(y_train)), labeled_indices)
    X_labeled = X_train[labeled_indices]
    y_labeled = y_train[labeled_indices]
    X_unlabeled = X_train[unlabeled_indices]
    y_unlabeled = np.array([-1] * len(X_unlabeled))
    
    basemodel = SGDClassifier(loss='log_loss', penalty='l1', max_iter=1000, tol=1e-3, random_state=42)
    
    # Supervised model
    supervised_model = SGDClassifier(loss='log_loss', penalty='l1', max_iter=1000, tol=1e-3, random_state=42)
    supervised_model.fit(X_labeled, y_labeled)
    supervised_score = supervised_model.score(X_test, y_test)
    print(f"Supervised SGDClassifier score: {supervised_score:.4f}")
    
    # Self-learning model
    self_learning_model = SelfLearningModel(
        basemodel=SGDClassifier(loss='log_loss', penalty='l1', max_iter=1000, tol=1e-3, random_state=42),
        threshold=0.5,
        max_iter=10,
        verbose=0
    )
    y_combined = np.array([-1] * len(y_train))
    y_combined[labeled_indices] = y_labeled
    self_learning_model.fit(X_train, y_combined)
    self_learning_score = self_learning_model.score(X_test, y_test)
    print(f"Self-learning SGDClassifier score: {self_learning_score:.4f}")
    
    # CPLE model
    cple_base_model = SGDClassifier(loss='log_loss', penalty='l1', max_iter=1000, tol=1e-3, random_state=42)
    cple_model = CPLELearningModel(basemodel=cple_base_model, verbose=1)
    cple_model.fit(X_labeled, y_labeled, X_unlabeled)
    cple_score = cple_model.score(X_test, y_test)
    print(f"CPLE semi-supervised SGDClassifier score: {cple_score:.4f}")

if __name__ == "__main__":
    main()
