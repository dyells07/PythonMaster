from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
from sklearn import metrics

def naive_bayes_classifier(X_train, y_train, X_test, y_test):
    vectorizer = CountVectorizer()
    X_train_counts = vectorizer.fit_transform(X_train)
    X_test_counts = vectorizer.transform(X_test)

    classifier = MultinomialNB()
    classifier.fit(X_train_counts, y_train)

    y_pred = classifier.predict(X_test_counts)

    print("Classification Report:")
    print(metrics.classification_report(y_test, y_pred))

    print("Confusion Matrix:")
    print(metrics.confusion_matrix(y_test, y_pred))

# Example usage:
X = ["document1 text", "document2 text", "document3 text"]
y = ["ham", "spam", "ham"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

naive_bayes_classifier(X_train, y_train, X_test, y_test)
