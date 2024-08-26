import random
import os
import subprocess
from graphviz import Digraph

class Leaf:
    def __init__(self, guess):
        self.guess = guess

class Node:
    def __init__(self, feature, left, right):
        self.feature = feature
        self.left = left
        self.right = right

def most_frequent_label(data):
    labels = [entry[-1] for entry in data]
    return max(set(labels), key=labels.count)

def split_data(data, feature_index):
    no_subset = [entry for entry in data if entry[feature_index] == 'no']
    yes_subset = [entry for entry in data if entry[feature_index] == 'yes']
    return no_subset, yes_subset

def score_split(no_subset, yes_subset):
    def majority_vote_accuracy(subset):
        if not subset:
            return 0
        majority_label = most_frequent_label(subset)
        return sum(1 for entry in subset if entry[-1] == majority_label)
    
    return majority_vote_accuracy(no_subset) + majority_vote_accuracy(yes_subset)

def DecisionTreeTrain(data, remaining_features):
    guess = most_frequent_label(data)
    
    if len(set(entry[-1] for entry in data)) == 1 or not remaining_features:
        return Leaf(guess)
    
    best_feature = None
    best_score = -1
    
    for f in remaining_features:
        no_subset, yes_subset = split_data(data, f)
        score = score_split(no_subset, yes_subset)
        
        if score > best_score:
            best_score = score
            best_feature = f
            best_no_subset = no_subset
            best_yes_subset = yes_subset
    
    remaining_features = [f for f in remaining_features if f != best_feature]
    left = DecisionTreeTrain(best_no_subset, remaining_features)
    right = DecisionTreeTrain(best_yes_subset, remaining_features)
    
    return Node(best_feature, left, right)

def DecisionTreeTest(tree, test_point):
    if isinstance(tree, Leaf):
        return tree.guess
    elif isinstance(tree, Node):
        feature_index = tree.feature
        if test_point[feature_index] == 'yes':
            return DecisionTreeTest(tree.left, test_point)
        else:
            return DecisionTreeTest(tree.right, test_point)

def render_tree(tree, dot=None, parent_name=None, edge_label=None):
    if dot is None:
        dot = Digraph(comment='Decision Tree')
        dot.attr(size='10,10')

    if isinstance(tree, Leaf):
        dot.node(name=str(id(tree)), label=tree.guess, shape='ellipse', color='lightblue')
        if parent_name:
            dot.edge(parent_name, str(id(tree)), label=edge_label)
    elif isinstance(tree, Node):
        feature_name = f'Feature {tree.feature}'
        dot.node(name=str(id(tree)), label=feature_name, shape='box', color='lightgreen')
        if parent_name:
            dot.edge(parent_name, str(id(tree)), label=edge_label)
        render_tree(tree.left, dot=dot, parent_name=str(id(tree)), edge_label='yes')
        render_tree(tree.right, dot=dot, parent_name=str(id(tree)), edge_label='no')

    return dot

def generate_data(num_rows):
    labels = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
    data = []

    for _ in range(num_rows):
        row = [random.choice(['yes', 'no']) for _ in range(3)]  
        row.append(random.choice(labels))
        data.append(row)

    return data

data = generate_data(100)

remaining_features = [0, 1, 2]

tree = DecisionTreeTrain(data, remaining_features)

dot = render_tree(tree)
file_path = 'decision_tree'
dot.render(file_path, format='png', cleanup=True)
print(f"Decision tree rendered and saved as '{file_path}.png'.")

def open_image(file_path):
    if os.name == 'nt': 
        os.startfile(file_path + '.png')
    elif os.uname().sysname == 'Darwin':
        subprocess.run(['open', file_path + '.png'])
    else: 
        subprocess.run(['xdg-open', file_path + '.png'])

open_image(file_path)

test_point = ['yes', 'no', 'yes']
prediction = DecisionTreeTest(tree, test_point)
print("Prediction:", prediction) 
