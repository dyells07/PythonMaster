import numpy as np
import matplotlib.pyplot as plt
import time
import os

def load_data(fname):
    """ Load data from a CSV file and prepare it for gradient descent. """
    if not os.path.exists(fname):
        raise FileNotFoundError(f"Error: File '{fname}' not found. Check the file path.")
    
    points = np.loadtxt(fname, delimiter=',')
    X = np.c_[points[:, 0], np.ones(len(points))]  # Add bias column (intercept)
    y = points[:, 1]
    
    print(f"✅ Data loaded: X shape = {X.shape}, y shape = {y.shape}")
    return X, y

def evaluate_cost(X, y, params):
    """ Compute Mean Squared Error (MSE) cost function using vectorization. """
    errors = y - X @ params  # Vectorized error computation
    return np.mean(errors ** 2) / 2  # Normalized cost for consistency

def evaluate_gradient(X, y, params):
    """ Compute gradient of the cost function w.r.t. parameters using vectorization. """
    errors = y - X @ params
    return -X.T @ errors / len(y)  # Vectorized gradient computation

def gradient_descent(X, y, init_params, alpha=0.01, max_iters=10000, tolerance=1e-7):
    """ Perform optimized gradient descent with adaptive learning rate. """
    params = np.array(init_params, dtype=np.float64)
    cost_history = []
    prev_cost = float('inf')
    start_time = time.time()
    
    for i in range(max_iters):
        cost = evaluate_cost(X, y, params)
        gradient = evaluate_gradient(X, y, params)
        
        # Check convergence
        if abs(prev_cost - cost) < tolerance:
            break
        
        # Adaptive learning rate decay if divergence occurs
        if cost > prev_cost:
            alpha *= 0.9  # Reduce learning rate if cost increases
        
        # Update parameters
        params -= alpha * gradient
        cost_history.append(cost)
        prev_cost = cost
        
        # Print updates every 100 iterations
        if i % 100 == 0:
            print(f"Iteration {i:5d} | Cost: {cost:.6f} | Params: {params}")

    elapsed_time = time.time() - start_time
    print(f"\n🚀 Optimization complete in {elapsed_time:.4f} seconds")
    print(f"🔹 Final cost: {prev_cost:.6f}")
    print(f"🔹 Optimal parameters: {params}\n")
    
    return params, cost_history

# Set up paths dynamically
script_dir = os.path.dirname(os.path.abspath(__file__))
data_file = os.path.join(script_dir, "data", "1_points.csv")

# Initialize parameters
init_params = [4.5, 2.0]
alpha = 0.05  # Adjusted for stability

# Load data
try:
    X, y = load_data(data_file)
    # Run gradient descent
    optimal_params, cost_history = gradient_descent(X, y, init_params, alpha)
    
    # Plot cost history
    plt.figure(figsize=(8, 5))
    plt.plot(cost_history, label="Cost over iterations", color="b", linewidth=2)
    plt.xlabel("Iterations")
    plt.ylabel("Cost")
    plt.title("Gradient Descent Cost Reduction")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.show()
except FileNotFoundError as e:
    print(e)
