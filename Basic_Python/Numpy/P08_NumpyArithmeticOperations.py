# Author: Bipin Khanal

import numpy as np

# Create two NumPy arrays
first_array = np.arange(12).reshape(3, 4)
second_array = np.arange(4)

print("First Array:\n", first_array)
print("Second Array:\n", second_array)

# Perform arithmetic operations (NumPy broadcasting applies)
print("Addition:\n", np.add(first_array, second_array))
print("Subtraction:\n", np.subtract(first_array, second_array))
print("Multiplication:\n", np.multiply(first_array, second_array))

# Handle division safely (avoid division by zero warnings)
np.seterr(divide='ignore', invalid='ignore')
division_result = np.divide(first_array, second_array, where=second_array != 0)
print("Division:\n", division_result)

# Using NumPy power function efficiently
array = np.array([1, 2, 3])
print("Exponentiation (Squared Values):", np.power(array, 2))
