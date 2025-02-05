# Author: Bipin Khanal

import numpy as np

# Define angles in degrees
angles = np.array([0, 30, 45, 60, 90, 180, 360])

# Convert angles to radians
radians = np.radians(angles)

# Compute trigonometric functions
sine = np.sin(radians)
cosine = np.cos(radians)
tangent = np.tan(radians)

# Print results
print("Sine values:", sine)
print("Cosine values:", cosine)
print("Tangent values:", tangent)

# Compute inverse sine and convert to degrees
sine_inv = np.degrees(np.arcsin(sine))
print("Inverse Sine (in degrees):", sine_inv)

# Rounding operations
print("Rounded sine values:", np.round(sine, 4))
print("Floor values:", np.floor(sine))
print("Ceil values:", np.ceil(sine))
