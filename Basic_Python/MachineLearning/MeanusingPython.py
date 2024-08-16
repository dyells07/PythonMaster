import numpy as np
import matplotlib.pyplot as plt

speed = [99, 86, 87, 88, 111, 86, 103, 87, 94, 78, 77, 85, 86]

# Calculate the mean using numpy
mean_speed = np.mean(speed)

print("Mean Speed:", mean_speed)

# Plot the data
plt.figure(figsize=(8, 6))
plt.plot(speed, marker='o', linestyle='-', color='b', label='Speed')
plt.axhline(mean_speed, color='r', linestyle='--', label='Mean Speed')
plt.xlabel('Index')
plt.ylabel('Speed')
plt.title('Speed Data and Mean Speed')
plt.legend()
plt.grid(True)
plt.show()
