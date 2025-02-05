# Author: Bipin Khanal

import numpy as np

abc = ['abc']
xyz = ['xyz']

# String concatenation
print(np.char.add(abc, xyz))    # ['abcxyz']
print(np.char.add(abc, 'pqr'))  # ['abcpqr']

# String multiplication
print(np.char.multiply(abc, 3)) # ['abcabcabc']

# Centering a string with fill characters
print(np.char.center(abc, 20, fillchar='*'))  # ['********abc*********']

# Capitalizing first letter
print(np.char.capitalize('hello world'))        # Hello world

# Title case
print(np.char.title('hello how are you?'))      # Hello How Are You?

# Convert to lowercase
print(np.char.lower(['HELLO', 'WORLD']))        # ['hello' 'world']

# Convert to uppercase
print(np.char.upper('hello'))                   # HELLO

# Splitting a string
print(np.char.split('BIPIN KHANAL'))            # ['BIPIN', 'KHANAL']
print(np.char.split('2017-02-11', sep='-'))     # ['2017', '02', '11']

# Optimized way to join characters in a string
print(':'.join('dmy'))                          # d:m:y

# Handling an array of strings efficiently
arr = np.array(['dmy', 'ymd', 'mdy'])
print(np.array([':'.join(s) for s in arr]))  
# Output: ['d:m:y' 'y:m:d' 'm:d:y']
