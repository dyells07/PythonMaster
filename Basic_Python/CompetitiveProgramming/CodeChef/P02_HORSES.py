# There are N horses in the stable. The skill of the horse i is represented by an integer S[i].
# The Chef needs to pick 2 horses for the race such that the difference in their skills is minimum.
# This way, he would be able to host a very interesting race. Your task is to help him do this and
# report the minimum difference that is possible between 2 horses in the race.
#
# Input:
# First line of the input file contains a single integer T, the number of test cases.
# Every test case starts with a line containing the integer N.
# The next line contains N space separated integers where the i-th integer is S[i].
#
# Output:
# For each test case, output a single line containing the minimum difference that is possible.
#
# Constraints:
# 1 ≤ T ≤ 10
# 2 ≤ N ≤ 5000
# 1 ≤ S[i] ≤ 1000000000
#
# Example:
#
# Input:
# 1
# 5
# 4 9 1 32 13
#
# Output:
# 3

# MY SOLUTION:

T = int(input())
for _ in range(T):
    N = int(input())
    S = list(map(int, input().split()))
    S.sort()
    min_diff = float('inf')
    for i in range(N - 1):
        diff = S[i+1] - S[i]
        if diff < min_diff:
            min_diff = diff
    print(min_diff)

# OUTPUT for above solution:
# 1
# 5
# 4 9 1 32 13
# 3
