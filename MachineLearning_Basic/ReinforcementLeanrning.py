import numpy as np
import gym
import random

env = gym.make('Taxi-v3')

Q = np.zeros([env.observation_space.n, env.action_space.n])

learning_rate = 0.1
discount_factor = 0.99
exploration_rate = 1.0
exploration_decay = 0.995
min_exploration_rate = 0.01
episodes = 1000
max_steps_per_episode = 100

def choose_action(state):
    if random.uniform(0, 1) < exploration_rate:
        return env.action_space.sample()
    else:
        return np.argmax(Q[state])

def q_learning():
    global exploration_rate

    for episode in range(episodes):
        state = env.reset()
        total_reward = 0

        for _ in range(max_steps_per_episode):
            action = choose_action(state)
            next_state, reward, done, _ = env.step(action)

            best_next_action = np.argmax(Q[next_state])
            td_target = reward + discount_factor * Q[next_state][best_next_action]
            td_error = td_target - Q[state][action]
            Q[state][action] += learning_rate * td_error

            state = next_state
            total_reward += reward

            if done:
                break

        exploration_rate = max(min_exploration_rate, exploration_rate * exploration_decay)

        print(f'Episode {episode + 1}/{episodes} - Total Reward: {total_reward}')

if __name__ == "__main__":
    q_learning()
