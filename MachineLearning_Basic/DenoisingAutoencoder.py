import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt

class DenoisingAutoencoder(tf.keras.Model):
    def __init__(self, n_visible, n_hidden):
        super(DenoisingAutoencoder, self).__init__()
        self.n_visible = n_visible
        self.n_hidden = n_hidden

        # Initialize weights and biases
        self.W = tf.Variable(tf.random.uniform(
            [self.n_visible, self.n_hidden],
            minval=-4 * np.sqrt(6. / (self.n_hidden + self.n_visible)),
            maxval=4 * np.sqrt(6. / (self.n_hidden + self.n_visible)),
            dtype=tf.float32
        ))

        self.b_hidden = tf.Variable(tf.zeros([self.n_hidden], dtype=tf.float32))
        self.b_visible = tf.Variable(tf.zeros([self.n_visible], dtype=tf.float32))

    def get_corrupted_input(self, input, corruption_level):
        return tf.nn.dropout(input, rate=corruption_level)

    def get_hidden_values(self, input):
        return tf.nn.sigmoid(tf.matmul(input, self.W) + self.b_hidden)

    def get_reconstructed_input(self, hidden):
        W_prime = tf.transpose(self.W)
        return tf.nn.sigmoid(tf.matmul(hidden, W_prime) + self.b_visible)

    def get_cost(self, input, corruption_level):
        # Corrupt the input
        tilde_x = self.get_corrupted_input(input, corruption_level)
        
        # Forward pass to hidden layer
        hidden = self.get_hidden_values(tilde_x)
        
        # Reconstruct the input
        z = self.get_reconstructed_input(hidden)
        
        # Compute the reconstruction error
        cost = tf.reduce_mean(0.5 * tf.square(z - input))
        return cost

# Example usage
def train_denoising_autoencoder():
    # Hyperparameters
    n_visible = 784  # For example, this could be for MNIST images
    n_hidden = 500
    learning_rate = 0.001
    corruption_level = 0.3
    epochs = 50
    batch_size = 128

    # Instantiate the model
    model = DenoisingAutoencoder(n_visible, n_hidden)

    # Prepare the dataset (e.g., MNIST)
    (x_train, _), (x_test, _) = tf.keras.datasets.mnist.load_data()
    x_train = x_train.reshape(-1, 784).astype(np.float32) / 255.0
    x_test = x_test.reshape(-1, 784).astype(np.float32) / 255.0

    # Define the optimizer
    optimizer = tf.optimizers.Adam(learning_rate)

    # List to store loss values for plotting
    loss_values = []

    # Training loop
    for epoch in range(epochs):
        epoch_loss = 0
        for i in range(0, len(x_train), batch_size):
            batch_x = x_train[i:i+batch_size]

            with tf.GradientTape() as tape:
                cost = model.get_cost(batch_x, corruption_level)

            # Compute and apply gradients
            gradients = tape.gradient(cost, model.trainable_variables)
            if gradients:  # Ensure gradients are not empty
                optimizer.apply_gradients(zip(gradients, model.trainable_variables))

            epoch_loss += cost.numpy()

        avg_loss = epoch_loss / (len(x_train) // batch_size)
        loss_values.append(avg_loss)
        print(f'Epoch {epoch + 1}/{epochs}, Loss: {avg_loss}')

    # Plotting the loss over epochs
    plt.figure(figsize=(10, 6))
    plt.plot(range(epochs), loss_values, marker='o', linestyle='-', color='b')
    plt.title('Training Loss Over Epochs')
    plt.xlabel('Epoch')
    plt.ylabel('Loss')
    plt.grid(True)
    plt.show()

# Call the training function
train_denoising_autoencoder()
