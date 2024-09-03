import tensorflow as tf
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.datasets import mnist
import matplotlib.pyplot as plt

class StackedDenoisingAutoencoder:
    def __init__(self, input_dim, hidden_layers_sizes, dropout_rate=0.2):
        self.input_dim = input_dim
        self.hidden_layers_sizes = hidden_layers_sizes
        self.dropout_rate = dropout_rate
        self.build_model()

    def build_model(self):
        self.model = tf.keras.Sequential()

        # Encoder layers
        for units in self.hidden_layers_sizes:
            self.model.add(Dense(units, activation='sigmoid'))
            self.model.add(Dropout(self.dropout_rate))

        # Decoder layers (reverse the encoding process)
        for units in reversed(self.hidden_layers_sizes[:-1]):
            self.model.add(Dense(units, activation='sigmoid'))

        self.model.add(Dense(self.input_dim, activation='sigmoid'))

    def compile(self, learning_rate=0.001):
        self.model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate),
                           loss='mean_squared_error')

    def train(self, x_train, x_val, epochs=50, batch_size=256):
        history = self.model.fit(x_train, x_train,
                                 validation_data=(x_val, x_val),
                                 epochs=epochs,
                                 batch_size=batch_size,
                                 shuffle=True)
        return history

    def get_encoded_data(self, x):
        encoder = tf.keras.Model(inputs=self.model.input, outputs=self.model.layers[len(self.hidden_layers_sizes) - 1].output)
        return encoder.predict(x)

# Load and preprocess data
(x_train, _), (x_test, _) = mnist.load_data()
x_train = x_train.reshape(-1, 784) / 255.0
x_test = x_test.reshape(-1, 784) / 255.0

# Split the data into training and validation sets
x_val = x_train[-10000:]
x_train = x_train[:-10000]

# Initialize and train the SdA
sda = StackedDenoisingAutoencoder(input_dim=784, hidden_layers_sizes=[512, 256, 128], dropout_rate=0.2)
sda.compile(learning_rate=0.001)
history = sda.train(x_train, x_val, epochs=50, batch_size=256)

# Plot the training and validation loss
plt.plot(history.history['loss'], label='Training Loss')
plt.plot(history.history['val_loss'], label='Validation Loss')
plt.title('SdA Training and Validation Loss')
plt.xlabel('Epochs')
plt.ylabel('Loss')
plt.legend()
plt.show()

# Encode and decode some digits
encoded_imgs = sda.get_encoded_data(x_test[:10])

# Display the original and reconstructed images
n = 10  # Number of digits to display
plt.figure(figsize=(20, 4))
for i in range(n):
    # Display original
    ax = plt.subplot(2, n, i + 1)
    plt.imshow(x_test[i].reshape(28, 28))
    plt.gray()
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)

    # Display reconstructed (encoded then decoded)
    ax = plt.subplot(2, n, i + 1 + n)
    plt.imshow(encoded_imgs[i].reshape(28, 28))
    plt.gray()
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
plt.show()
