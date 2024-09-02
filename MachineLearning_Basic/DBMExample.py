import numpy as np
import tensorflow as tf
from tensorflow.keras import layers, models

class HiddenLayer(tf.keras.layers.Layer):
    def __init__(self, n_in, n_out, activation=tf.nn.sigmoid):
        super(HiddenLayer, self).__init__()
        self.dense = layers.Dense(n_out, activation=activation, input_shape=(n_in,))

    def call(self, inputs):
        return self.dense(inputs)

class RBM(tf.keras.layers.Layer):
    def __init__(self, n_visible=784, n_hidden=500):
        super(RBM, self).__init__()
        self.n_visible = n_visible
        self.n_hidden = n_hidden
        self.W = tf.Variable(tf.random.uniform([n_visible, n_hidden], 
                                               -4 * np.sqrt(6. / (n_hidden + n_visible)), 
                                               4 * np.sqrt(6. / (n_hidden + n_visible))))
        self.hbias = tf.Variable(tf.zeros([n_hidden]))
        self.vbias = tf.Variable(tf.zeros([n_visible]))

    def call(self, inputs):
        hidden_probabilities = tf.nn.sigmoid(tf.matmul(inputs, self.W) + self.hbias)
        return hidden_probabilities

class DBN(tf.keras.Model):
    def __init__(self, n_ins=784, hidden_layers_sizes=[500, 500], n_outs=10):
        super(DBN, self).__init__()
        self.sigmoid_layers = []
        self.rbm_layers = []

        for i, n_hidden in enumerate(hidden_layers_sizes):
            self.sigmoid_layers.append(HiddenLayer(n_in=n_ins if i == 0 else hidden_layers_sizes[i-1], n_out=n_hidden))
            self.rbm_layers.append(RBM(n_visible=n_ins if i == 0 else hidden_layers_sizes[i-1], n_hidden=n_hidden))

        self.logLayer = layers.Dense(n_outs, activation='softmax')

    def call(self, inputs):
        x = inputs
        for layer in self.sigmoid_layers:
            x = layer(x)
        return self.logLayer(x)

    def finetune_cost(self, inputs, labels):
        predictions = self.call(inputs)
        return tf.reduce_mean(tf.nn.sparse_softmax_cross_entropy_with_logits(labels=labels, logits=predictions))

    def errors(self, inputs, labels):
        predictions = self.call(inputs)
        return tf.reduce_mean(tf.cast(tf.not_equal(tf.argmax(predictions, axis=1), labels), tf.float32))

# Instantiate the DBN model
numpy_rng = np.random.RandomState(123)
print('... building the model')
dbn = DBN(n_ins=28 * 28, hidden_layers_sizes=[1000, 800, 720], n_outs=10)

# Example input data (batch_size, 784)
input_data = np.random.rand(10, 28 * 28).astype(np.float32)
labels = np.random.randint(0, 10, size=(10,)).astype(np.int32)

# Forward pass
output = dbn(input_data)
cost = dbn.finetune_cost(input_data, labels)
error_rate = dbn.errors(input_data, labels)

print("Output:", output)
print("Cost:", cost)
print("Error rate:", error_rate)
