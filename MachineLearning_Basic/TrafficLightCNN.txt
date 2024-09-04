import tensorflow as tf
from tensorflow.keras import layers, models
from tensorflow.keras.datasets import cifar10
from tensorflow.keras.utils import to_categorical
import matplotlib.pyplot as plt
import numpy as np
from sklearn.model_selection import train_test_split
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint
import pandas as pd
from tensorflow.keras.preprocessing.image import load_img, img_to_array

# 1. Load and preprocess the CIFAR-10 dataset
(x_train, y_train), (x_test, y_test) = cifar10.load_data()

x_train = x_train.astype('float32') / 255.0
x_test = x_test.astype('float32') / 255.0

y_train = to_categorical(y_train, 10)
y_test = to_categorical(y_test, 10)

x_train, x_val, y_train, y_val = train_test_split(
    x_train, y_train, test_size=0.1, random_state=42
)

print(f"Training samples: {x_train.shape[0]}")
print(f"Validation samples: {x_val.shape[0]}")
print(f"Test samples: {x_test.shape[0]}")

# 2. Define the CNN architecture
def create_cnn_model(input_shape=(32, 32, 3), num_classes=10):
    model = models.Sequential()

    # Convolutional Layer 1
    model.add(layers.Conv2D(32, (5, 5), activation='relu', padding='same', input_shape=input_shape, name='conv2d_1'))
    model.add(layers.BatchNormalization())
    model.add(layers.MaxPooling2D(pool_size=(2, 2)))
    model.add(layers.Dropout(0.25))

    # Convolutional Layer 2
    model.add(layers.Conv2D(64, (5, 5), activation='relu', padding='same', name='conv2d_2'))
    model.add(layers.BatchNormalization())
    model.add(layers.MaxPooling2D(pool_size=(2, 2)))
    model.add(layers.Dropout(0.25))

    # Flatten and Fully Connected Layers
    model.add(layers.Flatten())
    model.add(layers.Dense(500, activation='tanh'))
    model.add(layers.BatchNormalization())
    model.add(layers.Dropout(0.5))

    # Output Layer
    model.add(layers.Dense(num_classes, activation='softmax'))

    return model

model = create_cnn_model()
model.summary()

# 3. Compile the model
model.compile(
    loss='categorical_crossentropy',
    optimizer=tf.keras.optimizers.Adam(),
    metrics=['accuracy']
)

# 4. Train the model
callbacks = [
    EarlyStopping(monitor='val_accuracy', patience=10, verbose=1, restore_best_weights=True),
    ModelCheckpoint('best_cifar10_model.keras', monitor='val_accuracy', save_best_only=True, verbose=1)
]

history = model.fit(
    x_train, y_train,
    epochs=100,
    batch_size=64,
    validation_data=(x_val, y_val),
    callbacks=callbacks,
    verbose=2
)

# 5. Evaluate the model
test_loss, test_accuracy = model.evaluate(x_test, y_test, verbose=0)
print(f"Test Accuracy: {test_accuracy * 100:.2f}%")

# 6. Visualize training history
plt.figure(figsize=(12,5))

plt.subplot(1,2,1)
plt.plot(history.history['accuracy'], label='Train Accuracy')
plt.plot(history.history['val_accuracy'], label='Validation Accuracy')
plt.title('Model Accuracy')
plt.ylabel('Accuracy')
plt.xlabel('Epoch')
plt.legend(loc='lower right')

plt.subplot(1,2,2)
plt.plot(history.history['loss'], label='Train Loss')
plt.plot(history.history['val_loss'], label='Validation Loss')
plt.title('Model Loss')
plt.ylabel('Loss')
plt.xlabel('Epoch')
plt.legend(loc='upper right')

plt.show()

# 7. Visualize convolutional filters
def visualize_filters(model, layer_name):
    # Get the weights of the specified layer
    filters, biases = model.get_layer(name=layer_name).get_weights()
    print(f"Shape of filters: {filters.shape}")

    # Normalize filter values to 0-1 for visualization
    f_min, f_max = filters.min(), filters.max()
    filters = (filters - f_min) / (f_max - f_min)

    n_filters = filters.shape[3]
    n_columns = 8
    n_rows = n_filters // n_columns + 1

    plt.figure(figsize=(n_columns, n_rows))
    for i in range(n_filters):
        # Get the filter
        f = filters[:, :, :, i]
        # Plot each channel separately
        for j in range(3):
            ax = plt.subplot(n_rows, n_columns*3, i*3 + j +1)
            ax.set_xticks([])
            ax.set_yticks([])
            plt.imshow(f[:, :, j], cmap='viridis')
    plt.show()

# Visualize filters of the first convolutional layer
visualize_filters(model, 'conv2d_1')

# 8. Make predictions and compare with ground truth
predictions = model.predict(x_test)
predicted_classes = np.argmax(predictions, axis=1)
true_classes = np.argmax(y_test, axis=1)

def display_predictions(x, y_true, y_pred, class_names, num=10):
    plt.figure(figsize=(15, 5))
    for i in range(num):
        plt.subplot(2, 5, i+1)
        plt.imshow(x[i])
        plt.title(f"True: {class_names[y_true[i]]}\nPred: {class_names[y_pred[i]]}")
        plt.axis('off')
    plt.show()

class_names = ['airplane','automobile','bird','cat','deer',
               'dog','frog','horse','ship','truck']

display_predictions(x_test, true_classes, predicted_classes, class_names)
