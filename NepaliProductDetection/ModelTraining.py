# Import required libraries
import numpy as np
import tensorflow as tf
from tensorflow.keras.applications.resnet50 import ResNet50, preprocess_input
from tensorflow.keras.layers import Dense, GlobalAveragePooling2D
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.image import ImageDataGenerator

# Constants
data_path = './ProductDataSets'  # Folder structure: plant_dataset/train, plant_dataset/test
img_height, img_width = 224, 224
batch_size = 32
num_classes = 5  # Modify according to your dataset

# Data generators with augmentation for training and validation
data_gen = ImageDataGenerator(
    preprocessing_function=preprocess_input,
    validation_split=0.2,
    rotation_range=30,
    horizontal_flip=True,
    zoom_range=0.2,
)

train_generator = data_gen.flow_from_directory(
    data_path + '/train',
    target_size=(img_height, img_width),
    batch_size=batch_size,
    subset='training',
    class_mode='categorical'
)

val_generator = data_gen.flow_from_directory(
    data_path + '/train',
    target_size=(img_height, img_width),
    batch_size=batch_size,
    subset='validation',
    class_mode='categorical'
)

# Load the pre-trained ResNet50 model
base_model = ResNet50(weights='imagenet', include_top=False, input_shape=(img_height, img_width, 3))

# Freeze the base layers
for layer in base_model.layers:
    layer.trainable = False

# Add custom layers for plant disease classification
x = base_model.output
x = GlobalAveragePooling2D()(x)
x = Dense(256, activation='relu')(x)
predictions = Dense(num_classes, activation='softmax')(x)

# Define the full model
model = Model(inputs=base_model.input, outputs=predictions)

# Compile the model
model.compile(optimizer='adam', loss='categorical_crossentropy', metrics=['accuracy'])

# Train the model
model.fit(
    train_generator,
    epochs=10,
    validation_data=val_generator
)

# Save the trained model
model.save('plant_disease_model.h5')

# Example prediction on new images
def predict_disease(image_path):
    from tensorflow.keras.preprocessing import image
    img = image.load_img(image_path, target_size=(img_height, img_width))
    img_array = image.img_to_array(img)
    img_array = np.expand_dims(img_array, axis=0)
    img_array = preprocess_input(img_array)

    prediction = model.predict(img_array)
    class_index = np.argmax(prediction, axis=1)
    class_labels = list(train_generator.class_indices.keys())

    return class_labels[class_index[0]]

# Usage example
result = predict_disease('./example_leaf.jpg')
print('Predicted Disease:', result)
