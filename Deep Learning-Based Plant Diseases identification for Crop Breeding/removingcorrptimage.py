import os
from PIL import Image

def remove_corrupted_images(directory):
    removed_count = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            filepath = os.path.join(root, file)
            try:
                with Image.open(filepath) as img:
                    img.verify()  # verify image integrity
            except (IOError, SyntaxError, Image.UnidentifiedImageError):
                print(f'Removing corrupted file: {filepath}')
                os.remove(filepath)
                removed_count += 1
    print(f'Total corrupted files removed: {removed_count}')

# Apply this to your dataset directories
remove_corrupted_images('./plant_dataset/train/tomato_leaf_disease_images')
remove_corrupted_images('./plant_dataset/test/tomato_leaf_disease_images')
