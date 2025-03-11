import os
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Settings
search_query = "Singing Bowls"
num_images = 100
output_dir = "Singing_Bowls_Paintings"

# Create output directory
os.makedirs(output_dir, exist_ok=True)

# Selenium driver setup (CORRECTED)
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service)
driver.get(f'https://www.google.com/search?tbm=isch&q={search_query}')
# Scroll to load images
last_height = driver.execute_script("return document.body.scrollHeight")
while True:
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height or len(driver.find_elements(By.TAG_NAME, 'img')) > num_images + 20:
        break
    last_height = new_height

# Parse images
soup = BeautifulSoup(driver.page_source, 'html.parser')
img_tags = soup.find_all("img")

print(f"Found {len(img_tags)} images, downloading...")

# Download images
count = 0
for img_tag in img_tags[1:]:
    img_url = img_tag.get('src') or img_tag.get('data-src')
    if not img_url or "http" not in img_url:
        continue
    try:
        img_data = requests.get(img_url).content
        with open(f"{output_dir}/image_{count + 1}.jpg", 'wb') as f:
            f.write(img_data)
        count += 1
        print(f"Downloaded image {count}")
        if count >= num_images:
            break
    except Exception as e:
        print(f"Error downloading image {count + 1}: {e}")

# Close browser
driver.quit()

print(f"Downloaded {count} images successfully!")
