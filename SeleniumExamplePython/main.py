from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import chromedriver_autoinstaller
import time
import pyperclip
import sys
import os
import platform

# Function to get the default Chrome profile path
def get_chrome_profile_path():
    os_name = platform.system()
    if os_name == "Windows":
        return "--user-data-dir=" + os.path.join(os.getenv("LOCALAPPDATA"), "Google", "Chrome", "User Data")
    elif os_name == "Darwin":  # macOS
        return "--user-data-dir=" + os.path.expanduser("~/Library/Application Support/Google/Chrome")
    elif os_name == "Linux":
        return "--user-data-dir=" + os.path.expanduser("~/.config/google-chrome")
    else:
        raise Exception("Unsupported operating system: " + os_name)

# Install ChromeDriver automatically
chromedriver_autoinstaller.install()

# Set Chrome options
options = webdriver.ChromeOptions()
options.add_argument(get_chrome_profile_path())  # Add the dynamically detected Chrome profile path
driver = webdriver.Chrome(options=options)
driver.maximize_window()

# Open WhatsApp Web
driver.get("https://web.whatsapp.com/")
wait = WebDriverWait(driver, 300)

# Read group names from the file (use a default if not passed as argument)
groups = []
try:
    if len(sys.argv) > 1:
        with open(sys.argv[1], 'r', encoding='utf8') as f:
            groups = [group.strip() for group in f.readlines()]
    else:
        print("No groups file provided, using default group.")
        groups = ['Testing_500']  # Default to Testing_500 group if no file is provided
except FileNotFoundError:
    print("File not found, using default group.")
    groups = ['Testing_500']  # Default to Testing_500 group if file is missing

# Read the message content from the file
with open('msg.txt', 'r', encoding='utf8') as f:
    msg = f.read()

# Iterate through the groups and send messages
for index, item in enumerate(groups):
    try:
        search_xpath = '//div[@contenteditable="true"][@data-tab="3"]'
        if index > 0:
            search_box = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, search_xpath)))
        else:
            search_box = wait.until(EC.presence_of_element_located((By.XPATH, search_xpath)))
        pyperclip.copy(item)
        search_box.clear()
        search_box.send_keys(Keys.CONTROL + "v")
        time.sleep(3)

        x_arg = f'//span[@title="{item}"]'
        group_title = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, x_arg)))
        group_title.click()
        time.sleep(3)

        inp_xpath = '//div[@contenteditable="true"][@data-tab="10"]'
        input_box = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, inp_xpath)))

        pyperclip.copy(msg)
        input_box.clear()
        input_box.send_keys(Keys.CONTROL + "v")
        time.sleep(2)
        input_box.send_keys(Keys.ENTER)
        time.sleep(1)

        # Attach files if provided
        try:
            if len(sys.argv) > 2:
                attachment_box = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@title = "Attach"]')))
                attachment_box.click()
                time.sleep(1)

                image_box = WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, '//input[@accept="image/*,video/mp4,video/3gpp,video/quicktime"]')))
                image_box.send_keys(sys.argv[2])
                time.sleep(2)

                send_button = WebDriverWait(driver, 5).until(EC.presence_of_element_located(
                    (By.XPATH, '//span[@data-icon="send"]')))
                send_button.click()
                time.sleep(2)
        except IndexError:
            pass
    except Exception as e:
        print(e)
        continue
