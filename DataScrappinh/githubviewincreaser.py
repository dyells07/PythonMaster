from selenium import webdriver
from multiprocessing import Pool, cpu_count
import time

# URL to open
url = "https://github.com/dyells07"

# Total number of requests
num_requests = 10000

# Number of parallel browser instances (adjust based on CPU cores)
num_processes = min(cpu_count(), 10)  # Use up to 10 processes or max CPU cores

# Number of requests per process
requests_per_process = num_requests // num_processes


def open_url(times):
    """ Opens the URL multiple times using Selenium. """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Runs without UI
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=options)  # Initialize WebDriver

    for i in range(times):
        driver.get(url)
        print(f"Opened {url} - Process {i+1}/{times}")

    driver.quit()  # Close browser after requests


if __name__ == "__main__":
    start_time = time.time()

    # Create a pool of workers
    with Pool(processes=num_processes) as pool:
        pool.map(open_url, [requests_per_process] * num_processes)

    print(f"✅ Completed {num_requests} requests in {time.time() - start_time:.2f} seconds")
