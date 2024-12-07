from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
import csv
from datetime import datetime, timedelta
import time
import pytz  # Add this import for timezone handling
import boto3
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Output to console
    ]
)

# Function to upload file to S3
def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"File uploaded to S3: {bucket_name}/{s3_key}")
        return True  # Indicate success
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False  # Indicate failure

# Function to delete local file
def delete_local_file(file_path):
    try:
        os.remove(file_path)
        print(f"Deleted local file: {file_path}")
    except Exception as e:
        print(f"Error deleting file {file_path}: {e}")

logging.info("Initializing WebDriver.")

# Initialize WebDriver
options = Options()
options.add_argument("--headless")  # Run in headless mode if needed

# Provide the geckodriver path
service = Service("/usr/local/bin/geckodriver")
driver = webdriver.Firefox(service=service, options=options)
# driver = webdriver.Firefox()  # Use the appropriate WebDriver for your browser
driver.maximize_window()

logging.info("WebDriver initialized successfully.")

# URL of the Nifty forum topic
url = "https://mmb.moneycontrol.com/forum-topics/stocks/nifty-50-244399.html"

# Open the URL
try:
    logging.info(f"Loading URL: {url}")
    driver.get(url)
    logging.info("Page loaded successfully.")
except Exception as e:
    logging.error(f"Error loading URL: {e}")
    driver.save_screenshot("error_screenshot.png")  # Capture browser state

# Wait for the comments section to load
logging.info("Waiting for the comments section to load.")
wait = WebDriverWait(driver, 10)
wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.topicPage_read_list__14okt")))

logging.info("Comments section loaded successfully.")

# Start time for timeout implementation
start_time = time.time()
timeout_duration = 240  # Timeout duration in seconds (4 minutes)

# Extract comments
comments_data = []
logging.info("Starting comment extraction.")
while len(comments_data) < 100:  # Keep scraping until 100 comments are collected
    # Check for timeout
    elapsed_time = time.time() - start_time
    if elapsed_time > timeout_duration:
        logging.info("Timeout reached. Stopping comment extraction.")
        break
    
    # Scroll to the bottom of the page
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    logging.info(f"Scrolled to the bottom of the page. Current comment count: {len(comments_data)}.")
    time.sleep(5)  # Wait for 10 seconds to allow comments to load
    
    # Re-fetch comments containers after scrolling
    comments_containers = driver.find_elements(By.CSS_SELECTOR, "div.topicPage_read_list__14okt")
    
    # Set IST timezone
    ist_timezone = pytz.timezone('Asia/Kolkata')

    # Use a set to track unique entries
    unique_comments = set()

    for container in comments_containers:
        try:
            # Extract the username
            username_element = container.find_element(By.CSS_SELECTOR, "div.postItem_user_area__28qZr div.postItem_user_name__ixoND div.postItem_username__2r1i_")
            username = username_element.text.strip() if username_element else "Anonymous"

            # Extract the comment content
            comment_content_element = container.find_element(By.CSS_SELECTOR, "div.postItem_text_paragraph__3XhZQ span")
            comment_content = comment_content_element.text.strip() if comment_content_element else "No content available"

            # Extract the relative or absolute timestamp
            timestamp_element = container.find_element(By.CSS_SELECTOR, "div.postItem_price__1yXow")
            relative_time_raw = timestamp_element.text.strip() if timestamp_element else "Unknown"

            # Remove the "schedule" prefix if it exists
            relative_time = relative_time_raw.replace("schedule", "").strip()

            # Determine the timestamp type
            if any(unit in relative_time for unit in ["hour", "hours", "min", "mins", "sec", "secs"]):
                # Handle relative time
                current_time = datetime.now(ist_timezone)  # Current time in IST
                hours = 0
                minutes = 0
                seconds = 0

                # Parse hours
                if "hour" in relative_time:
                    hours_part = relative_time.split("hour")[0].strip()
                    hours = int(hours_part.split()[-1]) if hours_part else 0

                # Parse minutes
                if "min" in relative_time:
                    minutes_part = relative_time.split("hour")[-1] if "hour" in relative_time else relative_time
                    minutes = int(minutes_part.split("min")[0].strip().split()[-1]) if minutes_part else 0

                # Parse seconds
                if "sec" in relative_time:
                    seconds_part = relative_time.split("min")[-1] if "min" in relative_time else relative_time
                    seconds = int(seconds_part.split("sec")[0].strip().split()[-1]) if seconds_part else 0

                # Subtract relative time from IST current time
                post_time = current_time - timedelta(hours=hours, minutes=minutes, seconds=seconds)

                # Format timestamp in IST
                timestamp = post_time.strftime("%Y-%m-%d %H:%M:%S") + " IST"

            elif any(month in relative_time for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]):
                # Handle absolute time with date (e.g., "9:37 AM Nov 29th")
                try:
                    # Remove "th", "st", "nd", "rd" from the date
                    cleaned_time = relative_time.replace("th", "").replace("st", "").replace("nd", "").replace("rd", "")

                    # Parse the absolute timestamp (assumes format like "9:37 AM Nov 29")
                    post_time = datetime.strptime(cleaned_time, "%I:%M %p %b %d")

                    # Add the current year if not specified
                    post_time = post_time.replace(year=datetime.now().year)

                    # Keep the timestamp as is (already in IST)
                    timestamp = post_time.strftime("%Y-%m-%d %H:%M:%S") + " IST"
                except Exception as e:
                    print(f"Error parsing absolute timestamp: {e}")
                    timestamp = "Timestamp not available"

            else:
                # Default case if format is unrecognized
                timestamp = "Timestamp not available"

            # Check for duplicates
            comment_tuple = (username, comment_content, timestamp)
            if comment_tuple not in unique_comments:
                unique_comments.add(comment_tuple)
                comments_data.append({
                    "timestamp": timestamp,
                    "username": username,
                    "comment_content": comment_content,
                })

        except Exception as e:
            logging.error(f"Error extracting comment: {e}")

logging.info(f"Finished comment extraction. Total comments extracted: {len(comments_data)}.")

# Save comments to CSV
timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
csv_filename = f"nifty_forum_comments_{timestamp_str}.csv"

with open(csv_filename, mode="w", encoding="utf-8", newline="") as csvfile:
    fieldnames = ["timestamp", "username", "comment_content"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for comment in comments_data:
        writer.writerow(comment)

logging.info(f"Comments saved to {csv_filename}")

# Upload CSV file to S3
bucket_name = "news-scraped-data"  # Replace with your S3 bucket name
csv_s3_key = f"csv/{csv_filename}"

if upload_to_s3(csv_filename, bucket_name, csv_s3_key):
    logging.info("File uploaded to S3 successfully.")
    delete_local_file(csv_filename)

# Close WebDriver
driver.quit()
logging.info("WebDriver closed. Script completed.")
