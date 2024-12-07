import requests
from bs4 import BeautifulSoup, Comment
import re
import csv
import os
from datetime import datetime
import boto3

# Get current Timestamp
timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")

# Replace this URL with the link to the page you want to scrape
url = "https://www.moneycontrol.com/news/tags/nifty.html/news/"

# Fetch the content of the page
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    html_content = response.text

    # Save HTML content to a file
    filename = f"moneycontrol_news_{timestamp_str}"
    html_filename = filename + ".html"
    with open(html_filename, "w", encoding="utf-8") as file:
        file.write(html_content)
    
    print(f"HTML content has been saved to {html_filename}")
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")


def extract_moneycontrol_news(html_content):
    # Parse HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the div with class "topictabpane" and id "t_top"
    news_section = soup.find("div", class_="topictabpane", id="t_top")
    news_items = []

    if news_section:
        # Find all list items in the news section
        for li in news_section.find_all("li", class_="clearfix"):
            # Extract title and URL from the <a> tag within <h2>
            title_tag = li.find("h2").find("a")
            title = title_tag.get_text(strip=True) if title_tag else "No title available"
            url = title_tag['href'] if title_tag else "No URL available"
            
            # Extract description from the <p> tag
            description = li.find("p").get_text(strip=True) if li.find("p") else "No description available"
            
            # Extract and clean timestamp from the HTML comment
            timestamp_comment = li.find(string=lambda text: isinstance(text, Comment))
            if timestamp_comment:
                timestamp = re.sub(r'<[^>]+>', '', timestamp_comment.strip())  # Remove any <span> tags
            else:
                timestamp = "Timestamp not available"

            # Append the news item details to the list
            news_items.append({
                "title": title,
                "url": url,
                "description": description,
                "timestamp": timestamp
            })

    return news_items


def save_to_csv(news_items, filename):
    # Define CSV columns
    fieldnames = ["timestamp", "title", "url", "description"]

    # Write to CSV file
    with open(filename, mode="w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for item in news_items:
            writer.writerow(item)
    print(f"Data saved to {filename}")


def upload_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"File uploaded to S3: {bucket_name}/{s3_key}")
        return True  # Indicate success
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False  # Indicate failure


def delete_local_file(file_path):
    try:
        os.remove(file_path)
        print(f"Deleted local file: {file_path}")
    except Exception as e:
        print(f"Error deleting file {file_path}: {e}")


# Extract news items directly from the HTML content
news_details = extract_moneycontrol_news(html_content)

# Generate a CSV filename with the current timestamp
csv_filename = f"moneycontrol_news_{timestamp_str}.csv"

# Save news data to CSV
save_to_csv(news_details, csv_filename)

# Upload the HTML and CSV files to S3
bucket_name = "news-scraped-data"  # Replace with your S3 bucket name
html_s3_key = f"html/{html_filename}"
csv_s3_key = f"csv/{csv_filename}"

# Upload HTML file and delete it locally if successful
if upload_to_s3(html_filename, bucket_name, html_s3_key):
    delete_local_file(html_filename)

# Upload CSV file and delete it locally if successful
if upload_to_s3(csv_filename, bucket_name, csv_s3_key):
    delete_local_file(csv_filename)
