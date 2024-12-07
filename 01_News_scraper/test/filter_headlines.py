from bs4 import BeautifulSoup, Comment
import json
import re
import csv
from datetime import datetime
import boto3

'''
# Load the HTML file
file_path = "page_content.html"

with open(file_path, "r", encoding="utf-8") as file:
    html_content = file.read()

# Parse HTML content with BeautifulSoup
soup = BeautifulSoup(html_content, "html.parser")

# Display the parsed content for verification (optional)
#print(soup.prettify()[:500])

# Example selector based on assumed structure (update as needed)
top_news_section = soup.find_all("h3", class_="mkt-big-ttl")

# Extract and print each headline
for news in top_news_section:
    headline = news.get_text(strip=True)
    print(headline)
'''

def extract_moneycontrol_news(file_path):
    # Load and parse the HTML file
    with open(file_path, "r", encoding="utf-8") as file:
        soup = BeautifulSoup(file, "html.parser")

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

def upload_to_s3(filename, bucket_name, s3_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(filename, bucket_name, s3_key)
        print(f"File uploaded to S3: {bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

# Execution
file_path = "page_content_mc1.html"
news_details = extract_moneycontrol_news(file_path)

# Generate a filename with the current timestamp
timestamp_str = datetime.now().strftime("%Y%m%d%H%M%S")
csv_filename = f"moneycontrol_news_{timestamp_str}.csv"

# Save news data to CSV
save_to_csv(news_details, csv_filename)

# Upload the CSV file to S3 with a filename reflecting the execution timestamp
bucket_name = "news-scraped-data"  # Replace with your S3 bucket name
s3_key = csv_filename  # Filename in S3

# Uncomment the following line if running in an environment with AWS credentials
upload_to_s3(csv_filename, bucket_name, s3_key)

# Print each news item with details
# for index, news in enumerate(news_details, start=1):
#     print(f"{index}. Title: {news['title']}")
#     print(f"   URL: {news['url']}")
#     print(f"   Description: {news['description']}")
#     print(f"   Timestamp: {news['timestamp']}")
#     print()  # Blank line for readability