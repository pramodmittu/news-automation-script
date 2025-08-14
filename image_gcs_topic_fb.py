import requests
import time
from google.cloud import storage
from google.cloud.pubsub_v1 import PublisherClient
import psycopg2
from psycopg2 import Error
import json

# The `facebook-sdk` library has been removed and replaced with a direct `requests` call
# to the Facebook Graph API to avoid a persistent versioning error.
# The `publish_to_facebook` function has been updated to upload the image file directly
# to Facebook, rather than using a URL, to resolve the "Missing or invalid image file" error.

# ==============================================================================
# CONFIGURATION
# You must change these variables to match your Placid, GCS, Pub/Sub, and FB setup.
# ==============================================================================
# Your private API token from Placid.
API_TOKEN = "placid-e5rlpoxpbjwmciue-vrbobvcadehx634j"

# The template UUID from your Placid design.
TEMPLATE_UUID = "u2fae8lhersy7"

# GCS Bucket configuration.
GCS_BUCKET_NAME = "news_image_title"

# PostgreSQL database configuration.
DB_HOST = "34.168.6.247"
DB_NAME = "newsauto_db"
DB_USER = "postgres"
DB_PASSWORD = "Mittu@9966"

# Google Cloud Pub/Sub configuration.
# Replace with your Google Cloud Project ID.
GCP_PROJECT_ID = "adept-snow-468005-a1"
PUBSUB_TOPIC_ID = "News_auto_topic"

# Facebook Graph API configuration.
# Replace with your Page Access Token and Page ID.
FB_PAGE_ACCESS_TOKEN = "EAA9V2VmBcsEBPEYUq95e1A99Myrjd4v1duE27WGutXUYGdS0OUYZBwatPtihjVuDs0KHZBtOZB1NqcAQEw0bwxdWxB0IzyksPGtbp6XbRZC0SWz1cS2h4QQUSlYX1OiLhU0O6i7Vt7C62gpz2BsMPhb1jsU6ZAAZC0T9Neh7m9fU39ZAKrtkrm4U4khqblMrHw1xkojNjnd"
FB_PAGE_ID = "772261789296157"


def fetch_data_from_db():
    """
    Connects to a PostgreSQL database and fetches the data for the next unposted article.
    The function fetches the article with the lowest ID that has an 'Over_all_status' of 'NOT_POSTED'.

    Returns:
        A dictionary with the article id, image URL, and title text, or None if not found.
    """
    print("Connecting to PostgreSQL database to fetch the next unposted article...")
    conn = None
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Query to fetch the next article by ID with status 'NOT_POSTED'
        sql_query = "SELECT id, url_to_image, title FROM articles WHERE Over_all_status = 'NOT_POSTED' ORDER BY id desc LIMIT 1;"
        cursor.execute(sql_query)

        db_data = cursor.fetchone()

        if db_data:
            article_id, image_url, title = db_data
            return {"id": article_id, "image_url": image_url, "title": title}
        else:
            print("No data found for any article with status 'NOT_POSTED'.")
            return None

    except (Exception, Error) as error:
        print(f"Error while connecting to PostgreSQL or fetching data: {error}")
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection closed.")


def update_db_status(article_id, status_col, status):
    """
    Connects to the database and updates a specific status column for a given article.
    """
    print(f"Updating '{status_col}' for article ID {article_id} to '{status}'...")
    conn = None
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME
        )
        cursor = conn.cursor()

        sql_update_query = f"UPDATE articles SET {status_col} = %s WHERE id = %s;"
        cursor.execute(sql_update_query, (status, article_id))

        conn.commit()
        print(f"✅ Status updated successfully for article ID {article_id}.")

    except (Exception, Error) as error:
        print(f"Error updating article status: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def make_api_request(url, method, headers, json_data=None):
    """
    A helper function to make an API request with specified headers and data.
    """
    try:
        if method.upper() == "POST":
            response = requests.post(url, headers=headers, json=json_data)
        else:
            response = requests.get(url, headers=headers)

        response.raise_for_status()  # Raises an exception for 4xx or 5xx status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        if 'response' in locals() and response.content:
            print("Response content:", response.content.decode())
        return None


def upload_to_gcs(file_path, bucket_name, destination_blob_name):
    """
    Uploads a file to a Google Cloud Storage bucket.
    """
    print(f"\n⬆️ Uploading to GCS bucket: {bucket_name}...")
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(file_path)

        print(f"✅ File {file_path} uploaded to {destination_blob_name}.")
        return blob.public_url
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return None


def publish_to_pubsub(project_id, topic_id, message_data):
    """
    Publishes a message to a Google Cloud Pub/Sub topic.
    """
    publisher = PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # Data must be a bytestring
    data = json.dumps(message_data).encode("utf-8")

    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")


def publish_to_facebook(page_id, access_token, message, image_path):
    """
    Publishes a photo with a message to a Facebook Page by uploading the file directly.
    Returns True on success, False otherwise.
    """
    print(f"Publishing to Facebook page {page_id}...")
    try:
        api_version = "18.0"
        url = f"https://graph.facebook.com/v{api_version}/{page_id}/photos"

        # Prepare the multipart-form-data request with the image file
        files = {
            'source': open(image_path, 'rb')
        }

        params = {
            'access_token': access_token,
            'message': message
        }

        response = requests.post(url, params=params, files=files)
        response.raise_for_status()  # Raise an exception for bad status codes

        print("✅ Successfully published to Facebook.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error publishing to Facebook: {e}")
        if 'response' in locals() and response.content:
            print("Response content:", response.content.decode())
        return False


# ==============================================================================
# MAIN SCRIPT EXECUTION
# ==============================================================================
if __name__ == "__main__":

    # Fetch data from the database for the next unposted article.
    db_payload = fetch_data_from_db()

    if not db_payload:
        exit("Failed to fetch data from the database. Exiting.")

    article_id = db_payload["id"]
    image_url_from_db = db_payload["image_url"]
    title_from_db = db_payload["title"]

    # --- STEP 1: Send a POST request to generate the image ---
    print("\n🚀 Initiating image generation with POST request...")
    post_url = "https://api.placid.app/api/rest/images"

    post_headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }

    # Construct the JSON body for the POST request
    json_body = {
        "template_uuid": TEMPLATE_UUID,
        "layers": {
            "img": {
                "image": image_url_from_db
            },
            "title": {
                "text": title_from_db
            }
        }
    }

    response_data = make_api_request(post_url, "POST", post_headers, json_body)

    if not response_data:
        exit("Initial POST request failed. Exiting.")

    # Update the status to indicate image generation is in progress
    update_db_status(article_id, 'Over_all_status', "GENERATION_IN_PROGRESS")

    # --- STEP 2: Poll for the image status using the polling_url ---
    polling_url = response_data.get("polling_url")
    image_status = response_data.get("status")

    print(f"\n⏳ Image queued. Polling URL: {polling_url}")

    if not polling_url:
        exit("Failed to get polling URL from the initial response.")

    polling_headers = {
        "Authorization": f"Bearer {API_TOKEN}"
    }

    final_image_url = None

    while image_status not in ["completed", "finished"]:
        print(f"Current status: {image_status}. Waiting...")
        time.sleep(5)

        response_data = make_api_request(polling_url, "GET", polling_headers)

        if not response_data:
            exit("Polling request failed. Exiting.")

        image_status = response_data.get("status")

    if image_status in ["completed", "finished"]:
        final_image_url = response_data.get("image_url")

    # --- STEP 3: Download the final image and post to Facebook ---
    if final_image_url:
        print(f"\n✅ Image generation complete. Downloading from: {final_image_url}")

        temp_filename = "temp_placid_image.jpg"
        with requests.get(final_image_url, stream=True) as r:
            r.raise_for_status()
            with open(temp_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        print("\n🌐 Publishing image to Facebook...")
        fb_success = publish_to_facebook(FB_PAGE_ID, FB_PAGE_ACCESS_TOKEN, title_from_db, temp_filename)

        if fb_success:
            # --- STEP 4: Upload to GCS and update database after successful FB post ---
            gcs_output_filename = f"{article_id}.jpg"
            gcs_public_url = upload_to_gcs(temp_filename, GCS_BUCKET_NAME, gcs_output_filename)

            if gcs_public_url:
                # Update status to GCS_UPLOAD_SUCCESSFUL
                update_db_status(article_id, 'Over_all_status', "POSTED_TO_FB")

                print("\n📬 Publishing image URL to Pub/Sub topic...")
                message_data = {
                    "article_id": article_id,
                    "image_url": gcs_public_url,
                    "title": title_from_db
                }
                publish_to_pubsub(GCP_PROJECT_ID, PUBSUB_TOPIC_ID, message_data)
            else:
                update_db_status(article_id, 'Over_all_status', "GCS_UPLOAD_FAILED")
        else:
            update_db_status(article_id, 'Over_all_status', "FB_POST_FAILED")
    else:
        print("❌ Could not get the final image URL.")
