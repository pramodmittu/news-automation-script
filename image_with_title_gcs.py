import requests
import time
from google.cloud import storage
import psycopg2
from psycopg2 import Error

# ==============================================================================
# CONFIGURATION
# You must change these variables to match your Placid and GCS setup.
# ==============================================================================
# Your private API token from Placid.
API_TOKEN = "placid-e5rlpoxpbjwmciue-vrbobvcadehx634j"

# The template UUID from your Placid design.
TEMPLATE_UUID = "u2fae8lhersy7"

# GCS Bucket configuration.
GCS_BUCKET_NAME = "news_image_title"
# The GCS_OUTPUT_FILENAME is now set dynamically in the main script.

# PostgreSQL database configuration.
DB_HOST = "34.168.6.247"
DB_NAME = "newsauto_db"
DB_USER = "postgres"
DB_PASSWORD = "Mittu@9966"


def fetch_data_from_db():
    """
    Connects to a PostgreSQL database and fetches the data for the image.
    This function now also fetches the 'id' of the article to be processed.

    Returns a dictionary with the article id, image URL, and title text.
    """
    print("Connecting to PostgreSQL database...")
    conn = None
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME
        )
        cursor = conn.cursor()

        # Modified query to also select the 'id' column.
        sql_query = "SELECT id, url_to_image, title FROM articles where id = 2;"
        cursor.execute(sql_query)

        db_data = cursor.fetchone()

        if db_data:
            article_id, image_url, title = db_data
            return {"id": article_id, "image_url": image_url, "title": title}
        else:
            print("No data found in the database.")
            return None

    except (Exception, Error) as error:
        print(f"Error while connecting to PostgreSQL or fetching data: {error}")
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection closed.")


def update_db_status(article_id, status):
    """
    Connects to the database and updates the status of a specific article.
    """
    print(f"Updating status for article ID {article_id} to '{status}'...")
    conn = None
    try:
        conn = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME
        )
        cursor = conn.cursor()

        sql_update_query = "UPDATE articles SET status = %s WHERE id = %s;"
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
        return True
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return False


# ==============================================================================
# MAIN SCRIPT EXECUTION
# ==============================================================================
if __name__ == "__main__":

    # Fetch data from the database
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

    # --- STEP 2: Poll for the image status using the polling_url ---
    polling_url = response_data.get("polling_url")
    image_status = response_data.get("status")

    print(f"\n⏳ Image queued. Polling URL: {polling_url}")

    if not polling_url:
        exit("Failed to get polling URL from the initial response.")

    # Use the same Bearer token for polling
    polling_headers = {
        "Authorization": f"Bearer {API_TOKEN}"
    }

    final_image_url = None  # Initialize the variable before the loop

    while image_status != "completed" and image_status != "finished":
        print(f"Current status: {image_status}. Waiting...")
        time.sleep(5)  # Wait for 5 seconds before checking again

        response_data = make_api_request(polling_url, "GET", polling_headers)

        if not response_data:
            exit("Polling request failed. Exiting.")

        image_status = response_data.get("status")

    # Check for both "completed" and "finished" after the loop.
    if image_status == "completed" or image_status == "finished":
        final_image_url = response_data.get("image_url")

    # --- STEP 3: Download the final image and upload to GCS ---
    if final_image_url:
        print(f"\n✅ Image generation complete. Downloading from: {final_image_url}")

        # Download the image to a temporary file
        temp_filename = "temp_placid_image.jpg"
        with requests.get(final_image_url, stream=True) as r:
            r.raise_for_status()
            with open(temp_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        # Define the output filename using the unique article ID
        gcs_output_filename = f"{article_id}.jpg"

        # Upload the downloaded image to GCS
        upload_success = upload_to_gcs(temp_filename, GCS_BUCKET_NAME, gcs_output_filename)

        # --- STEP 4: Update the database status ---
        if upload_success:
            update_db_status(article_id, "sent_gcs")
    else:
        print("❌ Could not get the final image URL.")
