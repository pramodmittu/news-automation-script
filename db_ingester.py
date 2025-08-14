import requests
import json
import psycopg2
import datetime

# --- Database Configuration ---
DB_HOST = "34.168.6.247"
DB_NAME = "newsauto_db"
DB_USER = "postgres"
DB_PASS = "Mittu@9966"  # The password you set
# --- End Database Configuration ---

# --- API Configuration ---
# Your News API endpoint
NEWS_API_URL = "https://newsapi.org/v2/top-headlines?country=us&apiKey=7c428f9f96a8477bb4ab71881eb9139f"


# --- End API Configuration ---

def fetch_and_store_articles():
    conn = None
    cur = None
    try:
        # Establish a connection to your PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()

        print("Successfully connected to the database.")

        # --- Step 1: Fetch data from the API ---
        print(f"Fetching news from: {NEWS_API_URL}")
        response = requests.get(NEWS_API_URL)
        response.raise_for_status()

        full_api_output = response.json()
        articles_data = full_api_output.get('articles', [])

        print(f"Fetched {len(articles_data)} news articles from API.")

        # --- Step 2: Loop through articles and insert into the database ---
        inserted_count = 0
        for article in articles_data:
            # Extract and format data for the 'articles' table
            source_id = article.get('source', {}).get('id')
            source_name = article.get('source', {}).get('name')

            published_at_str = article.get('publishedAt')
            published_at_dt = None
            if published_at_str:
                try:
                    # Convert the ISO 8601 string to a datetime object
                    published_at_dt = datetime.datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
                except ValueError:
                    print(f"Warning: Could not parse publishedAt: {published_at_str}. Skipping timestamp.")

            # Prepare the data as a tuple, ordered to match your table columns
            article_data = (
                source_id,
                source_name,
                article.get('author'),
                article.get('title'),
                article.get('description'),
                article.get('url'),
                article.get('urlToImage'),
                published_at_dt,
                article.get('content')
            )

            # Define the SQL INSERT query with placeholders
            insert_sql = """
            INSERT INTO articles (source_id, source_name, author, title, description, url, url_to_image, published_at, content)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url) DO NOTHING;
            """

            # Execute the insert statement, passing the data tuple
            cur.execute(insert_sql, article_data)
            inserted_count += cur.rowcount

        # --- Step 3: Commit the transaction and close the connection ---
        conn.commit()
        print(f"Successfully inserted {inserted_count} new articles into the database.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching news from API: {e}")
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Database error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    fetch_and_store_articles()