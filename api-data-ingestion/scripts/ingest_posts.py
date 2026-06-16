import os
import logging

import requests
import psycopg2

from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

API_URL = os.getenv("API_URL")

db_config = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}


def fetch_posts():
    logging.info("Fetching data from API")

    response = requests.get(API_URL, timeout=30)

    response.raise_for_status()

    data = response.json()

    logging.info(f"Retrieved {len(data)} records")

    return data


def insert_posts(posts):
    connection = None

    try:
        connection = psycopg2.connect(**db_config)

        cursor = connection.cursor()

        query = """
        INSERT INTO posts (id, user_id, title, body)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id)
        DO UPDATE SET
            user_id = EXCLUDED.user_id,
            title = EXCLUDED.title,
            body = EXCLUDED.body;
        """

        for post in posts:
            cursor.execute(
                query,
                (
                    post["id"],
                    post["userId"],
                    post["title"],
                    post["body"]
                )
            )

        connection.commit()

        logging.info("Data inserted successfully")

    except Exception as error:
        if connection:
            connection.rollback()

        logging.error(error)

        raise

    finally:
        if connection:
            cursor.close()
            connection.close()


def main():
    posts = fetch_posts()

    logging.info("Sample record:")

    logging.info(posts[0])

    insert_posts(posts)


if __name__ == "__main__":
    main()