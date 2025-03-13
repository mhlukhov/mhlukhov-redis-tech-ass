import random
import time
import uuid
import logging
import redis
from datetime import datetime, timedelta

# Redis Configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
TARGET_DURATION = timedelta(minutes=1)
BATCH_SIZE = 1000
CHANNEL_NAME = "messages:published"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_redis_connection():
    """Establishes a Redis connection using a connection pool."""
    try:
        pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        return redis.Redis(connection_pool=pool)
    except redis.ConnectionError as e:
        logging.error(f"Redis connection failed: {e}")
        exit(1)

def publisher():
    """Publishes messages to a Redis channel in batches."""
    connection = get_redis_connection()
    start_time = datetime.now()
    total_messages = 0

    try:
        while datetime.now() - start_time < TARGET_DURATION:
            p = connection.pipeline()
            messages = [{ "message_id": str(uuid.uuid4()) } for _ in range(BATCH_SIZE)]

            # Batch publish messages
            for message in messages:
                p.publish(CHANNEL_NAME, str(message))

            p.execute()
            total_messages += BATCH_SIZE
            logging.info(f"Published {BATCH_SIZE} messages. Total: {total_messages}")

            # Reduce sleep time variance for better throughput
            time.sleep(random.uniform(0.1, 0.2))

    except redis.RedisError as e:
        logging.error(f"Redis operation failed: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info(f"Total messages published: {total_messages}")

if __name__ == "__main__":
    publisher()
