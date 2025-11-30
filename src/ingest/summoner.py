import os
import requests
from dotenv import load_dotenv
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from a .env file
load_dotenv()

api_key = os.getenv("RIOT_API_KEY")  # Load API key from environment variable
kafka_bootstrap_servers = ['localhost:9092', 'localhost:9192']
kafka_topic = "tft_summoner"

headers = {
    "X-Riot-Token": api_key
}

print(headers)

class KafkaLeagueProducer:
    def __init__(self, bootstrap_servers=kafka_bootstrap_servers, topic=kafka_topic):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            retry_backoff_ms=300,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True
        )
        logger.info(f"Kafka producer initialized for topic: {topic}")
    
    def parse_and_send_summoners(self, tier, division, raw_data):
        """Parse summoner data and send individual summoner messages to Kafka"""
        if not isinstance(raw_data, (dict, list)):
            logger.error(f"Invalid data format for {tier} {division}")
            return 0
        
        ingest_ts = datetime.now().isoformat()
        successful_sends = 0
        
        # Handle different API response formats
        summoners = []
        
        if tier.lower() in ["challenger", "grandmaster", "master"]:
            # Format: {"tier": "CHALLENGER", "entries": [...]}
            if isinstance(raw_data, dict) and "entries" in raw_data:
                summoners = raw_data["entries"]
                api_tier = raw_data.get("tier", tier.upper())
            else:
                logger.error(f"Expected 'entries' key for {tier} {division}, got: {type(raw_data)}")
                return 0
        else:
            # Format: [{"puuid": "...", "tier": "IRON", ...}, ...]
            if isinstance(raw_data, list):
                summoners = raw_data
                api_tier = tier.upper()
            else:
                logger.error(f"Expected list format for {tier} {division}, got: {type(raw_data)}")
                return 0
        
        # Process each summoner
        for summoner in summoners:
            try:
                # Transform summoner data to desired format
                message = {
                    "puuid": summoner.get("puuid", ""),
                    "region": "VN2",
                    "tier": summoner.get("tier", api_tier),
                    "rank": summoner.get("rank", division if division else "I"),
                    "leaguePoints": summoner.get("leaguePoints", 0),
                    "wins": summoner.get("wins", 0),
                    "losses": summoner.get("losses", 0),
                    "ingest_ts": ingest_ts
                }
                
                # Create unique key for each summoner
                key = f"{message['puuid']}_{tier}_{division}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # Send to Kafka
                future = self.producer.send(self.topic, key=key, value=message)
                record_metadata = future.get(timeout=10)
                
                successful_sends += 1
                logger.debug(f"Sent summoner {message['puuid']} from {tier} {division} - "
                           f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                
            except KafkaError as e:
                logger.error(f"Failed to send summoner from {tier} {division} to Kafka: {e}")
            except Exception as e:
                logger.error(f"Error processing summoner from {tier} {division}: {e}")
        
        logger.info(f"Successfully sent {successful_sends}/{len(summoners)} summoners for {tier} {division}")
        return successful_sends
    
    def close(self):
        """Close the Kafka producer"""
        self.producer.close()
        logger.info("Kafka producer closed")

def get_league_entries(tier, division):
    """Fetch league entries from Riot API"""
    if tier in ["challenger", "grandmaster", "master"]:
        url = f"https://vn2.api.riotgames.com/tft/league/v1/{tier}"
    else:
        url = f"https://vn2.api.riotgames.com/tft/league/v1/entries/{tier}/{division}"
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            logger.info(f"Successfully fetched {len(data) if isinstance(data, list) else 'unknown'} entries for {tier} {division}")
            return data
        elif response.status_code == 429:
            logger.warning(f"Rate limited for {tier} {division}. Status: {response.status_code}")
            return {
                "error": response.status_code,
                "message": "Rate limited",
                "tier": tier,
                "division": division
            }
        else:
            logger.error(f"API error for {tier} {division}. Status: {response.status_code}, Response: {response.text}")
            return {
                "error": response.status_code,
                "message": response.text,
                "tier": tier,
                "division": division
            }
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception for {tier} {division}: {e}")
        return {
            "error": "request_exception",
            "message": str(e),
            "tier": tier,
            "division": division
        }

def fetch_and_process_one(tier, division, producer):
    """Fetch league entries for one (tier, division) and send to Kafka"""
    logger.info(f"[THREAD] Processing Tier: {tier}, Division: {division}")

    league_data = get_league_entries(tier, division)

    # Nếu response là dạng error (dict có key 'error')
    if isinstance(league_data, dict) and "error" in league_data:
        logger.error(f"API error for {tier} {division}: {league_data}")
        return {
            "tier": tier,
            "division": division,
            "sent": 0,
            "failed": 1,
        }

    # Ngược lại parse & gửi vào Kafka
    sent_count = producer.parse_and_send_summoners(tier, division, league_data)

    return {
        "tier": tier,
        "division": division,
        "sent": sent_count,
        "failed": 0,
    }


def main():
    """Main function to fetch and produce league data to Kafka"""
    if not api_key:
        logger.error("Error: RIOT_API_KEY not found in environment variables.")
        return
    
    # Initialize Kafka producer
    producer = KafkaLeagueProducer()
    
    try:
        tiers = ["challenger", "grandmaster", "master",
                 "DIAMOND", "PLATINUM", "GOLD", "SILVER", "BRONZE", "IRON"]
        total_summoners_sent = 0
        failed_requests = 0

        # Chuẩn bị list (tier, division) cần gọi API
        tasks = []
        for tier in tiers:
            divisions = ["I", "II", "III", "IV"] if tier not in ["challenger", "grandmaster", "master"] else [""]
            for division in divisions:
                tasks.append((tier, division))

        # Số worker: có thể chỉnh nhỏ/lớn tùy rate limit & sức máy
        max_workers = min(8, len(tasks))  # ví dụ tối đa 8 thread

        logger.info(f"Starting ThreadPoolExecutor with max_workers={max_workers}, total tasks={len(tasks)}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(fetch_and_process_one, tier, division, producer): (tier, division)
                for tier, division in tasks
            }

            for future in as_completed(future_to_task):
                tier, division = future_to_task[future]
                try:
                    result = future.result()
                    sent = result["sent"]
                    failed = result["failed"]
                    total_summoners_sent += sent
                    failed_requests += failed

                    logger.info(
                        f"[DONE] Tier={tier}, Division={division} -> sent={sent}, failed={failed}"
                    )
                except Exception as e:
                    logger.error(f"Unhandled exception for {tier} {division}: {e}")
                    failed_requests += 1

        logger.info(
            f"Data production complete. Total summoners sent: {total_summoners_sent}, "
            f"Failed requests: {failed_requests}"
        )

        
        logger.info(f"Data production complete. Total summoners sent: {total_summoners_sent}, Failed requests: {failed_requests}")
                
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        # Close Kafka producer
        producer.close()

if __name__ == "__main__":
    main()
                