import os
import time
import json
import logging
from dotenv import load_dotenv
from typing import List, Dict, Optional, Set
from collections import deque
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from kafka import KafkaProducer

load_dotenv()

# ===========================
# Logging
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("tft_match_pipeline")


# ===========================
# Config
# ===========================
RIOT_API_KEY = os.getenv("RIOT_API_KEY")
if not RIOT_API_KEY:
    logger.error("RIOT_API_KEY environment variable is not set.")
    raise SystemExit(1)

# Region theo ví dụ bạn đưa
REGION = "sea"

# File input / cache
SUMMONER_LIST_FILE = "./src/ingest/summonerList.txt"
MATCH_LIST_FILE = "./src/ingest/matchList.txt"

# Số match muốn lấy cho mỗi puuid
MATCH_COUNT_PER_PUUID = 20

# Rate limit (dev key Riot thường ~100 req / 120s, ta set thấp hơn cho an toàn)
RATE_LIMIT_REQUESTS = 90
RATE_LIMIT_WINDOW_SEC = 120

# Thread pool
MAX_WORKERS = 8

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

MATCH_TOPIC = os.getenv("KAFKA_TOPIC_MATCH", "tft_match")
MATCH_PARTICIPANT_TOPIC = os.getenv("KAFKA_TOPIC_MATCH_PARTICIPANT", "tft_match_participant")
PARTICIPANT_UNIT_TOPIC = os.getenv("KAFKA_TOPIC_PARTICIPANT_UNIT", "tft_participant_unit")


# ===========================
# Rate limiter
# ===========================
class RateLimiter:
    """
    Sliding-window rate limiter dùng chung cho nhiều thread.
    max_calls: số request tối đa trong period giây.
    """

    def __init__(self, max_calls: int, period: float):
        self.max_calls = max_calls
        self.period = period
        self.lock = Lock()
        self.calls = deque()

    def wait(self):
        """Block tới khi được phép call tiếp theo."""
        with self.lock:
            now = time.time()

            # Xóa các call đã quá window
            while self.calls and now - self.calls[0] > self.period:
                self.calls.popleft()

            if len(self.calls) >= self.max_calls:
                sleep_for = self.period - (now - self.calls[0])
                if sleep_for > 0:
                    logger.info(
                        f"[RateLimiter] Sleeping {sleep_for:.2f}s to respect rate limit"
                    )
                    time.sleep(sleep_for)
                    now = time.time()
                    while self.calls and now - self.calls[0] > self.period:
                        self.calls.popleft()

            # Đánh dấu 1 call mới
            self.calls.append(time.time())


# ===========================
# File helpers
# ===========================
def load_summoner_list(path: str) -> List[str]:
    if not os.path.exists(path):
        logger.error(f"summonerList file not found: {path}")
        return []

    summoners = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            summoners.append(line)
    logger.info(f"Loaded {len(summoners)} summoners from {path}")
    return summoners


def load_match_ids(path: str) -> Set[str]:
    if not os.path.exists(path):
        logger.info(f"matchList file not found, will create new: {path}")
        return set()

    match_ids: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            mid = line.strip()
            if mid:
                match_ids.add(mid)
    logger.info(f"Loaded {len(match_ids)} matchIds from {path}")
    return match_ids


def append_match_id(path: str, match_id: str):
    # Mở ở chế độ append, ghi mỗi matchId 1 dòng
    with open(path, "a", encoding="utf-8") as f:
        f.write(match_id + "\n")


# ===========================
# Riot API calls
# ===========================
def get_match_ids_by_puuid(
    puuid: str,
    rate_limiter: RateLimiter,
    start: int = 0,
    count: int = MATCH_COUNT_PER_PUUID,
    max_retries: int = 5,
) -> Optional[List[str]]:
    """
    Gọi:
      GET https://{REGION}.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids
      ?start={start}&count={count}

    Trả về list matchId, hoặc None nếu fail.
    """
    base_url = f"https://{REGION}.api.riotgames.com"
    endpoint = f"/tft/match/v1/matches/by-puuid/{puuid}/ids"

    params = {"start": start, "count": count}
    headers = {"X-Riot-Token": RIOT_API_KEY}

    for attempt in range(1, max_retries + 1):
        try:
            rate_limiter.wait()
            resp = requests.get(
                base_url + endpoint,
                params=params,
                headers=headers,
                timeout=5,
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "1"))
                logger.warning(
                    f"[429] Rate limited on by-puuid for puuid={puuid}, sleeping {retry_after}s (attempt {attempt})"
                )
                time.sleep(retry_after)
                continue

            if not resp.ok:
                logger.error(
                    f"[ERROR] {resp.status_code} by-puuid for puuid={puuid}: {resp.text}"
                )
                if 500 <= resp.status_code < 600:
                    backoff = 1.0 * attempt
                    logger.info(f"Retrying by-puuid after {backoff:.1f}s")
                    time.sleep(backoff)
                    continue
                return None

            data = resp.json()
            if not isinstance(data, list):
                logger.error(f"Unexpected by-puuid response for puuid={puuid}: {data}")
                return None

            return data

        except requests.RequestException as e:
            logger.error(f"[EXCEPTION] by-puuid for puuid={puuid}: {e}")
            backoff = 1.0 * attempt
            logger.info(f"Retrying by-puuid after {backoff:.1f}s")
            time.sleep(backoff)

    logger.error(f"[FAIL] Max retries exceeded by-puuid for puuid={puuid}")
    return None


def get_match_detail(
    match_id: str,
    rate_limiter: RateLimiter,
    max_retries: int = 5,
) -> Optional[Dict]:
    """
    Gọi:
      GET https://{REGION}.api.riotgames.com/tft/match/v1/matches/{match_id}

    Trả về dict match detail, hoặc None nếu fail.
    """
    base_url = f"https://{REGION}.api.riotgames.com"
    endpoint = f"/tft/match/v1/matches/{match_id}"
    headers = {"X-Riot-Token": RIOT_API_KEY}

    for attempt in range(1, max_retries + 1):
        try:
            rate_limiter.wait()
            resp = requests.get(
                base_url + endpoint,
                headers=headers,
                timeout=5,
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "1"))
                logger.warning(
                    f"[429] Rate limited on match detail for matchId={match_id}, "
                    f"sleeping {retry_after}s (attempt {attempt})"
                )
                time.sleep(retry_after)
                continue

            if not resp.ok:
                logger.error(
                    f"[ERROR] {resp.status_code} match detail for matchId={match_id}: {resp.text}"
                )
                if 500 <= resp.status_code < 600:
                    backoff = 1.0 * attempt
                    logger.info(f"Retrying match detail after {backoff:.1f}s")
                    time.sleep(backoff)
                    continue
                return None

            data = resp.json()
            return data

        except requests.RequestException as e:
            logger.error(f"[EXCEPTION] match detail for matchId={match_id}: {e}")
            backoff = 1.0 * attempt
            logger.info(f"Retrying match detail after {backoff:.1f}s")
            time.sleep(backoff)

    logger.error(f"[FAIL] Max retries exceeded for matchId={match_id}")
    return None


# ===========================
# Parsing match into 3 "tables"
# ===========================
def extract_match_row(match_detail: Dict) -> Dict:
    """
    match: gameId, game_datetime, game_length, game_version,
           tft_game_type, tft_set_core_name, tft_set_number
    """
    info = match_detail.get("info", {})
    metadata = match_detail.get("metadata", {})
    return {
        "match_id": metadata.get("match_id"),
        "gameId": info.get("gameId"),
        "game_datetime": info.get("game_datetime"),
        "game_length": info.get("game_length"),
        "game_version": info.get("game_version"),
        "tft_game_type": info.get("tft_game_type"),
        "tft_set_core_name": info.get("tft_set_core_name"),
        "tft_set_number": info.get("tft_set_number"),
    }


def extract_match_participants(match_detail: Dict) -> List[Dict]:
    """
    matchParticipant: gameId, puuid, placement, gold_left, last_round,
                      level, players_eliminated, total_damage_to_players
    """
    info = match_detail.get("info", {})
    metadata = match_detail.get("metadata", {})
    game_id = info.get("gameId")
    participants = info.get("participants", []) or []

    rows = []
    for p in participants:
        row = {
            "match_id": metadata.get("match_id"),
            "gameId": game_id,
            "puuid": p.get("puuid"),
            "placement": p.get("placement"),
            "gold_left": p.get("gold_left"),
            "last_round": p.get("last_round"),
            "level": p.get("level"),
            "players_eliminated": p.get("players_eliminated"),
            "total_damage_to_players": p.get("total_damage_to_players"),
        }
        rows.append(row)
    return rows


def extract_participant_units(match_detail: Dict) -> List[Dict]:
    """
    participantUnit: gameId, puuid, character_id, tier, item1, item2, item3
    (parse từ itemNames list)
    """
    info = match_detail.get("info", {})
    metadata = match_detail.get("metadata", {})
    game_id = info.get("gameId")
    participants = info.get("participants", []) or []

    rows = []
    for p in participants:
        puuid = p.get("puuid")
        units = p.get("units", []) or []
        for u in units:
            item_names = u.get("itemNames", []) or []
            item1 = item_names[0] if len(item_names) >= 1 else None
            item2 = item_names[1] if len(item_names) >= 2 else None
            item3 = item_names[2] if len(item_names) >= 3 else None

            row = {
                "match_id": metadata.get("match_id"),
                "gameId": game_id,
                "puuid": puuid,
                "character_id": u.get("character_id"),
                "tier": u.get("tier"),
                "item1": item1,
                "item2": item2,
                "item3": item3,
            }
            rows.append(row)
    return rows


# ===========================
# Process per matchId
# ===========================
def process_match_id(
    match_id: str,
    match_ids_set: Set[str],
    match_ids_lock: Lock,
    rate_limiter: RateLimiter,
    producer: KafkaProducer,
):
    """
    - Check matchId trong set/file:
        - Nếu đã có => skip luôn.
        - Nếu mới:
            + Ghi vào file + add vào set (trong lock)
            + Gọi API match detail
            + Parse và send 3 topic Kafka
    """

    # Kiểm tra cache + cập nhật atomically
    with match_ids_lock:
        if match_id in match_ids_set:
            logger.info(f"[SKIP] matchId {match_id} already in matchList")
            return
        # matchId mới
        match_ids_set.add(match_id)
        append_match_id(MATCH_LIST_FILE, match_id)

    logger.info(f"[NEW] Processing matchId={match_id}")

    match_detail = get_match_detail(match_id, rate_limiter=rate_limiter)
    if match_detail is None:
        logger.warning(f"[WARN] Failed to get match detail for matchId={match_id}")
        return

    # Parse 3 bảng
    match_row = extract_match_row(match_detail)
    match_participants = extract_match_participants(match_detail)
    participant_units = extract_participant_units(match_detail)

    # Produce Kafka
    try:
        # Send match with gameId as key
        match_key = str(match_row.get("gameId", match_id))
        producer.send(MATCH_TOPIC, key=match_key, value=match_row)

        for mp in match_participants:
            # Send match participant with gameId_puuid as key
            participant_key = f"{mp.get('gameId')}_{mp.get('puuid')}"
            producer.send(MATCH_PARTICIPANT_TOPIC, key=participant_key, value=mp)

        for unit in participant_units:
            # Send participant unit with gameId_puuid_characterId as key
            unit_key = f"{unit.get('gameId')}_{unit.get('puuid')}_{unit.get('character_id')}"
            producer.send(PARTICIPANT_UNIT_TOPIC, key=unit_key, value=unit)

        logger.info(
            f"[PRODUCED] matchId={match_id}, "
            f"participants={len(match_participants)}, units={len(participant_units)}"
        )
    except Exception as e:
        logger.error(f"[KAFKA ERROR] when sending matchId={match_id}: {e}")


# ===========================
# Process per summoner (puuid)
# ===========================
def process_summoner(
    puuid: str,
    match_ids_set: Set[str],
    match_ids_lock: Lock,
    rate_limiter: RateLimiter,
    producer: KafkaProducer,
):
    logger.info(f"[PUUID] Processing puuid={puuid}")

    match_ids = get_match_ids_by_puuid(puuid, rate_limiter=rate_limiter)
    if not match_ids:
        logger.warning(f"No matchIds returned for puuid={puuid}")
        return

    logger.info(f"[PUUID] puuid={puuid} returned {len(match_ids)} matchIds")

    for mid in match_ids:
        process_match_id(
            match_id=mid,
            match_ids_set=match_ids_set,
            match_ids_lock=match_ids_lock,
            rate_limiter=rate_limiter,
            producer=producer,
        )


# ===========================
# Main
# ===========================
def create_kafka_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=[s.strip() for s in KAFKA_BOOTSTRAP_SERVERS.split(",") if s.strip()],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        linger_ms=50,  # gộp message nhẹ
        batch_size=16384,
        retries=3,
        acks='all'  # Wait for all replicas to acknowledge
    )
    return producer


def main():
    summoners = load_summoner_list(SUMMONER_LIST_FILE)
    if not summoners:
        logger.error("No summoners to process, exiting.")
        return

    match_ids_set = load_match_ids(MATCH_LIST_FILE)
    match_ids_lock = Lock()

    rate_limiter = RateLimiter(
        max_calls=RATE_LIMIT_REQUESTS,
        period=RATE_LIMIT_WINDOW_SEC,
    )

    producer = create_kafka_producer()

    # Dùng ThreadPool để xử lý nhiều puuid song song
    num_workers = min(MAX_WORKERS, len(summoners))
    logger.info(f"Starting ThreadPoolExecutor with max_workers={num_workers}")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_puuid = {
            executor.submit(
                process_summoner,
                puuid,
                match_ids_set,
                match_ids_lock,
                rate_limiter,
                producer,
            ): puuid
            for puuid in summoners
        }

        for future in as_completed(future_to_puuid):
            puuid = future_to_puuid[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"[EXCEPTION] processing puuid={puuid}: {e}", exc_info=True)

    logger.info("Flushing Kafka producer...")
    producer.flush()
    producer.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
