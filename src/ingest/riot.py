import os
import requests
from dotenv import load_dotenv
import json

# Load environment variables from a .env file
load_dotenv()

api_key = os.getenv("RIOT_API_KEY")  # Load API key from environment variable

headers = {
    "X-Riot-Token": api_key
}

def make_request(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        return {
            "error": response.status_code,
            "message": response.text
        }

def my_account_info():
    url = "https://vn2.api.riotgames.com/tft/summoner/v1/summoners/me"
    return make_request(url)

def get_account_info(puuid):
    url = f"https://sea.api.riotgames.com/tft/match/v1/matches/by-puuid/{puuid}/ids"
    return make_request(url)

def get_match_details(match_id):
    url = f"https://sea.api.riotgames.com/tft/match/v1/matches/{match_id}"
    return make_request(url)

def my_account(gameName, tagLine):
    url = f"/riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}"
    return make_request(url)

if __name__ == "__main__":
    if not api_key:
        print("Error: RIOT_API_KEY not found in environment variables.")
    else:
        match_id_prefix = "VN2_"
        match_id_number = 1131294000

        while True:
            match_id = f"{match_id_prefix}{match_id_number:010}"
            print(f"Trying match_id: {match_id}")

            match_info = get_match_details(match_id)

            # If valid match info is returned, append the match id number to an existing csv file
            if "error" not in match_info:
                with open("match_list.csv", "a") as csv_file:
                    csv_file.write(f"{match_id_prefix}{match_id_number}\n")
                print(f"Appended match info for {match_id} to match_list.csv")

            # Check if 429 Rate Limit error then sleep for a while
            if match_info.get("error") == 429:
                print("Rate limit exceeded. Sleeping for 2 minutes...")
                import time
                time.sleep(120)

            else:
                match_id_number += 1