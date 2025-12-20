"""
Crawl TFT data from Community Dragon (cdragon)
Source: raw.communitydragon.org
"""
import requests
import json
import os


def download_cdragon_data():
    """Download TFT data from Community Dragon"""
    
    print("="*70)
    print("COMMUNITY DRAGON - TFT Data Crawler")
    print("="*70)
    
    url = 'https://raw.communitydragon.org/latest/cdragon/tft/en_us.json'
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
    }
    
    try:
        print(f"\nDownloading: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            print(f"✓ Download successful ({len(response.content)} bytes)")
            
            # Parse JSON
            data = response.json()
            print(f"✓ JSON parsed successfully")
            
            # Save to file
            os.makedirs('test_data', exist_ok=True)
            output_file = 'test_data/cdragon_en_us.json'
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            print(f"✓ Saved to {output_file}")
            
            # Show structure
            print("\n" + "="*70)
            print("DATA STRUCTURE")
            print("="*70)
            
            if isinstance(data, dict):
                print(f"\nTop-level keys ({len(data)} total):")
                for key in list(data.keys())[:20]:
                    value = data[key]
                    if isinstance(value, dict):
                        print(f"  {key}: dict with {len(value)} items")
                    elif isinstance(value, list):
                        print(f"  {key}: list with {len(value)} items")
                    else:
                        print(f"  {key}: {type(value).__name__}")
                
                if len(data.keys()) > 20:
                    print(f"  ... and {len(data.keys()) - 20} more keys")
                
                # Look for units/champions data
                unit_keys = [k for k in data.keys() if 'set' in k.lower() or 'champion' in k.lower() or 'unit' in k.lower()]
                if unit_keys:
                    print(f"\n✓ Found potential unit/champion keys:")
                    for key in unit_keys[:10]:
                        print(f"  - {key}")
            
            return data
            
        else:
            print(f"✗ Download failed: HTTP {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


def main():
    data = download_cdragon_data()
    
    if data:
        print("\n" + "="*70)
        print("SUCCESS!")
        print("="*70)
        print("Data downloaded and saved to test_data/cdragon_en_us.json")
        print("\nNext steps:")
        print("1. Examine the JSON structure")
        print("2. Find the units/champions data")
        print("3. Extract unit-trait relationships")
    else:
        print("\n✗ Failed to download data")


if __name__ == "__main__":
    main()
