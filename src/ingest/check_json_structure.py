"""
Quick script to check top-level keys in cdragon JSON
"""
import json

with open('test_data/cdragon_en_us.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("Top-level keys in cdragon_en_us.json:")
print("="*70)

for key in data.keys():
    value = data[key]
    if isinstance(value, dict):
        print(f"\n{key}: dict with {len(value)} keys")
        # Show some sub-keys
        if len(value) > 0:
            sub_keys = list(value.keys())[:5]
            print(f"  Sample keys: {sub_keys}")
    elif isinstance(value, list):
        print(f"\n{key}: list with {len(value)} items")
        if len(value) > 0 and isinstance(value[0], dict):
            print(f"  First item keys: {list(value[0].keys())[:10]}")
    else:
        print(f"\n{key}: {type(value).__name__}")

print("\n" + "="*70)
print(f"Total top-level keys: {len(data.keys())}")
