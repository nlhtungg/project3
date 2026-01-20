"""
Parse Community Dragon TFT data and export to CSV files
Input: test_data/cdragon_en_us.json
Output: units.csv, items.csv, traits.csv, augments.csv
Then upload to MinIO s3a://bronze/csv/
"""
import json
import pandas as pd
import os
import re
import boto3
from botocore.client import Config


def clean_html_tags(text):
    """Remove HTML tags from text"""
    if not text:
        return ''
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def load_cdragon_data():
    """Load the Community Dragon JSON file"""
    file_path = '/opt/test_data/cdragon_en_us.json'
    
    print("Loading Community Dragon data...")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"✓ Loaded successfully")
    return data


def parse_units(data):
    """Parse units/champions from all sets"""
    print("\n--- Parsing Units ---")
    
    units_list = []
    
    # Get sets data
    sets = data.get('sets', {})
    
    if not sets:
        print("✗ No sets found")
        return []
    
    # Get all numeric set keys and sort them
    set_numbers = sorted([int(k) for k in sets.keys() if k.isdigit()])
    print(f"Found {len(set_numbers)} sets: {set_numbers}")
    
    # Parse units from all sets
    for set_num in set_numbers:
        set_data = sets.get(str(set_num), {})
        champions = set_data.get('champions', [])
        
        print(f"  Set {set_num}: {len(champions)} champions")
        
        for champ in champions:
            # Skip training dummies, golems, etc.
            api_name = champ.get('apiName', '')
            if any(x in api_name.lower() for x in ['dummy', 'golem', 'krug', 'wolf', 'razorbeak', 'scuttler', 'herald', 'dragon', 'voidspawn']):
                continue
            
            traits = champ.get('traits', [])
            stats = champ.get('stats', {})
            ability = champ.get('ability', {})
            
            unit_entry = {
                'set': set_num,
                'unit_id': api_name,
                'unit_name': champ.get('name', ''),
                'cost': champ.get('cost', ''),
                'trait1': traits[0] if len(traits) > 0 else '',
                'trait2': traits[1] if len(traits) > 1 else '',
                'trait3': traits[2] if len(traits) > 2 else '',
                'trait4': traits[3] if len(traits) > 3 else '',
                'num_traits': len(traits),
                'all_traits': ', '.join(traits),
                'ability_name': ability.get('name', ''),
                'ability_desc': clean_html_tags(ability.get('desc', '')),
                'health': stats.get('hp', ''),
                'armor': stats.get('armor', ''),
                'magic_resist': stats.get('magicResist', ''),
                'attack_damage': stats.get('damage', ''),
                'attack_speed': stats.get('attackSpeed', ''),
                'attack_range': stats.get('range', ''),
                'mana_start': stats.get('initialMana', ''),
                'mana_max': stats.get('mana', ''),
                'crit_chance': stats.get('critChance', ''),
                'crit_multiplier': stats.get('critMultiplier', ''),
            }
            
            units_list.append(unit_entry)
    
    print(f"✓ Parsed {len(units_list)} playable units")
    return units_list


def parse_traits(data):
    """Parse traits from all sets"""
    print("\n--- Parsing Traits ---")
    
    traits_list = []
    
    # Get sets data
    sets = data.get('sets', {})
    if not sets:
        return []
    
    # Get all numeric set keys and sort them
    set_numbers = sorted([int(k) for k in sets.keys() if k.isdigit()])
    
    # Parse traits from all sets
    for set_num in set_numbers:
        set_data = sets.get(str(set_num), {})
        traits = set_data.get('traits', [])
        
        print(f"  Set {set_num}: {len(traits)} traits")
    
        print(f"  Set {set_num}: {len(traits)} traits")
        
        for trait in traits:
            trait_entry = {
                'set': set_num,
                'trait_id': trait.get('apiName', ''),
                'trait_name': trait.get('name', ''),
                'trait_desc': clean_html_tags(trait.get('desc', '')),
                'icon': trait.get('icon', ''),
            }
            
            # Initialize all 12 tiers with empty values
            for tier_num in range(1, 13):
                trait_entry[f'tier{tier_num}_min'] = ''
                trait_entry[f'tier{tier_num}_max'] = ''
                trait_entry[f'tier{tier_num}_style'] = ''
            
            # Get effect tiers and fill in actual values
            effects = trait.get('effects', [])
            for i, effect in enumerate(effects):
                tier_num = i + 1
                if tier_num <= 12:  # Only process up to tier 12
                    trait_entry[f'tier{tier_num}_min'] = effect.get('minUnits', '')
                    trait_entry[f'tier{tier_num}_max'] = effect.get('maxUnits', '')
                    trait_entry[f'tier{tier_num}_style'] = effect.get('style', '')
            
            traits_list.append(trait_entry)
    
    print(f"✓ Parsed {len(traits_list)} traits")
    return traits_list


def parse_items(data):
    """Parse items from the items array"""
    print("\n--- Parsing Items ---")
    
    items_list = []
    
    items = data.get('items', [])
    print(f"Found {len(items)} total items")
    
    # Filter for actual items (not augments)
    actual_items = []
    for item in items:
        api_name = item.get('apiName', '')
        composition = item.get('composition', [])
        
        # Items have composition (components) or are basic items
        # Augments start with TFT*_Augment
        if 'Augment' not in api_name and 'TeamupAugment' not in api_name:
            # Only include if it has composition or is a basic component
            if composition or len(composition) == 0:
                actual_items.append(item)
    
    print(f"Filtered to {len(actual_items)} actual items (excluding augments)")
    
    for item in actual_items:
        composition = item.get('composition', [])
        api_name = item.get('apiName', '')
        
        # Extract set number from apiName (e.g., TFT14_Item -> 14)
        set_num = None
        match = re.match(r'TFT(\d+)_', api_name)
        if match:
            set_num = int(match.group(1))
        
        # Parse effects/stats - keep as JSON string
        effects = item.get('effects', {})
        effects_json = json.dumps(effects) if effects else ''
        
        item_entry = {
            'set': set_num if set_num else '',
            'item_id': api_name,
            'item_name': item.get('name', ''),
            'item_desc': clean_html_tags(item.get('desc', '')),
            'icon': item.get('icon', ''),
            'component1': composition[0] if len(composition) > 0 else '',
            'component2': composition[1] if len(composition) > 1 else '',
            'num_components': len(composition),
            'unique': item.get('unique', False),
            'effects': effects_json,
        }
        
        items_list.append(item_entry)
    
    print(f"✓ Parsed {len(items_list)} items")
    return items_list


def parse_augments(data):
    """Parse augments from the items array"""
    print("\n--- Parsing Augments ---")
    
    augments_list = []
    
    items = data.get('items', [])
    
    # Filter for augments only
    augments = []
    for item in items:
        api_name = item.get('apiName', '')
        if 'Augment' in api_name or 'TeamupAugment' in api_name:
            augments.append(item)
    
    print(f"Found {len(augments)} augments")
    
    for augment in augments:
        api_name = augment.get('apiName', '')
        
        # Extract set number from apiName (e.g., TFT14_Augment -> 14)
        set_num = None
        match = re.match(r'TFT(\d+)_', api_name)
        if match:
            set_num = int(match.group(1))
        
        # Determine tier from API name or other clues
        tier = 'Unknown'
        
        # Try to determine tier from name or icon
        icon = augment.get('icon', '').lower()
        if '_i.' in icon or 'silver' in icon:
            tier = 'Silver'
        elif '_ii.' in icon or 'gold' in icon:
            tier = 'Gold'
        elif '_iii.' in icon or 'prismatic' in icon:
            tier = 'Prismatic'
        
        augment_entry = {
            'set': set_num if set_num else '',
            'augment_id': api_name,
            'augment_name': augment.get('name', ''),
            'augment_desc': clean_html_tags(augment.get('desc', '')),
            'tier': tier,
            'icon': augment.get('icon', ''),
            'associated_traits': ', '.join(augment.get('associatedTraits', [])),
        }
        
        augments_list.append(augment_entry)
    
    print(f"✓ Parsed {len(augments_list)} augments")
    return augments_list


def upload_to_minio(file_paths):
    """Upload CSV files to MinIO bucket bronze/csv"""
    print("\n--- Uploading to MinIO ---")
    
    try:
        # Configure S3 client for MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # Upload each file
        bucket_name = 'bronze'
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            s3_key = f'csv/{file_name}'
            
            if os.path.exists(file_path):
                s3_client.upload_file(file_path, bucket_name, s3_key)
                print(f"✓ Uploaded {file_name} to s3a://{bucket_name}/{s3_key}")
            else:
                print(f"✗ File not found: {file_path}")
        
        print(f"\n✓ All files uploaded to MinIO successfully!")
        print(f"   Access path: s3a://bronze/csv/")
        
    except Exception as e:
        print(f"\n✗ Error uploading to MinIO: {str(e)}")
        print(f"   Note: Files are still saved locally in /opt/test_data/")
        raise


def save_to_csv(units, traits, items, augments):
    """Save all data to CSV files"""
    print("\n--- Saving to CSV ---")
    
    os.makedirs('/opt/test_data', exist_ok=True)
    
    csv_files = []
    
    # Save units
    if units:
        df_units = pd.DataFrame(units)
        df_units = df_units.sort_values(['set', 'cost', 'unit_name'])
        units_path = '/opt/test_data/units.csv'
        df_units.to_csv(units_path, index=False, encoding='utf-8')
        csv_files.append(units_path)
        print(f"✓ Saved {len(units)} units to {units_path}")
    
    # Save traits
    if traits:
        df_traits = pd.DataFrame(traits)
        df_traits = df_traits.sort_values(['set', 'trait_name'])
        traits_path = '/opt/test_data/traits.csv'
        df_traits.to_csv(traits_path, index=False, encoding='utf-8')
        csv_files.append(traits_path)
        print(f"✓ Saved {len(traits)} traits to {traits_path}")
    
    # Save items
    if items:
        df_items = pd.DataFrame(items)
        df_items = df_items.sort_values(['set', 'item_name'])
        items_path = '/opt/test_data/items.csv'
        df_items.to_csv(items_path, index=False, encoding='utf-8')
        csv_files.append(items_path)
        print(f"✓ Saved {len(items)} items to {items_path}")
    
    # Save augments
    if augments:
        df_augments = pd.DataFrame(augments)
        df_augments = df_augments.sort_values(['set', 'tier', 'augment_name'])
        augments_path = '/opt/test_data/augments.csv'
        df_augments.to_csv(augments_path, index=False, encoding='utf-8')
        csv_files.append(augments_path)
        print(f"✓ Saved {len(augments)} augments to {augments_path}")
    
    # Upload to MinIO
    upload_to_minio(csv_files)
    
    return df_units, df_traits, df_items, df_augments


def show_summary(df_units, df_traits, df_items, df_augments):
    """Show summary of parsed data"""
    print("\n" + "="*70)
    print("PARSING COMPLETE!")
    print("="*70)
    
    if df_units is not None:
        print(f"\n📊 UNITS ({len(df_units)} total)")
        print(f"   Sets: {sorted(df_units['set'].unique())}")
        print(f"\n   Cost distribution:")
        print(df_units['cost'].value_counts().sort_index().to_string())
        print(f"\n   Sample units (Set 14):")
        set14_units = df_units[df_units['set'] == 14]
        sample = set14_units[['unit_name', 'cost', 'all_traits']].head(10)
        print(sample.to_string(index=False))
    
    if df_traits is not None:
        print(f"\n📊 TRAITS ({len(df_traits)} total)")
        print(f"   Sets: {sorted(df_traits['set'].unique())}")
        print(f"\n   Sample traits (Set 14):")
        set14_traits = df_traits[df_traits['set'] == 14]
        sample = set14_traits[['trait_name']].head(15)
        print(sample.to_string(index=False))
    
    if df_items is not None:
        print(f"\n📊 ITEMS ({len(df_items)} total)")
        items_with_set = df_items[df_items['set'] != '']
        if len(items_with_set) > 0:
            print(f"   Sets: {sorted(items_with_set['set'].unique())}")
        print(f"   Components (0 components): {len(df_items[df_items['num_components'] == 0])}")
        print(f"   Completed items (2 components): {len(df_items[df_items['num_components'] == 2])}")
    
    if df_augments is not None:
        print(f"\n📊 AUGMENTS ({len(df_augments)} total)")
        augments_with_set = df_augments[df_augments['set'] != '']
        if len(augments_with_set) > 0:
            print(f"   Sets: {sorted(augments_with_set['set'].unique())}")
        tier_counts = df_augments['tier'].value_counts()
        for tier, count in tier_counts.items():
            print(f"   {tier}: {count}")


def main():
    print("="*70)
    print("COMMUNITY DRAGON TFT DATA PARSER")
    print("="*70)
    
    # Load data
    data = load_cdragon_data()
    
    # Parse all data
    units = parse_units(data)
    traits = parse_traits(data)
    items = parse_items(data)
    augments = parse_augments(data)
    
    # Save to CSV
    df_units, df_traits, df_items, df_augments = save_to_csv(units, traits, items, augments)
    
    # Show summary
    show_summary(df_units, df_traits, df_items, df_augments)


if __name__ == "__main__":
    main()
