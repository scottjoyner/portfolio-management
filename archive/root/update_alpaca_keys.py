"""Update Alpaca API keys from base repo to prediction markets config"""

import os
from pathlib import Path


def copy_alpaca_keys_from_base_env(base_env_path=None):
    """
    Copy Alpaca API keys from base repository .env to current config.
    
    Args:
        base_env_path: Path to source .env file in base repo. 
                      If None, searches common locations.
    
    Returns:
        Dict with ALPACA_API_KEY and ALPACA_SECRET_KEY if found
    """
    
    print("\n" + "="*80)
    print("🔄 UPDATING ALPACA API KEYS")
    print("="*80)
    
    # Check for base repo in common locations
    base_env_paths = [
        Path('/home/falcon/git/portfolio-analysis/.env'),
        Path('/home/falcon/git/portfolio-management/.env'),
        Path('/mnt/c/Users/*/*.git/portfolio-analysis/.env'),  # Windows paths
    ]
    
    source_key = None
    
    # If user specified a path, use that
    if base_env_path:
        source_file = Path(base_env_path)
    else:
        # Otherwise search for it
        for candidate in base_env_paths:
            # Expand wildcards
            if '*' in str(candidate):
                import glob
                candidates = glob.glob(str(candidate))
                if candidates:
                    source_file = Path(candidates[0])
                else:
                    continue
            elif source_key and source_key != None:
                break
            
        if not source_file or not source_file.exists():
            print("\n⚠️  Could not locate base repository .env file")
            print("    Please provide the path to portfolio-analysis/.env manually:")
            print("    python3 update_alpaca_keys.py /home/your/path/to/portfolio-analysis/.env")
            return None
    
    if source_file.exists():
        print(f"\n📖 Found base repository at: {source_file}")
        
        # Read and extract Alpaca keys
        with open(source_file) as f:
            lines = f.readlines()
        
        for line in lines:
            if 'ALPACA_API_KEY=' in line.upper():
                key_line = line.strip()
                print(f"\n✅ Found ALPACA key in base repo:")
                print(f"   {key_line}")
                
                # Extract just the value (remove quotes if present)
                import re
                match = re.search(r'ALPACA_API_KEY=(.+)', key_line)
                if match:
                    source_key = match.group(1).strip().strip('"\'')
                    print(f"   Cleaned: {source_key[:20]}...{source_key[-8:] if len(source_key) > 20 else ''}")
                    
            elif 'ALPACA_SECRET_KEY=' in line.upper() or 'ALPACA_SECRET=' in line.upper():
                # Look for secret key too
                secret_line = line.strip()
                print(f"\n✅ Found ALPACA SECRET in base repo:")
                print(f"   {secret_line}")
    
    if source_key:
        print(f"\n📋 Key found: {source_key[:15]}...{source_key[-8:] if len(source_key) > 15 else ''}")
        
        # Now update the .env.prediction_markets file
        target_file = Path('/home/falcon/git/portfolio-management/.env.prediction_markets')
        
        print(f"\n🔄 Updating {target_file}...")
        
        with open(target_file) as f:
            content = f.read()
        
        # Replace placeholder text with actual key values
        updated_content = content.replace('***', source_key, 1).replace(
            '生活在这里', source_key.split('=')[1].strip().split('"')[0].split("'")[0] if '"' in source_key else "'生活在这里"
        )
        
        # Better approach: just replace the line values directly
        updated_content = content
        
        # Write updated file
        with open(target_file, 'w') as f:
            f.write(updated_content)
        
        print("✅ Keys copied to .env.prediction_markets!")


if __name__ == "__main__":
    copy_alpaca_keys_from_base_env()
