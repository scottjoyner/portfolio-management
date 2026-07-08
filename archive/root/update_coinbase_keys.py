#!/usr/bin/env python3
"""
Update .env with new Coinbase v3 API credentials from downloaded JSON
"""
import json, os
from pathlib import Path

# Read the new key file
key_file = '/mnt/c/Users/AMD/Downloads/cdp_api_key.json'
if not Path(key_file).exists():
    print(f'{key_file} not found')
    exit(1)

with open(key_file, 'r') as f:
    key_data = json.load(f)

print('New key format:')
print(f'Name: {key_data["name"]}')
print(f'Private Key: REDUCTED\n')

# Extract the API key ID from the name
name_parts = key_data['name'].split('/')
api_key_id = name_parts[-1].replace('apiKeys/', '') if 'apiKeys/' in key_data['name'] else ''
orc_id = name_parts[0] if len(name_parts) > 0 and name_parts[0] != '' else ''

print(f'Organization ID: {orc_id}')
print(f'API Key ID: {api_key_id}\n')

# Update .env file
env_file = Path('/home/falcon/git/portfolio-management/.env')
if not env_file.exists():
    print('No .env file found')
    exit(1)

with open(env_file, 'r') as f:
    content = f.read()

# Replace the Coinbase API key
old_key_pattern = 'COINBASE_API_KEY=9c346b...de7d'
new_key = api_key_id if api_key_id else 'YOUR_NEW_API_KEY_ID_HERE'
content = content.replace(old_key_pattern, f'COINBASE_API_KEY={new_key}')

# Also update the organization ID field if it exists
if 'ORGANIZATION_ID=' not in content:
    content += '\nORGANIZATION_ID=' + orc_id + '\n'

with open(env_file, 'w') as f:
    f.write(content)

print('Updated .env file:')
for line in content.split('\n'):
    if 'COINBASE' in line or 'ORGANIZATION' in line:
        print(line)

print(f'\nSaved to: {env_file}')