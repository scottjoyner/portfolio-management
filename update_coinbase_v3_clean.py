#!/usr/bin/env python3
import json
from pathlib import Path

key_file = '/mnt/c/Users/AMD/Downloads/cdp_api_key.json'
with open(key_file, 'r') as f:
    key_data = json.load(f)

name = key_data['name']
orc_id = name.split('/')[0].replace('organizations/', '')
api_key_id = name.split('/')[-1].replace('apiKeys/', '')

print(f'Organization ID: {orc_id}')
print(f'API Key ID: {api_key_id}\n')

env_file = Path('/home/falcon/git/portfolio-management/.env')
with open(env_file, 'r') as f:
    lines = [l.strip() for l in f.readlines()]

new_lines = []
for line in lines:
    if any(x in line.upper() for x in ['COINBASE_API_KEY', 'COINBASE_SECRET', 'ORGANIZATION_ID']):
        continue  # Remove old v3 entries
    new_lines.append(line)

new_lines.append('')
new_lines.append('# === NEW v3 API Credentials (requires private key extraction) ===')
new_lines.append(f'ORGANIZATION_ID={orc_id}')
new_lines.append("COINBASE_API_KEY=***  # placeholder - replace with actual ID from downloaded JSON")
new_lines.append('COINBASE_PRIVATE_KEY_PATH=/mnt/c/Users/AMD/Downloads/cdp_api_key.json')

with open(env_file, 'w') as f:
    f.write('\n'.join(new_lines))

print(f'Updated .env now has {len(new_lines)} lines\n')
print('Added v3 credentials:')
for line in new_lines[-4:]:
    print(line)

# Also update the private key path file to point to actual location
private_key_file = '/mnt/c/Users/AMD/Downloads/cdp_api_key.json'
if Path(private_key_file).exists():
    with open(private_key_file, 'r') as f:
        content = f.read()
    print(f'\nPrivate key file exists: {private_key_file}')
    print(f'File size: {len(content)} chars')