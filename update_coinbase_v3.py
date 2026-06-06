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
    if 'COINBASE_API_KEY=' in line.upper():
        continue
    elif 'COINBASE_SECRET' in line.upper() or 'ORGANIZATION_ID' in line.upper():
        continue
    new_lines.append(line)

new_lines.append('')
print(f'Adding new v3 credentials...\n')
new_lines.append('# === NEW v3 API Credentials (requires private key extraction) ===')
new_lines.append(f'ORGANIZATION_ID={orc_id}')
new_lines.append("COINBASE_API_KEY=***  # placeholder for actual ID")
new_lines.append('COINBASE_PRIVATE_KEY_PATH=/mnt/c/Users/AMD/Downloads/cdp_api_key.json')

with open(env_file, 'w') as f:
    f.write('\n'.join(new_lines))

print(f'Updated .env now has {len(new_lines)} lines')