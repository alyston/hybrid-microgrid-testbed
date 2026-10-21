"""Verify supplied reanalysis inputs without requiring original laboratory files."""
from pathlib import Path
import hashlib
import json

root = Path(__file__).resolve().parent
entries = json.loads((root / 'FILE_MANIFEST.json').read_text(encoding='utf-8'))
for entry in entries:
    path = root / entry['file']
    data = path.read_bytes()
    assert len(data) == entry['bytes'], f"Size mismatch: {entry['file']}"
    assert hashlib.sha256(data).hexdigest() == entry['sha256'], f"Hash mismatch: {entry['file']}"
print(f'Verified {len(entries)} supplied input/documentation files.')
