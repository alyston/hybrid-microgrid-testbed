"""Recompute statistics from supplied deidentified extracts: python reproduce_statistics.py."""
from pathlib import Path
import json, hashlib
import numpy as np
import pandas as pd
P=Path(__file__).resolve().parent
def summary(x):
 x=np.asarray(x,dtype=float)
 return dict(n=len(x),mean=float(x.mean()),median=float(np.median(x)),p95=float(np.quantile(x,.95)),p99=float(np.quantile(x,.99)),min=float(x.min()),max=float(x.max()))
mb=pd.read_csv(P/'modbus_matched_operations.csv');assert len(mb)==2704
external=(mb.physical_response_relative_ns-mb.physical_request_relative_ns)/1e6
internal=(mb.virtual_response_relative_ns-mb.virtual_request_relative_ns)/1e6
assert ((mb.physical_request_relative_ns<=mb.virtual_request_relative_ns)&(mb.virtual_request_relative_ns<=mb.virtual_response_relative_ns)&(mb.virtual_response_relative_ns<=mb.physical_response_relative_ns)).all()
e=pd.read_csv(P/'eip_nominal_deidentified.csv');sc=e[e.operation=='SCAN_SUMMARY']
assert len(sc)==1264 and int((sc.scan_total_ms<=200).sum())==1263
assert int((e.status=='stale_write').sum())==168
for r in json.loads((P/'source_snapshots_manifest.json').read_text(encoding='utf-8')):
 assert hashlib.sha256((P/r['archive_file']).read_bytes()).hexdigest()==r['sha256']
result={'modbus_external_ms':summary(external),'modbus_internal_ms':summary(internal),'modbus_pooled_ms':summary(np.r_[external,internal]),'eip_scans_ms':summary(sc.scan_total_ms),'eip_budget_fraction_pct':100*int((sc.scan_total_ms<=200).sum())/len(sc),'eip_stale_writes':int((e.status=='stale_write').sum())}
(P/'recomputed_results.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
print('Verified April acquisitions: 2704 operations; 5408 segment pairs; 1264 scans; 168 stale writes.')
