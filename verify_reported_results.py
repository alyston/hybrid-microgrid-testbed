"""Check the expanded reanalysis against published values, not simulated results."""
from pathlib import Path
import json, csv
import pandas as pd
P=Path(__file__).resolve().parent
r=json.loads((P/'expanded_reanalysis_results.json').read_text(encoding='utf-8'));checks=[]
def close(name,actual,expected,digits=2):
    assert abs(actual-expected)<=.5001*10**(-digits),(name,actual,expected)
    checks.append({'check':name,'actual':actual,'reported':expected,'decimal_places':digits})
def statrow(name,x,n,mean,median,p95,maxv=None,digits=2):
    assert x['n']==n,(name,x['n'],n)
    for k,v in [('mean',mean),('median',median),('p95',p95)]:close(name+' '+k,x[k],v,digits)
    if maxv is not None:close(name+' max',x['max'],maxv,digits)

statrow('Table 8 MB read',r['MB']['application']['4'],1370,18.85,15.09,35.47)
statrow('Table 8 MB write',r['MB']['application']['16'],1334,20.45,15.18,45.55)
statrow('Table 8 EIP read',r['EIP']['application']['READ_COM_REF'],7584,5.77,5.77,7.31)
statrow('Table 8 EIP write',r['EIP']['application']['WRITE_STATUS_FEEDBACK'],7584,11.66,12.08,13.67)
for proto,key,sd,p99 in [('MB','4',13.45,36.97),('MB','16',14.24,45.93),('EIP','READ_COM_REF',2.53,11.86),('EIP','WRITE_STATUS_FEEDBACK',2.33,18.87)]:
    close('Table 8 '+proto+' '+key+' population SD',r[proto]['application'][key]['std_population'],sd);close('Table 8 '+proto+' '+key+' p99',r[proto]['application'][key]['p99'],p99)
for key,n,mean,med,p95,mx in [('read_sweep',187,850.32,847.09,866.20,885.48),('write_sweep',222,865.46,856.20,953.85,1090.26),('gap',222,128.72,125.16,148.05,234.42),('cycle',222,994.18,980.58,1084.78,1265.99)]:
    statrow('Table 9 '+key,r['MB']['application']['reconstructed'][key],n,mean,med,p95,mx)
assert r['MB']['application']['reconstructed']['exceed_1000_ms']==41
for key,mean,med,p95,mx in [('scan_write_ms',70.09,69.71,78.36,86.94),('scan_latency_ms',50.56,50.55,51.05,51.76),('scan_read_ms',34.75,34.99,41.21,216.26),('scan_total_ms',155.40,155.26,165.47,342.32)]:
    statrow('Table 10 '+key,r['EIP']['application']['phases'][key],1264,mean,med,p95,mx)
assert r['EIP']['application']['within_200_ms']==1263 and r['EIP']['application']['stale_writes']==168
for proto,key,n,mean,med,p95,mx in [('EIP','physical_input',134070,10.003,10.004,10.239,11.389),('MB','physical_input',138614,10.001,10.006,10.318,11.671),('EIP','command_cycle',30368,50,50.011,50.695,59.307),('MB','command_cycle',27724,49.999,50.002,50.770,55.569),('EIP','controller_request',29417,50.237,50.038,62.148,103.892),('MB','controller_request',2704,489.587,471.554,937.503,1229.919)]:
    v=r[proto]['routers'][key];assert v['events']==n
    statrow('Table 11 '+proto+' '+key,v['positive_inter_event_ms'],n-6,mean,med,p95,mx,3)
for proto,n,dur,gp,kind,pairs,mean,p95,p99,rtx in [('EIP',133824,251.83,238.20,'EtherNetIP',22667,5.72,7.14,11.90,12),('MB',16945,228.89,20.98,'Modbus',5408,20.04,45.23,51.35,0)]:
    v=r[proto]['captures']['EMS'];assert v['complete_packets']==n and v['tcp_payload_sequence_repeats']==rtx and v['rtt_ms'][kind]['n']==pairs
    close('Table 12 '+proto+' duration',v['duration_s'],dur);close('Table 12 '+proto+' goodput',v['industrial_payload_goodput_kbps'],gp)
    for key,val in [('mean',mean),('p95',p95),('p99',p99)]:close('Table 12 '+proto+' '+key,v['rtt_ms'][kind][key],val)
for proto,lo,hi,mean,p95lo,p95hi,p99lo,p99hi,gp in [('EIP',22345,22345,10.003,10.217,10.221,10.318,10.322,70.38),('MB',22927,22933,10.002,10.296,10.298,10.421,10.425,70.39)]:
    arr=[r[proto]['captures'][d]['dominant_230xx_udp'] for d in ['POI','BESS','GMG','PV','LD0','LD1']]
    # Report each endpoint's exact values; published ranges are checked at their precision.
    assert min(a['packets'] for a in arr)==lo and max(a['packets'] for a in arr)==hi
    close('Table 13 '+proto+' mean',sum(a['iat_ms']['mean'] for a in arr)/6,mean,3)
    for field,mn,mx in [('p95',p95lo,p95hi),('p99',p99lo,p99hi)]:
        close('Table 13 '+proto+' minimum '+field,min(a['iat_ms'][field] for a in arr),mn,3);close('Table 13 '+proto+' maximum '+field,max(a['iat_ms'][field] for a in arr),mx,3)
    close('Table 13 '+proto+' goodput',sum(a['payload_goodput_kbps'] for a in arr)/6,gp)

pk=pd.read_csv(P/'nominal_evidence/MB_EMS_packets.csv.gz',keep_default_na=False)
mb=pd.read_csv(P/'modbus_matched_operations.csv')
first=mb.iloc[0]
req=pk[(pk.mb_tid.astype(str)==str(int(first.transaction_id)))&(pk.mb_uid.astype(str)==str(int(first.unit_id)))&(pk.dst_port.astype(str)=='502')]
offset=int(req.relative_ns.min())-int(first.physical_request_relative_ns)
observed=set(zip(pk.relative_ns.astype('int64'),pk.payload_sha256))
for x in mb.itertuples():
    for field,digest in [('physical_request_relative_ns',x.request_sha256),('virtual_request_relative_ns',x.request_sha256),('virtual_response_relative_ns',x.response_sha256),('physical_response_relative_ns',x.response_sha256)]:
        assert (int(getattr(x,field))+offset,digest) in observed
out={'status':'PASS','checked_table_groups':[8,9,10,11,12,13],'rounded_numeric_checks':len(checks),'modbus_operations_verified_at_four_packet_boundaries':len(mb),'new_experiments':False,'checks':checks}
(P/'verification_report.json').write_text(json.dumps(out,indent=2),encoding='utf-8')
print(json.dumps({k:v for k,v in out.items() if k!='checks'}))
