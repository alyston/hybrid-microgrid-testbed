"""Recompute nominal table inputs from the distributed address-free extracts.
Run: python reanalyze_available_evidence.py
No simulation or laboratory experiment is executed.
"""
from pathlib import Path
from collections import defaultdict, deque
import json, hashlib, platform
import numpy as np
import pandas as pd

P=Path(__file__).resolve().parent;D=P/'nominal_evidence'
def stats(v):
    a=np.asarray(v,dtype=float)
    return dict(n=len(a),mean=float(np.mean(a)),median=float(np.median(a)),std_population=float(np.std(a)),p95=float(np.quantile(a,.95)),p99=float(np.quantile(a,.99)),max=float(np.max(a))) if len(a) else dict(n=0)

def application(proto):
    d=pd.read_csv(D/f'{proto}_EMS_application.csv.gz',keep_default_na=False)
    out={}
    if proto=='EIP':
        for op in ['READ_COM_REF','WRITE_STATUS_FEEDBACK']:
            a=d[d.operation==op];out[op]=stats(a.duration_ms)
        a=d[d.operation=='SCAN_SUMMARY']
        out['phases']={c:stats(a[c]) for c in ['scan_write_ms','scan_latency_ms','scan_read_ms','scan_total_ms']}
        out['within_200_ms']=int((a.scan_total_ms<=200).sum());out['scan_count']=len(a)
        stale=d[d.status=='stale_write'];out['stale_writes']=len(stale);out['stale_scan_indices']=int(stale.loop_index.nunique())
    else:
        for fc in [4,16]:out[str(fc)]=stats(d.loc[d.function_code==fc,'duration_ms'])
        seq=['POI','BESS','GMG','PV','LD0','LD1'];sweeps={}
        for fc in [4,16]:
            a=d[d.function_code==fc].reset_index(drop=True);found=[]
            for i in range(len(a)-5):
                q=a.iloc[i:i+6]
                if q.device_name.tolist()==seq:
                    found.append(dict(start_ns=int(q.iloc[0].relative_ns),end_ns=int(q.iloc[-1].relative_ns)+round(float(q.iloc[-1].duration_ms)*1e6)))
            sweeps[str(fc)]=found
        starts=d[(d.function_code==16)&(d.device_name=='POI')].relative_ns.to_numpy(dtype=np.int64)
        cycles=np.diff(starts)/1e6;gaps=[]
        for s in sweeps['16']:
            later=starts[starts>s['start_ns']]
            if len(later):gaps.append((later[0]-s['end_ns'])/1e6)
        out['reconstructed']={'definition':'last request start plus its duration minus first request start; gaps end at next POI write start','read_sweep':stats([(x['end_ns']-x['start_ns'])/1e6 for x in sweeps['4']]),'write_sweep':stats([(x['end_ns']-x['start_ns'])/1e6 for x in sweeps['16']]),'gap':stats(gaps),'cycle':stats(cycles),'exceed_1000_ms':int((cycles>1000).sum())}
    return out

def routers(proto):
    frames=[pd.read_csv(D/f'{proto}_{dev}_router.csv.gz',keep_default_na=False) for dev in ['POI','BESS','GMG','PV','LD0','LD1']]
    all_rows=pd.concat(frames,ignore_index=True);out={}
    filters={'physical_input':(all_rows.event_type=='physical_io')&(all_rows.direction=='eth0_in_from_opal'),'command_cycle':all_rows.event_type=='command_cycle','controller_request':all_rows.event_type=='modbus_request'}
    for k,mask in filters.items():
        a=all_rows[mask]
        out[k]={'events':len(a),'positive_inter_event_ms':stats(a.loc[a.inter_event_ms>0,'inter_event_ms']),'local_duration_ms':stats(a.duration_ms),'error_rows':int(a.error_present.astype(str).str.lower().eq('true').sum())}
    skips=0
    for f in frames:
        a=f[(f.event_type=='physical_io')&(f.direction=='eth0_in_from_opal')]
        skips+=int((np.diff(a.rx_version.to_numpy(dtype=np.int64))!=1).sum())
    out['nonunit_receive_version_increments']=skips
    return out

def capture(proto,dev):
    df=pd.read_csv(D/f'{proto}_{dev}_packets.csv.gz',dtype=str,keep_default_na=False)
    pending=defaultdict(deque);rtts=defaultdict(list);hi={};flows=defaultdict(list);udpbytes=defaultdict(int)
    retrans=app_packets=app_payload_bytes=app_payload_packets=reset=zero=0
    for row in df.to_dict('records'):
        v={k:int(row[k]) if row[k] else 0 for k in ['relative_ns','src_port','dst_port','payload_bytes','seq','flags','tcp_window','mb_tid','mb_uid','mb_fc','eip_cmd','eip_context']}
        t=v['relative_ns'];sp=v['src_port'];dp=v['dst_port'];pl=v['payload_bytes'];ports={sp,dp};flow=(row['src'],sp,row['dst'],dp)
        if (502 in ports or 44818 in ports or 2222 in ports):
            app_packets+=1;app_payload_bytes+=pl;app_payload_packets+=int(pl>0)
        if row['transport']=='TCP':
            reset+=bool(v['flags']&4);zero+=int(v['tcp_window']==0)
            if pl:
                end=v['seq']+pl
                if flow in hi and v['seq']<hi[flow] and end<=hi[flow]:retrans+=1
                hi[flow]=max(hi.get(flow,0),end)
            for port,kind,valid,identifier in [(502,'Modbus',row['mb_tid']!='',(v['mb_tid'],v['mb_uid'],v['mb_fc']&127)),(44818,'EtherNetIP',row['eip_cmd']!='',(v['eip_cmd'],v['eip_context']))]:
                if valid and port in ports:
                    if dp==port:pending[(kind,flow,identifier)].append(t)
                    elif sp==port:
                        rev=(row['dst'],dp,row['src'],sp);q=pending[(kind,rev,identifier)]
                        if q:
                            t0=q.popleft()
                            if t>=t0:rtts[kind].append((t-t0)/1e6)
        if row['transport']=='UDP' and (23000<=sp<23100 or 23000<=dp<23100):
            flows[flow].append(t);udpbytes[flow]+=pl
    times=df.relative_ns.astype('int64');duration=(times.iloc[-1]-times.iloc[0])/1e9
    out={'complete_packets':len(df),'duration_s':float(duration),'industrial_packets':app_packets,'industrial_payload_packets':app_payload_packets,'industrial_payload_goodput_kbps':app_payload_bytes*8/1000/duration,'tcp_payload_sequence_repeats':retrans,'tcp_resets':int(reset),'tcp_zero_windows':zero,'rtt_ms':{k:stats(a) for k,a in rtts.items()},'unmatched_requests':sum(len(q) for q in pending.values())}
    if flows:
        flow=max(flows,key=lambda k:len(flows[k]));a=np.asarray(flows[flow],dtype=np.int64)
        out['dominant_230xx_udp']={'packets':len(a),'iat_ms':stats(np.diff(a)/1e6),'payload_goodput_kbps':udpbytes[flow]*8/1000/((a[-1]-a[0])/1e9)}
    return out

def main():
    provenance=json.loads((D/'PROVENANCE.json').read_text(encoding='utf-8'))
    for p in provenance:
        assert hashlib.sha256((D/p['extract']).read_bytes()).hexdigest()==p['extract_sha256']
    result={'scope':'Archived acquisitions of 30 April 2026; no new experiment','units':'milliseconds unless otherwise stated','quantiles':'linear interpolation at (n-1)*p','runtime':{'python':platform.python_version(),'numpy':np.__version__,'pandas':pd.__version__}}
    for proto in ['EIP','MB']:
        result[proto]={'application':application(proto),'routers':routers(proto),'captures':{dev:capture(proto,dev) for dev in ['EMS','POI','BESS','GMG','PV','LD0','LD1']}}
    (P/'expanded_reanalysis_results.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print('Recomputed application, router and packet statistics for both April acquisitions.')
if __name__=='__main__':main()
