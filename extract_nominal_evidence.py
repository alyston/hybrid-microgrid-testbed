"""Export April records without network addresses or payload values.
Usage: python extract_nominal_evidence.py --data-root /path/to/Dados
The original acquisition files are read-only. No controller/network is contacted.
"""
from pathlib import Path
import argparse, csv, gzip, hashlib, json, struct
import numpy as np
import pandas as pd
from packet_fields import parse_packet, modbus_parse, eip_parse

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def packets(path,audit):
    with path.open('rb') as f:
        h=f.read(24)
        opts={b'\xd4\xc3\xb2\xa1':('<',1000),b'\xa1\xb2\xc3\xd4':('>',1000),b'\x4d\x3c\xb2\xa1':('<',1),b'\xa1\xb2\x3c\x4d':('>',1)}
        endian,factor=opts[h[:4]];link=struct.unpack(endian+'I',h[20:24])[0]
        while True:
            ph=f.read(16)
            if not ph:break
            if len(ph)<16:
                audit['truncated_tail_bytes']=len(ph);break
            sec,frac,cap,wire=struct.unpack(endian+'IIII',ph);data=f.read(cap)
            if len(data)<cap:
                audit['truncated_tail_bytes']=16+len(data);audit['incomplete_record_expected_bytes']=16+cap;break
            yield sec*10**9+frac*factor,cap,wire,link,data

def extract(data_root,out):
    out.mkdir(exist_ok=True);provenance=[]
    def record(p,target,n,extra):
        provenance.append(dict(source=p.relative_to(data_root).as_posix(),source_sha256=sha(p),source_bytes=p.stat().st_size,extract=target.relative_to(out).as_posix(),extract_sha256=sha(target),rows=n,**extra))
    for proto in ['EIP','MB']:
        root=data_root/f'Dados_Simul_30_04_26_{proto}'/f'Dados_Simul_30_04_26_{proto}'
        for dev in ['EMS','POI','BESS','GMG','PV','LD0','LD1']:
            p=root/f'capture{dev}.pcap';target=out/f'{proto}_{dev}_packets.csv.gz'
            ids={};contexts={};fields=['frame','relative_ns','captured_bytes','wire_bytes','transport','src','dst','src_port','dst_port','seq','ack','flags','tcp_window','payload_bytes','payload_sha256','mb_tid','mb_uid','mb_fc','eip_cmd','eip_context']
            unknown=fragmented=0;t0=None;n=0;tail={}
            with gzip.open(target,'wt',encoding='utf-8',newline='') as g:
                w=csv.DictWriter(g,fieldnames=fields);w.writeheader()
                for n,(t,cap,wire,link,raw) in enumerate(packets(p,tail),1):
                    if t0 is None:t0=t
                    v=parse_packet(raw,link) or {};l4=v.get('l4','OTHER')
                    if v.get('frag_offset',0):fragmented+=1
                    row=dict(frame=n,relative_ns=t-t0,captured_bytes=cap,wire_bytes=wire,transport=l4)
                    for k in ['src','dst']:
                        addr=v.get(k+'_ip')
                        if addr is not None:
                            if addr not in ids:ids[addr]=f'host{len(ids)+1}'
                            row[k]=ids[addr]
                    for k in ['src_port','dst_port','seq','ack','tcp_window']:
                        if k in v:row[k]=v[k]
                    if 'tcp_flags' in v:row['flags']=v['tcp_flags']
                    payload=v.get('payload',b'');row['payload_bytes']=len(payload)
                    ports={v.get('src_port'),v.get('dst_port')}
                    if payload and l4=='TCP' and (502 in ports or 44818 in ports):
                        row['payload_sha256']=hashlib.sha256(payload).hexdigest()
                    if payload and l4=='TCP' and 502 in ports:
                        m=modbus_parse(payload)
                        if m:row.update({k:m[k] for k in ['mb_tid','mb_uid','mb_fc']})
                        else:unknown+=1
                    if payload and l4=='TCP' and 44818 in ports:
                        e=eip_parse(payload)
                        if e:
                            c=e['eip_context']
                            if c not in contexts:contexts[c]=len(contexts)+1
                            row.update(eip_cmd=e['eip_cmd'],eip_context=contexts[c])
                        else:unknown+=1
                    w.writerow(row)
            record(p,target,n,dict(clock='capture-local; relative nanoseconds from first packet',unparsed_industrial_payload_packets=unknown,fragment_offsets_nonzero=fragmented,**tail))
        for dev in ['POI','BESS','GMG','PV','LD0','LD1']:
            p=root/f'device_router_metrics{dev}.csv';d=pd.read_csv(p,keep_default_na=False)
            ts=pd.to_datetime(d.timestamp,format='ISO8601',utc=True).astype('int64');d.insert(0,'source_row',np.arange(2,len(d)+2));d.insert(1,'relative_ns',ts-ts.iloc[0])
            # Free-form and absolute-time fields are excluded; numeric/boolean status evidence is retained.
            for col in ['error','last_error']:
                d[col+'_present']=d[col].astype(str).ne('')
            drop=['timestamp','peer_addr','client_addr','last_source','mem_update_at','rx_updated_at','error','last_error']
            d=d.drop(columns=[c for c in drop if c in d]);target=out/f'{proto}_{dev}_router.csv.gz';d.to_csv(target,index=False,compression='gzip')
            record(p,target,len(d),dict(clock='Go log-local relative nanoseconds; logged intervals retained'))
        p=root/'plc_scan_metrics.csv';d=pd.read_csv(p,keep_default_na=False)
        ts=pd.to_datetime(d.timestamp,format='ISO8601',utc=True).astype('int64');d.insert(0,'source_row',np.arange(2,len(d)+2));d.insert(1,'relative_ns',ts-ts.iloc[0]);d['error_present']=d.error.astype(str).ne('')
        d=d.drop(columns=[c for c in ['timestamp','client_addr','virtual_ip','details','error'] if c in d]);target=out/f'{proto}_EMS_application.csv.gz';d.to_csv(target,index=False,compression='gzip')
        record(p,target,len(d),dict(clock='Go application-log local relative nanoseconds'))
    (out/'PROVENANCE.json').write_text(json.dumps(provenance,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(dict(files=len(provenance),records=sum(x['rows'] for x in provenance),unparsed_payload_packets=sum(x.get('unparsed_industrial_payload_packets',0) for x in provenance)),indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--data-root',type=Path,required=True);parser.add_argument('--output',type=Path,default=Path(__file__).resolve().parent/'nominal_evidence');a=parser.parse_args();extract(a.data_root,a.output)
