"""Packet field decoders adapted from the archived April analysis script.
Only Ethernet/cooked IPv4/IPv6 without extension headers and unfragmented TCP/UDP.
No TCP stream reassembly; this limitation is reported in the evidence map.
"""
import struct

def ipv6_addr(b):
    # Compact not important; use colon hex groups
    return ':'.join(f'{struct.unpack("!H", b[i:i+2])[0]:x}' for i in range(0,16,2))

def parse_l2(data, linktype):
    # returns eth_type and payload offset
    if linktype == 1:  # Ethernet
        if len(data) < 14: return None, None
        offset=14
        eth_type=struct.unpack('!H', data[12:14])[0]
        while eth_type in (0x8100,0x88a8,0x9100):
            if len(data)<offset+4: return None,None
            eth_type=struct.unpack('!H', data[offset+2:offset+4])[0]
            offset += 4
        return eth_type, offset
    elif linktype == 113:  # Linux cooked v1
        if len(data) < 16: return None,None
        eth_type=struct.unpack('!H', data[14:16])[0]
        return eth_type, 16
    elif linktype == 276:  # Linux cooked v2, protocol first 2 bytes, fixed 20 bytes
        if len(data) < 20: return None,None
        eth_type=struct.unpack('!H', data[0:2])[0]
        return eth_type, 20
    elif linktype == 228:  # IPv4/IPv6 raw? no L2
        if len(data)<1: return None,None
        ver=data[0]>>4
        return (0x0800 if ver==4 else 0x86dd if ver==6 else None), 0
    else:
        return None, None

def parse_ipv4(data, offset):
    if len(data)<offset+20: return {'eth_type':0x0800,'malformed_ip':1}
    ip_start=offset; vihl=data[ip_start]; ver=vihl>>4; ihl=(vihl&0xf)*4
    if ver!=4 or len(data)<ip_start+ihl: return {'eth_type':0x0800,'malformed_ip':1}
    total_len=struct.unpack('!H', data[ip_start+2:ip_start+4])[0]
    proto=data[ip_start+9]
    src='.'.join(map(str,data[ip_start+12:ip_start+16])); dst='.'.join(map(str,data[ip_start+16:ip_start+20]))
    flags_frag=struct.unpack('!H', data[ip_start+6:ip_start+8])[0]; frag_offset=flags_frag & 0x1fff
    out={'eth_type':0x0800,'ip_version':4,'ip_proto':proto,'src_ip':src,'dst_ip':dst,'ip_total_len':total_len,'ip_payload_len':max(0,total_len-ihl),'frag_offset':frag_offset}
    return parse_l4(data, ip_start+ihl, min(ip_start+total_len,len(data)), out)

def parse_ipv6(data, offset):
    if len(data)<offset+40: return {'eth_type':0x86dd,'malformed_ip':1}
    ip_start=offset
    ver=data[ip_start]>>4
    if ver!=6: return {'eth_type':0x86dd,'malformed_ip':1}
    payload_len=struct.unpack('!H', data[ip_start+4:ip_start+6])[0]
    nxt=data[ip_start+6]
    src=ipv6_addr(data[ip_start+8:ip_start+24]); dst=ipv6_addr(data[ip_start+24:ip_start+40])
    out={'eth_type':0x86dd,'ip_version':6,'ip_proto':nxt,'src_ip':src,'dst_ip':dst,'ip_total_len':payload_len+40,'ip_payload_len':payload_len,'frag_offset':0}
    # Minimal no extension header support; ICMPv6 ignored, TCP/UDP if nxt=6/17
    return parse_l4(data, ip_start+40, min(ip_start+40+payload_len,len(data)), out)

def parse_l4(data, l4_start, ip_end, out):
    proto=out['ip_proto']
    if proto==6 and out.get('frag_offset',0)==0 and len(data)>=l4_start+20:
        sp,dp=struct.unpack('!HH', data[l4_start:l4_start+4])
        seq,ack=struct.unpack('!II', data[l4_start+4:l4_start+12])
        doff=(data[l4_start+12]>>4)*4; flags=data[l4_start+13]
        win=struct.unpack('!H', data[l4_start+14:l4_start+16])[0]
        payload_start=l4_start+doff
        payload=data[payload_start:ip_end] if payload_start<=ip_end else b''
        out.update({'l4':'TCP','src_port':sp,'dst_port':dp,'seq':seq,'ack':ack,'tcp_hdr_len':doff,'tcp_flags':flags,'tcp_window':win,'payload_len':len(payload),'payload':payload})
    elif proto==17 and out.get('frag_offset',0)==0 and len(data)>=l4_start+8:
        sp,dp,ulen,uck=struct.unpack('!HHHH', data[l4_start:l4_start+8])
        payload=data[l4_start+8:min(l4_start+ulen, len(data))]
        out.update({'l4':'UDP','src_port':sp,'dst_port':dp,'udp_len':ulen,'payload_len':len(payload),'payload':payload})
    else:
        out.update({'l4':{1:'ICMP',58:'ICMPv6',6:'TCP-frag',17:'UDP-frag'}.get(proto, f'IP{proto}')})
    return out

def parse_packet(data, linktype):
    eth_type, offset=parse_l2(data, linktype)
    if eth_type is None: return None
    if eth_type==0x0800: return parse_ipv4(data, offset)
    if eth_type==0x86dd: return parse_ipv6(data, offset)
    return {'eth_type':eth_type}

def modbus_parse(payload):
    if len(payload) < 8: return None
    tid, pid, length = struct.unpack('!HHH', payload[:6])
    if pid != 0 or length < 2 or length > 260 or len(payload) < 6+length: return None
    return {'mb_tid':tid,'mb_len':length,'mb_uid':payload[6],'mb_fc':payload[7], 'mb_adu_len':6+length}

def eip_parse(payload):
    if len(payload) < 24: return None
    cmd, length = struct.unpack('<HH', payload[:4])
    valid_cmds={0x0000,0x0001,0x0004,0x0063,0x0064,0x0065,0x0066,0x006f,0x0070,0x0072,0x0073}
    if cmd not in valid_cmds or length > 65511 or len(payload) < 24+length: return None
    return {'eip_cmd':cmd,'eip_len':length,'eip_session':struct.unpack('<I',payload[4:8])[0],'eip_status':struct.unpack('<I',payload[8:12])[0],'eip_context':payload[12:20].hex(),'eip_options':struct.unpack('<I',payload[20:24])[0],'eip_total_len':24+length}
