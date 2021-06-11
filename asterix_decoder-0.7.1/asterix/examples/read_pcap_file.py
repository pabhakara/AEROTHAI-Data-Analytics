__author__ = 'dsalanti'

import asterix
import dpkt
#from pcapng import FileScanner

# Read example file from packet resources
sample_filename = asterix.get_sample_file('batrsda01li_CAT62.pcapng')

path = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Data samples/ADS-B Data PCAP/'
filename = 'ADSB13112020.pcap'
#print('batrsda01li_CAT62.pcapng')/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Data samples/asterix_decoder-0.7.1/asterix
sample_filename = path + filename
with open(sample_filename, 'rb') as f:
    pcap = dpkt.pcap.Reader(f)

    cntr = 1
    for ts, buf in pcap:
        eth = dpkt.ethernet.Ethernet(buf)
        data = eth.ip.udp.data

        hexdata = ":".join("{:02x}".format(ord(c)) for c in str(data))
        print('Parsing packet %d : %s' % (cntr, hexdata))
        cntr += 1

        # Parse data
        parsed = asterix.parse(data)
        print(parsed)
