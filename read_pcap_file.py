__author__ = 'dsalanti'

import asterix
import dpkt

#path = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Data samples/'
#filename = 'wireshark_sbapmtn01li_OPS_CAT62_2021-06-11.pcap'

path = '/Users/pongabha/Dropbox/Workspace/AEROTHAI Data Analytics/Data samples/'
filename = 'test.pcap'

# Read example file from packet resources
#sample_filename = asterix.get_sample_file(path+filename)

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
        parsed = asterix.parse(data,Verbose=False)
        print(parsed)
