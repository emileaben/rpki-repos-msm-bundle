#!/usr/bin/env python3
import sys
import socket
import json
import requests
import time
import os
import urllib
from ripe.atlas.cousteau import (
    AtlasCreateRequest,
    AtlasSource,
    Traceroute,
)

API_ENDPOINT="http://jdr.aws.nlnetlabs.nl/api/v1/uris"
MSM_LOG="rpki-repo-measurements.jsonf"

RSYNC_PORT=873
RRDP_PORT=443

# read atlas key
KEY=""
with open("/Users/eaben/.atlas/auth") as inf:
    KEY = inf.readline()
    KEY = KEY.rstrip('\n')

## build state from the msm log
msm_state = {}
if not os.path.exists( MSM_LOG ):
    with open(MSM_LOG, 'w'): pass
else:
    with open(MSM_LOG, 'r') as inf:
        # read per line
        for line in inf:
            data = json.loads( line )
            key = ( data['hostname'], data['port'], data['af'] )
            if 'action' in data:
                if data['action'] == 'started':
                    msm_state[ key ] = {'msm_id': data['msm_id'] }
                elif data['action'] == 'stopped': # to be implemented
                    del( msms_state[ key ] ) # capstone

log_fh = open(MSM_LOG, 'a')

# load template
with open("template.json") as inf:
    msm_def = json.load( inf )

r = requests.get(API_ENDPOINT)
d = r.json()

def measure( port, hostname, afs ):
    for af in afs:
        if (hostname,port,af) in msm_state:
            msm_state[ (hostname,port,af) ]['state'] = 'active'
        else:
            # the mapping between API and cousteau drives me nuts!
            msm_def['definitions'][0]['port'] = port
            msm_def['definitions'][0]['target'] = hostname
            msm_def['definitions'][0]['af'] = af
            traceroute = Traceroute( ** msm_def['definitions'][0] )
            source = AtlasSource( ** msm_def['probes'][0] )
            start = int( time.time() ) + 60
            atlas_request = AtlasCreateRequest(
                key = KEY,
                measurements = [traceroute],
                sources = [source],
                start_time = start
            )
            (is_success, response) = atlas_request.create()
            msm_id = None
            if is_success:
                msm_id = response['measurements'][0]
                json.dump({
                    'hostname': hostname,
                    'port': port,
                    'af': af,
                    'action': 'started',
                    'ts': start,
                    'msm_id': msm_id
                }, log_fh )
                log_fh.write("\n")
                msm_state[ (hostname,port,af) ] = {'msm_id': msm_id, 'state': 'new'}
                print("new msm created for %s %s %s (id: %s)" % (hostname,port,af,msm_id), file=sys.stderr )
            else:
                print("msm start failed for %s %s %s: %s" % ( hostname, port, af, response ), file=sys.stderr )

def do_checks( url, scheme ):
    # scheme is one of 'rrdp' or 'rsync'
    u = urllib.parse.urlparse( url )
    hostname = u.hostname
    if not hostname:
        return   
    port = u.port
    afs = set()
    try:
        ips = list(map(lambda x: x[4][0], socket.getaddrinfo(hostname,None)))
    except:
        print("hostlookup failed: %s" % hostname)
        return
    #print( ips )
    for ip in ips:
        if ':' in ip:
            afs.add( 6 )
        else:
            afs.add( 4 )
    if not port: # and u.scheme==scheme:
        if scheme=='rrdp':
            port = RRDP_PORT
        elif scheme=='rsync':
            port = RSYNC_PORT
    if port and hostname and len( afs ) > 0: # afs==0 = local dns resolution problem????
        measure( port, hostname, afs )

for entry in d['data']:
    #print(entry)
    for scheme in ('rrdp','rsync'):
        if scheme in entry: 
            do_checks( entry[scheme], scheme )

for entry,val in msm_state.items():
    # if not state started/new we need to stop (directly or at some point?)
    if 'state' in val and val['state'] in ('new','active'):
        pass
    else:
        print("WARN: state not new or active: %s, %s" % (entry,val))
