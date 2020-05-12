import json, os, sys, time
from couchbase_v2.bucket import Bucket
from couchbase.exceptions import CouchbaseError, TempFailException
from couchbase_core.cluster import PasswordAuthenticator

COUCHBASE_CREDS_F = "couchbase_creds.json"
THING_JSON_DIR = "thing_json"

from argparse import ArgumentParser
parser = ArgumentParser()
parser.add_argument("-c", help="Config File", default=COUCHBASE_CREDS_F)
parser.add_argument("-C", help="CouchBase Cluster address (use couchbase:// or couchbases://)", 
                    dest="cluster", required=True)
parser.add_argument("-b", help="Thing Bucket Name", dest="thing_bucket", default="thingiverse_things")
parser.add_argument("-t", help="Things JSON directory", dest="thing_json_dir", default=THING_JSON_DIR)

args = parser.parse_args()

try:
    with open(args.c, 'r') as f:
        COUCHBASE_CREDS = json.load(f)
except IOError:
    print(f"Copy {COUCHBASE_CREDS_F}.example to {args.c}.json and add creds first")
    sys.exit(2)

# Authenticate with cluster
thing_bucket = Bucket(f"{args.cluster}/{args.thing_bucket}", **COUCHBASE_CREDS)

n = 0
for root, dirs, files in os.walk(args.thing_json_dir):
    print(f"Processing {root}")
    donefile = os.path.join(root, ".done")
    if os.path.exists(donefile):
        print(f"Already done")
        continue
    jns = {}
    # Load up the entire root
    for fn in files:
        fullfn = os.path.join(root, fn)
        if n % 100 == 0:
            print(f"Loading {fullfn}\r")
        n += 1
        with open(fullfn, 'r') as f:
            jn = json.load(f)
            retries = 0
            while retries < 10:
                try:
                    thing_bucket.upsert(str(jn['id']), jn)
                    break
                except TempFailException:
                    print("failed")
                    time.sleep(3)
                    retries += 1
                    if 100 == retries:
                        sys.exit(2)
    # Leave a marker in the dir
    with open(donefile, "w") as f:
        f.write("1")
    del jns
    print(f"Done")
