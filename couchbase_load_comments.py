import json, os, sys
from couchbase_v2.bucket import Bucket
from couchbase.exceptions import CouchbaseError
from couchbase_core.cluster import PasswordAuthenticator

COUCHBASE_CREDS_F = "couchbase_creds.json"
THING_JSON_DIR = "thing_comments_json"

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

for root, dirs, files in os.walk(args.thing_json_dir):
    for fn in files:
        fullfn = os.path.join(root, fn)
        print(f"Loading {fullfn}")
        with open(fullfn, 'r') as f:
            jn = json.load(f)
            fnid = fn.split(".")[0]
            thing_bucket.upsert(fnid, jn)
