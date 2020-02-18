import os, sys, time, logging
import requests
import json
from pprint import PrettyPrinter
pp = PrettyPrinter(indent=4)
from thingiverse import Thingiverse
try:
    with open('CLIENTSECRET', 'r') as f:
        client_secret = f.read().strip()
except IOError:
    logging.error("Couldn't find CLIENTSECRET, bailing.")
    sys.exit(2)
a = Thingiverse({'client_id': '5ffba659ef3f0df1e4b0', 
                 'client_secret': client_secret, 
                 'redirect_uri': 'http://foo'})

token = None
try:
    with open('ACCESSTOKEN', 'r') as f:
        token = f.read().strip()
except IOError:
    logging.warning("Couldn't find access token, will request a new one")
a.connect(token)

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", help="increase output verbosity",
                    action="store_true")
parser.add_argument("start", type=int, help="Thing ID to start from", default="2")

args = parser.parse_args()
if args.verbose:
    print("Debug logging")
    logging.basicConfig(level=logging.DEBUG)
else:
    print("Normal logging")
    logging.basicConfig(level=logging.INFO)

stepsize = 500
interval_between_fetchgroups = 600
for group in range(args.start, 2000000, stepsize):
    logging.info("Getting all things in range {}-{}".format(group, group+stepsize-1))
    for thing_id in range(group, group+stepsize):
        thing_fname = 'thing_json/{}.json'.format(thing_id)
        if os.path.exists(thing_fname):
            with open(thing_fname, 'r') as f:
                t = f.read()
        else:
            logging.info("Getting thing {}".format(thing_id))
            while True:
                try:
                    t = a.get_thing(thing_id)
                    break
                except:
                    logging.debug("Exception in get thing")
        if t and 'error' not in t:
            with open(thing_fname, 'w') as f:
                f.write(json.dumps(t))
            # Get comments
            comments_fname = 'thing_comments_json/{}.json'.format(thing_id)
            if os.path.exists(comments_fname):
                with open(comments_fname, 'r') as f:
                    c = f.read()
            else:
                c = a.get_thing_comments(thing_id)
                with open(comments_fname, 'w') as f:
                    f.write(json.dumps(c))
            # Get images
            images = a.get_thing_image(thing_id)
            if images and 'error' not in images:
                for i in images:
                    iid = i['id']
                    for s in i['sizes']:
                        if s['size'] == 'large' and s['type'] == 'display':
                            print("Downloading image {}: {}".format(iid, s['url']))
                            try:
                                iext = s['url'].split('.')[-1]
                                fname = 'images/{}.{}'.format(iid, iext)
                                if not os.path.exists(fname):
                                    idata = None
                                    retries = 0
                                    while idata is None and retries < 10:
                                        idata = requests.get(s['url'], timeout=(8, 120))
                                        retries += 1
                                    print("Received {} bytes".format(len(idata.content)))
                                    with open(fname, 'wb') as f:
                                        f.write(idata.content)
                                else:
                                    print("Image already exists")
                            except:
                                print("Exception downloading, moving on")
                                import traceback
                                traceback.print_exc()

            i = a.get_thing_zip(thing_id)
            if i and 'error' not in i:
                fid = thing_id
                fname = 'files/{}.zip'.format(fid)
                if not os.path.exists(fname):
                    if 'public_url' in i:
                        print("Downloading file {}: {}".format(fid, i['public_url']))
                        try:
                            # fdata = a.get_it(i['public_url'])
                            fdata = None
                            retries = 0
                            while fdata is None and retries < 10:
                                fdata = requests.get(i['public_url'], timeout=(8, 10000))
                                retries += 1
                            print("Received {} bytes".format(len(fdata.content)))
                            with open(fname, 'wb') as f:
                                f.write(fdata.content)
                        except:
                            print("Exception downloading, moving on")
                            import traceback
                            traceback.print_exc()
                else:
                    print("File already exists")
        else:
            print("Error fetching, moving on")
    print("Group done, sleeping {} seconds...".format(interval_between_fetchgroups))
    time.sleep(interval_between_fetchgroups)
