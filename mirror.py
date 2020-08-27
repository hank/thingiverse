import os, sys, time, logging, json, pickle, urllib
from datetime import datetime
import requests
from bitmap import BitMap
from pprint import PrettyPrinter
pp = PrettyPrinter(indent=4)
from thingiverse import Thingiverse


def dirname_from_id(i):
    newfdr = str(i).zfill(8)[0:4]
    return newfdr

try:
    with open('CLIENTSECRET', 'r') as f:
        client_secret = f.read().strip()
except IOError:
    logging.error("Couldn't find CLIENTSECRET, bailing.")
    sys.exit(2)
a = Thingiverse({'client_id': '5ffba659ef3f0df1e4b0', 
                 'client_secret': client_secret, 
                 'redirect_uri': 'http://foo'},
                 loglevel='debug')

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
parser.add_argument("-d", "--deadmap", help="Bitmap file for dead things")
parser.add_argument("start", type=int, help="Thing ID to start from", default="2")

args = parser.parse_args()
if args.verbose:
    print("Debug logging")
    logging.basicConfig(level=logging.DEBUG)
else:
    print("Normal logging")
    logging.basicConfig(level=logging.INFO)

# Deal with dead lists
DEADMAP = None
if args.deadmap:
    try:
        print("Attempting to open deadmap")
        with open(args.deadmap, 'rb') as f:
            DEADMAP = pickle.load(f)
    except IOError:
        print("Creating new deadmap")
        DEADMAP = BitMap(5000000)
else:
    print("No deadmap")

# API calls per window
max_api_calls = 300
# 5 minute windows
window_seconds = 300

api_calls = 0
last_thing = args.start
window_start_time = datetime.now()
logging.info(f"Starting window at {window_start_time}")
try:
    for thing_id in range(last_thing, 5000000):
        if DEADMAP.test(thing_id):
            logging.info("Skipping dead thing {}".format(thing_id))
            continue
        logging.info("Getting thing {}".format(thing_id))
        dn = dirname_from_id(thing_id)
        os.makedirs(f'thing_json/{dn}', exist_ok=True)
        thing_fname = f'thing_json/{dn}/{thing_id}.json'
        t = None
        try:
            if os.path.exists(thing_fname):
                with open(thing_fname, 'r') as f:
                    t = json.load(f)
        except json.decoder.JSONDecodeError:
            t = None
        if not t:
            while True:
                try:
                    t = a.get_thing(thing_id)
                    logging.info("API CALL MADE")
                    api_calls += 1
                    break
                except KeyboardInterrupt as e:
                    raise e
                except:
                    logging.debug("Exception in get thing")
        if t and 'error' not in t:
            with open(thing_fname, 'w') as f:
                f.write(json.dumps(t))
            # Get comments
            dn = dirname_from_id(thing_id)
            os.makedirs(f'thing_comments_json/{dn}', exist_ok=True)
            comments_fname = f'thing_comments_json/{dn}/{thing_id}.json'
            if os.path.exists(comments_fname):
                with open(comments_fname, 'r') as f:
                    c = json.load(f)
            else:
                c = a.get_thing_comments(thing_id)
                logging.info("API CALL MADE")
                api_calls += 1
                if c:
                    with open(comments_fname, 'w') as f:
                        f.write(json.dumps(c))
                        logging.info("Wrote comment json for thing {}".format(thing_id))
            # Get user avatars
            if c and 'users' in c and isinstance(c['users'], dict):
                for uid, u in c['users'].items():
                    if 'user_avatar' in u:
                        try:
                            uext = u['user_avatar'].split('.')[-1]
                            dn = dirname_from_id(uid)
                            os.makedirs(f'user_avatars/{dn}', exist_ok=True)
                            fname = f'user_avatars/{dn}/{uid}.{uext}'
                            if not os.path.exists(fname):
                                print("Downloading avatar {}: {}".format(uid, u['user_avatar']))
                                try:
                                    clen = 0
                                    with requests.get(u['user_avatar'], timeout=(8, 10000), stream=True) as r:
                                        r.raise_for_status()
                                        with open(fname, 'wb') as f:
                                            for chunk in r.iter_content(chunk_size=65536):
                                                if chunk: # filter out keep-alive new chunks
                                                    f.write(chunk)
                                                    clen += len(chunk)
                                    print("Received {} bytes".format(clen))
                                except:
                                    print("Exception downloading, moving on")
                                    import traceback
                                    traceback.print_exc()
                            else:
                                print("Avatar already exists")
                        except KeyboardInterrupt as e:
                            raise e
                        except:
                            print("Exception downloading, moving on")
                            import traceback
                            traceback.print_exc()
                    
            # Get images
            dn = dirname_from_id(thing_id)
            os.makedirs(f'image_json/{dn}', exist_ok=True)
            image_json_fname = f'image_json/{dn}/{thing_id}.json'
            if os.path.exists(image_json_fname):
                with open(image_json_fname, 'r') as f:
                    images = json.load(f)
            else:
                images = a.get_thing_image(thing_id)
                logging.info("API CALL MADE")
                api_calls += 1
                # Save image json for later
                if images:
                    with open(image_json_fname, 'w') as f:
                        f.write(json.dumps(images))
                        logging.info("Wrote image json for thing {}".format(thing_id))

            if images and 'error' not in images:
                for i in images:
                    iid = i['id']
                    for s in i['sizes']:
                        if s['size'] == 'large' and s['type'] == 'display':
                            try:
                                iext = s['url'].split('.')[-1]
                                dn = dirname_from_id(iid)
                                os.makedirs(f'images/{dn}', exist_ok=True)
                                fname = f'images/{dn}/{iid}.{iext}'
                                if not os.path.exists(fname):
                                    print("Downloading image {}: {}".format(iid, s['url']))
                                    try:
                                        clen = 0
                                        with requests.get(s['url'], timeout=(8, 10000), stream=True) as r:
                                            r.raise_for_status()
                                            with open(fname, 'wb') as f:
                                                for chunk in r.iter_content(chunk_size=65536):
                                                    if chunk: # filter out keep-alive new chunks
                                                        f.write(chunk)
                                                        clen += len(chunk)
                                        print("Received {} bytes".format(clen))
                                    except:
                                        print("Exception downloading, moving on")
                                        import traceback
                                        traceback.print_exc()
                                else:
                                    print("Image already exists")
                            except KeyboardInterrupt as e:
                                raise e
                            except:
                                print("Exception downloading, moving on")
                                import traceback
                                traceback.print_exc()
            fid = thing_id
            dn = dirname_from_id(fid)
            os.makedirs(f'files/{dn}', exist_ok=True)
            fname = f'files/{dn}/{fid}.zip'
            if not os.path.exists(fname):
                i = a.get_thing_zip(thing_id)
                logging.info("API CALL MADE")
                api_calls += 1
                if i and 'error' not in i:
                    if 'public_url' in i:
                        print("Downloading file {}: {}".format(fid, i['public_url']))
                        try:
                            with requests.get(i['public_url'], timeout=(8, 10000), stream=True) as r:
                                r.raise_for_status()
                                with open(fname, 'wb') as f:
                                    clen = 0
                                    for chunk in r.iter_content(chunk_size=65536):
                                        if chunk: # filter out keep-alive new chunks
                                            f.write(chunk)
                                            clen += len(chunk)
                                print("Received {} bytes".format(clen))
                        except KeyboardInterrupt as e:
                            raise e
                        except:
                            print("Exception downloading, trying modified version")
                            import traceback
                            traceback.print_exc()
                            o = urllib.parse.urlparse(i['public_url'])
                            o = o._replace(netloc='cdn.thingiverse.com')
                            new_url = urllib.parse.urlunparse(o)
                            print("Downloading file {}: {}".format(fid, new_url))
                            try:
                                with requests.get(new_url, timeout=(8, 10000), stream=True) as r:
                                    r.raise_for_status()
                                    with open(fname, 'wb') as f:
                                        clen = 0
                                        for chunk in r.iter_content(chunk_size=65536):
                                            if chunk: # filter out keep-alive new chunks
                                                f.write(chunk)
                                                clen += len(chunk)
                                    print("Received {} bytes".format(clen))
                            except KeyboardInterrupt as e:
                                raise e
                            except:
                                print("Exception downloading, giving up")
                else:
                    print("Error getting zip")
            else:
                print("File already exists")
        else:
            print("Error fetching thing")
            if DEADMAP is not None:
                print("Adding to deadmap")
                DEADMAP.set(thing_id)
        last_thing += 1
        if api_calls >= max_api_calls:
            window_left = window_seconds - (datetime.now() - window_start_time).total_seconds()
            if window_left < 0:
                window_left = 0
            print("Max API calls reached for the window, sleeping {} seconds...".format(window_left))
            time.sleep(window_left)
            api_calls = 0
            window_start_time = datetime.now()
            logging.info(f"Starting new window at {window_start_time}")
except KeyboardInterrupt:
    print("Keyboard Interrupt!")
finally:
    print("Exiting...")
    if DEADMAP is not None:
        print("Saving off deadmap")
        with open(args.deadmap, 'wb') as f:
            pickle.dump(DEADMAP, f)
    else:
        print("No deadmap to save")
