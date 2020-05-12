import os, sys, shutil
for root, dirs, files in os.walk(sys.argv[1]):
    print(f"Processing {root}")
    for fn in files:
        newfdr = fn.split(".")[0].zfill(8)[0:4]
        destdir = os.path.join(root, newfdr)
        print(f"Moving {fn} to {destdir}")
        os.makedirs(destdir, exist_ok=True)
        shutil.move(os.path.join(root, fn), os.path.join(destdir, fn))
    break
