#!/usr/local/bin/python3
import argparse
import hashlib
import os
import sys
import errno
import time
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--no-empty', action='store_true', help="Skip empty files")
    parser.add_argument('--skip', action='append', default=[],
                        help="List of directory names to ignore")
    parser.add_argument(
        '--auto-delete', action='append', default=[],
        help="List of directories to automatically delete duplicates from"
    )
    parser.add_argument('--procs', type=int, default=cpu_count(),
                        help="Number of processes to use")
    parser.add_argument('--verbose', action='store_true', help="Verbose mode")
    parser.add_argument('directories', nargs='+')
    args = parser.parse_args()
    if args.verbose:
        print(args)
    return args


def hasher(filename):
    hash_func = hashlib.md5()
    try:
        with open(filename, 'rb') as fobj:
            while True:
                data = fobj.read(64 * 1024)
                if not data:
                    break
                hash_func.update(data)
        result = hash_func.hexdigest()
    except (PermissionError, FileNotFoundError) as e:
        result = None
    except Exception as e:
        print("Unhandled error: {}".format(e), file=sys.stderr)
        result = None
    return result, filename


def generate_file_list(directories, args):
    for topdir in directories:
        for path, dirs, filenames in os.walk(
                topdir.encode('utf-8'), onerror=lambda e: print(e, file=sys.stderr)):
            for directory in dirs:
                if directory in args.skip:
                    del dirs[dirs.index(directory)]
            for filename in filenames:
                fullpath = os.path.join(path, filename)
                if os.path.isfile(fullpath):
                    if os.path.islink(fullpath):
                        continue
                    if args.no_empty and os.path.getsize(fullpath) == 0:
                        continue
                    yield fullpath


def decider(hash_dict, auto_delete_list):
    keep_list, del_list = [], []
    for files in hash_dict.values():
        new_delete_files, new_keep_files = [], []
        files.sort()
        if len(files) > 1:
            for auto_delete_prefix in auto_delete_list:
                new_delete_files.extend([
                    filename for filename in files if filename.startswith(auto_delete_prefix)
                ])
            new_keep_files = [
                filename for filename in files if filename not in new_delete_files]
            if not new_keep_files:
                new_keep_files.append(new_delete_files.pop())
        else:
            new_keep_files.extend(files)
        keep_list.extend(new_keep_files)
        del_list.extend(new_delete_files)
    return keep_list, del_list


def process_files(directories, args):
    hash_dict, error_list = {}, []
    with ProcessPoolExecutor(max_workers=args.procs) as executor:
        count = 0
        file_list = generate_file_list(directories, args)
        futures = (executor.submit(hasher, filename) for filename in file_list)
        for future in as_completed(futures):
            try:
                file_hash, filename = future.result()
                if not file_hash:
                    error_list.append(filename)
                    continue
                hashes = hash_dict.setdefault(file_hash, [])
                hashes.append(filename)
                count += 1
                print('\r{}'.format(count), end='', flush=True)
            except Exception as e:
                print(e)
        print()
    return hash_dict, error_list


def write_results(keep_list, delete_list, error_list, hash_dict, timings, args):
    res_dir = os.path.join(os.getcwd(), '{}_results_{}'.format(
        os.path.splitext(sys.argv[0])[0], str(os.getpid())))
    os.mkdir(res_dir)
    print("writing results into {}".format(res_dir))

    with open(os.path.join(res_dir, 'keep'), 'x') as fobj:
        fobj.writelines(("{}\n".format(line) for line in keep_list))

    with open(os.path.join(res_dir, 'delete'), 'x') as fobj:
        fobj.writelines(("{}\n".format(line) for line in delete_list))

    with open(os.path.join(res_dir, 'error'), 'x') as fobj:
        fobj.writelines(("{}\n".format(line) for line in error_list))

    with open(os.path.join(res_dir, 'hashes'), 'x') as fobj:
        for file_hash, filenames in hash_dict.items():
            fobj.writelines('{} {}\n'.format(file_hash, filename)
                            for filename in filenames)

    with open(os.path.join(res_dir, 'commandline'), 'x') as fobj:
        fobj.write('{}\n'.format(args))

    with open(os.path.join(res_dir, 'runtime'), 'x') as fobj:
        fobj.write('{}\n'.format(timings[1] - timings[0]))


args = parse_args()

if not os.access('.', os.W_OK):
    print("Error, no write access to current directory", file=sys.stderr)
    sys.exit(errno.EACCES)

start_time = time.perf_counter()

print("processing files")
hash_dict, error_list = process_files(
        [os.path.normpath(directory) for directory in args.directories],
        args
    )

print("analyzing files")
keep_list, delete_list = decider(
        hash_dict,
        [os.path.normpath(directory) for directory in args.auto_delete]
    )

print("writing out results")
write_results(
        keep_list,
        delete_list,
        error_list,
        hash_dict,
        (start_time, time.perf_counter()),
        args
    )
