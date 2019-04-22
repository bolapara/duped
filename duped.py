#!/usr/local/bin/python3
import argparse
import hashlib
import os
import sys
from multiprocessing import cpu_count, Pool


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


def generate_file_list(directories, skip_empty, skip_dirs):
    file_list, error_list = [], []

    for topdir in directories:
        for path, dirs, filenames in os.walk(
            topdir, onerror=lambda e: error_list.append(
                e.filename), topdown=True):
            for directory in dirs:
                if directory in skip_dirs:
                    del dirs[dirs.index(directory)]
            for filename in filenames:
                fullpath = os.path.join(path, filename)
                if os.path.isfile(fullpath):
                    if os.path.islink(fullpath):
                        continue
                    if skip_empty and os.path.getsize(fullpath) == 0:
                        continue
                    file_list.append(fullpath)
    return file_list, error_list


def hash_list_to_dict(hash_list):
    hash_dict, error_list = {}, []
    for file_hash, filename in hash_list:
        if not file_hash:
            error_list.append(filename)
        hashes = hash_dict.setdefault(file_hash, [])
        hashes.append(filename)
    return hash_dict, error_list


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

directories = [os.path.normpath(directory) for directory in args.directories]
delete_dirs = [os.path.normpath(directory) for directory in args.auto_delete]

print("building file list")
file_list, error_list = generate_file_list(
    directories, args.no_empty, args.skip)

print("processing {} files".format(len(file_list)))
with Pool(processes=args.procs) as pool:
    hash_list = pool.map(hasher, file_list)

print("parsing")
hash_dict, error_list = hash_list_to_dict(hash_list)

print("being the decider")
keep_list, delete_list = decider(hash_dict, delete_dirs)

print("writing out results")
extension = str(os.getpid())
with open('keep.{}'.format(extension), 'x') as fobj:
    fobj.write('\n'.join(keep_list))
    fobj.write('\n')

with open('delete.{}'.format(extension), 'x') as fobj:
    fobj.write('\n'.join(delete_list))
    fobj.write('\n')

with open('error.{}'.format(extension), 'x') as fobj:
    fobj.write('\n'.join(error_list))
    fobj.write('\n')
