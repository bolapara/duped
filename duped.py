#!/usr/local/bin/python3
import argparse
import hashlib
import os
import sys
import shelve
import shlex
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed


class HashDB(object):
    def __init__(self, filename):
        self._hash_dict = shelve.open(filename, 'c')
        self._error_list = []

    def add(self, file_hash, filename):
        existing_hash = self._hash_dict.setdefault(file_hash, [])
        existing_hash.append(filename)
        self._hash_dict[file_hash] = existing_hash

    def export_dict(self):
        return self._hash_dict

    def export_hashes(self):
        for file_hash, filenames in self._hash_dict.items():
            for filename in filenames:
                yield f"{file_hash} {filename.decode()}"

    def export_errors(self):
        return (line for line in self._error_list)


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
        print(f"Unhandled error: {e}", file=sys.stderr)
        result = None
    return filename, result


def generate_file_list(directories, skip_dirs, no_empty):
    for topdir in directories:
        for path, dirs, filenames in os.walk(
                topdir, onerror=lambda e: print(e, file=sys.stderr)):
            for directory in dirs:
                if directory in skip_dirs:
                    del dirs[dirs.index(directory)]
            for filename in filenames:
                fullpath = os.path.abspath(os.path.join(path, filename))
                if os.path.islink(fullpath):
                    continue
                if os.path.isfile(fullpath):
                    if no_empty and os.path.getsize(fullpath) == 0:
                        continue
                    yield fullpath


def decider(hash_dict, delete_list):
    keep_list, del_list = [], []
    for files in hash_dict.values():
        new_delete_files, new_keep_files = [], []
        files.sort()
        if len(files) > 1:
            for delete_prefix in delete_list:
                new_delete_files.extend([
                    filename for filename in files if filename.startswith(delete_prefix)
                ])
            new_keep_files = [
                filename for filename in files if filename not in new_delete_files
            ]
            if not new_keep_files:
                new_keep_files.append(new_delete_files.pop())
        else:
            new_keep_files.extend([filename for filename in files])
        keep_list.extend(new_keep_files)
        del_list.extend(new_delete_files)
    return keep_list, del_list


def hash_files(file_list):
    with ProcessPoolExecutor(max_workers=args.procs) as executor:

        futures = (executor.submit(hasher, filename) for filename in file_list)
        for future in as_completed(futures):
            try:
                yield future.result()
            except Exception as e:
                print(e)


def write_results(keep_list, delete_list, error_list, hash_list, work_dir, args):
    files = (
        ('keep', keep_list),
        ('delete', delete_list),
        ('error', error_list),
    )

    for filename, content in files:
        print(filename)
        with open(os.path.join(work_dir, filename), 'w') as fobj:
            fobj.writelines((f"{shlex.quote(line)}\n" for line in content))

    with open(os.path.join(work_dir, 'commandline'), 'w') as fobj:
        fobj.writelines(f"{line}\n" for line in [args])


def create_work_dir(base_path):
    work_dir = os.path.join(
        os.getcwd(),
        f"{os.path.splitext(base_path)[0]}_results_{str(os.getpid())}"
    )
    os.mkdir(work_dir)

    return work_dir


def build(args):
    skip = ['.git']
    skip.extend(args.skip)

    print("generating file list")

    file_list = generate_file_list(
        set(os.path.abspath(directory).encode() for directory in args.directories),
        set(directory.encode() for directory in skip),
        args.no_empty,
    )

    # remove duplicates
    file_set = set(file_list)

    work_dir = create_work_dir(sys.argv[0])
    hash_db = HashDB(os.path.join(work_dir, 'hash'))

    print("calculating file hashes")

    count = 0
    for filename, file_hash in hash_files(file_set):
        if not filename or not file_hash:
            print(f"Bug: filename: {filename} hash: {hash}")
            continue
        hash_db.add(file_hash, filename)
        count += 1
        if count % 10 == 0:
            print(f'\r{count}', end='', flush=True)

    print()
    print(f"Working directory is {work_dir}")


def preprocess(hash_db, delete_list):
    print("processing files")

    keep_list, delete_list = decider(
        hash_db.export_dict(),
        set(os.path.abspath(directory).encode() for directory in delete_list)
    )

    return (x.decode() for x in keep_list), (x.decode() for x in delete_list)


def process(args):
    work_dir = args.work_dir
    hash_db = HashDB(os.path.join(work_dir, 'hash'))

    keep_list, delete_list = preprocess(hash_db, args.delete)

    print(f"writing results into {work_dir}")
    write_results(
            keep_list,
            delete_list,
            hash_db.export_errors(),
            hash_db.export_hashes(),
            work_dir,
            args
        )


def delete(args):
    work_dir = args.work_dir
    hash_db = HashDB(os.path.join(work_dir, 'hash'))

    _, delete_list = preprocess(hash_db, args.delete)

    for filename in delete_list:
        if args.verbose:
            print(filename)
        try:
            os.remove(filename)
        except Exception as e:
            print(f"Error deleting {filename} ({e})")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--verbose', action='store_true', help="Verbose mode")
    subparsers = parser.add_subparsers()

    build_cmd = subparsers.add_parser('build',
        help="Build a database of files and their hashes"
    )
    build_cmd.add_argument(
        '--procs', type=int, default=(cpu_count() - 1) or 1, help="Number of processes to use"
    )
    build_cmd.add_argument('--no-empty', action='store_true', help="Skip empty files")
    build_cmd.add_argument(
        '--skip', action='append', default=[], help="List of directory names to ignore"
    )
    build_cmd.add_argument('directories', type=str, nargs='+')
    build_cmd.set_defaults(func=build)

    process_cmd = subparsers.add_parser('process', 
        help="Process list of directories and generate lists of files to keep or delete"
    )
    process_cmd.add_argument('--work_dir', type=str, required=True)
    process_cmd.add_argument(
        dest='delete', nargs='+', help="List of dirs to delete dupes from"
    )
    process_cmd.set_defaults(func=process)

    delete_cmd = subparsers.add_parser('delete',
        help="Process list of directories and delete duplicate files"
    )
    delete_cmd.add_argument('--work_dir', type=str, required=True)
    delete_cmd.add_argument(
        dest='delete', nargs='+', help="List of dirs to delete dupes from"
    )
    delete_cmd.set_defaults(func=delete)

    args = parser.parse_args()

    if args.verbose:
        print(args)

    return args


if __name__ == '__main__':
    args = parse_args()
    args.func(args)
