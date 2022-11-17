#!/usr/bin/env bash
# this script is intended to test the results of a 'process' run
# to ensure that a file on the delete list has a hash of a file that
# is also on the keep list
set -u
[ -d "$1" ] || { echo "pass a path to a work directory"; exit 1; } && WORK=$1
[[ -e "$WORK/keep" && -e "$WORK/delete" && -e "$WORK/hashes" ]] || { echo "pass a valid work directory"; exit 1; }
grep -x -F -f $WORK/delete $WORK/keep && { echo "error, delete files in keep list!"; exit 1; }
grep -x -F -f $WORK/keep $WORK/delete && { echo "error, keep files in delete list!"; exit 1; }
dhf=$(mktemp)
# BUG - this is a problem.  Will match substrings!
grep -F -f $WORK/delete $WORK/hashes | cut -f1 -d\  | sort -u > $dhf
khf=$(mktemp)
# BUG - this is a problem.  Will match substrings!
grep -F -f $WORK/keep $WORK/hashes | cut -f1 -d\  | sort -u > $khf
grep -v -f $khf $dhf && { echo "error, delete hash not in keep list!"; exit 1; }
echo "test passed"
rm $khf $dhf
