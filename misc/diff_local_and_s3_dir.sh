#!/bin/bash -eux

# Create files containing diffable recursive listings of a local dir
# and a corresponding dir in an s3 bucket.
#
# Fix your photo FOMO!

export BUCKET=foo
export PHOTOYEAR=2017

# Create file containing a list of files in the bucket;
# a recursive filename-only 'aws s3 ls' with spaces handled:
# https://stackoverflow.com/questions/36813327/how-to-display-only-files-from-aws-s3-ls-command
aws s3 ls s3://${BUCKET}/photos/${PHOTOYEAR} --recursive | awk '{$1=$2=$3=""; print $0}' | sed 's/^[ \t]*//' > ${PHOTOYEAR}-remote.txt

# Create file with list of files in local dir
#find /Users/bill/Pictures/s3/${BUCKET}/photos/${PHOTOYEAR} -type f > ${PHOTOYEAR}-local.txt
(cd ~/Pictures/s3/${BUCKET} && find photos/${PHOTOYEAR} -type f > ~/github/billagee/dropbox-to-s3/${PHOTOYEAR}-local.txt)
# Or with GNU find:
# brew install findutils
#(cd ~/Pictures/s3/ && gfind ${BUCKET}/photos/${PHOTOYEAR} -mindepth 1 -printf '%P\n' > ~/github/billagee/dropbox-to-s3/${PHOTOYEAR}-local.txt)

# Strip path out of prefixes - can find do this?
#:%s/\/Users\/bill\/Pictures\/s3\/${BUCKET}\//
