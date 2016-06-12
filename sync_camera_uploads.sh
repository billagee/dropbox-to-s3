#!/bin/bash -eu

# Moves files for the current year/month from "Dropbox/Camera Uploads/"
# to a local staging dir, then syncs the dir layout to an s3 bucket.

# NOTE: This script assumes your s3 layout resembles:
#s3cmd ls s3://$BUCKET_NAME/photos/2015/11/iPhone5/
#s3cmd ls s3://$BUCKET_NAME/photos/2015/11/iPhone5/video/

function box_out()
{
  local s=("$@") b w
  for l in "${s[@]}"; do
    ((w<${#l})) && { b="$l"; w="${#l}"; }
  done
  tput setaf 3
  echo " -${b//?/-}-
| ${b//?/ } |"
  for l in "${s[@]}"; do
    printf '| %s%*s%s |\n' "$(tput setaf 4)" "-$w" "$l" "$(tput setaf 3)"
  done
  echo "| ${b//?/ } |
 -${b//?/-}-"
  tput sgr 0
}

# Define local dir for current month's photos and videos
echo -n "Enter your s3 bucket name and press [ENTER]: "
read BUCKET_NAME
echo -n "Enter your target year and press [ENTER] (e.g. 2016): "
read TARGET_YEAR
echo -n "Enter your target month and press [ENTER] (e.g. 05): "
read TARGET_MONTH
LOCAL_BASE_DIR=~/Pictures/s3/$BUCKET_NAME/$TARGET_YEAR/$TARGET_MONTH
DEVICE=iPhone6s
LOCAL_DEVICE_DIR=$LOCAL_BASE_DIR/$DEVICE

read -p "sync image or video files? [i/v]: " -r FILE_TYPE
if [[ $FILE_TYPE =~ ^[Vv]$ ]]
then
    FILE_EXTENSION=mov
    WORKING_DIR=$LOCAL_DEVICE_DIR/video
else
    FILE_EXTENSION=jpg
    WORKING_DIR=$LOCAL_DEVICE_DIR
fi

echo "Local staging dir will be '$WORKING_DIR'"
read -p "Does that look OK? [y/n]: " -r STAGING_DIR_CONFIRMATION
if [[ $STAGING_DIR_CONFIRMATION =~ ^[Nn]$ ]]
then
    box_out "Aborting as requested."
    exit 1
fi

# Create local working dir
box_out "Creating '$WORKING_DIR'"
mkdir -p $WORKING_DIR

# Copy current month's files from Dropbox to local dir
box_out "About to cd into '$WORKING_DIR'"
cd $WORKING_DIR
# Using 'find -print' is another way to list files in the src dir:
#find ~/Dropbox/Camera\ Uploads -name "$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION" -print
box_out "Checking whether files matching '$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION' exist in dropbox..."
if [[ ! `ls -la ~/Dropbox/Camera\ Uploads/$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION` ]]
then
    box_out "No files in dropbox dir to move! Exiting."
    exit 1
fi

ls -la ~/Dropbox/Camera\ Uploads/$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION
box_out "About to move the above files to current dir:" "'$(pwd)'"

read -p "OK to proceed? [y/n]: " -r MOVE_CONFIRMATION
if [[ $MOVE_CONFIRMATION =~ ^[Yy]$ ]]
then
    mv ~/Dropbox/Camera\ Uploads/$TARGET_YEAR-$TARGET_MONTH-*.$FILE_EXTENSION .
    box_out "mv completed."
else
    box_out "Aborting as requested."
    exit 1
fi

# Print the dry run output of syncing local dir to s3
box_out "Performing dry-run of s3cmd sync..."
s3cmd sync --dry-run --exclude '.DS_Store' $LOCAL_BASE_DIR s3://$BUCKET_NAME/photos/$TARGET_YEAR/
box_out "About to sync the files above to s3."

read -p "OK? [y/n]: " -r SYNC_CONFIRMATION
if [[ $SYNC_CONFIRMATION =~ ^[Yy]$ ]]
then
    s3cmd sync --exclude '.DS_Store' $LOCAL_BASE_DIR s3://$BUCKET_NAME/photos/$TARGET_YEAR/
else
    box_out "Aborting."
    exit 1
fi

# To sync the entire current year's files:
#s3cmd sync --dry-run --exclude '.DS_Store' 2016 s3://$BUCKET_NAME/photos/
#s3cmd sync --exclude '.DS_Store' 2016 s3://$BUCKET_NAME/photos/
