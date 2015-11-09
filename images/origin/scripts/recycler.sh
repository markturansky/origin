#!/bin/bash

# 'recycler' performs an 'rm -rf' on a volume to scrub it clean before it's
# reused as a cluster resource. This script is intended to be used in a pod that
# performs the scrub. The container in the pod should succeed or fail based on
# the exit status of this script.

set -e -o pipefail
shopt -s dotglob nullglob

if [[ $# -ne 1 ]]; then
    echo >&2 "Usage: $0 some/path/to/scrub"
    exit 1
fi

# first and only arg is the directory to scrub
dir=$1

if [[ ! -d ${dir} ]]; then
    echo >&2 "Error: scrub directory '${dir}' does not exist"
    exit 1
fi

# remount to nfsv3 so not restricted by nfs v4 acls, see nfs4_acl(5)
tmp=${dir}
mnt=`grep -w "${dir}" /proc/mounts |grep -w nfs4`
exp=""
[[ ! -z ${mnt} ]] &&  exp=`echo ${mnt}|awk '{print $1}'`
[[ ! -z ${exp} ]] && tmp=`mktemp -d` && mount ${exp} ${tmp} -o vers=3,nolock
# shred all files
#find ${tmp} -type f -exec shred -fuvz {} \;
# sudo to the file's uid/gid and delete it
stat -c "%u %d %n" ${tmp} |xargs -n 3 bash -c 'sudo -u "#$1"  touch $3/start' argv0
find ${tmp} -type f -exec stat -c "%u %g %n"  {} \; |xargs -n 3 bash -c 'sudo -u "#$1" rm  $3' argv0

#delete directories
find ${tmp} -mindepth 1  -type d  -exec stat -c "%n %u %g"  {} \;|sort -k 1 -rg | xargs -n 3 bash -c 'sudo -u "#$2" rm -rf $1' argv0

# remove everything that was left, keeping the directory for re-use as a volume
if rm -rfv ${dir}/*; then
    echo 'Scrub OK'
    exit 0
fi

echo 'Scrub failed'
exit 1
