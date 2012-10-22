#!/bin/bash

VERS=`echo $1 | cut -d'-' -f1`
RELEASE=`echo $1 | cut -d'-' -f2- | sed -e "s/-/_/g"`
if [ $VERS = $RELEASE ]
then
    RELEASE='0'
fi

NAME="idepositbox-client"
SOURCEFILE="$NAME-${VERS}.tar.gz"
SOURCEDIR="$NAME-${VERS}"

rpmTopDir=/tmp/build/rpm

rm -rf $rpmTopDir
set -x
mkdir -p $rpmTopDir/{SOURCES,SRPMS,BUILD,SPECS,RPMS}

dst="${rpmTopDir}/${SOURCEDIR}"
[ ! -d "$dst" ] && mkdir "$dst"
cp -rp client ${rpmTopDir}/"${SOURCEDIR}"
cp -rp fabnet/core/fri_base.py ${rpmTopDir}/"${SOURCEDIR}"/client/

[ -f dest/${SOURCEFILE} ] && rm -f dest/${SOURCEFILE}

cd ${rpmTopDir}
(
tar czf  ./${SOURCEFILE} ./${SOURCEDIR}  && \
rm -rf  ${rpmTopDir}/${SOURCEDIR}
)

if [ -z $2 ]
then
    echo "Output directory is not passed!"
else
    cp ${rpmTopDir}/${SOURCEFILE} $2
    if [ $? -ne 0 ] ; then
        echo "$0: copy failed."
        exit 2
    fi
fi
