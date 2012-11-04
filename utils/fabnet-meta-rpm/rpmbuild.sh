#!/bin/bash

VERS=`echo $1 | cut -d'-' -f1`
RELEASE=`echo $1 | cut -d'-' -f2- | sed -e "s/-/_/g"`
if [ $VERS = $RELEASE ]
then
    RELEASE='0'
fi


SPECFILE="fabnet-meta-rpm.spec"
NAME=`cat $SPECFILE | grep '%define name' | awk '{print $3}'`

rpmTopDir=/tmp/build/rpm

rm -rf $rpmTopDir
set -x
mkdir -p $rpmTopDir/{SOURCES,SRPMS,BUILD,SPECS,RPMS}

#Copy files to build directory
cp fabnet.repo $rpmTopDir/BUILD

#Define version and release in spec-file
sed -e "s/vNNN/${VERS}/g" -e "s/rNNN/${RELEASE}/g" < ./$SPECFILE >  ${rpmTopDir}/SPECS/$SPECFILE

cd ${rpmTopDir}

rpmbuild  -bb  --clean ${rpmTopDir}/SPECS/$SPECFILE
if [ $? -ne 0 ] ; then
    echo "$0: rpm build failed."
    exit 2
fi

if [ -z $2 ]
then
    echo "Output directory is not passed!"
else
    cp ${rpmTopDir}/RPMS/noarch/${NAME}*.rpm $2
    if [ $? -ne 0 ] ; then
        echo "$0: rpm copy failed."
        exit 2
    fi
fi
