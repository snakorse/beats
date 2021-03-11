#!/bin/bash

ROOTPATH=`pwd`
BUILDPATH=$1
export GOPATH=${BUILDPATH}

mkdir -p ${BUILDPATH}/src/github.com/elastic
if [ -d ${BUILDPATH}/src/github.com/elastic/beats ]; then
    rm -rf ${BUILDPATH}/src/github.com/elastic/beats
fi
if [ -L ${BUILDPATH}/src/github.com/elastic/beats ]; then
    rm -f ${BUILDPATH}/src/github.com/elastic/beats
fi
ln -s ${ROOTPATH} ${BUILDPATH}/src/github.com/elastic/beats

cd filebeat
make
