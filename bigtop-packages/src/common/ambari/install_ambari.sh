#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -ex

usage() {
  echo "
usage: $0 <options>
  Required not-so-options:
     --build-dir=DIR             path to ambari dist.dir
     --prefix=PREFIX             path to install into
     --source-dir=DIR            path to the source code
  "
  exit 1
}

copy_common () {
  AMBARI_COMMON_DIR=${2}/lib/ambari_commons   
  JINJA_DIR=${2}/lib/ambari_jinja2
  SIMPLEJSON_DIR=${2}/lib/ambari_simplejson   
  RESOURCE_MANAGEMENT_DIR=${2}/lib/resource_management

  install -d -m 0755 $AMBARI_COMMON_DIR
  cp -ra $1/ambari-common/src/main/python/ambari_commons/* $AMBARI_COMMON_DIR
  chmod -R 0755 $AMBARI_COMMON_DIR
  
  install -d -m 0755 $JINJA_DIR
  cp -ra $1/ambari-common/src/main/python/ambari_jinja2/ambari_jinja2/* $JINJA_DIR
  chmod -R 0755 $JINJA_DIR

  install -d -m 0755 $SIMPLEJSON_DIR
  cp -ra $1/ambari-common/src/main/python/ambari_simplejson/* $SIMPLEJSON_DIR
  chmod -R 0755 $SIMPLEJSON_DIR

  install -d -m 0755 $RESOURCE_MANAGEMENT_DIR
  cp -ra $1/ambari-common/src/main/python/resource_management/* $RESOURCE_MANAGEMENT_DIR
  chmod -R 0755 $RESOURCE_MANAGEMENT_DIR
}

OPTS=$(getopt \
  -n $0 \
  -o '' \
  -l 'prefix:' \
  -l 'source-dir:' \
  -l 'build-dir:' -- "$@")

if [ $? != 0 ] ; then
    usage
fi

eval set -- "$OPTS"
while true ; do
    case "$1" in
        --prefix)
        PREFIX=$2 ; shift 2
        ;;
        --build-dir)
        BUILD_DIR=$2 ; shift 2
        ;;
        --source-dir)
        SOURCE_DIR=$2 ; shift 2
        ;;
        --)
        shift ; break
        ;;
        *)
        echo "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
done

for var in PREFIX BUILD_DIR SOURCE_DIR ; do
  if [ -z "$(eval "echo \$$var")" ]; then
    echo Missing param: $var
    usage
  fi
done

#Ambari Server
LIB_DIR=${LIB_DIR:-/usr/lib/ambari-server}
ETC_DIR=${ETC_DIR:-/etc/ambari-server}
CONF_DIR=${CONF_DIR:-${ETC_DIR}/conf}

VAR_LIB_DIR=/var/lib/ambari-server
SBIN_DIR=/usr/sbin
SERVER_DIR=$BUILD_DIR/ambari-server/target/ambari-server-*-dist/ambari-server*

install -d -m 0755 $PREFIX/$ETC_DIR
install -d -m 0755 $PREFIX/$SBIN_DIR
install -d -m 0755 $PREFIX/$VAR_LIB_DIR/data
install -d -m 0755 $PREFIX/$VAR_LIB_DIR/data/cache
install -d -m 0755 $PREFIX/$VAR_LIB_DIR/data/tmp

install -d -m 0755 $PREFIX/var/run/ambari-server/bootstrap
install -d -m 0755 $PREFIX/var/run/ambari-server/stack-recommendations
install -d -m 0755 $PREFIX/var/log/ambari-server
install -d -m 0755 $PREFIX/$CONF_DIR


copy_common $SOURCE_DIR ${PREFIX}/${LIB_DIR}


install -d -m 0755 $PREFIX/$LIB_DIR/web
cp -ra  $SOURCE_DIR/ambari-web/public/* ${PREFIX}/${LIB_DIR}/web

cp -a $SOURCE_DIR/ambari-server/src/main/python/ambari-server.py ${PREFIX}/${SBIN_DIR}
cp -a $SOURCE_DIR/ambari-server/src/main/python/ambari_server_main.py ${PREFIX}/${SBIN_DIR}
cp -a $SOURCE_DIR/ambari-server/target/ambari-server ${PREFIX}/${SBIN_DIR}

chmod 0755 $PREFIX/$SBIN_DIR/*

cp -a  $SOURCE_DIR/ambari-common/src/main/unix/ambari-python-wrap ${PREFIX}/${VAR_LIB_DIR}
cp -a  $SOURCE_DIR/ambari-server/conf/unix/ambari-env.sh ${PREFIX}/${VAR_LIB_DIR}
cp -a  $SOURCE_DIR/ambari-server/conf/unix/ambari-sudo.sh ${PREFIX}/${VAR_LIB_DIR}
cp -a  $SOURCE_DIR/ambari-server/conf/unix/install-helper.sh ${PREFIX}/${VAR_LIB_DIR}

chmod 0755 ${PREFIX}/${VAR_LIB_DIR}/*
chmod 0700 ${PREFIX}/${VAR_LIB_DIR}/*.sh


install -d -m 0700 $PREFIX/$VAR_LIB_DIR

install -d -m 0755 $PREFIX/$VAR_LIB_DIR/resources
install -d -m 0755 $PREFIX/$VAR_LIB_DIR/resources/apps
install -d -m 0755 $PREFIX/$VAR_LIB_DIR/resources/common-services
install -d -m 0755 $PREFIX/$VAR_LIB_DIR/resources/views

cp -a  $SOURCE_DIR/ambari-server/src/main/resources/slider_resources/README.txt ${PREFIX}/${VAR_LIB_DIR}/resources

install -d -m 0755 $PREFIX/$VAR_LIB_DIR/keys/db

cp -ar $SOURCE_DIR/ambari-server/src/main/resources/db ${PREFIX}/${VAR_LIB_DIR}/keys
cp -a  $SOURCE_DIR/ambari-server/conf/unix/ca.config ${PREFIX}/${VAR_LIB_DIR}/keys

chmod 700 ${PREFIX}/${VAR_LIB_DIR}/keys/db/*


cp -a  $SOURCE_DIR/ambari-server/target/classes/Ambari-*.sql ${PREFIX}/${VAR_LIB_DIR}/resources
cp -a  $SOURCE_DIR/ambari-server/src/main/resources/Ambari-*.sql ${PREFIX}/${VAR_LIB_DIR}/resources
cp -a  $SOURCE_DIR/ambari-server/target/DBConnectionVerification.jar ${PREFIX}/${VAR_LIB_DIR}/resources
cp -a  $SOURCE_DIR/ambari-server/src/main/resources/role_command_order.json ${PREFIX}/${VAR_LIB_DIR}/resources
cp -a  $SOURCE_DIR/ambari-server/target/version ${PREFIX}/${VAR_LIB_DIR}/resources

cp -a  $SOURCE_DIR/ambari-server/conf/unix/ambari.properties ${PREFIX}/${CONF_DIR}
cp -a  $SOURCE_DIR/ambari-server/conf/unix/log4j.properties ${PREFIX}/${CONF_DIR}
cp -a  $SOURCE_DIR/ambari-server/conf/unix/krb5JAASLogin.conf ${PREFIX}/${CONF_DIR}




cp -ra $SERVER_DIR/lib/ambari-server/* ${PREFIX}/${LIB_DIR}/




cp -a  $SOURCE_DIR/ambari-server/src/main/resources/slider_resources/README.txt ${PREFIX}/var/lib/ambari-server/resources/apps
cp -ar $SOURCE_DIR/ambari-server/target/classes/common-services   ${PREFIX}/var/lib/ambari-server/resources/
cp -ar $SOURCE_DIR/ambari-server/src/main/resources/custom_action_definitions   ${PREFIX}/var/lib/ambari-server/resources/
cp -ar $SOURCE_DIR/ambari-server/src/main/resources/custom_actions ${PREFIX}/var/lib/ambari-server/resources/
cp -ar $SOURCE_DIR/ambari-server/src/main/resources/host_scripts ${PREFIX}/var/lib/ambari-server/resources/

chmod 0755 ${PREFIX}/var/lib/ambari-server/resources/custom_actions/scripts/*
chmod 0755 ${PREFIX}/var/lib/ambari-server/resources/host_scripts/*

cp -ar $SOURCE_DIR/ambari-server/src/main/resources/scripts ${PREFIX}/var/lib/ambari-server/resources/
cp -ar $SOURCE_DIR/ambari-server/src/main/python/upgradeHelper.py ${PREFIX}/var/lib/ambari-server/resources/scripts

chmod 0755 ${PREFIX}/var/lib/ambari-server/resources/scripts/*

cp -ar $SOURCE_DIR/ambari-server/src/main/resources/stacks ${PREFIX}/var/lib/ambari-server/resources/

chmod 0755 ${PREFIX}/var/lib/ambari-server/resources/stacks/stack_advisor.py

cp -ar $SOURCE_DIR/ambari-server/src/main/resources/upgrade ${PREFIX}/var/lib/ambari-server/resources/

install -d -m 0755 $PREFIX/usr/lib/python2.6/site-packages/

cp -ar $SOURCE_DIR/ambari-server/src/main/python/ambari_server ${PREFIX}/usr/lib/python2.6/site-packages/
cp -ar $SOURCE_DIR/ambari-server/src/main/python/bootstrap.py ${PREFIX}/usr/lib/python2.6/site-packages/ambari_server
cp -ar $SOURCE_DIR/ambari-server/src/main/python/setupAgent.py ${PREFIX}/usr/lib/python2.6/site-packages/ambari_server
cp -ar $SOURCE_DIR/ambari-server/src/main/python/os_check_type.py ${PREFIX}/usr/lib/python2.6/site-packages/ambari_server

chmod  0755 ${PREFIX}/usr/lib/python2.6/site-packages/ambari_server/*

cp -a  $SOURCE_DIR/ambari-admin/target/*.jar ${PREFIX}/var/lib/ambari-server/resources/views

# End of Ambari Server

LIB_DIR=/usr/lib/ambari-agent
ETC_DIR=/etc/ambari-agent
VAR_LIB_DIR=/var/lib/ambari-agent

CONF_DIR=${ETC_DIR}/conf
AGENT_BUILD_DIR=${BUILD_DIR}/ambari-agent/target
AGENT_DIR=${AGENT_BUILD_DIR}/ambari-agent-*/ambari_agent
AGENT_DEST_DIR=/usr/lib/python2.6/site-packages/ambari_agent

copy_common $SOURCE_DIR ${PREFIX}/${LIB_DIR}

cp -ra $SOURCE_DIR/ambari-agent/src/examples ${PREFIX}/${LIB_DIR}/lib

install -d -m 0755 $PREFIX/$AGENT_DEST_DIR
cp -ra $AGENT_DIR/* ${PREFIX}/${AGENT_DEST_DIR}
chmod -R 0755 ${PREFIX}/${AGENT_DEST_DIR}

install -d -m 0755 $PREFIX/$CONF_DIR
cp -a $SOURCE_DIR/ambari-agent/conf/unix/ambari-agent.ini ${PREFIX}/$CONF_DIR
cp -a $SOURCE_DIR/ambari-agent/conf/unix/logging.conf.sample ${PREFIX}/$CONF_DIR

install -d -m 0755 $PREFIX/$SBIN_DIR
cp -ra $AGENT_BUILD_DIR/src/ambari-agent ${PREFIX}/${SBIN_DIR}
chmod 0755 ${PREFIX}/${SBIN_DIR}/ambari-agent

install -d -m 0755 $PREFIX/$VAR_LIB_DIR
cp -a $SOURCE_DIR/ambari-agent/conf/unix/ambari-env.sh ${PREFIX}/${VAR_LIB_DIR}
cp -a $SOURCE_DIR/ambari-agent/conf/unix/install-helper.sh ${PREFIX}/${VAR_LIB_DIR}
cp -a $SOURCE_DIR/ambari-agent/conf/unix/upgrade_agent_configs.py ${PREFIX}/${VAR_LIB_DIR}
cp -a $SOURCE_DIR/ambari-agent/conf/unix/ambari-sudo.sh ${PREFIX}/${VAR_LIB_DIR}
cp -a $SOURCE_DIR/ambari-common/src/main/unix/ambari-python-wrap ${PREFIX}/${VAR_LIB_DIR}
chmod 700 ${PREFIX}/${VAR_LIB_DIR}/*

cp -ra $AGENT_BUILD_DIR/cache ${PREFIX}/$VAR_LIB_DIR
cp -ra $AGENT_BUILD_DIR/cache/custom_actions ${PREFIX}/${VAR_LIB_DIR}/cache

install -d -m 0755 ${PREFIX}/${VAR_LIB_DIR}/data

cp -a $AGENT_BUILD_DIR/src/version ${PREFIX}/${VAR_LIB_DIR}/data

install -d -m 0755 ${PREFIX}/etc/init.d
cp -a $SOURCE_DIR/ambari-agent/etc/init.d/ambari-agent ${PREFIX}/etc/init.d
chmod 0755 ${PREFIX}/etc/init.d/ambari-agent


install -d -m 0755 ${PREFIX}/var/log
install -d -m 0755 ${PREFIX}/var/run
install -d -m 0755 ${PREFIX}/var/run/ambari-agent
install -d -m 0755 ${PREFIX}/var/lib/ambari-agent/tmp
install -d -m 0755 ${PREFIX}/var/lib/ambari-agent/keys
install -d -m 0755 ${PREFIX}/var/log/ambari-agent
install -d -m 0755 ${PREFIX}/etc/rc.d/init.d


#Ambari Groovy Client 


PREFIX_GROOVY_CLIENT=${PREFIX}/groovy-client
OPT_DIR=/opt/groovy-client
CLIENT_BUILD_DIR=${BUILD_DIR}/ambari-client/groovy-client

install -d -m 0755 ${PREFIX_GROOVY_CLIENT}/${OPT_DIR}
cp -a ${CLIENT_BUILD_DIR}/target/groovy-client*.jar ${PREFIX_GROOVY_CLIENT}/${OPT_DIR}

#Ambari Python Client

LIB_DIR=/usr/lib
CLIENT_BUILD_DIR=${SOURCE_DIR}/ambari-client/python-client

#install -d -m 0755 ${PREFIX_GROOVY_CLIENT}/${LIB_DIR}
#cp -a ${CLIENT_BUILD_DIR}/src/main/python-client ${PREFIX_GROOVY_CLIENT}/${LIB_DIR}
