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

%define ambari_name ambari 
%define _binaries_in_noarch_packages_terminate_build   0
%define _unpackaged_files_terminate_build 0

%if  %{?suse_version:1}0
%define doc_ambari %{_docdir}/ambari-doc
%else
%define doc_ambari %{_docdir}/ambari-doc-%{kite_version}
%endif

# disable repacking jars
%define __os_install_post %{nil}

Name: ambari
Version: %{ambari_version}
Release: %{ambari_release}
Summary: Ambari Server
URL: http://ambari.apache.org
Group: Development
BuildArch: noarch
Buildroot: %(mktemp -ud %{_tmppath}/apache-%{ambari_name}-%{version}-%{release}-XXXXXX)
License: ASL 2.0 
Source0: apache-%{ambari_name}-%{ambari_version}-src.tar.gz
Source1: do-component-build 
Source2: install_%{ambari_name}.sh
Source3: bigtop.bom
Requires: openssl, postgresql-server >= 8.1, python >= 2.6, curl
autoprov: yes
autoreq: no

%description 
Ambari Server

%prep
%setup -n apache-%{ambari_name}-%{ambari_base_version}-src

%build
bash $RPM_SOURCE_DIR/do-component-build

%install
%__rm -rf $RPM_BUILD_ROOT
AMBARI_VERSION=%{ambari_version} bash $RPM_SOURCE_DIR/install_ambari.sh \
          --build-dir=`pwd` \
          --source-dir=`pwd` \
          --prefix=$RPM_BUILD_ROOT
 
%pre
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
# limitations under the License

STACKS_FOLDER="/var/lib/ambari-server/resources/stacks"
STACKS_FOLDER_OLD=/var/lib/ambari-server/resources/stacks_$(date '+%d_%m_%y_%H_%M').old

COMMON_SERVICES_FOLDER="/var/lib/ambari-server/resources/common-services"
COMMON_SERVICES_FOLDER_OLD=/var/lib/ambari-server/resources/common-services_$(date '+%d_%m_%y_%H_%M').old

AMBARI_VIEWS_FOLDER="/var/lib/ambari-server/resources/views"
AMBARI_VIEWS_BACKUP_FOLDER="$AMBARI_VIEWS_FOLDER/backups"

if [ -d "/etc/ambari-server/conf.save" ]
then
    mv /etc/ambari-server/conf.save /etc/ambari-server/conf_$(date '+%d_%m_%y_%H_%M').save
fi

if [ -d "$STACKS_FOLDER" ]
then
    mv -f "$STACKS_FOLDER" "$STACKS_FOLDER_OLD"
fi

if [ -d "$COMMON_SERVICES_FOLDER_OLD" ]
then
    mv -f "$COMMON_SERVICES_FOLDER" "$COMMON_SERVICES_FOLDER_OLD"
fi

if [ ! -d "$AMBARI_VIEWS_BACKUP_FOLDER" ] && [ -d "$AMBARI_VIEWS_FOLDER" ]
then
    mkdir "$AMBARI_VIEWS_BACKUP_FOLDER"
fi

if [ -d "$AMBARI_VIEWS_FOLDER" ] && [ -d "$AMBARI_VIEWS_BACKUP_FOLDER" ]
then
    cp -u $AMBARI_VIEWS_FOLDER/*.jar $AMBARI_VIEWS_BACKUP_FOLDER/
fi

exit 0

%post
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
# limitations under the License

if [ -e "/etc/init.d/ambari-server" ]; then # Check is needed for upgrade
    # Remove link created by previous package version
    rm /etc/init.d/ambari-server
fi

ln -s /usr/sbin/ambari-server /etc/init.d/ambari-server

case "$1" in
  1) # Action install
    if [ -f "/var/lib/ambari-server/install-helper.sh" ]; then
        /var/lib/ambari-server/install-helper.sh install
    fi
    chkconfig --add ambari-server
  ;;
  2) # Action upgrade
    if [ -f "/var/lib/ambari-server/install-helper.sh" ]; then
        /var/lib/ambari-server/install-helper.sh upgrade
    fi
  ;;
esac

exit 0

%preun
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
# limitations under the License

# WARNING: This script is performed not only on uninstall, but also
# during package update. See http://www.ibm.com/developerworks/library/l-rpm2/
# for details

if [ "$1" -eq 0 ]; then  # Action is uninstall
    /usr/sbin/ambari-server stop > /dev/null 2>&1
    if [ -d "/etc/ambari-server/conf.save" ]; then
        mv /etc/ambari-server/conf.save /etc/ambari-server/conf_$(date '+%d_%m_%y_%H_%M').save
    fi

    if [ -e "/etc/init.d/ambari-server" ]; then
        # Remove link created during install
        rm /etc/init.d/ambari-server
    fi

    mv /etc/ambari-server/conf /etc/ambari-server/conf.save

    if [ -f "/var/lib/ambari-server/install-helper.sh" ]; then
      /var/lib/ambari-server/install-helper.sh remove
    fi

    chkconfig --list | grep ambari-server && chkconfig --del ambari-server
fi

exit 0

%posttrans
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
# limitations under the License


RESOURCE_MANAGEMENT_DIR="/usr/lib/python2.6/site-packages/resource_management"
RESOURCE_MANAGEMENT_DIR_SERVER="/usr/lib/ambari-server/lib/resource_management"
JINJA_DIR="/usr/lib/python2.6/site-packages/ambari_jinja2"
JINJA_SERVER_DIR="/usr/lib/ambari-server/lib/ambari_jinja2"

# remove RESOURCE_MANAGEMENT_DIR if it's a directory
if [ -d "$RESOURCE_MANAGEMENT_DIR" ]; then  # resource_management dir exists
  if [ ! -L "$RESOURCE_MANAGEMENT_DIR" ]; then # resource_management dir is not link
    rm -rf "$RESOURCE_MANAGEMENT_DIR"
  fi
fi
# setting resource_management shared resource
if [ ! -d "$RESOURCE_MANAGEMENT_DIR" ]; then
  ln -s "$RESOURCE_MANAGEMENT_DIR_SERVER" "$RESOURCE_MANAGEMENT_DIR"
fi

# setting jinja2 shared resource
if [ ! -d "$JINJA_DIR" ]; then
  ln -s "$JINJA_SERVER_DIR" "$JINJA_DIR"
fi

exit 0

%package server
Summary: Ambari Server
Group: Development/Libraries

%description server
Ambari Server

%package agent
Summary: Ambari Agent
Group: Development/Libraries
%description agent
Ambari Agent

%files server
%defattr(644,root,root,755)
 /usr/lib/ambari-server/ambari-metrics-common-2.2.0.0.0.jar
 /usr/lib/ambari-server/cloning-1.9.2.jar
 /usr/lib/ambari-server/httpclient-4.2.5.jar
 /usr/lib/ambari-server/quartz-jobs-2.2.1.jar
 /usr/lib/ambari-server/ambari-views-2.2.0.0.0.jar
 /usr/lib/ambari-server/api-ldap-model-1.0.0-M26.jar
 /usr/lib/ambari-server/jersey-json-1.18.jar
 /usr/lib/ambari-server/spring-aop-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/spring-beans-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/javax.inject-1.jar
 /usr/lib/ambari-server/mailapi-1.5.2.jar
 /usr/lib/ambari-server/api-asn1-ber-1.0.0-M26.jar
 /usr/lib/ambari-server/spring-expression-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/stax-api-1.0-2.jar
 /usr/lib/ambari-server/jsp-2.1-glassfish-2.1.v20100127.jar
 /usr/lib/ambari-server/spring-security-ldap-3.1.2.RELEASE.jar
 /usr/lib/ambari-server/jetty-servlets-8.1.17.v20150415.jar
 /usr/lib/ambari-server/jackson-core-asl-1.9.9.jar
 /usr/lib/ambari-server/slf4j-api-1.7.2.jar
 /usr/lib/ambari-server/spring-security-config-3.1.2.RELEASE.jar
 /usr/lib/ambari-server/javax.servlet-3.0.0.v201112011016.jar
 /usr/lib/ambari-server/jetty-client-8.1.17.v20150415.jar
 /usr/lib/ambari-server/eclipselink-2.5.2.jar
 /usr/lib/ambari-server/asm-3.3.1.jar
 /usr/lib/ambari-server/jetty-webapp-8.1.17.v20150415.jar
 /usr/lib/ambari-server/guice-multibindings-3.0.jar
 /usr/lib/ambari-server/jetty-xml-8.1.17.v20150415.jar
 /usr/lib/ambari-server/jetty-io-8.1.17.v20150415.jar
 /usr/lib/ambari-server/api-i18n-1.0.0-M26.jar
 /usr/lib/ambari-server/postgresql-9.3-1101-jdbc4.jar
 /usr/lib/ambari-server/apacheds-i18n-2.0.0-M19.jar
 /usr/lib/ambari-server/jackson-annotations-2.1.4.jar
 /usr/lib/ambari-server/velocity-1.7.jar
 /usr/lib/ambari-server/jersey-core-1.18.jar
 /usr/lib/ambari-server/guice-3.0.jar
 /usr/lib/ambari-server/ant-launcher-1.7.1.jar
 /usr/lib/ambari-server/objenesis-2.1.jar
 /usr/lib/ambari-server/spring-asm-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/javax.servlet-api-3.0.1.jar
 /usr/lib/ambari-server/jetty-util-8.1.17.v20150415.jar
 /usr/lib/ambari-server/jetty-server-8.1.17.v20150415.jar
 /usr/lib/ambari-server/spring-ldap-core-1.3.1.RELEASE.jar
 /usr/lib/ambari-server/spring-jdbc-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/commonj.sdo-2.1.1.jar
 /usr/lib/ambari-server/hadoop-annotations-2.6.0.jar
 /usr/lib/ambari-server/spring-security-core-3.1.2.RELEASE.jar
 /usr/lib/ambari-server/javax.persistence-2.1.0.jar
 /usr/lib/ambari-server/oro-2.0.8.jar
 /usr/lib/ambari-server/jackson-xc-1.9.9.jar
 /usr/lib/ambari-server/jackson-jaxrs-1.9.9.jar
 /usr/lib/ambari-server/spring-web-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/jetty-security-8.1.17.v20150415.jar
 /usr/lib/ambari-server/jsp-api-2.1-glassfish-2.1.v20100127.jar
 /usr/lib/ambari-server/jetty-continuation-8.1.17.v20150415.jar
 /usr/lib/ambari-server/apacheds-kerberos-codec-2.0.0-M19.jar
 /usr/lib/ambari-server/ecj-3.5.1.jar
 /usr/lib/ambari-server/ehcache-2.10.0.jar
 /usr/lib/ambari-server/jersey-servlet-1.18.jar
 /usr/lib/ambari-server/commons-csv-1.1.jar
 /usr/lib/ambari-server/activation-1.1.jar
 /usr/lib/ambari-server/jersey-guice-1.18.jar
 /usr/lib/ambari-server/commons-httpclient-3.1.jar
 /usr/lib/ambari-server/spring-security-web-3.1.2.RELEASE.jar
 /usr/lib/ambari-server/httpcore-4.2.4.jar
 /usr/lib/ambari-server/guice-servlet-3.0.jar
 /usr/lib/ambari-server/gson-2.2.2.jar
 /usr/lib/ambari-server/commons-collections-3.2.1.jar
 /usr/lib/ambari-server/smtp-1.5.2.jar
 /usr/lib/ambari-server/guice-assistedinject-3.0.jar
 /usr/lib/ambari-server/objenesis-tck-1.2.jar
 /usr/lib/ambari-server/jsr305-1.3.9.jar
 /usr/lib/ambari-server/cglib-2.2.2.jar
 /usr/lib/ambari-server/jersey-client-1.11.jar
 /usr/lib/ambari-server/jaxb-api-2.2.2.jar
 /usr/lib/ambari-server/guava-14.0.1.jar
 /usr/lib/ambari-server/jetty-http-8.1.17.v20150415.jar
 /usr/lib/ambari-server/commons-logging-1.1.1.jar
 /usr/lib/ambari-server/aopalliance-1.0.jar
 /usr/lib/ambari-server/commons-lang-2.5.jar
 /usr/lib/ambari-server/spring-core-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/kerberos-client-2.0.0-M19.jar
 /usr/lib/ambari-server/log4j-1.2.16.jar
 /usr/lib/ambari-server/spring-context-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/snmp4j-1.10.1.jar
 /usr/lib/ambari-server/commons-net-1.4.1.jar
 /usr/lib/ambari-server/antlr-2.7.7.jar
 /usr/lib/ambari-server/commons-codec-1.8.jar
 /usr/lib/ambari-server/quartz-2.2.1.jar
 /usr/lib/ambari-server/mimepull-1.9.3.jar
 /usr/lib/ambari-server/ant-1.6.5.jar
 /usr/lib/ambari-server/mina-core-2.0.9.jar
 /usr/lib/ambari-server/jetty-servlet-8.1.17.v20150415.jar
 /usr/lib/ambari-server/api-asn1-api-1.0.0-M26.jar
 /usr/lib/ambari-server/jersey-multipart-1.18.jar
 /usr/lib/ambari-server/derby-10.9.1.0.jar
 /usr/lib/ambari-server/spring-tx-3.0.7.RELEASE.jar
 /usr/lib/ambari-server/api-util-1.0.0-M26.jar
 /usr/lib/ambari-server/c3p0-0.9.1.1.jar
 /usr/lib/ambari-server/jersey-server-1.18.jar
 /usr/lib/ambari-server/slf4j-log4j12-1.7.2.jar
 /usr/lib/ambari-server/guice-persist-3.0.jar
 /usr/lib/ambari-server/ant-1.7.1.jar
 /usr/lib/ambari-server/jaxb-impl-2.2.3-1.jar
 /usr/lib/ambari-server/web
 /usr/lib/ambari-server/ambari-server-2.2.0.0.0.jar
 /usr/lib/ambari-server/lib/ambari_commons
 /usr/lib/ambari-server/lib/resource_management
 /usr/lib/ambari-server/commons-io-2.1.jar
 /usr/lib/ambari-server/jackson-mapper-asl-1.9.13.jar
 /usr/lib/ambari-server/jdk.tools-1.7.jar
 /var/lib/ambari-server/resources/README.txt
%attr(755,root,root) /usr/lib/ambari-server/lib/ambari_jinja2
%attr(755,root,root) /usr/lib/ambari-server/lib/ambari_simplejson
%attr(755,root,root) /usr/sbin/ambari-server.py
%attr(755,root,root) /usr/sbin/ambari-server
%attr(755,root,root) /usr/sbin/ambari_server_main.py
%attr(755,root,root) /var/lib/ambari-server//ambari-python-wrap
%config  /etc/ambari-server/conf
%config %attr(700,root,root) /var/lib/ambari-server//ambari-env.sh
%attr(700,root,root) /var/lib/ambari-server//ambari-sudo.sh
%attr(700,root,root) /var/lib/ambari-server//install-helper.sh
 /var/lib/ambari-server/keys/ca.config
%attr(700,root,root) /var/lib/ambari-server/keys/db
%dir  /var/run/ambari-server/bootstrap
%dir  /var/run/ambari-server/stack-recommendations
%dir  /var/log/ambari-server
 /var/lib/ambari-server/resources/Ambari-DDL-SQLAnywhere-DROP.sql
 /var/lib/ambari-server/resources/Ambari-DDL-MySQL-CREATE.sql
 /var/lib/ambari-server/resources/role_command_order.json
 /var/lib/ambari-server/resources/Ambari-DDL-Postgres-DROP.sql
 /var/lib/ambari-server/resources/Ambari-DDL-MySQL-DROP.sql
 /var/lib/ambari-server/resources/Ambari-DDL-Postgres-EMBEDDED-CREATE.sql
 /var/lib/ambari-server/resources/Ambari-DDL-SQLServer-CREATELOCAL.sql
 /var/lib/ambari-server/resources/Ambari-DDL-Postgres-CREATE.sql
 /var/lib/ambari-server/resources/Ambari-DDL-Postgres-EMBEDDED-DROP.sql
 /var/lib/ambari-server/resources/Ambari-DDL-SQLAnywhere-CREATE.sql
 /var/lib/ambari-server/resources/Ambari-DDL-Oracle-DROP.sql
 /var/lib/ambari-server/resources/DBConnectionVerification.jar
 /var/lib/ambari-server/resources/Ambari-DDL-SQLServer-DROP.sql
 /var/lib/ambari-server/resources/Ambari-DDL-SQLServer-CREATE.sql
 /var/lib/ambari-server/resources/Ambari-DDL-Oracle-CREATE.sql
%dir %attr(755,root,root) /var/lib/ambari-server/data/tmp
%dir %attr(700,root,root) /var/lib/ambari-server/data/cache
%attr(755,root,root) /var/lib/ambari-server/resources/apps
%attr(755,root,root) /var/lib/ambari-server/resources/scripts
%attr(755,root,root) /var/lib/ambari-server/resources/views
%dir  /var/lib/ambari-server/resources/upgrade
 /var/lib/ambari-server/resources/upgrade/ddl
 /var/lib/ambari-server/resources/upgrade/dml
 /var/lib/ambari-server/resources/common-services
 /var/lib/ambari-server/resources/upgrade/catalog
 /var/lib/ambari-server/resources/stacks/HDP
%attr(755,root,root) /var/lib/ambari-server/resources/stacks/stack_advisor.py
%attr(755,root,root) /usr/lib/python2.6/site-packages/ambari_server
%dir  /var/run/ambari-server
 /var/lib/ambari-server/resources/version
 /var/lib/ambari-server/resources/custom_action_definitions
%attr(755,root,root) /var/lib/ambari-server/resources/custom_actions
%attr(755,root,root) /var/lib/ambari-server/resources/host_scripts

%files agent
%attr(-,root,root) /usr/lib/python2.6/site-packages/ambari_agent
%attr(755,root,root) /var/lib/ambari-agent//ambari-python-wrap
%attr(755,root,root) /var/lib/ambari-agent//ambari-sudo.sh
%attr(-,root,root) /usr/lib/ambari-agent/lib/ambari_commons
%attr(-,root,root) /usr/lib/ambari-agent/lib/resource_management
%attr(755,root,root) /usr/lib/ambari-agent/lib/ambari_jinja2
%attr(755,root,root) /usr/lib/ambari-agent/lib/ambari_simplejson
%attr(755,root,root) /usr/lib/ambari-agent/lib/examples
%attr(755,root,root) /etc/ambari-agent/conf/ambari-agent.ini
%attr(755,root,root) /etc/ambari-agent/conf/logging.conf.sample
%attr(755,root,root) /usr/sbin/ambari-agent
%config %attr(700,root,root) /var/lib/ambari-agent/ambari-env.sh
%attr(700,root,root) /var/lib/ambari-agent/install-helper.sh
%attr(700,root,root) /var/lib/ambari-agent/upgrade_agent_configs.py
%dir %attr(755,root,root) /var/run/ambari-agent
%dir %attr(755,root,root) /var/lib/ambari-agent/data
%dir %attr(777,root,root) /var/lib/ambari-agent/tmp
%dir %attr(755,root,root) /var/lib/ambari-agent/keys
%dir %attr(755,root,root) /var/log/ambari-agent
%attr(755,root,root) /etc/rc.d/init.d
%attr(755,root,root) /var/lib/ambari-agent/data
%attr(755,root,root) /var/lib/ambari-agent/cache/custom_actions/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/templates/client.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/templates/runtime.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/falcon_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/falcon_server_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/falcon_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/package/scripts/falcon.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/configuration/falcon-runtime.properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/configuration/falcon-startup.properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/configuration/falcon-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FALCON/0.5.0.2.1/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/package/scripts/upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/package/scripts/kms_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/package/scripts/kms.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/package/scripts/kms_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/ranger-kms-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/kms-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/kms-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/dbks-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/kms-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/ranger-kms-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/kms-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/ranger-kms-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER_KMS/0.5.0.2.3/configuration/ranger-kms-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PXF/3.0.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PXF/3.0.0/package/templates/pxf-env.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PXF/3.0.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PXF/3.0.0/package/scripts/pxf.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PXF/3.0.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PXF/3.0.0/configuration/pxf-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/spark_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/spark_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/job_history_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/setup_spark.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/package/scripts/spark_thrift_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/configuration/spark-defaults.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/configuration/spark-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/configuration/spark-log4j-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.2.0.2.2/configuration/spark-metrics-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.3.1.2.3/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.4.1.2.3/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SPARK/1.4.1.2.3/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/alerts/alert_ha_namenode_health.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/alerts/alert_checkpoint_time.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/alerts/alert_datanode_unmounted_data_dir.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/alerts/alert_upgrade_finalized.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/templates/exclude_hosts_list.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/templates/slaves.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/templates/hdfs.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/files/checkWebUI.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/datanode_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/hdfs_nfsgateway.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/namenode_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/namenode_ha_state.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/balancer-emulator/balancer.log
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/balancer-emulator/balancer-err.log
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/balancer-emulator/hdfs-command.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/journalnode.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/hdfs_rebalance.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/hdfs_snamenode.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/datanode.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/hdfs_namenode.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/snamenode.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/zkfc_slave.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/hdfs_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/nfsgateway.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/install_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/hdfs.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/hdfs_datanode.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/setup_ranger_hdfs.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/namenode.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/journalnode_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/package/scripts/utils.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/configuration/hadoop-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/configuration/hadoop-policy.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/configuration/hdfs-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/configuration/ssl-client.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/configuration/ssl-server.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/configuration/core-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HDFS/2.1.0.2.0/configuration/hdfs-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/files/atlas-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/scripts/metadata.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/scripts/atlas_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/scripts/metadata_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/configuration/atlas-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ATLAS/0.1.0.2.3/configuration/application-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/alerts/alert_flume_agent_status.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/templates/flume.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/templates/log4j.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/templates/flume-metrics2.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/flume_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/flume.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/flume_handler.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/flume_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/package/scripts/service_mapping.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/configuration/flume-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/FLUME/1.4.0.2.0/configuration/flume-conf.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/templates/zookeeper_client_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/templates/zookeeper_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/templates/configuration.xsl.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/templates/zoo.cfg.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/files/zkSmoke.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/files/zkEnv.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/files/zkService.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/files/zkServer.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/zookeeper_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/zookeeper.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/zookeeper_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/package/scripts/zookeeper_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/configuration/zoo.cfg.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/configuration/zookeeper-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ZOOKEEPER/3.4.5.2.0/configuration/zookeeper-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/alerts/alert_check_oozie_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/templates/adminusers.txt.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/templates/oozie-log4j.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/files/oozieSmoke2.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/files/prepareOozieHdfsDirectories.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/files/wrap_ooziedb.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/oozie_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/oozie_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/oozie_server_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/oozie_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/check_oozie_server_status.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/package/scripts/oozie.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/configuration/oozie-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.0.0.2.0/configuration/oozie-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.2.0.2.3/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.2.0.2.3/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.2.0.2.3/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.2.0.2.3/configuration/oozie-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/OOZIE/4.2.0.2.3/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/files/pigSmoke.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/scripts/pig_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/scripts/pig.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/configuration/pig-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/configuration/pig-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/PIG/0.12.0.2.0/configuration/pig-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/scripts/sqoop_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/package/scripts/sqoop.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SQOOP/1.4.4.2.0/configuration/sqoop-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/YARN_metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/YARN_widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/alerts/alert_nodemanager_health.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/alerts/alert_nodemanagers_summary.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/templates/container-executor.cfg.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/templates/exclude_hosts_list.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/templates/yarn.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/templates/taskcontroller.cfg.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/templates/mapreduce.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/files/validateYarnComponentStatusWindows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/nodemanager_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/mapred_service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/resourcemanager.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/mapreduce2_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/yarn_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/yarn.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/install_jars.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/nodemanager.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/historyserver.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/setup_ranger_yarn.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/package/scripts/application_timeline_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/configuration-mapred/mapred-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/configuration-mapred/mapred-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/configuration/yarn-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/configuration/yarn-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/configuration/yarn-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/configuration/capacity-scheduler.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/YARN/2.1.0.2.0/MAPREDUCE2_metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/templates/kadm5_acl.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/templates/krb5_conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/templates/kdc_conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/scripts/kerberos_common.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/scripts/kerberos_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/scripts/kerberos_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/package/scripts/utils.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/configuration/krb5-conf.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KERBEROS/1.10.3-10/configuration/kerberos-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hbase_queryserver_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hbase_master_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hbase_client_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hbase.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hadoop-metrics2-hbase.properties-GANGLIA-RS.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/regionservers.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hbase-smoke.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hadoop-metrics2-hbase.properties-GANGLIA-MASTER.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hbase_grant_permissions.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/templates/hbase_regionserver_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/files/hbaseSmokeVerify.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/files/draining_servers.rb
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/hbase.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/phoenix_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/setup_ranger_hbase.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/hbase_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/functions.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/phoenix_queryserver.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/hbase_decommission.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/hbase_master.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/hbase_regionserver.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/hbase_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/package/scripts/hbase_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/configuration/hbase-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/configuration/hbase-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/configuration/hbase-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HBASE/0.96.0.2.0/configuration/hbase-policy.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/templates/gangliaLib.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/templates/gangliaEnv.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/templates/ganglia.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/templates/gangliaClusters.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/templates/rrd.py.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/startGmetad.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/gmetad.init
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/stopRrdcached.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/checkRrdcached.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/stopGmond.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/checkGmond.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/teardownGanglia.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/startRrdcached.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/gmetadLib.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/gmond.init
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/stopGmetad.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/checkGmetad.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/setupGanglia.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/gmondLib.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/rrdcachedLib.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/files/startGmond.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/ganglia.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/functions.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/ganglia_server_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/ganglia_monitor_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/ganglia_monitor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/ganglia_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/GANGLIA/3.5.0/configuration/ganglia-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/alerts/alert_ambari_metrics_monitor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/hbase_master_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/hbase_client_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/ams_zookeeper_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/regionservers.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/ams.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/metric_monitor.ini.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/hadoop-metrics2-hbase.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/hbase_grant_permissions.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/hbase_regionserver_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/smoketest_metrics.json.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/ams_collector_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/templates/metric_groups.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/YARN.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/STORM.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/HBASE.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/HOST.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/FLUME.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/AMBARI_METRICS.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/KAFKA.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/service-metrics/HDFS.txt
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/files/hbaseSmokeVerify.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/hbase.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/metrics_monitor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/metrics_collector.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/ams_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/functions.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/split_points.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/hbase_master.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/hbase_regionserver.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/hbase_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/service_mapping.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/ams.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/package/scripts/status.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-hbase-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-hbase-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-hbase-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-hbase-security-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/storm-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-hbase-policy.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/AMBARI_METRICS/0.1.0/configuration/ams-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/package/scripts/mahout.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/package/scripts/mahout_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/configuration/mahout-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/MAHOUT/1.0.0.2.3/configuration/mahout-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/templates/storm-slider-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/files/hbaseSmokeVerify.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/scripts/slider_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/package/scripts/slider.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/configuration/slider-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/configuration/slider-client.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/SLIDER/0.60.0.2.2/configuration/slider-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/hadoop-metrics2-accumulo.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/tracers.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/monitor_logger.xml.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/generic_logger.xml.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/slaves.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/masters.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/monitor.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/gc.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/templates/auditLog.xml.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/files/accumulo-metrics.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_monitor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_tserver.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_tracer.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_gc.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_configuration.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_master.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/package/scripts/accumulo_script.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/configuration/accumulo-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/configuration/client.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/configuration/accumulo-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/ACCUMULO/1.6.1.2.2.0/configuration/accumulo-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/scripts/tez_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/scripts/pre_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/scripts/tez.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/configuration/tez-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/TEZ/0.4.0.2.1/configuration/tez-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/templates/hawq-profile.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/templates/hawq-hosts.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/templates/slaves.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/common.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/hawqmaster.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/master_helper.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/hawqstandby.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/constants.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/hawqsegment.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/hawqstatus.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/package/scripts/utils.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/configuration/hawq-limits-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/configuration/gpcheck-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/configuration/hawq-sysctl-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HAWQ/2.0.0/configuration/hawq-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/alerts/alert_webhcat_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/alerts/alert_hive_metastore.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/alerts/alert_hive_thrift_port.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/templates/templeton_smoke.pig.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/templates/hive.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/templates/startHiveserver2.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/addMysqlUser.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/hiveSmoke.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/startMetastore.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/hiveserver2Smoke.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/hcatSmoke.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/hiveserver2.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/pigSmoke.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/removeMysqlUser.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/hiveTezSetup.cmd
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/files/templetonSmoke.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/etc/hive-schema-0.12.0.mysql.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/etc/hive-schema-0.12.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/etc/hive-schema-0.12.0.postgres.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/webhcat.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hive_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/setup_atlas_hive.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/webhcat_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hive.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hive_metastore.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hive_server_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hcat_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hive_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/webhcat_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/setup_ranger_hive.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/mysql_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hcat.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hcat_service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/mysql_users.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/mysql_utils.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/webhcat_service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/mysql_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/scripts/hive_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/etc/hive-schema-0.12.0.mysql.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/etc/hive-schema-0.12.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/etc/hive-schema-0.12.0.postgres.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/hive-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/webhcat-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/webhcat-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/hive-exec-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/webhcat-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/hive-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/hive-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/configuration/hcat-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/alerts/check_supervisor_process_win.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/templates/worker-launcher.cfg.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/templates/client_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/templates/storm_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/templates/config.yaml.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/templates/storm-metrics2.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/files/wordCount.jar
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/supervisor_prod.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/supervisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/storm_upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/nimbus_prod.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/storm_yaml_utils.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/rest_api.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/setup_ranger_storm.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/drpc_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/ui_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/storm.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/nimbus.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/package/scripts/supervisord_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/configuration/storm-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/STORM/0.9.1.2.1/configuration/storm-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/templates/krb5JAASLogin.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/files/validateKnoxStatus.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/params_windows.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/setup_ranger_knox.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/params_linux.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/knox.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/knox_gateway.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/package/scripts/knox_ldap.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/configuration/topology.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/configuration/gateway-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/configuration/ranger-knox-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/configuration/users-ldif.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/configuration/gateway-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/configuration/knox-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KNOX/0.5.0.2.2/configuration/ldap-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/alerts/alert_ranger_admin_passwd_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/setup_ranger.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/ranger_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/setup_ranger_xml.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/ranger_admin.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/package/scripts/ranger_usersync.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/configuration/ranger-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/configuration/ranger-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/configuration/admin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/RANGER/0.4.0/configuration/usersync-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/templates/kafka_client_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/templates/kafka_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/templates/kafka.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/kafka_broker.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/upgrade.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/kafka.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/setup_ranger_kafka.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/package/scripts/utils.py
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/configuration/kafka-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/configuration/kafka-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/common-services/KAFKA/0.8.1.2.2/configuration/kafka-broker.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/stack_advisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/FALCON/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/FALCON/configuration/falcon-startup.properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/FALCON/configuration/falcon-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER_KMS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/stack_advisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/SPARK/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/SPARK/configuration/spark-thrift-sparkconf.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/SPARK/configuration/spark-hive-site-override.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/configuration/ranger-hdfs-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/configuration/ranger-hdfs-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/configuration/ranger-hdfs-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/configuration/hadoop-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/configuration/hdfs-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HDFS/configuration/ranger-hdfs-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/ATLAS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/FLUME/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/PIG/configuration/pig-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/YARN_widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration-mapred/mapred-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/ranger-yarn-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/ranger-yarn-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/ranger-yarn-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/ranger-yarn-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/yarn-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/yarn-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/yarn-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/YARN/configuration/capacity-scheduler.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KERBEROS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/configuration/ranger-hbase-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/configuration/hbase-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/configuration/ranger-hbase-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/configuration/hbase-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/configuration/ranger-hbase-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/configuration/ranger-hbase-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HBASE/themes/theme.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/MAHOUT/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/SLIDER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/ACCUMULO/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/ACCUMULO/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/ACCUMULO/widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/ACCUMULO/configuration/accumulo-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/TEZ/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/TEZ/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/TEZ/configuration/tez-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/configuration/ranger-hive-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/configuration/webhcat-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/configuration/hive-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/configuration/ranger-hive-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/configuration/hive-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/configuration/ranger-hive-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/HIVE/configuration/ranger-hive-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/ranger-storm-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/storm-cluster-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/ranger-storm-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/storm-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/storm-worker-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/ranger-storm-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/ranger-storm-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/STORM/configuration/storm-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KNOX/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KNOX/configuration/ranger-knox-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KNOX/configuration/ranger-knox-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KNOX/configuration/ranger-knox-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KNOX/configuration/ranger-knox-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/alerts.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/configuration/ranger-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/configuration/ranger-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/configuration/admin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/configuration/usersync-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/configuration/ranger-admin-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/configuration/ranger-ugsync-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/RANGER/themes/theme_version_2.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/configuration/ranger-kafka-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/configuration/kafka-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/configuration/ranger-kafka-security.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/configuration/kafka-broker.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/configuration/ranger-kafka-audit.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/services/KAFKA/configuration/ranger-kafka-policymgr-ssl.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/upgrades/upgrade-2.3.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/upgrades/config-upgrade.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/upgrades/nonrolling-upgrade-2.3.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/FALCON/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/FALCON/configuration/falcon-startup.properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/FALCON/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/stack_advisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/SPARK/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HDFS/configuration/hadoop-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HDFS/configuration/hdfs-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HDFS/configuration/core-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HDFS/configuration/hdfs-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HDFS/configuration/ranger-hdfs-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HDFS/themes/theme.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/FLUME/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/OOZIE/configuration/oozie-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/OOZIE/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/themes-mapred/theme.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/configuration-mapred/mapred-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/configuration-mapred/mapred-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/configuration/yarn-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/configuration/yarn-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/configuration/capacity-scheduler.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/YARN/themes/theme.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/KERBEROS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HBASE/configuration/hbase-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HBASE/configuration/hbase-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HBASE/configuration/ranger-hbase-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HBASE/themes/theme.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/SLIDER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/TEZ/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/TEZ/configuration/tez-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HIVE/configuration/webhcat-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HIVE/configuration/hive-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HIVE/configuration/ranger-hive-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HIVE/configuration/hive-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HIVE/configuration/hiveserver2-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/HIVE/themes/theme.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/STORM/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/STORM/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/STORM/configuration/storm-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/STORM/configuration/ranger-storm-plugin-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/STORM/configuration/storm-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/KNOX/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/RANGER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/RANGER/themes/theme_version_1.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/services/KAFKA/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/upgrades/upgrade-2.3.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/upgrades/config-upgrade.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/upgrades/nonrolling-upgrade-2.2.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/upgrades/nonrolling-upgrade-2.3.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/upgrades/upgrade-2.2.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.2/configuration/cluster-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/stack_advisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/FLUME/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/KERBEROS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/GANGLIA/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/AMBARI_METRICS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/widgets.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/configuration/cluster-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/after-INSTALL/scripts/hook.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/after-INSTALL/scripts/shared_initialization.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/after-INSTALL/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/templates/topology_mappings.data.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/templates/exclude_hosts_list.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/templates/commons-logging.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/templates/hadoop-metrics2.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/templates/health_check.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/templates/include_hosts_list.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/files/checkForFormat.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/files/task-log4j.properties
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/files/topology_script.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/files/fast-hdfs-resource.jar
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/scripts/hook.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/scripts/shared_initialization.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-START/scripts/rack_awareness.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-RESTART/scripts/hook.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-ANY/files/changeToSecureUid.sh
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-ANY/scripts/hook.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-ANY/scripts/shared_initialization.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-ANY/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-INSTALL/scripts/hook.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-INSTALL/scripts/shared_initialization.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-INSTALL/scripts/repo_initialization.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/before-INSTALL/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6/hooks/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/FALCON/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/stack_advisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/SPARK/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/FLUME/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/OOZIE/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/YARN/configuration-mapred/mapred-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/YARN/configuration-mapred/core-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/YARN/configuration-mapred/mapred-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/YARN/configuration/yarn-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/YARN/configuration/capacity-scheduler.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/KERBEROS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/HBASE/configuration/hbase-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/MAHOUT/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/SLIDER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/ACCUMULO/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/ACCUMULO/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/ACCUMULO/configuration/accumulo-log4j.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/package/templates/glusterfs.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/package/templates/glusterfs-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/package/scripts/glusterfs.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/package/scripts/glusterfs_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/configuration/hadoop-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/GLUSTERFS/configuration/core-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/TEZ/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/TEZ/kerberos.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/TEZ/configuration/tez-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/HIVE/configuration/webhcat-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/HIVE/configuration/hive-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/STORM/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/KNOX/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/RANGER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/services/KAFKA/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/configuration/cluster-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/configuration/cluster-env.xml.version
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.3.GlusterFS/configuration/cluster-env.xml.noversion
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/blueprints/multinode-default.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/blueprints/singlenode-default.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/FALCON/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/FALCON/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/stack_advisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HDFS/configuration/hdfs-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/FLUME/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/OOZIE/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/PIG/configuration/pig-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/YARN/configuration/yarn-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/YARN/configuration/yarn-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/KERBEROS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/TEZ/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HIVE/etc/hive-schema-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HIVE/etc/hive-schema-0.13.0.mysql.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HIVE/etc/hive-schema-0.13.0.postgres.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HIVE/etc/upgrade-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HIVE/etc/upgrade-0.12.0-to-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/HIVE/configuration/hive-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/services/STORM/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/upgrades/nonrolling-upgrade-2.3.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/blueprints/multinode-default.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/blueprints/singlenode-default.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/templates/falcon-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/templates/startup.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/templates/client.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/templates/runtime.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/scripts/falcon_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/scripts/falcon_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/package/scripts/falcon.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/configuration/falcon-runtime.properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/configuration/falcon-startup.properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/configuration/falcon-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FALCON/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/FLUME/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/OOZIE/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/PIG/configuration/pig-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/templates/container-executor.cfg.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/templates/exclude_hosts_list.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/templates/yarn-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/templates/yarn.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/templates/mapreduce.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/files/validateYarnComponentStatus.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/mapred_service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/resourcemanager.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/mapreduce2_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/yarn_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/yarn.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/nodemanager.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/historyserver.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/package/scripts/application_timeline_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration-mapred/mapred-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration-mapred/core-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration-mapred/ssl-client.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration-mapred/ssl-server.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration-mapred/mapred-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration/mapred-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration/yarn-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration/yarn-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/YARN/configuration/capacity-scheduler.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HBASE/configuration/hbase-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/package/templates/glusterfs.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/package/templates/glusterfs-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/package/scripts/glusterfs.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/package/scripts/glusterfs_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/configuration/hadoop-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/GLUSTERFS/configuration/core-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/package/templates/tez-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/package/scripts/tez_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/package/scripts/tez.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/configuration/tez-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/TEZ/configuration/tez-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HIVE/etc/hive-schema-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HIVE/etc/hive-schema-0.13.0.mysql.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HIVE/etc/hive-schema-0.13.0.postgres.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HIVE/etc/upgrade-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HIVE/etc/upgrade-0.12.0-to-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/HIVE/configuration/hive-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/templates/storm_jaas.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/templates/config.yaml.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/templates/storm-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/files/wordCount.jar
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/supervisor_prod.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/supervisor.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/nimbus_prod.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/rest_api.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/drpc_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/ui_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/storm.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/yaml_config.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/nimbus.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/package/scripts/supervisord_service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/configuration/storm-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/services/STORM/configuration/storm-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.1.GlusterFS/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/role_command_order.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HDFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HDFS/configuration/hadoop-policy.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HDFS/configuration/hdfs-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HDFS/configuration/global.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HDFS/configuration/core-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/ZOOKEEPER/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/OOZIE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/OOZIE/configuration/oozie-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/PIG/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/PIG/configuration/pig-properties.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/SQOOP/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/metrics.json
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/templates/container-executor.cfg.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/templates/yarn-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/templates/yarn.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/templates/mapreduce.conf.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/files/validateYarnComponentStatus.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/mapred_service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/__init__.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/resourcemanager.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/mapreduce2_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/yarn_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/yarn.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/status_params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/service.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/nodemanager.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/historyserver.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/package/scripts/application_timeline_server.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/configuration-mapred/mapred-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/configuration-mapred/core-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/configuration-mapred/mapred-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/configuration/mapred-site.xml.2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/configuration/yarn-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/configuration/yarn-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/YARN/configuration/capacity-scheduler.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HBASE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HBASE/configuration/hbase-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/package/templates/glusterfs.properties.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/package/templates/glusterfs-env.sh.j2
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/package/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/package/scripts/glusterfs.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/package/scripts/service_check.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/package/scripts/glusterfs_client.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/package/scripts/params.py
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/configuration/hadoop-env.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/GLUSTERFS/configuration/core-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HIVE/metainfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HIVE/etc/hive-schema-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HIVE/etc/hive-schema-0.13.0.mysql.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HIVE/etc/hive-schema-0.13.0.postgres.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HIVE/etc/upgrade-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HIVE/etc/upgrade-0.12.0-to-0.13.0.oracle.sql
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/services/HIVE/configuration/hive-site.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/stacks/HDP/2.0.6.GlusterFS/repos/repoinfo.xml
%attr(755,root,root) /var/lib/ambari-agent/cache/host_scripts/.hash
%attr(755,root,root) /var/lib/ambari-agent/cache/host_scripts/alert_disk_space.py
%attr(755,root,root) /var/lib/ambari-agent/cache/custom_actions

