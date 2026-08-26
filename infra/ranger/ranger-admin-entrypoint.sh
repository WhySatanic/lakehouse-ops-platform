#!/bin/bash
set -euo pipefail

if [ ! -e "${RANGER_HOME}/.setupDone" ]; then
  cp "${RANGER_SCRIPTS}/ranger-admin-install.properties" "${RANGER_HOME}/admin/install.properties"
  {
    echo "db_root_password=${POSTGRES_PASSWORD}"
    echo "db_user=${RANGER_DB_USER}"
    echo "db_password=${RANGER_DB_PASSWORD}"
    echo "rangerAdmin_password=${RANGER_DB_PASSWORD}"
    echo "rangerTagsync_password=${RANGER_DB_PASSWORD}"
    echo "rangerUsersync_password=${RANGER_DB_PASSWORD}"
    echo "keyadmin_password=${RANGER_DB_PASSWORD}"
  } >> "${RANGER_HOME}/admin/install.properties"

  cd "${RANGER_HOME}/admin"
  ./setup.sh
  rm -f "${RANGER_HOME}/admin/install.properties"
  touch "${RANGER_HOME}/.setupDone"
  setup_ranger=true
else
  setup_ranger=false
fi

config="${RANGER_HOME}/admin/conf/ranger-admin-default-site.xml"
property=ranger.admin.allow.unauthenticated.download.access

if ! grep -q "<name>${property}</name>" "$config"; then
  echo "Ranger download access property is missing" >&2
  exit 1
fi

sed -i "/<name>${property}<\/name>/,/<\/property>/ s#<value>false</value>#<value>true</value>#" "$config"

cd "${RANGER_HOME}/admin"
./ews/ranger-admin-services.sh start

if [ "$setup_ranger" = true ]; then
  sleep 30
  python3 "${RANGER_SCRIPTS}/create-ranger-services.py"
fi

ranger_admin_pid=$(pgrep -f org.apache.ranger.server.tomcat.EmbeddedServer | head -1 || true)
if [ -z "$ranger_admin_pid" ]; then
  echo "Ranger Admin process exited" >&2
  exit 1
fi

tail --pid="$ranger_admin_pid" -f /dev/null
