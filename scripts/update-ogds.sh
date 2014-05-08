#!/bin/sh
PROFILE_PATH="/home/zope/.opengever/.profile.sh"
CONFIG_PATH="/home/zope/.opengever/.config.sh"

if [ -f $PROFILE_PATH ]
  then
    echo "Loading site-specific environment from $PROFILE_PATH..."
    source $PROFILE_PATH
fi

if [ -f $CONFIG_PATH ]
  then
    echo "Loading site-specific configuration from $CONFIG_PATH..."
    source $CONFIG_PATH
else
    echo "ERROR: Configuration not found at $CONFIG_PATH"
    echo "Please create the file and set configuration variables according"
    echo "to opengever.maintenance/scripts/.config.sh.template."
    exit 1
fi

echo "Updating OGDS used by client '${FIRST_CLIENT_ID}' at '${FIRST_CLIENT_PATH}'..."
${FIRST_CLIENT_PATH}/bin/instance0 run ${FIRST_CLIENT_PATH}/src/opengever.maintenance/scripts/start_synchronisation.py -s ${FIRST_CLIENT_ID}
