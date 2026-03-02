#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <username> <hostname>"
    exit 1
fi

THIS_DIR=$(dirname "$(readlink -f "$0")")
source $THIS_DIR"/constants.sh"
source "$THIS_DIR/../../shared_utils.sh"

USERNAME=$1
HOSTNAME=$2
DESTINATION_DIR=$REMOTE_ROOT_DIR"code/"

COMPONENTS_CONF_FILE="$THIS_DIR/../../components.conf"
readarray -t COMPONENTS < <(load_components_config "$COMPONENTS_CONF_FILE")
readarray -t DIRS_TO_RSYNC < <(build_rsync_paths "$THIS_DIR" "${COMPONENTS[@]}")

echo "The following directories will be rsynced to $HOSTNAME:$DESTINATION_DIR:"
for DIR in "${DIRS_TO_RSYNC[@]}"; do
    echo "  $DIR"
done

RSYNC_OUTPUT=$(perform_rsync "$USERNAME" "$HOSTNAME" "$DESTINATION_DIR" "$OPTIONS" "${DIRS_TO_RSYNC[@]}")

echo ""
SYNCED_COMPONENTS=($(parse_rsync_output "$RSYNC_OUTPUT" "${DIRS_TO_RSYNC[@]}" "${COMPONENTS[@]}"))

if [ ${#SYNCED_COMPONENTS[@]} -eq 0 ]; then
    echo "No components had changes to sync."
else
    echo "Components that were synced due to changes:"
    for COMPONENT in "${SYNCED_COMPONENTS[@]}"; do
        echo "  $COMPONENT"
    done

    echo ""
    echo "Installing synced internal components on $HOSTNAME..."

    # Filter out Utilities component as it doesn't have an installation directory
    INSTALLABLE_COMPONENTS=()
    for COMPONENT in "${SYNCED_COMPONENTS[@]}"; do
        if [ "$COMPONENT" != "Utilities" ]; then
            INSTALLABLE_COMPONENTS+=("$COMPONENT")
        fi
    done

    if [ ${#INSTALLABLE_COMPONENTS[@]} -gt 0 ]; then
        # Use the existing only_install_internal_components.sh script on the remote
        ssh $OPTIONS $USERNAME@$HOSTNAME "cd $DESTINATION_DIR/Utilities/installation && ./only_install_internal_components.sh ${INSTALLABLE_COMPONENTS[*]}"
        echo "Installation complete for $HOSTNAME"
    else
        echo "No installable components to process for $HOSTNAME"
    fi
fi
