aptible login --email="${APTIBLE_USER}" --password="${APTIBLE_PASSWORD}"

echo "Logged in through Aptible for [user: ${APTIBLE_USER}]"

aptible ssh --app commons-production "$@"
