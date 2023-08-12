#/bin/bash

set -o errexit

readonly REQUIRED_ENV_VARS=(
  "POSTGRES_USER"
  "DB_NAME"
  "DB_PASSWORD"
  "DB_PORT"
  "DB_USERNAME"
  "TEST_DB_NAME"
  "TEST_DB_PASSWORD"
  "TEST_DB_PORT"
  "TEST_DB_USERNAME"
)

main(){
  check_environment_variables_set
  init_user_and_database
}

check_environment_variables_set(){
  for required_environment_variable in ${REQUIRED_ENV_VARS[@]}; do
    if [[ -z "${!required_environment_variable}" ]]; then
      echo "Missing the following environment variable '$required_environment_variable'.
    You must set the following environment variables:
      ${REQUIRED_ENV_VARS[@]}
Aborting."
      exit 1
    fi
  done
}

init_user_and_database(){
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $DB_USERNAME WITH PASSWORD '$DB_PASSWORD';
    ALTER ROLE $DB_USERNAME SUPERUSER;
    CREATE DATABASE $DB_NAME;
    GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USERNAME;

    CREATE USER $TEST_DB_USERNAME WITH PASSWORD '$TEST_DB_PASSWORD';
    ALTER ROLE $TEST_DB_USERNAME SUPERUSER;
    CREATE DATABASE $TEST_DB_NAME;
    GRANT ALL PRIVILEGES ON DATABASE $TEST_DB_NAME TO $TEST_DB_USERNAME;
EOSQL
}

main "$@"