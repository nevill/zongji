#!/bin/bash
mysqlPorts=( 3351 3355 3356 3357 )
sqlModes=( ANSI_QUOTES "" )
testModes=( big_numbers big_number_strings "" date_strings )
for testMode in "${testModes[@]}"; do
  for mode in "${sqlModes[@]}"; do
    for i in "${mysqlPorts[@]}"; do
      while ! mysqladmin ping -h127.0.0.1 -P$i --silent; do
        echo "$(date) - still trying $i"
        sleep 1
      done
      echo "$(date) - connected successfully $i"
      echo -e "\033[1;35m Running test on port $i using mode '$mode' with testMode:$testMode \033[0m"
      TEST_MODE=$testMode TEST_SESSION_SQL_MODE=$mode TEST_MYSQL_PORT=$i npm test || exit $?
    done
  done
done
