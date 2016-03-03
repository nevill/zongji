<<<<<<< HEAD
#!/bin/sh

echo -e "\033[1;35m Run test on 5.1.73\033[0m"
TEST_MYSQL_PORT=3351 npm test || exit $?
echo -e "\033[1;35m Run test on 5.5.41\033[0m"
TEST_MYSQL_PORT=3355 npm test || exit $?
# echo -e "\033[1;35m Run test on 5.6.13\033[0m"
# TEST_MYSQL_PORT=3456 npm test || exit $?
echo -e "\033[1;35m Run test on 5.6.22\033[0m"
TEST_MYSQL_PORT=3356 npm test || exit $?
=======
#!/bin/bash
mysqlPorts=( 3351 3355 3356 3357 )
for i in "${mysqlPorts[@]}"; do
  while ! mysqladmin ping -h127.0.0.1 -P$i --silent; do
    echo "$(date) - still trying $i"
    sleep 1
  done
  echo "$(date) - connected successfully $i"
  echo -e "\033[1;35m Running test on port $i\033[0m"
  TEST_MYSQL_PORT=$i npm test || exit $?
done
>>>>>>> upstream/master
