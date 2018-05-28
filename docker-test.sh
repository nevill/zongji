#!/bin/bash
MYSQL_HOSTS="mysql55 mysql56 mysql57"

for hostname in ${MYSQL_HOSTS}; do
  echo $hostname
  docker run -it --network=zongji_default -e MYSQL_HOST=$hostname -w /build -v $PWD:/build node:8 npm test
done
