#!/bin/bash
MYSQL_HOSTS="mysql55 mysql56 mysql57"

for hostname in ${MYSQL_HOSTS}; do
  echo $hostname + node 8
  docker run -it --network=zongji_default -e MYSQL_HOST=$hostname -w /build -v $PWD:/build node:8 npm test
  echo $hostname + node 10
  docker run -it --network=zongji_default -e MYSQL_HOST=$hostname -w /build -v $PWD:/build node:10 npm test
  echo $hostname + node 12
  docker run -it --network=zongji_default -e MYSQL_HOST=$hostname -w /build -v $PWD:/build node:12 npm test
done
