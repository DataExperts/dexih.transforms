#!/usr/bin/env bash

docker run -d --name mysql -p 3306:3306 -p 33060:33060 -p 8080:8080 -e MYSQL_ROOT_PASSWORD=password mysql