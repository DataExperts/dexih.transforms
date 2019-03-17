#!/usr/bin/env bash

docker run --name=sqlserver -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=Password12!' -p 1433:1433 -d microsoft/mssql-server-linux