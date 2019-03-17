#!/usr/bin/env bash

docker run --name postgres -p 5433:5432 -e POSTGRES_PASSWORD=password -d postgres