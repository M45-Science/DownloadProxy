#!/bin/bash
git pull
go build
killall goHTTPCacher
