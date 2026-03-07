#!/bin/sh

exec /usr/bin/redis-server --bind 127.0.0.1 --protected-mode yes --port 6379 --dir $SNAP_DATA
