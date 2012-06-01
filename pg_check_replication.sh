#!/bin/sh
#
# Shell script for checking the replication between the primary and secondary
# PostgreSQL Servers in a Streaming Replication Setup.
#
# Author:    Ali Onur Uyar
# Email:     aouyar@gmail.com
# Copyright: 2011, Ali Onur Uyar
# License:   GPLv3

source /var/lib/pgsql/9.0/pg_manage.conf

if [ "$SERVERTYPE" = "PRIMARY" ]; then
        echo PRIMARY: $(hostname)
	psql -c 'select pg_current_xlog_location();'
	echo
	ssh $SECONDARYHOST 'echo SECONDARY: $(hostname); psql -c "select pg_last_xlog_receive_location(), pg_last_xlog_replay_location();"'
else
        echo SECONDARY: $(hostname)
	psql -c 'select pg_last_xlog_receive_location(), pg_last_xlog_replay_location();'
        echo
	ssh $PRIMARYHOST 'echo PRIMARY: $(hostname); psql -c "select pg_current_xlog_location();"'
fi

