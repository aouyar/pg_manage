pg_manage - Backup / Recovery Scripts for PostgreSQL Database
=============================================================

About
-----

Scripts for automating automating backup and recovery tasks for 
_PostgreSQL Database Server_. The following operations have been implemented:

* Sending archive logs to backup server.
* Retrieval of archive logs from backup server.
* Initialization of the database.
* Generation of hot backups with archive logging.
* Recovery from hot backup.
* Dump (export) and restore (import) of databases.
* Initial synchronization of database servers for setting up replication.
* checking the replication between the primary and secondary 
  _PostgreSQL Servers_ in a _Streaming Replication_ setup.
  
Sample configuration files have been provided for configuring the 
_PostgreSQL Database Server_ for archive logging, recovery using archive logs
and _Streaming Replication_.

For information on other projects you can check 
my [GitHub Personal Page](http://aouyar.github.com)
and [GitHub Profile](https://github.com/aouyar).


Documentation
-------------

The documentation for the project and sample graphs for plugins will be 
published in the [pg_manage Project Web Page](http://aouyar.github.com/pg_manage/)


Licensing
---------

_pg_manage_ is copyrighted free software made available under the terms of the 
_GPL License Version 3_ or later.

See the _COPYING_ file that acompanies the code for full licensing information.


Credits
-------

_pg_manage_ has been developed 
by [aouyar](https://github.com/aouyar) (Ali Onur Uyar).
