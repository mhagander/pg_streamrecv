========================================================
 pg_streamrecv - PostgreSQL replication stream receiver
========================================================

Introduction
============
pg_streamrecv is a program designed to use the PostgreSQL 9.0+ streaming replication functionality to generate transaction log files     mimicking a normal log archiving directory. This is intended to solve two main issues with the regular transaction logging:

1. Without *archive_timeout* set, there is an infintely long window of possible data-loss when the database write activity is low, since transaction logfiles are only shipped every 16Mb. And even with *archive_timeout* set to a reasonable value, this can be a significant window of dataloss.

2. If *archive_timeout* is set low to work around the issues in (1),       the volume of transaction logs getting archived grows huge since files of 16Mb are shipped even if they are not filled.

Operation
=========
pg_streamrecv will connect to the server and start a replication stream. As data is received, it gets written to a file with a normal WAL segment name in the *inprogress* directory. When a complete file is received, it gets moved into the main archiving directory. If pg_streamrecv is restarted for some reason (crash, stop/start...), it will look at files in the *inprogress* directory, and if it's there, it will move it away under a safe name and *retransmit* this file from the beginning.

The partial file is left under a different name, in case the partial segment is the last there is - if the server had a catastrophic failure, this will be the very latest transactions and should not be thrown away. Only when the segment has been retransmitted past this point from the master, the file is removed.

Integrating with archive_command
================================
pg_streamrecv is in most cases *not* enough to run on it's own. It relies on the WAL sender to be able to send all the segments not yet sent - and the master does not give a guarantee on this, only that it will keep *keep_wal_segments* segments around. Setting *archive_command* will guarantee that the segment is sent before it's being removed on the master.

**TODO**: Document some ways of setting up an archive_command that works well together with pg_streamrecv.

Usage
=====
::

	pg_streamrecv -c <connectionstring> -d <directory> [-v]


connectionstring
	The PostgreSQL connection string to use, and should specify both server and user. It should *not* specify the database - pg_streamrecv will automatically connect to both the *postgres* database and the *replication* pseudo-database. The string is in "libpq-format".

directory
	The directory to write WAL files to. pg_streamrecv will automatically create a subdirectory called *inprogress* in this directory, and move all segments into it as they are received.

-v
	Add -v to get more verbose output.

