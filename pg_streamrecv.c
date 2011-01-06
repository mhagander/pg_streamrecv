/*
 * pg_streamrecv.c - receive streaming WAL logs or streaming base backups
 *                   from a PostgreSQL walsender.
 *
 *
 * Copyright (c) 2010-2011 PostgreSQL Global Development Group
 * Copyright (c) 2010-2011 Magnus Hagander <magnus@hagander.net>
 *
 * This software is released under the PostgreSQL Licence
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#include <getopt.h>

#include "postgres.h"
#include <libpq-fe.h>

#include "pg_streamrecv.h"

/* Options from the commandline */
char	   *connstr = NULL;
char	   *basedir = NULL;
int			verbose = 0;
bool		showprogress = false;


static void
Usage()
{
	printf("Usage:\n");
	printf("\n");
	printf("Log streaming mode:\n");
    printf(" pg_streamrecv -c <connectionstring> -d <directory> [-v]\n");
	printf("\n");
	printf(" -c <str>         libpq connection string to connect with\n");
	printf(" -d <directory>   directory to write WAL files to\n");
	printf(" -v               verbose\n");
	printf("\n");
	printf("Base backup mode:\n");
	printf(" pg_streamrecv -c <connectionstring> -b <directory> [-t] [-v]\n");
	printf("\n");
	printf(" -c               libpq connection string to connect with\n");
	printf(" -b <directory>   directory to write base backup to\n");
	printf(" -p               show progress indicator (slower)\n");
	printf(" -r               generate recovery.conf for streaming backup\n");
	printf(" -t               generate tar file(s) in the directory instead\n");
	printf("                  of unpacked data directory\n");
	printf(" -v               verbose\n");
	printf("\n");
	exit(1);
}


PGconn *
connect_server(int replication)
{
	char buf[MAXPGPATH];
	PGconn *conn;

	if (replication)
		sprintf(buf, "%s dbname=replication replication=true", connstr);
	else
		sprintf(buf, "%s dbname=postgres", connstr);

	if (verbose > 1)
		printf("Connecting to '%s'\n", buf);
	conn = PQconnectdb(buf);
	if (!conn || PQstatus(conn) != CONNECTION_OK)
	{
		fprintf(stderr, "Failed to connect to server for replication: %s\n",
				PQerrorMessage(conn));
		exit(1);
	}
	return conn;
}


void
verify_dir_is_empty(char *dirname)
{
	DIR *d = opendir(dirname);
	struct dirent *de;

	if (!d)
	{
		fprintf(stderr, "Directory '%s' does not exist\n", dirname);
		exit(1);
	}

	while ((de = readdir(d)) != NULL)
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;
		fprintf(stderr, "Directory '%s' is not empty!\n", dirname);
		exit(1);
	}
	closedir(d);
}


void
CheckPGResult(PGconn *conn, PGresult *res, char *operation, int expected)
{
	if (res == NULL)
	{
		fprintf(stderr, "Failed to %s: %s\n",
				operation,
				PQerrorMessage(conn));
		exit(1);
	}

	if (PQresultStatus(res) != expected)
	{
		fprintf(stderr, "Failed to %s: %s\n",
				operation,
				PQresultErrorMessage(res));
		exit(1);
	}
}



int
main(int argc, char *argv[])
{
	char		c;
	bool		tarmode = false,
				recoveryconf = false;
	bool		do_logstream = false,
				do_basebackup = false;
	struct stat st;

	while ((c = getopt(argc, argv, "c:d:b:prtv")) != -1)
	{
		switch (c)
		{
			case 'c':
				connstr = strdup(optarg);
				break;
			case 'd':
				basedir = strdup(optarg);
				do_logstream = true;
				break;
			case 'b':
				basedir = strdup(optarg);
				do_basebackup = true;
				break;
			case 'v':
				verbose++;
				break;
			case 'p':
				showprogress = true;
				break;
			case 'r':
				recoveryconf = true;
				break;
			case 't':
				tarmode = true;
				break;
			default:
				Usage();
				exit(1);
		}
	}

	if (optind != argc)
		Usage();

	if (!connstr || !basedir)
		Usage();

	if (do_basebackup && do_logstream)
	{
		fprintf(stderr, "Can't do both base backup and log streaming at once!\n");
		exit(1);
	}

	/*
	 * Verify that the target directory exists
	 */
	if (stat(basedir, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		fprintf(stderr, "Base directory %s does not exist\n", basedir);
		exit(1);
	}

	if (do_basebackup)
	{
		BaseBackup(tarmode, recoveryconf);
		exit(0);
	}
	else
	{
		if (tarmode)
		{
			fprintf(stderr, "Tar mode can only be set for base backups\n");
			exit(1);
		}
		if (recoveryconf)
		{
			fprintf(stderr, "recovery.conf can only be generated for base backups\n");
			exit(1);
		}
		if (showprogress)
		{
			fprintf(stderr, "progress report can only be shown for base backups\n");
			exit(1);
		}

		LogStreaming();
	}

	return 0;
}
