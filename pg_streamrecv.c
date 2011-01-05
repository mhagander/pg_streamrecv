
/*
 * pg_streamrecv.c - receive a PostgreSQL 9.0+ replication stream and store
 *					 it in files like a standard log archive directory
 *
 *
 * Copyright (c) 2010 PostgreSQL Global Development Group
 * Copyright (c) 2010 Magnus Hagander <magnus@hagander.net>
 *
 * This software is released under the PostgreSQL Licence
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>

#include <getopt.h>


#include "postgres.h"
#include "access/xlog_internal.h"

#include <libpq-fe.h>

/* Options from the commandline */
char	   *connstr = NULL;
char	   *basedir = NULL;
int			verbose = 0;
bool		tarmode = false;
bool		recoveryconf = false;
bool		showprogress = false;


/* Other global variables */
int			timeline;
char		current_walfile_name[64];
char	   *remove_when_passed_name = NULL;
int			remove_when_passed_size;


#define ISHEX(x) ((x >= '0' && x <= '9') || (x >= 'A' && x <= 'F'))

#define STREAMING_HEADER_SIZE (1+8+8+8)

void
Usage()
{
	printf("Usage:\n");
	printf("\n");
	printf("Streaming mode:\n");
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

static PGconn *
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

/*
 * Initiate streaming replication at the given point in the WAL,
 * rounded off to the beginning of the segment it's in.
 */
PGresult *
start_streaming(PGconn *conn, char *xlogpos)
{
	unsigned int uxlogid;
	unsigned int uxrecoff;
	char		buf[64];

	if (sscanf(xlogpos, "%X/%X", &uxlogid, &uxrecoff) != 2)
	{
		fprintf(stderr, "Invalid format of current xlog location: %s\n",
				xlogpos);
		exit(1);
	}

	/*
	 * Round off so we always start at the beginning of a file.
	 */
	if (uxrecoff % XLogSegSize != 0)
		uxrecoff -= uxrecoff % XLogSegSize;

	if (verbose > 1)
	{
		printf("Current location %s, starting replication from %X/%X\n",
			   xlogpos, uxlogid, uxrecoff);
	}

	sprintf(buf, "START_REPLICATION %X/%X", uxlogid, uxrecoff);
	return PQexec(conn, buf);
}

/*
 * Open a new WAL file in the inprogress directory, corresponding to
 * the WAL location in startpoint.
 */
static int
open_walfile(XLogRecPtr startpoint)
{
	int			f;
	char		fn[256];

	XLogFileName(current_walfile_name, timeline,
				 startpoint.xlogid, startpoint.xrecoff / XLogSegSize);

	if (verbose)
		printf("Opening segment %s\n", current_walfile_name);

	sprintf(fn, "%s/inprogress/%s", basedir, current_walfile_name);
	f = open(fn, O_WRONLY | O_CREAT | O_EXCL, 0666);
	if (f == -1)
	{
		fprintf(stderr, "Failed to open wal segment %s: %m", fn);
		exit(1);
	}
	return f;
}

/*
 * Move the current file from inprogress to the base directory.
 * (assumes the file has been closed)
 */
static void
rename_current_walfile()
{
	char		src[256];
	char		dest[256];

	if (verbose > 1)
		printf("Moving file %s into place\n", current_walfile_name);

	sprintf(src, "%s/inprogress/%s", basedir, current_walfile_name);
	sprintf(dest, "%s/%s", basedir, current_walfile_name);
	if (rename(src, dest) != 0)
	{
		fprintf(stderr, "Failed to move WAL segment %s: %m",
				current_walfile_name);
		exit(1);
	}
}

/*
 * Convert a WAL filename to a log position in the %X/%X format.
 * Optionally add one segment to the position before converting
 * it, thus pointing at the next segment.
 */
static char *
filename_to_logpos(char *filename, int add_segment)
{
	char		buf[64];
	uint32		tli,
				log,
				seg;

	XLogFromFileName(filename, &tli, &log, &seg);
	if (add_segment)
		NextLogSeg(log, seg);
	sprintf(buf, "%X/%X", log, seg * XLogSegSize);
	return strdup(buf);
}


/*
 * Figure out where to start replicating from, by looking at these
 * options:
 *
 * 1. If there is an in-progress file, start from the start of that file
 * 2. Look for the latest file in the archive location, start after that
 * 3. Start from the beginning of current WAL segment with a warning
 */
static char *
get_streaming_start_point()
{
	DIR		   *dir;
	struct dirent *dirent;
	char		buf[256];
	char	   *filename = NULL;
	struct stat st;
	int			i;

	/*
	 * Start by checking if there is a file in the inprogress directory.
	 */
	sprintf(buf, "%s/inprogress", basedir);
	dir = opendir(buf);
	if (!dir)
	{
		fprintf(stderr, "Failed to open inprogress directory %s: %m", buf);
		exit(1);
	}

	while ((dirent = readdir(dir)) != NULL)
	{
		char		fn[256];

		if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
			continue;
		if (filename)
		{
			fprintf(stderr,
					"In progress directory contains more than one file!\n");
			exit(1);
		}
		sprintf(fn, "%s/%s", buf, dirent->d_name);
		if (stat(fn, &st) != 0)
		{
			fprintf(stderr, "Failed to stat file %s: %m", fn);
			exit(1);
		}
		if (!S_ISREG(st.st_mode))
		{
			fprintf(stderr,
					"In progress directory contains non-file entry %s\n",
					dirent->d_name);
			exit(1);
		}

		filename = strdup(dirent->d_name);
	}
	closedir(dir);

	if (filename != NULL)
	{
		/*
		 * Something exists in the inprogress directory, try
		 * to figure out what it is. It can be:
		 * 1. a started segment file
		 * 2. a segment file saved away
		 * 3. something unknown
		 */
		if (strlen(filename) == 24)
		{
			/*
			 * Looks like a segment, double-check characters
			 */
			char		src[256];
			char		dest[256];

			for (i = 0; i < 24; i++)
			{
				if (!ISHEX(filename[i]))
				{
					fprintf(stderr,
							"Unknown file '%s' found in inprogress directory.\n",
							filename);
					exit(1);
				}
			}

			/*
			 * Indeed we have a partial segment. Let's save it away. 
			 */
			fprintf(stderr,
					"Partial segment %s found. Saving aside, and attempting re-request.\n",
					filename);
			sprintf(src, "%s/inprogress/%s", basedir, filename);
			sprintf(dest, "%s/inprogress/%s.save", basedir, filename);
			if (rename(src, dest) != 0)
			{
				fprintf(stderr, "Failed to rename %s to %s: %m\n", src, dest);
				exit(1);
			}

			/*
			 * Save information about this partial segment so we can remove
			 * it when the retransmission of the segment has passed the point
			 * we were at before.
			 */
			remove_when_passed_name = strdup(dest);
			if (stat(remove_when_passed_name, &st) != 0)
			{
				fprintf(stderr, "Failed to stat file %s: %m\n",
						remove_when_passed_name);
				exit(1);
			}
			remove_when_passed_size = st.st_size;

			/*
			 * Existing file moved away. Now return the WAL location at the
			 * start of this segment to re-transfer it.
			 */
			return filename_to_logpos(filename, 0);
		}
		else if (strlen(filename) == 29)
		{
			/*
			 * Segment with ".save" at the end?
			 */
			if (strcmp(filename + 24, ".save") == 0)
			{
				fprintf(stderr,
						"A file called '%s' exists in the inprogress directory.\n",
						filename);
				fprintf(stderr,
						"This file is left over from a previous attempt to recover,\n");
				fprintf(stderr,
						"and you will need to figure out manually if you should delete\n");
				fprintf(stderr,
						"this file, or try to use it for manual recovery.\n");
				exit(1);
			}
		}
		fprintf(stderr, "Unknown file '%s' found in inprogress directory.\n",
				filename);
		exit(1);
	}


	/*
	 * No file found in the inprogress directory. Let's see if we can find
	 * something in the main archive directory.
	 */
	dir = opendir(basedir);
	if (!dir)
	{
		fprintf(stderr, "Failed to open base directory %s: %m", basedir);
		exit(1);
	}

	/*
	 * Read through all files in the main directory, sort all segments
	 * and get the one with the highest value.
	 */
	memset(buf, 0, sizeof(buf));
	while ((dirent = readdir(dir)) != NULL)
	{
		int			issegment;

		if (!strcmp(dirent->d_name, ".") || !strcmp(dirent->d_name, ".."))
			continue;

		/*
		 * Segment files hasve 24 character names
		 */
		if (strlen(dirent->d_name) != 24)
			continue;

		issegment = 1;
		for (i = 0; i < 24; i++)
		{
			if (!ISHEX(dirent->d_name[i]))
			{
				issegment = 0;
				break;
			}
		}
		if (!issegment)
			continue;

		/*
		 * Valid log segment name, remember it if it's the highest one
		 * we've seen so far.
		 */
		if (buf[0])
		{
			if (strcmp(dirent->d_name, buf) > 0)
				strcpy(buf, dirent->d_name);
		}
		else
		{
			/*
			 * First segment seen
			 */
			strcpy(buf, dirent->d_name);
		}
	}
	closedir(dir);

	if (buf[0])
	{
		/*
		 * Found a segment, convert it to a WAL location and request the
		 * segment following it.
		 */
		return filename_to_logpos(buf, 1);
	}

	/*
	 * Nothing found, create sometihng new
	 */
	fprintf(stderr,
			"Nothing found in archive directory, starting streaming from current position.\n");
	return NULL;
}

static void
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

static void
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

static void
ReceiveTarFile(PGconn *conn, PGresult *res)
{
	char fn[MAXPGPATH];
	char *copybuf = NULL;
	int spacesize = 0;
	int donesize = 0;
	FILE *tarfile;

	if (PQgetisnull(res, 0, 0))
		/* Base tablespaces */
		sprintf(fn, "%s/base.tar", basedir);
	else
		/* Specific tablespace */
		sprintf(fn, "%s/%s.tar", basedir, PQgetvalue(res, 0, 0));

	spacesize = atol(PQgetvalue(res, 0, 2));
	donesize = 0;

	tarfile = fopen(fn, "wb");

	/* Get the COPY data stream */
	res = PQgetResult(conn);
	CheckPGResult(conn, res, "get copy out", PGRES_COPY_OUT);

	while (1)
	{
		int r;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		r = PQgetCopyData(conn, &copybuf, 0);
		if (r == -1)
		{
			/* End of chunk */
			fclose(tarfile);

			/* There will be a second result telling us how the COPY went */
			res = PQgetResult(conn);
			CheckPGResult(conn, res, "receive chunk", PGRES_COMMAND_OK);
			break;
		}
		else if (r == -2)
		{
			fprintf(stderr, "Error reading COPY data: %s\n",
					PQerrorMessage(conn));
			exit(1);
		}

		fwrite(copybuf, r, 1, tarfile);
		if (showprogress)
		{
			donesize += r;
			if (!verbose)
				printf("Completed %i/%i kB (%i%%)\r",
					   donesize / 1024, spacesize,
					   (donesize / 1024) * 100 / spacesize);
		}
	} /* while (1) */

	if (copybuf != NULL)
		PQfreemem(copybuf);
}


static void
ReceiveAndUnpackTarFile(PGconn *conn, PGresult *res)
{
	char current_path[MAXPGPATH];
	char *copybuf = NULL;
	int spacesize = 0;
	int donesize = 0;
	int current_len_left;
	int current_padding;
	FILE *file = NULL;

	if (PQgetisnull(res, 0, 0))
		strcpy(current_path, basedir);
	else
		strcpy(current_path, PQgetvalue(res, 0, 1));

	spacesize = atol(PQgetvalue(res, 0, 2));
	donesize = 0;

	/* Make sure we're unpacking into an empty directory */
	verify_dir_is_empty(current_path);

	/* Get the COPY data */
	res = PQgetResult(conn);
	CheckPGResult(conn, res, "get copy out", PGRES_COPY_OUT);

	while (1)
	{
		int r;

		if (copybuf != NULL)
		{
			PQfreemem(copybuf);
			copybuf = NULL;
		}

		r = PQgetCopyData(conn, &copybuf, 0);

		if (r == -1)
		{
			/* End of chunk */
			if (file)
				fclose(file);

			/* Get another result to check the end of COPY */
			res = PQgetResult(conn);
			CheckPGResult(conn, res, "receive chunk", PGRES_COMMAND_OK);
			break;
		}
		else if (r == -2)
		{
			fprintf(stderr, "Error reading copy data: %s\n",
					PQerrorMessage(conn));
			exit(1);
		}

		if (file == NULL)
		{
			/* No current file, so this must be the header for a new file */
			char fn[MAXPGPATH];

			if (r != 512)
			{
				fprintf(stderr, "Invalid tar block header size: %i\n", r);
				exit(1);
			}

			if (sscanf(copybuf + 124, "%11o", &current_len_left) != 1)
			{
				fprintf(stderr, "Failed to parse file size!\n");
				exit(1);
			}

			/* All files are padded up to 512 bytes */
			current_padding = ((current_len_left + 511) & ~511) - current_len_left;

			/* First part of header is zero terminated filename */
			sprintf(fn, "%s/%s", current_path, copybuf);
			if (fn[strlen(fn)-1] == '/')
			{
				/* Ends in a slash means directory or symlink to directory */
				if (copybuf[156] == '5')
				{
					/* Directory */
					fn[strlen(fn)-1] = '\0'; /* Remove trailing slash */
					if (mkdir(fn, S_IRWXU) != 0) /* XXX: permissions */
					{
						fprintf(stderr, "Could not create directory \"%s\": %m\n",
								fn);
						exit(1);
					}
				}
				else if (copybuf[156] == '2')
				{
					/* Symbolic link */
					fprintf(stderr, "Don't know how to deal with symbolic link yet\n");
					exit(1);
				}
				else
				{
					fprintf(stderr, "Unknown link indicator '%c'\n",
							copybuf[156]);
					exit(1);
				}
				continue; /* directory or link handled */
			}

			/* regular file */
			file = fopen(fn, "wb"); /* XXX: permissions & owner */
			if (!file)
			{
				fprintf(stderr, "Failed to create file '%s': %m\n", fn);
				exit(1);
			}

			if (verbose)
				printf("Starting write to file %s (size %i kB, total done %i / %i kB (%i%%))\n",
					   fn, current_len_left / 1024,
					   donesize / 1024, spacesize,
					   (donesize / 1024) * 100 / spacesize);

			if (current_len_left == 0)
			{
				/* Done with this file, next one will be a new tar header */
				fclose(file);
				file = NULL;
				continue;
			}
		} /* new file */
		else
		{
			/* Continuing blocks in existing file */
			if (current_len_left == 0 && r == current_padding)
			{
				/*
				 * Received the padding block for this file, ignore it and
				 * close the file, then move on to the next tar header.
				 */
				fclose(file);
				file = NULL;
				continue;
			}

			fwrite(copybuf, r, 1, file); /* XXX: result code */
			if (showprogress)
			{
				donesize += r;
				if (!verbose)
					printf("Completed %i/%i kB (%i%%)\r",
						   donesize / 1024, spacesize,
						   (donesize / 1024) * 100 / spacesize);
			}

			current_len_left -= r;
			if (current_len_left == 0 && current_padding == 0)
			{
				/*
				 * Received the last block, and there is no padding to be
				 * expected. Close the file and move on to the next tar
				 * header.
				 */
				fclose(file);
				file = NULL;
				continue;
			}
		} /* continuing data in existing file */
	} /* loop over all data blocks */

	if (file != NULL)
	{
		fprintf(stderr, "Last file was never finsihed!\n");
		exit(1);
	}

	if (copybuf != NULL)
		PQfreemem(copybuf);
}

static void
BaseBackup()
{
	PGconn *conn;
	PGresult *res;
	FILE *tarfile = NULL;
	char current_path[MAXPGPATH];

	/*
	 * Connect in replication mode to the server
	 */
	conn = connect_server(1);

	sprintf(current_path, "BASE_BACKUP %s;pg_streamrecv base backup",
			showprogress?"PROGRESS":"");
	if (PQsendQuery(conn, current_path) == 0)
	{
		fprintf(stderr, "Failed to start base backup: %s\n",
				PQerrorMessage(conn));
		exit(1);
	}

	/*
	 * Start receiving chunks
	 */
	while (1)
	{
		res = PQgetResult(conn);
		if (res == NULL)
			/* Last resultset has been received. We're done here. */
			break;

		CheckPGResult(conn, res, "get first result", PGRES_TUPLES_OK);

		if (tarmode)
			ReceiveTarFile(conn, res);
		else
			ReceiveAndUnpackTarFile(conn, res);
		PQclear(res);

	} /* Loop over all tablespaces */

	if (showprogress && !verbose)
		printf("\n"); /* Need to move to next line */

	/*
	 * End of copy data. Final result is already checked inside the loop.
	 */
	PQfinish(conn);

	/*
	 * Create directories that are excluded in the dump
	 */
	if (!tarmode)
	{
		sprintf(current_path, "%s/pg_xlog", basedir);
		mkdir(current_path, S_IRWXU);
		sprintf(current_path, "%s/pg_tblspc", basedir);
		mkdir(current_path, S_IRWXU);
	}

	if (recoveryconf)
	{
		sprintf(current_path, "%s/recovery.conf", basedir);
		tarfile = fopen(current_path, "w");
		if (!tarfile)
		{
			fprintf(stderr, "could not create \"%s\": %m\n", current_path);
			exit(1);
		}
		fprintf(tarfile, "standby_mode=on\n");
		fprintf(tarfile, "primary_conninfo='%s'\n", connstr);
		fclose(tarfile);
	}

	printf("Base backup completed.\n");
}


static void
LogStreaming(void)
{
	PGconn	   *conn;
	PGresult   *res;
	char	   *current_xlog;
	int			walfile = -1;
	char		buf[128];
	struct stat st;

	/*
	 * Create inprogress directory if it does not exist
	 */
	sprintf(buf, "%s/inprogress", basedir);
	if (stat(buf, &st) != 0)
	{
		/*
		 * Not there
		 */
		if (mkdir(buf, 0777) != 0)
		{
			fprintf(stderr, "failed to create directory %s: %m", buf);
			exit(1);
		}
	}
	else
	{
		if (!S_ISDIR(st.st_mode))
		{
			fprintf(stderr, "%s is not a directory.\n", buf);
			exit(1);
		}
	}

	/*
	 * Figure out where to start if there are existing files
	 * available.
	 */
	current_xlog = get_streaming_start_point();
	if (current_xlog == NULL)
	{
		/*
		 * Nothing found in the archive directory, so connect to
		 * the master and ask for the current xlog location, and
		 * derive the streaming start point from that.
		 */
		conn = connect_server(0);

		/*
		 * Get the current xlog location
		 */
		res = PQexec(conn, "SELECT pg_current_xlog_location()");
		if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "Failed to get current xlog location: %s\n",
					PQresultErrorMessage(res));
			exit(1);
		}
		current_xlog = strdup(PQgetvalue(res, 0, 0));
		if (verbose)
			printf("Current xlog location: %s\n", current_xlog);
		PQclear(res);
		PQfinish(conn);
	}


	/*
	 * Connect in replication mode to the server
	 */
	conn = connect_server(1);

	/*
	 * Identify the server and get the timeline
	 */
	res = PQexec(conn, "IDENTIFY_SYSTEM");
	if (!res || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "Failed to identify system: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}
	if (verbose)
	{
		printf("Systemid: %s\n", PQgetvalue(res, 0, 0));
		printf("Timeline: %s\n", PQgetvalue(res, 0, 1));
	}
	timeline = atoi(PQgetvalue(res, 0, 1));
	PQclear(res);

	/*
	 * Start streaming the log
	 */
	res = start_streaming(conn, current_xlog);
	if (!res ||
		(PQresultStatus(res) != PGRES_COPY_OUT && PQresultStatus(res) != PGRES_COPY_BOTH))
	{
		fprintf(stderr, "Failed to start replication: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);

	while (1)
	{
		char	   *copybuf = NULL;
		XLogRecPtr	startpoint;
		int			xlogoff;
		int			r = PQgetCopyData(conn, &copybuf, 0);

		if (r == -1)
			break;
		if (r == -2)
		{
			fprintf(stderr, "Error reading copy data: %s\n", PQerrorMessage(conn));
			exit(1);
		}
		if (r < STREAMING_HEADER_SIZE + 1)
		{
			fprintf(stderr, "Received %i bytes in a copy data block, shorter than the required %i\n", r, STREAMING_HEADER_SIZE + 1);
			exit(1);
		}
		if (copybuf[0] != 'w')
		{
			fprintf(stderr, "Received invalid copy data type: %c\n",
					copybuf[0]);
			exit(1);
		}
		memcpy(&startpoint, copybuf + 1, 8);	/* sizeof(XlogRecPtr) == 8 */

		/*
		 * Figure out how far into this logfile this block should go
		 */
		xlogoff = startpoint.xrecoff % XLogSegSize;

		if (walfile > -1)
		{
			if (xlogoff == 0)
			{
				/*
				 * Switched to a new file. Verify size of the old one
				 */
				if (lseek(walfile, 0, SEEK_CUR) != XLogSegSize)
				{
					fprintf(stderr,
							"Received record at offset 0 while file size still only %li\n",
							lseek(walfile, 0, SEEK_CUR));
					exit(1);
				}

				/*
				 * Offset zero in a new file - close the old one.
				 * Always fsync the old file, so we can get a write-ordering
				 * guarantee against the new file.
				 */
				fsync(walfile);
				close(walfile);
				if (remove_when_passed_name)
				{
					printf
						("Removing file %s from inprogress directory - segment transfer complete.\n",
						 remove_when_passed_name);
					if (unlink(remove_when_passed_name) != 0)
					{
						fprintf(stderr, "Failed to remove file %s: %m",
								remove_when_passed_name);
						exit(1);
					}
					free(remove_when_passed_name);
					remove_when_passed_name = NULL;
				}
				rename_current_walfile();
				walfile = open_walfile(startpoint);
			}
			else
			{
				/*
				 * Not a new segment, so verify that position in file matches
				 */
				if (lseek(walfile, 0, SEEK_CUR) != xlogoff)
				{
					fprintf(stderr,
							"Received xlog record for offset %i but writing at offset %li\n",
							xlogoff, lseek(walfile, 0, SEEK_CUR));
					exit(1);
				}
				/*
				 * Position matches, so just write the data out further down
				 */
			}
		}
		else
		{
			/*
			 * No current walfile - open a new one
			 */
			if (xlogoff != 0)
			{
				fprintf(stderr,
						"Received xlog record for offset %i with no file open - needs to start at xlog boundary!\n",
						xlogoff);
				exit(1);
			}
			walfile = open_walfile(startpoint);
		}
		if (verbose > 1)
			printf("Received one batch, size %i\n", r - STREAMING_HEADER_SIZE);
		if (write
			(walfile, copybuf + STREAMING_HEADER_SIZE,
			 r - STREAMING_HEADER_SIZE) != r - STREAMING_HEADER_SIZE)
		{
			fprintf(stderr, "Failed to write %i bytes to file %s: %m",
					r - STREAMING_HEADER_SIZE, current_walfile_name);
			exit(1);
		}

		PQfreemem(copybuf);

		/*
		 * If there is a saved awayn file to remove when we've passed a
		 * certain point in the WAL stream and we have actually passed
		 * this point, then remove the file.
		 */
		if (remove_when_passed_name &&
			remove_when_passed_size < lseek(walfile, 0, SEEK_CUR))
		{
			printf
				("Removing file %s from inprogress directory - current transfer passed point in file.\n",
				 remove_when_passed_name);
			if (unlink(remove_when_passed_name) != 0)
			{
				fprintf(stderr, "Failed to remove file %s: %m",
						remove_when_passed_name);
				exit(1);
			}
			free(remove_when_passed_name);
			remove_when_passed_name = NULL;
		}
	}


	/*
	 * End of copy data, check the final result. In case the server shut
	 * down, it will send a proper "command ok" result. If something
	 * went wrong, it will send an error message that should show up
	 * here.
	 */
	res = PQgetResult(conn);
	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "Replication error: %s\n", PQresultErrorMessage(res));
		exit(1);
	}
	PQfinish(conn);

	if (verbose)
		printf("Replication stream finished.\n");
}

int
main(int argc, char *argv[])
{
	char		c;
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
		BaseBackup();
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
