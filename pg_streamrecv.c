
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


/* Other global variables */
int			timeline;
char		current_walfile_name[64];
char	   *remove_when_passed_name = NULL;
int			remove_when_passed_size;


#define ISHEX(x) ((x >= '0' && x <= '9') || (x >= 'A' && x <= 'F'))

#define STREAMING_HEADER_SIZE (1+8+8+8)

/*
 * XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
 * This is from libpgport, use from there when importing
 * XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
 */
int
pg_mkdir_p(char *path, int omode)
{
	struct stat sb;
	mode_t		numask,
				oumask;
	int			last,
				retval;
	char	   *p;

	retval = 0;
	p = path;

#ifdef WIN32
	/* skip network and drive specifiers for win32 */
	if (strlen(p) >= 2)
	{
		if (p[0] == '/' && p[1] == '/')
		{
			/* network drive */
			p = strstr(p + 2, "/");
			if (p == NULL)
			{
				errno = EINVAL;
				return -1;
			}
		}
		else if (p[1] == ':' &&
				 ((p[0] >= 'a' && p[0] <= 'z') ||
				  (p[0] >= 'A' && p[0] <= 'Z')))
		{
			/* local drive */
			p += 2;
		}
	}
#endif

	/*
	 * POSIX 1003.2: For each dir operand that does not name an existing
	 * directory, effects equivalent to those caused by the following command
	 * shall occcur:
	 *
	 * mkdir -p -m $(umask -S),u+wx $(dirname dir) && mkdir [-m mode] dir
	 *
	 * We change the user's umask and then restore it, instead of doing
	 * chmod's.  Note we assume umask() can't change errno.
	 */
	oumask = umask(0);
	numask = oumask & ~(S_IWUSR | S_IXUSR);
	(void) umask(numask);

	if (p[0] == '/')			/* Skip leading '/'. */
		++p;
	for (last = 0; !last; ++p)
	{
		if (p[0] == '\0')
			last = 1;
		else if (p[0] != '/')
			continue;
		*p = '\0';
		if (!last && p[1] == '\0')
			last = 1;

		if (last)
			(void) umask(oumask);

		/* check for pre-existing directory */
		if (stat(path, &sb) == 0)
		{
			if (!S_ISDIR(sb.st_mode))
			{
				if (last)
					errno = EEXIST;
				else
					errno = ENOTDIR;
				retval = -1;
				break;
			}
		}
		else if (mkdir(path, last ? omode : S_IRWXU | S_IRWXG | S_IRWXO) < 0)
		{
			retval = -1;
			break;
		}
		if (!last)
			*p = '/';
	}

	/* ensure we restored umask */
	(void) umask(oumask);

	return retval;
}


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
ensure_directory_exists(char *filename)
{
	char path[MAXPGPATH];
	char *c;

	strcpy(path, filename);
	c = strrchr(path, '/');
	if (!c)
		return; /* No path in it, so assume exists */
	*c = '\0';

	if (access(path, R_OK | X_OK) != 0)
	{
		if (pg_mkdir_p(path, S_IRWXU) != 0)
		{
			fprintf(stderr, "Failed to create directory %s: %m", path);
		}
	}
}


static void
BaseBackup()
{
	PGconn *conn;
	PGresult *res;
	FILE *tarfile = NULL;
	char current_path[MAXPGPATH];
	bool firstchunk = true;
	int current_len_left;
	int current_padding;

	/*
	 * Connect in replication mode to the server
	 */
	conn = connect_server(1);

	res = PQexec(conn, "BASE_BACKUP pg_streamrecv base backup");
	if (!res || PQresultStatus(res) != PGRES_COPY_OUT)
	{
		fprintf(stderr, "Failed to start base backup: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}
	PQclear(res);


	/*
	 * Start receiving chunks
	 */
	while (1)
	{
		char *copybuf = NULL;

		int r = PQgetCopyData(conn, &copybuf, 0);

		if (r == -1)
		{
			/*
			 * End of this chunk, close the current file
			 * (both in tar and non-tar mode)
			 */
			if (tarfile)
			{
				fclose(tarfile);
				tarfile = NULL;
			}

			firstchunk = true;

			/*
			 * See if there is another chunk to be had
			 */
			res = PQgetResult(conn);
			if (PQresultStatus(res) == PGRES_COPY_OUT)
			{
				/* Another copy result coming -- another chunk */
				PQclear(res);
				continue;
			}
			if (PQresultStatus(res) != PGRES_COMMAND_OK)
			{
				fprintf(stderr, "Base backup error: %s\n",
						PQresultErrorMessage(res));
				exit(1);
			}

			/* Completed successfully */
			break;
		}
		else if (r == -2)
		{
			fprintf(stderr, "Error reading copy data: %s\n",
					PQerrorMessage(conn));
			exit(1);
		}

		/* Received a chunk of data */
		if (firstchunk)
		{
			/*
			 * First block in chunk - contains header
			 */
			char fn[128];

			/*
			 * Receiving header, format is:
			 * <oid>;<fullpath>
			 * with both being empty for base directory
			 */
			if (strcmp(copybuf, ";") == 0)
			{
				/* base directory */
				if (tarmode)
					sprintf(fn, "%s/base.tar", basedir);
				else
					strcpy(current_path, basedir);
			}
			else
			{
				/* tablespace */
				char *c = strchr(copybuf, ';');
				if (c == NULL)
				{
					fprintf(stderr, "Invalid chunk header: '%s'\n", copybuf);
					exit(1);
				}

				*c = '\0';
				if (tarmode)
					sprintf(fn, "%s/%s.tar", basedir, copybuf);
				else
					strcpy(current_path, c+1);
			}

			if (tarmode)
				tarfile = fopen(fn, "wb");
			else
				verify_dir_is_empty(current_path);

			firstchunk = false;
			continue;
		}

		if (tarmode)
			fwrite(copybuf, r, 1, tarfile);
		else
		{
			if (tarfile == NULL)
			{
				char fn[MAXPGPATH];

				/* No current file, so this must be a header */
				if (r != 512)
				{
					fprintf(stderr, "Invalid block header size: %i\n", r);
					exit(1);
				}

				if (sscanf(copybuf + 124, "%11o", &current_len_left) != 1)
				{
					fprintf(stderr, "Failed to parse file size\n");
					exit(1);
				}
				current_padding = ((current_len_left + 511) & ~511) - current_len_left;

				sprintf(fn, "%s/%s", current_path, copybuf);
				ensure_directory_exists(fn);
				tarfile = fopen(fn, "wb");
				/* XXX: Set permissions on file? Owner? */
				if (!tarfile)
				{
					fprintf(stderr, "Failed to create file '%s': %m\n", copybuf);
					exit(1);
				}

				if (current_len_left == 0)
				{
					fclose(tarfile);
					tarfile = NULL;

					/* Next block is going to be a new header */
					continue;
				}

			}
			else
			{
				/* Continuing a file */
				if (current_len_left == 0 && r == current_padding)
				{
					/* Received padding! */
					fclose(tarfile);
					tarfile = NULL;
					continue;
				}
				if (r > current_len_left)
				{
					fprintf(stderr, "Received block size %i, but only %i remaining.\n", r, current_len_left);
					exit(1);
				}

				fwrite(copybuf, r, 1, tarfile);

				current_len_left -= r;
				if (current_len_left == 0 && current_padding == 0)
				{
					/* No padding, and we're done */
					fclose(tarfile);
					tarfile = NULL;
				}
			}
		}
		PQfreemem(copybuf);
	}

	if (tarfile != NULL)
	{
		printf("tarfile != null\n");
		fclose(tarfile);
		if (current_len_left != 0)
		{
			fprintf(stderr, "Last file was never finished!\n");
			exit(1);
		}
	}

	/*
	 * End of copy data. Final result is already checked inside the loop.
	 */
	PQfinish(conn);

	/*
	 * Create directories that are excluded in the dump
	 */
	if (!tarmode)
	{
		sprintf(current_path, "%s/pg_xlog/dummy", basedir);
		ensure_directory_exists(current_path);
		sprintf(current_path, "%s/pg_tblspc/dummy", basedir);
		ensure_directory_exists(current_path);
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

	while ((c = getopt(argc, argv, "c:d:b:rtv")) != -1)
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

		LogStreaming();
	}

	return 0;
}
