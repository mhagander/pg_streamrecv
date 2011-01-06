#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>

#include "postgres.h"
#include "libpq-fe.h"

#include "pg_streamrecv.h"

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

void
BaseBackup(bool tarmode, bool recoveryconf)
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
