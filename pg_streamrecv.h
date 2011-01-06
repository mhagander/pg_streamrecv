/* In pg_streamrecv.c */
void CheckPGResult(PGconn *conn, PGresult *res, char *operation, int expected);
void verify_dir_is_empty(char *dirname);
PGconn *connect_server(int replication);



/* In basebackup.c */
void BaseBackup(bool tarmode, bool recoveryconf);

/* In logstream.c */
void LogStreaming(void);

/* Global options */
extern char *basedir;
extern bool showprogress;
extern int verbose;
extern char *connstr;


/* Global macros */
#define ISHEX(x) ((x >= '0' && x <= '9') || (x >= 'A' && x <= 'F'))
