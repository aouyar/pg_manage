#!/usr/bin/perl
#
# Used by PostgreSQL Server to backup or restore archive logs.
# Must be configured in PostgreSQL configuration files:
# 	postgresql.conf	-> archive_command
# 	recovery.conf 	-> restore
#
# Used by operator for backup and recovery tasks.
#
# Author:    Ali Onur Uyar
# Email:     aouyar@gmail.com
# Copyright: 2011, Ali Onur Uyar
# License:   GPLv3


# Includes
use strict;
use POSIX qw(strftime);
use Sys::Hostname;
use File::Basename;
use Getopt::Std;


#
# DEFAULTS
#

my $DEBUG = 0;
my $DRYRUN = 0;
my $VERBOSE = 1;

# Default Paths
my $pgBaseDir = "/var/lib/pgsql/9.0";
my $pgDataDir = "$pgBaseDir/data";
my $pgXlogDir = "$pgDataDir/pg_xlog";
my $pgLogDir = "$pgDataDir/pg_log";
my $pgCompXlogDir = "$pgXlogDir/tmp";

# PostgreSQL Process Name (Used for checking if the Database is Online or Offline)
my $pgProc = "postmaster";

# PostgreSQL User
my $pgUser = "postgres";

# Default Trigger Filename and Path
my $triggerFile = "recovery.trigger";
my $triggerFilePath = "$pgDataDir/$triggerFile";

# Filename for Configuration File
my $configFile = "pg_manage.conf";

# Timing and retry defaults
my $restoreMaxRetries = 3;
my $restoreRetryInterval = 10;
my $WALcheckInterval = 60;
my $WALmaxWaitTime = 0;

# OS Paths
my $mtab_path = '/etc/mtab';

# Paths for Executables
my $cmd_cat = "/bin/cat";
my $cmd_find = "/usr/bin/find";
my $cmd_grep = "/bin/grep";
my $cmd_gunzip = "/bin/gunzip";
my $cmd_gzip = "/bin/gzip";
my $cmd_ifconfig = "/sbin/ifconfig";
my $cmd_pgrep = "/usr/bin/pgrep";
my $cmd_rm = "/bin/rm";
my $cmd_rsync = "/usr/bin/rsync";
my $cmd_ssh = "/usr/bin/ssh";
my $cmd_tar = "/bin/tar";
my $cmd_test = "/usr/bin/test";
my $cmd_xargs = "/usr/bin/xargs";
my $cmd_pgdump = "/usr/bin/pg_dump";
my $cmd_pgrestore = "/usr/bin/pg_restore";
my $cmd_psql = "/usr/bin/psql";


#
# READ AND CHECK CONFIGURATION
#

$DEBUG and print "\n\n";


# Read Configuration File
my %cfg = &ReadConfig();


# Check Configuration

my ($serverType, $primaryHost, $secondaryHost, 
	$backupURLhot, $backupURLarch, $backupURLdump, $backupDirHot, $backupDirArch, $backupDirDump);
my (%backupLocHot, %backupLocArch, %backupLocDump);

$cfg{'SERVERTYPE'} and $serverType = lc($cfg{'SERVERTYPE'});
$cfg{'PRIMARYHOST'} and $primaryHost = $cfg{'PRIMARYHOST'};
$cfg{'SECONDARYHOST'} and $secondaryHost = $cfg{'SECONDARYHOST'};
$cfg{'BACKUPURLHOT'} and $backupURLhot = $cfg{'BACKUPURLHOT'};
$cfg{'BACKUPURLARCH'} and $backupURLarch = $cfg{'BACKUPURLARCH'};
$cfg{'BACKUPURLDUMP'} and $backupURLdump = $cfg{'BACKUPURLDUMP'};
$cfg{'BACKUPDIRHOT'} and $backupDirHot = $cfg{'BACKUPDIRHOT'};
$cfg{'BACKUPDIRARCH'} and $backupDirArch = $cfg{'BACKUPDIRARCH'};
$cfg{'BACKUPDIRDUMP'} and $backupDirDump = $cfg{'BACKUPDIRDUMP'};

if ($serverType) {
	$serverType =~ /^primary|secondary$/ or die "ERROR: Invalid configuration. SERVERTYPE must be PRIMARY or SECONDARY.\n"; }
if ($primaryHost) {
	$primaryHost =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/ or die "ERROR: Invalid configuration. PRIMARYHOST ", $primaryHost, " invalid.\n"; }
if ($secondaryHost) {
	$secondaryHost =~ /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/ or die "ERROR: Invalid configuration. SECONDARYHOST ", $secondaryHost, " invalid.\n"; }
if ($backupURLhot) {
	%backupLocHot = &ParseDirectoryURL($backupURLhot) or die "ERROR: Invalid configuration. BACKUPURLHOT ", $backupURLhot, " invalid.\n";
}
if ($backupURLarch) {
	%backupLocArch = &ParseDirectoryURL($backupURLarch) or die "ERROR: Invalid configuration. BACKUPURLARCH ", $backupURLarch," invalid.\n";
}
if ($backupURLdump) {
	%backupLocDump = &ParseDirectoryURL($backupURLdump) or die "ERROR: Invalid configuration. BACKUPURLDUMP ", $backupURLdump," invalid.\n";
}
if ($backupDirHot) {
	$backupDirHot =~ /^[\w\/-]+$/ or die "ERROR: Invalid configuration. BACKUPDIRHOT ", $backupDirHot, " invalid.\n"; }
if ($backupDirArch) {
	$backupDirArch =~ /^[\w\/-]+$/ or die "ERROR: Invalid configuration. BACKUPDIRARCH ", $backupDirArch," invalid.\n"; }


# Check required command files.

(-x $cmd_cat) or die "ERROR: Executable $cmd_cat not found.\n";
(-x $cmd_find) or die "ERROR: Executable $cmd_find not found.\n";
(-x $cmd_grep) or die "ERROR: Executable $cmd_grep not found.\n";
(-x $cmd_gunzip) or die "ERROR: Executable $cmd_gunzip not found.\n";
(-x $cmd_gzip) or die "ERROR: Executable $cmd_gzip not found.\n";
(-x $cmd_ifconfig) or die "ERROR: Executable $cmd_ifconfig not found.\n";
(-x $cmd_pgrep) or die "ERROR: Executable $cmd_pgrep not found.\n";
(-x $cmd_rm) or die "ERROR: Executable $cmd_rm not found.\n";
(-x $cmd_rsync) or die "ERROR: Executable $cmd_rsync not found.\n";
(-x $cmd_ssh) or die "ERROR: Executable $cmd_ssh not found.\n";
(-x $cmd_tar) or die "ERROR: Executable $cmd_tar not found.\n";
(-x $cmd_test) or die "ERROR: Executable $cmd_test not found.\n";
(-x $cmd_xargs) or die "ERROR: Executable $cmd_xargs not found.\n";
(-x $cmd_psql) or die "ERROR: Executable $cmd_psql not found.\n";
(-x $cmd_pgdump) or die "ERROR: Executable $cmd_pgdump not found.\n";
(-x $cmd_pgrestore) or die "ERROR: Executable $cmd_pgrestore not found.\n";



#
# PARSE AND CHECK COMMAND LINE OPTIONS
#

my %cmdLineOpts;
if (not getopts('hdqnlc:s:f:p:r:b:t:i:I:w:D:j:', \%cmdLineOpts)) {
	print "ERROR: Parsing command line arguments.\n";
	&PrintUsageAndExit();
}

my ($loggingActive, $opCommand, 
	$WALfile, $WALpath, $lastReqWALfile, 
	$reqBackupFilename, $srcHostname, 
	$dbName, $numJobs);

if ($cmdLineOpts{'h'}) { &PrintUsage(); exit 0; }
if ($cmdLineOpts{'q'}) { $VERBOSE=0; }
if ($cmdLineOpts{'d'}) { $DEBUG=1; $VERBOSE=1; }
if ($cmdLineOpts{'n'}) { $DRYRUN=1; print "DRY RUN... Only testing... No execution...\n\n"; }
if ($cmdLineOpts{'l'}) { $loggingActive=1; }
if ($cmdLineOpts{'c'}) { $opCommand = $cmdLineOpts{'c'}; }
if ($cmdLineOpts{'s'}) { $srcHostname = $cmdLineOpts{'s'}; }
if ($cmdLineOpts{'f'}) { $WALfile = $cmdLineOpts{'f'}; }
if ($cmdLineOpts{'p'}) { $WALpath = $cmdLineOpts{'p'}; }
if ($cmdLineOpts{'r'}) { $lastReqWALfile = $cmdLineOpts{'r'}; }
if ($cmdLineOpts{'b'}) { $reqBackupFilename = $cmdLineOpts{'b'}; }
if ($cmdLineOpts{'t'}) { $restoreMaxRetries = $cmdLineOpts{'t'}; }
if ($cmdLineOpts{'i'}) { $restoreRetryInterval = $cmdLineOpts{'i'}; }
if ($cmdLineOpts{'I'}) { $WALcheckInterval = $cmdLineOpts{'I'}; }
if ($cmdLineOpts{'w'}) { $WALmaxWaitTime = $cmdLineOpts{'w'}; }
if ($cmdLineOpts{'D'}) { $dbName = $cmdLineOpts{'D'}; }
if ($cmdLineOpts{'j'}) { $numJobs = $cmdLineOpts{'j'}; }


if ($opCommand) {
	$opCommand =~ /^\w+$/ or die "ERROR: Value for -c option '$opCommand' invalid\n"; }
else { print "ERROR: Command argument (-c) is mandatory.\n"; &PrintUsageAndExit(); }

if ($srcHostname) { 
	$srcHostname =~ /^[\w\.-]+$/ or die "ERROR: Value for Source Hostname (-s) option '$srcHostname' is invalid\n"; }
if ($WALfile) { 
	$WALfile =~ /^[\w\.-]+$/ or die "ERROR: Value for WAL Filename (-f) option '$WALfile' is invalid\n"; }
if ($WALpath) { 
	$WALpath =~ /^[\w\/\.-]+$/ or die "ERROR: Value for WAL Path (-p) option '$WALpath' is invalid\n"; }
if ($lastReqWALfile) { 
	$lastReqWALfile =~ /^[\w\/\.-]+$/ or die "ERROR: Value for Last Required WAL File (-r) option '$lastReqWALfile' is invalid\n"; }
if ($reqBackupFilename) { 
	$reqBackupFilename  =~ /^[\w+\.-]+$/ or die "ERROR: Value for Backup Filename (-b) option '$reqBackupFilename' is invalid\n"; }
if ($restoreMaxRetries) { 
	($restoreMaxRetries  =~ /^\d+$/ and $restoreMaxRetries <= 10) or die "ERROR: Value for WAL Restore Retries (-t) option '$restoreMaxRetries' is invalid\n"; }
if ($restoreRetryInterval) { 
	($restoreRetryInterval  =~ /^\d+$/ and $restoreRetryInterval <= 300) or die "ERROR: Value for WAL Restore Retry Interval (-i) option '$restoreRetryInterval' is invalid\n"; }
if ($WALcheckInterval) { 
	($WALcheckInterval  =~ /^\d+$/ and $WALcheckInterval <= 1800) or die "ERROR: Value for WAL Check Interval (-I) option '$WALcheckInterval' is invalid\n"; }
if ($WALmaxWaitTime) { 
	($WALmaxWaitTime  =~ /^\d+$/ and $WALmaxWaitTime <= 3600) or die "ERROR: Value for Maximum WAL Wait Time (-w) option '$WALmaxWaitTime' is invalid\n"; }
if ($dbName) { 
	$dbName  =~ /^[\w+\-]+$/ or die "ERROR: Value for database name (-D) option '$dbName' is invalid\n"; }
if ($numJobs) { 
	($numJobs  =~ /^\d+$/) or die "ERROR: Value for number of parallel restore jobs (-j) option '$numJobs' is invalid\n"; }
else {
	$numJobs = 1;
}



#
# CHECK USER
#

&PGcheckScriptUser() or die "ERROR: The script must be executed by the PostgreSQL user ($pgUser).\n";



#
# INITIALIZATION
#

# Get Hostname
my $hostname = hostname();
if ($hostname =~ /^([\w\-]+)\..*/) {
	$hostname = $1;
}

# Initialize Date
my $curDate = &GetDate();

# Initialize Globals
my $backupDirArchHost;
my $backupDirHotHost;
my $backupDirDumpHost;
my $compWALpath;




#
# MAIN
#

my $retVal;

if ($opCommand eq "xlog_archive") {
	$loggingActive and &StartLoggingToFile("archive");
	$retVal = &OperXlogArchive();
}
elsif ($opCommand eq "xlog_restore") {
	$loggingActive and &StartLoggingToFile("restore");
	$retVal = &OperXlogRestore();
}
elsif ($opCommand eq 'recovery_trigger_fast') {
	$retVal = &OperTriggerRecoveryFast();
}
elsif ($opCommand eq 'recovery_trigger_smart') {
	$retVal = &OperTriggerRecoverySmart()
}
elsif ($opCommand eq 'recovery_trigger_cleanup') {
	$retVal = &OperTriggerRecoveryCleanup();
}
elsif ($opCommand eq 'db_destroy') {
	$retVal = &OperDBdestroy();
}
elsif ($opCommand eq 'db_hot_backup') {
	$retVal = &OperDBhotBackup();
}
elsif ($opCommand eq 'db_restore_backup') {
	$retVal = &OperDBrestoreBackup();
}
elsif ($opCommand eq 'db_initial_sync') {
	$retVal = &OperDBinitSync();
}
elsif ($opCommand eq 'db_export') {
	$retVal = &OperDBexport();
}
elsif ($opCommand eq 'db_import') {
	$retVal = &OperDBimport();
}
elsif ($opCommand eq 'check_primary') {
	$retVal = &OperCheckPrimary();
}
elsif ($opCommand eq 'check_secondary') {
	$retVal = &OperCheckSecondary();
}
else {
	print "ERROR: Invalid operation command $opCommand.\n";
	&PrintUsageAndExit();
}

if ($retVal) {
	exit 0;
}
else {
	exit 1;
}




#
# MAIN FUNCTIONS
#


# Compress and copy WAL File to Backup Server

sub OperXlogArchive() {
	my $ret;

	# Input Checking
	(%backupLocArch and $backupDirArch) or
		die "ERROR: Backup Location and/or Archive Directory not defined. Please check the configuration file $configFile.\n";
	($WALfile and $WALpath) or
		die "ERROR: WAL File (-f) and WAL Path (-p) arguments are mandatory for xlog_archive_operation.\n";
	
	# Initialization
	$backupDirArchHost = "$backupDirArch/$hostname";
	$compWALpath = "$pgCompXlogDir/$WALfile.gz";

	# Logging	
	print "\n", &GetTimestamp(), "     ARCHIVE     WAL: $WALfile", "\n";
	if ($VERBOSE) {
		print "WAL Path: $WALpath\n";
		printf "WAL Dest: $backupURLarch/$backupDirArchHost\n";
	}
	$DEBUG and print "Compressed WAL Path: $compWALpath\n";
	
	# Check backup destination
	&PGcheckBackupLoc(\%backupLocArch);

	# Verify existence of directory  for Temporary Files
	&PGcheckTmpXlogDir();

	# Archive WAL File
	$ret = &PGarchiveWALfile();

	# Cleanup Temp File
	&PGcleanupTmpWALfile();

	return $ret;
}


# Restore WAL File from Backup Server

sub OperXlogRestore() {
	my $ret;

	# Input Checking
	(%backupLocArch and $backupDirArch) or
		die "ERROR: Backup Location and/or Archive Directory not defined. Please check the configuration file $configFile.\n";
	($WALfile and $WALpath and $srcHostname) or
		die "ERROR: WAL File (-f), WAL Path (-p) and Archive Log Source Host (-s) and arguments are mandatory for xlog_restore_operation.\n";
	
	# Initialization
	$backupDirArchHost = "$backupDirArch/$srcHostname";
	$compWALpath = "$pgCompXlogDir/$WALfile.gz";

	# Logging	
	print "\n", &GetTimestamp(), "     RESTORE     WAL: $WALfile", "\n";
	if ($VERBOSE) {
		print "WAL Path: $WALpath\n";
		print "WAL Src : $backupURLarch/$backupDirArchHost\n";
		$lastReqWALfile and print "Restart WAL: $lastReqWALfile\n";
	}
	$DEBUG and print "Compressed WAL Path: $compWALpath\n";

	# Check backup destination
	&PGcheckBackupLoc(\%backupLocArch);

	# Verify existence of directory  for Temporary Files
	&PGcheckTmpXlogDir();

	# Check for History File
	if ($WALfile =~ /^[0-9A-F]{8}\.history$/) {
		$VERBOSE and print "Type: History File\n";
		if (&PGcheckRemoteWALfile()) {
			# Attempt restore of History File.
			$ret = &PGrestoreWALfile;

			# Cleanup Temp File
			&PGcleanupTmpWALfile();
			
			return $ret;
		}
		else {
			$VERBOSE and print "Restore: Skipped\n";
			return 0;
		}
	}

	my $recv;
	my $totalWaitTime = 0;
	while (1) {

		# Check Trigger FIle
		$recv = &PGcheckTriggerFile();
		if ($recv eq "fast") {
			print "\nFAST FAILOVER INITIATED.\n";
			return 0;
		}
		elsif ($recv eq "smart") {
			print "FAILOVER PENDING\n";
		}

		# Check WAL File
		if (&PGcheckRemoteWALfile()) {
			# Restore WAL File
			$ret = &PGrestoreWALfile();

			#Cleanup WAL Files
			$ret and &PGcleanupWALfiles();

			# Cleanup Temp File
			&PGcleanupTmpWALfile();

			return $ret;
		}
		elsif ($recv eq "smart") {
			print "\nFAST FAILOVER INITIATED.\n";
			return 0;
		}

		# Check Termination for Long Wait Time
		if ($WALmaxWaitTime > 0 and $totalWaitTime > $WALmaxWaitTime) {
			print "\nTIMEOUT WAITING FOR ARCHIVE FILE\n";
			print "TRIGGERING END OF RECOVERY FROM ARCHIVE LOGS.\n";
			return 0;
		}

		# Sleep
		sleep $WALcheckInterval;
		$totalWaitTime += $WALcheckInterval;
	}

	return 0;
}


# Trigger Immediate End of Recovery (Fast Recovery)

sub OperTriggerRecoveryFast() {
	my $ret;
	$ret = &PGcreateTriggerFile('fast');
	$ret and $VERBOSE and print "System setup for FAST recovery.\nRecovery will start immediately.\n"; 
	return $ret;
}


# Trigger End of Recovery once Existing Arhive Logs are Processed (Smart Recovery)

sub OperTriggerRecoverySmart() {
	my $ret;
	$ret = &PGcreateTriggerFile('smart');
	$ret and $VERBOSE and print "System setup for smart recovery.\nRecovery will start as soon as all existing archive logs are replayed.\n"; 
	return $ret;
}


# Trigger End of Recovery once Existing Arhive Logs are Processed (Smart Recovery)

sub OperTriggerRecoveryCleanup() {
	my $ret;
	$ret = &PGremoveTriggerFile();
	return $ret;
}


# Initialize PostgreSQL

sub OperDBdestroy() {
	my $ret;
	print "POSTGRESQL DATABASE - INITIALIZATION\n\n";
	
	if (&PGcheckDBstatus(0)) {
		warn "Operation Cancelled. Initialization of Online Database not permitted.\nDatabase must be offline. Please recheck the configuration.\n";
		return 0;
	}
	
	# Check Server Configuration
	&PGcheckPrimaryServer or &PGcheckSecondaryServer() or
		die "ERROR: Primary / Secondary Server configuration cannot be verified.\nPlease check recheck the configuration.\n";

	
	&PGinitConfirm() or die "Operation Cancelled.\n";
	$ret = &PGinitDB();
	return $ret;
}


# Hot Backup of PostgreSQL to Backup Server

sub OperDBhotBackup() {
	my $ret;
	my $syscmd;
	my $dbstatus;

	# Input Checking
	(%backupLocHot and $backupDirHot) or
		die "ERROR: Backup Location and/or Hot Backup Directory not defined. Please check the configuration file $configFile.\n";
	
	# Initialization
	$backupDirHotHost = "$backupDirHot/$hostname";
	
	# Check backup location
	&PGcheckBackupLoc(\%backupLocHot);

	printf "POSTGRESQL DATABASE - HOT BACKUP - START - %s\n\n", &GetTimestamp();
	
	$dbstatus = &PGcheckDBstatus();
	if ($dbstatus) {
		if (not &PGqueryStartBackup("", "Hot Backup $curDate")) {
			warn "ERROR: Failure in configuration of PostgreSQL Database for startup of Hot Backup.\nHot Backup Cancelled\n";
			return 0;
		}
	}
	else {
		warn "WARNING: Database offline. Executing Cold Backup instead\n";
	}

	# Execute Backup
	$ret = &PGbackupDB();

	if ($dbstatus) {
		if (not &PGqueryStopBackup("")) {
			warn "ERROR: Failure in configuration of PostgreSQL Database for end of Hot Backup.\nHot Backup Cancelled\n";
		}
	}

	printf "\nPOSTGRESQL DATABASE - HOT BACKUP - END - %s\n", &GetTimestamp();
	return $ret;
}


# Restore from PostgreSQL Database File Level Backup stored on Backup Server

sub OperDBrestoreBackup() {
	my $ret;
	my $syscmd;
	my $dbstatus;

	# Input Checking
	(%backupLocHot and $backupDirHot) or
		die "ERROR: Backup Location and/or Hot Backup Directory not defined. Please check the configuration file $configFile.\n";
	($reqBackupFilename and $srcHostname) or
		die "ERROR: Backup Source Host (-s) and backup filename (-b) arguments are mandatory for db_restore_backup operation.\n";
	
	# Check backup location
	&PGcheckBackupLoc(\%backupLocHot);
	
	print "POSTGRESQL DATABASE - RESTORE FROM BACKUP\n";
	print "Backup generated on $srcHostname to be restored on $hostname.\n\n";
	if (&PGcheckDBstatus(0)) {
		warn "Operation Cancelled. Restore over Online Database on local server ($hostname) not permitted.\nDatabase must be offline. Please recheck the configuration.\n";
		return 0;
	}
	&PGinitConfirm() or die "Operation Cancelled.\n";

	# Initialize Database
	$ret = &PGinitDB();
	if (not $ret) {
		warn "ERROR: Operation cancelled due to errors in initialization.\n";
		return 1;
	}

	printf "\nPOSTGRESQL DATABASE - RESTORE FROM BACKUP - START - %s\n\n", &GetTimestamp();

	$ret = &PGrestoreDB();

	printf "\nPOSTGRESQL DATABASE - RESTORE FROM BACKUP - END - %s\n\n", &GetTimestamp();

	# Remove left-over Trigger Files.
	&PGremoveTriggerFile();

	return $ret;
}


# Initial Sync of Secondary PostgreSQL Server with Primary Server

sub OperDBinitSync() {
	my $ret;
	my $syscmd;
	my $dbstatus;

	# Input Checking
	($primaryHost and $secondaryHost) or
		die "ERROR: Primary and secondary servers are not defined. Please check the configuration file $configFile.\n";
	($serverType) or
		die "ERROR: Server type is not defined. Please check the configuration file $configFile.\n";
	
	# Check Server Configuration
	&PGcheckSecondaryServer() or
		die "ERROR: Secondary Server configuration cannot be verified.\nThe current command is for replicating the contents of the Primary Server on the Secondary Server and the command must always be executed on the secondary server. Please check recheck the configuration.\n";

	print "POSTGRESQL DATABASE - INITIAL SYNC - $primaryHost ===> $secondaryHost\n\n";

	if (&PGcheckDBstatus(0)) {
		warn "Operation Cancelled. Initialization of Online Database not permitted.\nDatabase must be offline. Please recheck the configuration.\n";
		return 0;
	}

	$ret = &PGinitConfirm();
	if ($ret != 1) {
		warn "Operation Cancelled.\n";
		return 0;
	}

	printf "POSTGRESQL DATABASE - INITIAL SYNC - START - %s\n\n", &GetTimestamp();
	
	if (not &PGqueryStartBackup($primaryHost, "Hot Backup $curDate")) {
		warn "ERROR: Failure in configuration of PostgreSQL Database on remote host $primaryHost for startup of Hot Copy.\nHot Backup Cancelled\n";
		return 0;
	}

	# Execute Sync
	$ret = &PGrsyncDB();

	if (not &PGqueryStopBackup($primaryHost)) {
		warn "ERROR: Failure in configuration of PostgreSQL Database on remote host $primaryHost for end of Hot Backup.\nHot Backup Cancelled\n";
	}

	# Remove left-over Trigger Files.
	&PGremoveTriggerFile();

	printf "\nPOSTGRESQL DATABASE - INITIAL SYNC - END - %s\n", &GetTimestamp();
	return $ret;
}


# Binary Dump of PostgreSQL to Backup Server

sub OperDBexport() {
	my $ret;
	my $syscmd;
	my $dbstatus;

	# Input Checking
	(%backupLocDump and $backupDirDump) or
		die "ERROR: Backup Location and/or Dump (Export) Directory not defined. Please check the configuration file $configFile.\n";
	($dbName) or
		die "ERROR: Database name (-D) argument is mandatory for db_export operation.\n";
	
	# Initialization
	$backupDirDumpHost = "$backupDirDump/$hostname";
	
	# Check backup location
	&PGcheckBackupLoc(\%backupLocDump);

	printf "POSTGRESQL DATABASE - DUMP (EXPORT) - START - %s\n\n", &GetTimestamp();
	
	$dbstatus = &PGcheckDBstatus();
	if (not $dbstatus) {
		warn "ERROR: Database offline. Dump (Export) process cannot be executed unless the database is online.\nDatabase dump cancelled.\n";
		return 0;
	}

	# Execute Backup
	$ret = &PGdumpDB();

	printf "\nPOSTGRESQL DATABASE - DUMP (EXPORT) - END - %s\n", &GetTimestamp();
	return $ret;
}


# Restore from PostgreSQL Database Dump stored on Backup Server

sub OperDBimport() {
	my $ret;
	my $syscmd;
	my $dbstatus;

	# Input Checking
	(%backupLocDump and $backupDirDump) or
		die "ERROR: Backup Location and/or Dump (Export) Directory not defined. Please check the configuration file $configFile.\n";
	($reqBackupFilename and $srcHostname) or
		die "ERROR: Backup Source Host (-s), backup filename (-b) arguments are mandatory for db_import operation.\n";
	
	# Check backup location
	&PGcheckBackupLoc(\%backupLocDump);
	
	if ($numJobs and $numJobs > 0 and $backupLocDump{'type'} eq 'ssh') {
		warn "WARN: Parallel import is not posible with streaming import using SSH protocol. Using single threaded import instead.\n";
		$numJobs = 1;
	}
	
	print "POSTGRESQL DATABASE - RESTORE FROM DUMP\n";
	print "Dump generated on $srcHostname to be restored on $hostname.\n\n";
	$dbstatus = &PGcheckDBstatus();
	if (not $dbstatus) {
		warn "ERROR: Database offline. Dump (Export) process cannot be executed unless the database is online.\nDatabase dump cancelled.\n";
		return 0;
	}
	&PGrestoreDumpConfirm() or die "Operation Cancelled.\n";

	printf "\nPOSTGRESQL DATABASE - RESTORE FROM DUMP - START - %s\n\n", &GetTimestamp();

	$ret = &PGrestoreDump();

	printf "\nPOSTGRESQL DATABASE - RESTORE FROM DUMP - END - %s\n\n", &GetTimestamp();

	return $ret;
}


# Check if server is Primary

sub OperCheckPrimary() {
	my $ret = PGcheckPrimaryServer();
	if ($VERBOSE) {
		if ($ret) {
			print "Primary DB Server: YES\n"; }
		else {
			print "Primary DB Server: NO\n"; }
	}
	return $ret;
}


# Check if server is Secondary

sub OperCheckSecondary() {
	my $ret = PGcheckSecondaryServer();
	if ($VERBOSE) {
		if ($ret) {
			print "Secondary DB Server: YES\n"; }
		else {
			print "Secondary DB Server: NO\n"; }
	}
	return $ret;
}


#
# POSTGRESQL UTILITY FUNCTIONS
#


# Check existence of directory for storing Temporary Xlog Files.
# Create directory if necessary.
 
sub PGcheckTmpXlogDir() {
	if (-d $pgCompXlogDir )	{
		return 1;
	}
	if (! $DRYRUN) {
		mkdir $pgCompXlogDir or die "ERROR: Failure in creation of directory $pgCompXlogDir for temporary archive log files.\n";
	}
	$DEBUG and print "Created directory $pgCompXlogDir for temporary archive log files.\n";
	return 1;
}


# Check if backup location is ready.
# Returns 1 if ready, 0 otherwise.
 
sub PGcheckBackupLoc() {
	
	my $locInfo = $_[0];
	my $type = $locInfo->{'type'};
	my $dir = $locInfo->{'dir'};
	
	if ($type eq 'ssh') {
		return 1;
	}
	elsif ($type eq 'nfs' or $type eq 'cifs') {
		open FILE, "< $mtab_path" or die "ERROR: Failure opening mount tab $mtab_path for reading.\n";
		my @lines = <FILE>;
		close FILE;
		for my $line (@lines) {
			my @cols = split(/\s+/, $line);
			if ($cols[1] eq $dir) {
				return 1;
			} 
		}
		die "ERROR: Mount point for backups ($dir) is not mounted.\n";
	}
	elsif ($type eq 'file') {
		if (-d $locInfo->{'dir'}) {
			return 1;
		}
		else {
			die "ERROR: Backup destination directory ($dir) is invalid.\n";
		} 
	}
	else {
		die "ERROR: Backup destination is invalid.\n.";
	}
}


# Check user running the script.
# Return true if the script is run by the PostgreSQL Owner.
 
sub PGcheckScriptUser() {
	my $user = getpwuid($<);
	$DEBUG and print "Current User: $user\n";
	if ($user) {
		return ($user eq $pgUser);
	}
	else {
		die "ERROR: Failed to determine user executing the script.\n";
	}
}


# Check status of PostgreSQL Database.
# Return true if service is up, false otherwise.
# Simulate Online / Offline on Dry Run, by passing 1 / 0 as arg1.
 
sub PGcheckDBstatus() {
	my $argCount = @_;
	my $dryrunStatus = 1;
	if ($argCount == 1) {
		$dryrunStatus = $_[0];
	}
	my $ret = &SysExec("$cmd_pgrep $pgProc > /dev/null", $DRYRUN);
	if ($DRYRUN) {
		if ($dryrunStatus) {
			$ret = 0; }
		else {
			$ret = 1; }
	}
	if ($ret == 0) {
		$VERBOSE and print "PostgreSQL Database: ONLINE\n";
		return 1;
	}
	else {
		$VERBOSE and print "PostgreSQL Database: OFFLINE\n";
		return 0;
	}
}


# Check existence of Trigger File.
# Return one of the following values:
# 	none - No Recovery
# 	fast - Fast Recovery
# 	smart - Smart Recovery

sub PGcheckTriggerFile() {
	if (-r $triggerFilePath) {
		open FILE, "< $triggerFilePath" or die "ERROR: Failure opening trigger file $triggerFilePath for read.\n";
		my $line = <FILE>;
		close FILE;
		if ($line =~ /^fast/i) { return "fast"; }
		elsif ($line =~ /^smart/i) { return "smart"; }
		else { return "smart"; }
	}
	else {
		return "none";
	}
}


# Create Trigger File.
# First argument must contain the data written to the trigger file.

sub PGcreateTriggerFile() {
	my $data = $_[0];
	my $ret;

	if (not $DRYRUN) {
		open FILE, "> $triggerFilePath" or die "ERROR: Openining of Trigger File $triggerFilePath for write failed.\n";
		print FILE "$data\n" or die "ERROR: Write to Trigger File $triggerFilePath failed.\n";
		close FILE;
	}
	$DEBUG and print "Trigger File $triggerFilePath created.\n";
	return 1;
}


# Remove Trigger File

sub PGremoveTriggerFile() {

	if (-f $triggerFilePath) {
		if (not $DRYRUN) {
			unlink($triggerFilePath) or die "ERROR: Removal of Trigger File $triggerFilePath failed\n";
		}
		$VERBOSE and print "Trigger File $triggerFilePath removed.\n";
		return 1;
	}
	else {
		$DEBUG and print "No Trigger File $triggerFilePath to remove.\n";
		return 1;
	}
}


# Check existence of Compressed WAL File on Archive Directory of Backup Server.

sub PGcheckRemoteWALfile() {
	my $ret;

	if ($backupLocArch{'type'} eq 'ssh') {
		my $syscmd;
		my $sshConn;
		if ($backupLocArch{'user'}) {
			$sshConn = $backupLocArch{'user'} . '@' . $backupLocArch{'host'};
		}
		else {
			$sshConn = $backupLocArch{'host'};
		}
		my $backupDir = $backupLocArch{'dir'};
		$syscmd = "$cmd_ssh \"$sshConn\" $cmd_test -r \"$backupDir/$backupDirArchHost/$WALfile.gz\"";
		$ret = &SysExec($syscmd, $DRYRUN);
	}
	else {
		my $backupDir = $backupLocArch{'dir'};
		if (-r "$backupDir/$backupDirArchHost/$WALfile.gz") {
			$ret = 0;
		}
		else {
			$ret = 1;
		}
	}
	if ($ret == 0) {
		$VERBOSE and print "Status: Found\n";
		return 1;
	}
	else {
		$VERBOSE and print "Status: Not Found\n";
		return 0;
	}
}


# Compress and Rsync WAL File to Backup Server

sub PGarchiveWALfile() {
	my $ret;
	my $syscmd;

	# Compress WAL File
	if (not -r $WALpath) {
		warn "ERROR: Reading of WAL File $WALfile at path $WALpath failed.\n";
		return 0;
	}

	$syscmd = "nice -19 $cmd_gzip -c \"$WALpath\" > \"$compWALpath\"";
	$ret = SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "Generation of compressed WAL file at $compWALpath failed.\n";
		return 0;
	}
	$VERBOSE and print "Compress: OK\n";

	# Rsync WAL File
	my $rsyncDst = $backupLocArch{'rsyncurl'} . '/' . $backupDirArchHost;
	$syscmd = "nice -19 $cmd_rsync --delay-updates \"$compWALpath\" \"$rsyncDst\"";
	$ret = SysExec($syscmd, $DRYRUN);
	if( $ret != 0 ) {
	   	warn "ERROR: Rsync of compressed WAL file at $compWALpath to the Archive File Backup Destination $rsyncDst failed.\n";
		return 0;
	}
	$VERBOSE and print "Rsync: OK\n";
	return 1;
}


# Rsync Compressed WAL File from Backup Server and Decompress

sub PGrestoreWALfile() {
	my $ret;
	my $syscmd;

	# Retry Loop
	for (my $try=1; $try <= $restoreMaxRetries ; $try++) {
		$ret = 0;

		# Rsync WAL File
		my $rsyncSrc = $backupLocArch{'rsyncurl'} . '/' . $backupDirArchHost;
		$syscmd = "nice -19 $cmd_rsync --delay-updates \"$rsyncSrc/$WALfile.gz\" \"$compWALpath\"";
		$ret = SysExec($syscmd, $DRYRUN);
		if( $ret != 0 ) {
		   	warn "ERROR: Rsync of compressed WAL file $WALfile.gz from Archive File Backup Directory $rsyncSrc to $WALpath failed.\n";
			$VERBOSE and print "Rsync: FAIL\n";
			$VERBOSE and print "Try $try: FAIL\n";
			sleep($restoreRetryInterval);
			next;
		}
		$VERBOSE and print "Rsync: OK\n";

		# Uncompress WAL File
		$syscmd = "nice -19 $cmd_gunzip -c \"$compWALpath\" > \"$WALpath\"";
		$ret = SysExec($syscmd, $DRYRUN);
		if( $ret != 0 ) {
			warn "Decompression of WAL file at $compWALpath to $WALpath failed.\n";
			$VERBOSE and print "Decompress: FAIL\n";
			$VERBOSE and print "Try $try: FAIL\n";
			sleep($restoreRetryInterval);
			next;
		}
		$VERBOSE and print "Decompress: OK\n";
		$VERBOSE and print "Restore: OK\n";
		return 1;
	}
	$VERBOSE and print "Restore: FAIL\n";
	return 0;
}


# Remove WAL Files not necessary for Recovery on Next Restart

sub PGcleanupWALfiles() {
	my $ret = 1;
	if ($lastReqWALfile) {
		my @paths = glob("$pgXlogDir/0*");
		foreach my $path (@paths) {
			my $file = basename($path);
			if ($file =~ /^[0-9A-F]{24}$/ and -f $path and $file lt $lastReqWALfile) {
				$VERBOSE and print "Remove: $file\n";
				if (not $DRYRUN) {
					if (not unlink($path)) {
						warn "WARNING: Removal of WAL File $file failed.\n";
						$ret = 0;
					}
				}
			}
		}
	}
	return $ret;
}


# Cleanup Temporary Compressed WAL File

sub PGcleanupTmpWALfile() {
	if (not $DRYRUN and -r $compWALpath) {
		$DEBUG and print "Temp File: $compWALpath\n";
		if (unlink($compWALpath)) {
			$VERBOSE and print "Cleanup Temp: OK\n";
			return 1;
		}
		else {
			warn "WARNING: Removal of temporary file $compWALpath failed\n";
			$VERBOSE and print "Cleanup: FAIL\n";
			return 0;
		}
	}
	$VERBOSE and print "Cleanup Temp: OK\n";
	return 1;
}


# Confirm Initialization of Database.
# Returns true on confirmation from user.

sub PGinitConfirm() {
	my $input;
	print "WARNING! This operation will destroy all data in PostgreSQL Data Directory $pgDataDir on localhost ($hostname).\n";
	print "PostgreSQL Database will be initialized completely.\n";
	print "Type 'YES' to proceed with execution: ";
	$input = <STDIN>;
	chomp($input);
	if ($input eq 'YES') {
		return 1;
	}
	else {
		return 0;
	}
}


# Confirm Initialization of Database.
# Returns true on confirmation from user.

sub PGrestoreDumpConfirm() {
	my $input;
	print "WARNING! This operation will recreate PostgreSQL Databases in dump. The existing databases must be dropped manually before executing the restore on localhost ($hostname).\n";
	print "Type 'YES' to proceed with execution: ";
	$input = <STDIN>;
	chomp($input);
	if ($input eq 'YES') {
		return 1;
	}
	else {
		return 0;
	}
}


# Check IP of Server to verify Primary Server Configuration. 

sub PGcheckPrimaryServer() {
	my @ipList;
	my $ret;
	if ($serverType eq 'primary') {
		@ipList = &GetServerIPlist();
		$ret = grep($_ eq $primaryHost, @ipList);
		return $ret > 0;
	}
	else {
		return 0;
	}
}


# Check IP of Server to verify Secondary Server Configuration. 

sub PGcheckSecondaryServer() {
	my @ipList;
	my $ret;
	if ($serverType eq 'secondary') {
		@ipList = &GetServerIPlist();
		$ret = grep($_ eq $secondaryHost, @ipList);
		return $ret > 0;
	}
	else {
		return 0;
	}
}


# Initialize PostgreSQL Database.

sub PGinitDB() {
	my $ret;
	my $syscmd;

	$VERBOSE and print "Started Initialization of PostgreSQL Database.\n";

	$syscmd = "$cmd_find $pgDataDir/*/* -maxdepth 0 ! -name 'lost+found' | $cmd_xargs $cmd_rm -rf";
	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "ERROR: Failure in initialization of PostgreSQL Data Directory $pgDataDir.\n";
		return 0;
	}

	$syscmd = "$cmd_find $pgDataDir -type f | $cmd_xargs $cmd_rm -f";
	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "ERROR: Failure in initialization of PostgreSQL Data Directory $pgDataDir.\n";
		return 0;
	}
	$VERBOSE and print "End of Initialization of PostgreSQL Database.\n";

	return 1;
}


# Activate Hot Backup mode on Database.
#   Arg1: Server IP
#   Arg2: Label for Hot Backup
# 	Execute on local database if Server IP is empty.

sub PGqueryStartBackup() {
	my $host = $_[0];
	my $label = $_[1];
	my $ret;
	my $syscmd;
	
	my $sqlQuery = "SELECT pg_start_backup('$label');";
	$DEBUG and print "SQL Query: $sqlQuery\n";
	
	if ($host) {
		$syscmd = "echo \"$sqlQuery\" | $cmd_ssh \"$host\" '$cmd_psql -q'";
	}
	else {
		$syscmd = "$cmd_psql -q -c \"$sqlQuery\"";
		$host = "localhost";
	}

	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret == 0) {
		$VERBOSE and print "Signalled startup of Online Backup to PostgreSQL Database on $host.\n";
		return 1;
	}
	else {
		warn "ERROR: Failure in signalling startup of Online Backup to PostgreSQL Database on $host.\n";
		return 0;
	}
}


# Deactivate Hot Backup mode on Database.
#   Arg1: Server IP
# 	Execute on local database if Server IP is empty.

sub PGqueryStopBackup() {
	my $host = $_[0];
	my $ret;
	my $syscmd;
	
	my $sqlQuery = "SELECT pg_stop_backup();";
	$DEBUG and print "SQL Query: $sqlQuery\n";
	
	if ($host) {
		$syscmd = "echo \"$sqlQuery\" | $cmd_ssh \"$host\" '$cmd_psql -q'";
	}
	else {
		$syscmd = "$cmd_psql -q -c \"$sqlQuery\"";
		$host = "localhost";
	}

	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret == 0) {
		$VERBOSE and print "Signalled end of Online Backup to PostgreSQL Database on $host.\n";
		return 1;
	}
	else {
		warn "ERROR: Failure in signalling end of Online Backup to PostgreSQL Database on $host.\n";
		return 0;
	}
}


# Backup PostgreSQL Database to Remote Backup Server

sub PGbackupDB() {
	my $ret;
	my $syscmd;
	my $backupFilename = "hotbackup-$hostname-$curDate.tgz";
	my $backupFilePath = $backupLocHot{'dir'} . '/' . "$backupDirHotHost/$backupFilename";

	if ($backupLocHot{'type'} eq 'ssh') {
		my $sshConn;
		if ($backupLocHot{'user'}) {
			$sshConn = $backupLocHot{'user'} . '@' . $backupLocHot{'host'};
		}
		else {
			$sshConn = $backupLocHot{'host'};
		}
		$syscmd = "$cmd_tar ";
		$syscmd .= "--exclude 'lost+found' ";
		$syscmd .= "--exclude '$pgDataDir/recovery.*' ";
		$syscmd .= "--exclude '$pgDataDir/postmaster.pid' ";
		$syscmd .= "--exclude '$pgDataDir/pg_stat_tmp' ";
		$syscmd .= "-zcf - '$pgDataDir' | $cmd_ssh '$sshConn' '$cmd_cat > $backupFilePath'";
	}
	else {
		$syscmd = "$cmd_tar ";
		$syscmd .= "--exclude 'lost+found' ";
		$syscmd .= "--exclude '$pgDataDir/recovery.*' ";
		$syscmd .= "--exclude '$pgDataDir/postmaster.pid' ";
		$syscmd .= "--exclude '$pgDataDir/pg_stat_tmp' ";
		$syscmd .= "-zcf '$backupFilePath' '$pgDataDir'";
	}
	
	if ($VERBOSE) {
		printf "\nStart Backup of PostgreSQL Data Files - %s\n\n", &GetTimestamp();
		print "Backup Src : $pgDataDir\n";
		print "Backup Location: $backupURLhot\n"; 
		print "Backup Dir : $backupDirHotHost\n"; 
		print "Backup File: $backupFilename\n";
		print "\n";
	}

	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "ERROR: Failure in backup of PostgreSQL Datafiles in $pgDataDir to Backup file $backupFilename in directory $backupDirHotHost at $backupURLhot.\n";
		return 0;
	}

	if ($VERBOSE) {
		printf "\nEnd Backup of PostgreSQL Data Files - %s\n\n", &GetTimestamp();
	}

	return 1;
}


# Restore PostgreSQL Database Backup from Remote Backup Server

sub PGrestoreDB() {
	my $ret;
	my $syscmd;
	my $backupFilePath = $backupLocHot{'dir'} . '/' . "$backupDirHot/$srcHostname/$reqBackupFilename";

	if ($backupLocHot{'type'} eq 'ssh') {
		my $sshConn;
		if ($backupLocHot{'user'}) {
			$sshConn = $backupLocHot{'user'} . '@' . $backupLocHot{'host'};
		}
		else {
			$sshConn = $backupLocHot{'host'};
		}
		$syscmd = "$cmd_ssh '$sshConn' '$cmd_cat $backupFilePath' | $cmd_tar -C / -zxf -";
	}
	else {
		$syscmd = "$cmd_tar -C / -zxf $backupFilePath";
	}
	
	if ($VERBOSE) {
		printf "\nStart Restore of PostgreSQL Data Files - %s\n\n", &GetTimestamp();
		print "Backup generated on: $srcHostname\n";
		print "Backup Location: $backupURLhot\n"; 
		print "Backup Dir : $backupDirHot/$srcHostname\n"; 
		print "Backup File: $reqBackupFilename\n";
		print "\n";
	}

	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "ERROR: Failure in restore from file $reqBackupFilename from $backupURLhot, which contains backup of PostgreSQL Data Files in $pgDataDir of host $srcHostname.\n";
		return 0;
	}

	if ($VERBOSE) {
		printf "\nEnd Backup of PostgreSQL Data Files - %s\n\n", &GetTimestamp();
	}

	return 1;
}


# Sync PostgreSQL Database Files from Primary to Secondary Server

sub PGrsyncDB() {
	my $ret;
	my $syscmd;

	if ($DEBUG) {
		$syscmd = "$cmd_rsync -avz --stats --delete "; }
	else {
		$syscmd = "$cmd_rsync -az --stats --delete "; }
	$syscmd .= "--exclude 'lost+found' ";
	$syscmd .= "--exclude 'postmaster.pid' ";
	$syscmd .= "--exclude 'pg_stat_tmp' ";
	$syscmd .= "--exclude 'recovery.*' ";
	$syscmd .= "'$primaryHost:$pgDataDir/' '$pgDataDir'";
	
	if ($VERBOSE) {
		printf "\nStart Sync of PostgreSQL Data Files - %s\n\n", &GetTimestamp();
		print "Sync Dir        : $pgDataDir\n";
		print "Source Host     : $primaryHost\n"; 
		print "Destination Host: $secondaryHost\n"; 
		print "\n";
	}

	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "ERROR: Failure in rsync of PostgreSQL Datafiles in $pgDataDir from $primaryHost to $secondaryHost.\n";
		return 0;
	}

	if ($VERBOSE) {
		printf "\nEnd Sync of PostgreSQL Data Files - %s\n\n", &GetTimestamp();
	}

	return 1;
}


# Dump PostgreSQL Database to Remote Backup Server

sub PGdumpDB() {
	my $ret;
	my $syscmd;
	my $backupFilename = "$hostname-$dbName-bin-$curDate.dump";
	my $backupFilePath = $backupLocDump{'dir'} . '/' . "$backupDirDumpHost/$backupFilename";

	if ($backupLocDump{'type'} eq 'ssh') {
		my $sshConn;
		if ($backupLocDump{'user'}) {
			$sshConn = $backupLocDump{'user'} . '@' . $backupLocDump{'host'};
		}
		else {
			$sshConn = $backupLocDump{'host'};
		}
		$syscmd = "$cmd_pgdump -Fc -Z8 '$dbName' ";
		$syscmd .= "| $cmd_ssh '$sshConn' '$cmd_cat > '$backupFilePath'";
	}
	else {
		$syscmd = "$cmd_pgdump -Fc -Z8 '$dbName' ";
		$syscmd .= "> '$backupFilePath'";
	}
	
	if ($VERBOSE) {
		printf "\nStart Dump of PostgreSQL Database %s - %s\n\n", $dbName, &GetTimestamp();
		print "Backup Src : $pgDataDir\n";
		print "Backup Location: $backupURLdump\n"; 
		print "Backup Dir : $backupDirDumpHost\n"; 
		print "Backup File: $backupFilename\n";
		print "\n";
	}

	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "ERROR: Failure in dump of PostgreSQL Database $dbName to Backup file $backupFilename in directory $backupDirDumpHost at $backupURLdump.\n";
		return 0;
	}

	if ($VERBOSE) {
		printf "\nEnd Dump of PostgreSQL Database %s - %s\n\n", $dbName, &GetTimestamp();
	}

	return 1;
}


# Dump PostgreSQL Database to Remote Backup Server

sub PGrestoreDump() {
	my $ret;
	my $syscmd;
	my $backupFilePath = $backupLocDump{'dir'} . '/' . "$backupDirDump/$srcHostname/$reqBackupFilename";

	my $optJobs = "";
	if ($numJobs > 1) {
		$optJobs = "-j $numJobs";
	}
	if ($backupLocDump{'type'} eq 'ssh') {
		my $sshConn;
		if ($backupLocDump{'user'}) {
			$sshConn = $backupLocDump{'user'} . '@' . $backupLocDump{'host'};
		}
		else {
			$sshConn = $backupLocDump{'host'};
		}
		$syscmd = "$cmd_ssh '$sshConn' '$cmd_cat $backupFilePath' "; 
		$syscmd .= "| $cmd_pgrestore $optJobs -Fc -C -d '$pgUser'";
	}
	else {
		$syscmd = "$cmd_pgrestore $optJobs -Fc -C -d '$pgUser' '$backupFilePath'";
	}
	
	if ($VERBOSE) {
		printf "\nStart restore of PostgreSQL Database from dump.- %s\n\n", &GetTimestamp();
		print "Backup generated on: $srcHostname\n";
		print "Backup Location: $backupURLdump\n"; 
		print "Backup Dir : $backupDirDump/$srcHostname\n"; 
		print "Backup File: $reqBackupFilename\n";
		print "\n";
	}

	$ret = &SysExec($syscmd, $DRYRUN);
	if ($ret != 0) {
		warn "ERROR: Failure in restore from PostgreSQL Database from dump in $reqBackupFilename generated on $srcHostname from $backupURLdump.\n";
		return 0;
	}

	if ($VERBOSE) {
		printf "\nEnd restore of PostgreSQL Database from dump.- %s\n\n", &GetTimestamp();
	}

	return 1;
}



#
# GENERIC UTILITY FUNCTIONS
#


# Read Configuration File

sub ReadConfig() {
	my $config_path;
	if (-r "$pgBaseDir/$configFile") {
		$config_path = "$pgBaseDir/$configFile";
	}
	elsif (-r "./$configFile") {
		$config_path = "./$configFile";
	}
	else {
		die "ERROR: Configuration file $configFile not found.\n";
	}
	$DEBUG and print "Configuration File: $config_path\n";

	open FILE, "< $config_path" or die "ERROR: Failure opening configuration file $configFile for reading.\n";
	my @lines = <FILE>;
	close FILE;

	my %conf;
	foreach my $line (@lines) {
		chomp($line);
		if ($line =~ /^\s*(\w+)\s*=\s*(\S+)(\s*|\s*#.*)$/ ) {
			$conf{$1} = $2;
			$DEBUG and print $1, ": ", $2, "\n";
		}
	}
	$DEBUG and print "\n";
	return %conf;
}



# Parse Backup Directory Argument
# Receives URL as argument and returns dictionary.

sub ParseDirectoryURL() {
	my $url = $_[0];
	my %dirInfo;
	my $type;
	my $url_tail;
	if ($url =~ /^(ssh|nfs|cifs|smbfs|smb|file):\/\/(.*)$/) {
		$type = $1;
		$url_tail = $2;
	}
	else {
		$type = 'file';
		$url_tail = $url;
	}
	if ($type eq 'ssh') {
		if ($url_tail =~ /^(\w+)@([\w\-\.]+):([\w\-\/\.]+)$/) {
			$dirInfo{'type'} = $type;
			$dirInfo{'user'} = $1;
			$dirInfo{'host'} = $2;
			$dirInfo{'dir'} = $3;
			$dirInfo{'rsyncurl'} = sprintf "%s@%s:%s", $dirInfo{'user'}, $dirInfo{'host'}, $dirInfo{'dir'};
			$DEBUG and printf "URL: %s    Type: %s    Host: %s    User: %s    Dir: %s\n",
				$url, $dirInfo{'type'}, $dirInfo{'host'}, $dirInfo{'user'}, $dirInfo{'dir'};
		}
		elsif ($url_tail =~ /^([\w\-\.]+):([\w\-\/\.]+)$/) {
			$dirInfo{'type'} = $type;
			$dirInfo{'host'} = $1;
			$dirInfo{'dir'} = $2;
			$dirInfo{'rsyncurl'} = sprintf "%s:%s", $dirInfo{'host'}, $dirInfo{'dir'};
			$DEBUG and printf "URL: %s    Type: %s    Host: %s    Dir: %s\n", 
				$url, $dirInfo{'type'}, $dirInfo{'host'}, $dirInfo{'dir'};
		}
	}
	elsif ($url_tail =~ /^[\w\-\/\.]+$/) {
		if ($type eq 'smbfs' or $type eq 'smb') {
			$dirInfo{'type'} = 'cifs'
		}
		else {
			$dirInfo{'type'} = $type;
		}
		$dirInfo{'dir'} = $url_tail;
		$dirInfo{'rsyncurl'} = $url_tail;
		$DEBUG and printf "URL: %s    Type: %s    Dir: %s\n", $url, $dirInfo{'type'}, $dirInfo{'dir'};
	}
	return %dirInfo;
}


# Setup Logging
# Receives Log Name as Argument

sub StartLoggingToFile() {
	my $logName = $_[0];
	my $curWeekDay = &GetWeekDay();
	my $filePath = "$pgLogDir/$logName-$curWeekDay.log";
	if (-e $filePath and -M $filePath > 2) {
		unlink($filePath) or warn "ERROR: Initialization of log file $filePath failed.\n";
	} 
	open STDOUT, ">> $filePath" or  die "ERROR: Redirection of output to log file $filePath failed.\n";
	open STDERR, ">> $filePath" or  die "ERROR: Redirection of output to log file $filePath failed.\n";
}


# Function to get list of Server IP Addresses

sub GetServerIPlist() {
	my $cmd = "$cmd_ifconfig -a";
	$DEBUG and print "Execute: $cmd\n";
	my @lines = `$cmd`;
	if ($?) {
		die "ERROR: Execution of of command failed: $cmd\n";
	}

	my @ipList;
	foreach my $line (@lines) {
		my $ip;
		if ($line =~ /inet addr:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+/) {
			$ip = $1;
			$DEBUG and print "Server IP: $ip\n";
			push(@ipList, $ip);
		}
	}
	return @ipList;
}


# Function to execute external command in shell.
# 	Arg1: Command String
# 	Arg2: Simulate if true.
# Returns exit value of external command.

sub SysExec() {
	my $cmd = $_[0];
	my $dryrun = $_[1];

	$DEBUG and print "Execute: $cmd\n";
	if ($dryrun) {
		return 0;
	}
	else {
		system($cmd);
		if ($? == -1) {
			die "ERROR: Execution of command failed: $cmd\n";
		}
		else {
			return ($? >> 8);
		}
	}
}


# Function to Print Usage

sub PrintUsage() {
	print "Help on Usage\n";
	print "usage: pg_manage.pl -h\n";
	print "\n";
	
	print "Send Archive Log to Backup Server / Retrieve Archive Log from Backup Server\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c xlog_archive [-l] -f FILE -p PATH\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c xlog_restore [-l] [-t N] [-i SECS] [-I SECS] [-w SECS] -f FILE -p PATH [-r FILE]\n";
	print "\n";
	
	print "Create / Remove Trigger File\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c recovery_trigger_fast\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c recovery_trigger_smart\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c recovery_trigger_cleanup\n";
	print "\n";
	
	print "Initialize Database\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c db_destroy\n";
	print "\n";
	
	print "Hot Backup / Restore Database\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c db_hot_backup\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c db_restore_backup -s HOST -b BACKUPFILE\n";
	print "\n";
	
	print "Dump (Export) / Restore (Import) Database\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c db_export -D DATABASE\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c db_import [-j N] -s HOST -b BACKUPFILE\n";
	print "\n";
	
	print "Initial Sync of Servers: Primary -> Secondary\nRun command only on secondary server.\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c db_initial_sync\n";
	print "\n";
	
	print "Check Primary. Return 0 if server is primary\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c check_primary\n";
	print "\n";
	
	print "Check Secondary. Return 0 if server is secondary\n";
	print "usage: pg_manage.pl [-n] [-d] [-q] -c check_secondary\n";
	print "\n";
}


# Function to Print Usage and Exit

sub PrintUsageAndExit() {
	&PrintUsage();
	exit 1;
}


# Get Time Stamp

sub GetTimestamp() {
	return strftime("%Y-%m-%d %H:%M:%S", localtime());
}


# Get Date

sub GetDate() {
	return strftime("%Y-%m-%d", localtime());
}


# Get Week Day

sub GetWeekDay() {
	return strftime("%a", localtime());
}



