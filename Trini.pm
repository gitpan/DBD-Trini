package DBD::Trini;
use strict;
use Symbol;
use DBI;
# use Debug::ShowStuff ':all';
use vars qw[$VERSION $drh $err $errstr $sqlstate %dbd_keys %datatypes];

# DOCUMENTATION: See documentation at end of file


# version
$VERSION = '0.01';


#-----------------------------------------------------
# driver specific: setting up some globals
# 

# constants and sorta constants
use constant FORMAT_VER => '0.1';
use constant EOFI       => '|';  # "End Of FIeld", used only for positional fields, not database record fields
use constant EOL        => "\n";
use constant EOLLEN     => 1;
use constant EOLEN      => EOLLEN;
use constant POSWIDTH   => 8; # Hmm, maybe this should be a property of the database. Well, right now it ain't.
use constant MTEIGHT    => EOFI . (' ' x (POSWIDTH - 1) );

$errstr = '';    # holds error string for DBI::errstr
$sqlstate = '';  # holds error state  for DBI::state

@dbd_keys{qw[
	fh
	fieldnames
	in_creation
	recordset
	dir_path
	datafile_path
	pos
	pos_pos
	table_defs
	stmt
	write
	]}=();

# 
# driver specific: setting up some globals
#-----------------------------------------------------


# general
$drh = undef;	# holds driver handle once initialised
$err = 0;		# The $DBI::err value


#-----------------------------------------------------
# driver
# 
sub driver {
	return $drh if $drh;
	my($class, $attr) = @_;
	
	$class .= '::dr';
	
	($drh) = DBI::_new_drh(
		$class,
		{
			'Name' => 'Trini',
			'Version' => $VERSION,
			'Attribution' => "Pure Perl Database by Miko O'Sullivan",
		},
	);
	
	return $drh;
}
# 
# driver
#-----------------------------------------------------


# ??? don't know what this is for
sub default_user {
	die 'default_user not implemented';
	return ('','');
}


###########################################################################
# DBD::Trini::dr
# 
# ====== DRIVER ======
# 
package DBD::Trini::dr;
use strict;
# use Debug::ShowStuff ':all';
use Carp 'croak';
use vars qw[$imp_data_size];

$imp_data_size = 0;

#------------------------------------------------------------
# connect
# 
sub connect {
	# general
	my($drh, $dbname, $user, $auth, $opts)= @_;
	my($dbh) = DBI::_new_dbh($drh, {
		Name => $dbname,
		User => $user,
		Active => 1,
	});
	
	#-------------------------------------------------
	# driver specific
	# 
	$dbh->STORE('dir_path',      $dbname);
	$dbh->STORE('datafile_path', "$dbname/data");
	$dbh->STORE('in_creation', ! -e $dbname);
	
	# set read/write status
	unless ($opts->{'read_only'})
		{$dbh->STORE('write', 1)}
	
	# if creating, indicate that the database is in creation mode
	if ($opts->{'create'})
		{$dbh->STORE('in_creation', 1)}
	
	# else load database definition from file
	else
		{DBD::Trini::db::load($dbh)}
	
	# 
	# driver specific
	#-------------------------------------------------

	
	# general
	return $dbh;
}
# 
# connect
#------------------------------------------------------------


sub data_sources {
	die 'data_sources not implemented';
	return ('dbi:ExampleP:dir=.');	# possibly usefully meaningless
}

# general
sub disconnect_all {
	# we don't need to tidy up anything
}

# 
# DBD::Trini::dr
###########################################################################



###########################################################################
# DBD::Trini::db
# 
# ====== DATABASE ======
# 
package DBD::Trini::db;
use strict;
# use Debug::ShowStuff ':all';
use SQL::YASP ':all';
use Data::Taxi ':all';
use FileHandle;
use Carp 'croak';
use vars qw[$imp_data_size];

$imp_data_size = 0;
	

#-----------------------------------------------------------------------------------------
# prepare
# 
sub prepare {
	my($dbh, $statement, @attribs)= @_;
	my ($stmt, @fieldnames);
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# general
	# 
	
	# get statement handle object, and a ref to the hash in which the sth stores stuff
	my ($sth, $data) = DBI::_new_sth($dbh, {'Statement' => $statement});
	
	# set private name for field name list
	$data->{'FetchHashKeyName'} = 'fieldnames';
	
	# 
	# general
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# parse SQL (driver specific)
	# 
	
	# build a parsed statement
	$stmt = $sth->{'stmt'} = SQL::YASP->parse($statement, table_definitions=>$dbh->{'table_defs'});
	
	unless ($stmt) {
		DBI::set_err($dbh, 1, $SQL::YASP::errstr);
		return undef;
	}
	
	$stmt->{'dbh'} = $dbh;
	
	# build list of fieldnames
	foreach my $fieldname (keys %{$stmt->{'fields'}}) {
		# if fieldname is *, get all field names in the table
		if ($fieldname eq '*') {
			my $table_name = $stmt->{'from'}->{(keys %{$stmt->{'from'}})[0] };
			
			push
				@fieldnames,
				keys %{$dbh->{'table_defs'}->{$table_name}->{'col_defs'}};
		}
		
		# else just add to list
		else
			{push @fieldnames, $fieldname}
	}
	
	# 
	# parse SQL (driver specific)
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# driver specific but required
	# 
	# this section must set the following properties
	#	fieldnames
	#	NUM_OF_FIELDS
	#	NUM_OF_PARAMS
	# 
	$sth->STORE('fieldnames', \@fieldnames);                     # list of field names requested in the SQL
	$sth->STORE('NUM_OF_FIELDS', scalar(@fieldnames) );          # how many fields
	$sth->STORE('NUM_OF_PARAMS', $stmt->{'placeholder_count'});  # how many params
	# 
	# driver specific but required
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	# general
	return $sth;
}
# 
# prepare
#-----------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------
# subs I haven't implemented because I'm not sure where they're needed
# 
sub table_info     {die 'table_info not implemented'}
sub type_info_all  {die 'type_info_all not implemented'}
sub get_info       {die 'get_info not implemented'}
# 
# subs I haven't implemented because I'm not sure where they're needed
#-----------------------------------------------------------------------------------------


#--------------------------------------------------------------
# FETCH
# 
sub FETCH {
	my ($dbh, $key) = @_;
	
	# if it's an extended key
	if (exists $DBD::Trini::dbd_keys{$key})
		{return $dbh->{$key}}
	
	return $dbh->SUPER::FETCH($key);
}
# 
# FETCH
#--------------------------------------------------------------


#--------------------------------------------------------------
# STORE
# 
sub STORE {
	my ($dbh, $key, $value) = @_;
	
	# handle Autocommit
	if ($key eq 'AutoCommit') {
		# convert AutoCommit values to magic ones to let DBI
		# know that the driver has 'handled' the AutoCommit attribute
		#$value = ($value) ? -901 : -900;
		
		return 1;
		
		# TODO: unload and reload filehandle if necessary
		# use FileHandle::Rollback for rollback segments
	}
	
	# store extended stuff
	elsif (exists $DBD::Trini::dbd_keys{$key})
		{return $dbh->{$key} = $value}
	
	return $dbh->SUPER::STORE($key, $value);
}
# 
# STORE
#--------------------------------------------------------------


#--------------------------------------------------------------
# add_table
# 
# =head2 $db->add_table($tablename, %options);
# 
# Used to add a table to a database that is being created.  The first argument
# is the table name.  See documentation on object name rules.  This routine
# will croak if the table name starts with a _ unless the -internal option is
# sent, or if the name otherwise breaks object naming rules.
# 
# This sub returns a table object with which you can add columns.
# 
# =cut

sub add_table {
	my ($self, $tablename, %opts) = @_;
	my ($class);
	
	# only if we're in creation mode
	unless ($self->{'in_creation'})
		{croak 'can only use add_table in database creation mode'}
	
	# lowercase tablename
	$tablename = valid_object_name($tablename, %opts);
	
	# add to definitions
	$self->{'table_defs'} ||= SQL::YASP::get_ixhash();
	$self->{'table_defs'}->{$tablename} = { 'col_defs' => SQL::YASP::get_ixhash() };
	
	# return new table object
	return DBD::Trini::Table->new($self, $tablename, 'create' => 1);
}
#
# add_table
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_table
# 
# =head2 $db->get_table($tablename);
# 
# Returns a table object with which you can add, delete, and modify rows.
# 
# =cut

sub get_table {
	my ($self, $tablename) = @_;

	return DBD::Trini::Table->new($self, $tablename);
}
#
# get_table
#--------------------------------------------------------------


#--------------------------------------------------------------
# valid_object_name
# 
sub valid_object_name {
	my ($name, %opts) = @_;
	
	# lowercase tablename
	$name = lc($name);
	
	# make sure name conforms to object rules
	unless ($name =~ m|^[a-z0-9_]+$|)
		{croak $name . ': database object name must consist of only letters, numbers, and underscores'}
	
	# may not start with .
	if ( $name =~ m|^_|s ) {
		unless ($opts{'internal'})
			{croak $name . ': database object name may not start with _ unless it is designated as internal table'}
	}
	elsif ( $name !~ m|^[a-z]| )
		{croak $name . ': database object name must start with a letter'}
	
	return $name;
}
# 
# valid_object_name
#--------------------------------------------------------------


#--------------------------------------------------------------
# commit
# 
sub commit ($) {
	my $dbh = shift;
	
	if ($dbh->{'in_creation'})
		{write_structure($dbh, reload=>1)}
}
# 
# commit
#--------------------------------------------------------------


#--------------------------------------------------------------
# write_structure
# 
# Writes out the database file.
# 
sub write_structure {
	my ($dbh, %opts) = @_;
	my ($fh, $iidnum, $all_data, $otherdata);
	
	# default
	defined($opts{'reload'}) or $opts{'reload'} = 1;
	
	# start internal id at ascii 48, which is a nice human readable character
	$iidnum = 48;
	
	$dbh->{'dir_path'} or die 'no dir_path';
	
	# if directory does not already exist, create it
	unless (-d $dbh->{'dir_path'})
		{mkdir $dbh->{'dir_path'} or croak "unable to create data directory: $!"}
	
	# open file handle
	$fh = FileHandle->new("> $dbh->{'datafile_path'}")
		or die "cannot open db file for write: $!";
	binmode $fh;
	
	# write file headers, which currently consist of
	# just the data format version
	print $fh 'Data-Format:Trini.', DBD::Trini::FORMAT_VER, "\n\n";
	
	# loop through all registered data types, calling 
	# before_db_create for each class that has that method
	while ( my($type, $class) = each(%DBD::Trini::datatypes) ) {
		if ($class->can('before_db_create'))
			{$class->before_db_create($dbh, $type)}
	}
	
	# give tables internal ids
	while ( my($key, $tdef) = each %{$dbh->{'table_defs'}} ) {
		my(@regex, $iid, $totalwidth);
		
		# iid (internal table ID)
		# KLUDGE: right now do not allow internal ID's to go above 126
		if ($iidnum > 126){die 'too many tables'}
		$iid = chr($iidnum++);
		
		$dbh->{'table_defs'}->{$key}->{'internal_id'} = $iid;
		
		# regex always starts with (.)
		@regex = ('^(.)(.{', DBD::Trini::POSWIDTH, '})(.{', DBD::Trini::POSWIDTH, '})');
		$totalwidth = (DBD::Trini::POSWIDTH * 2) + 1;
		
		# calculate regexes for columns
		foreach my $cdef (values %{ $tdef->{'col_defs'} } ) {
			my $lwidth = length($cdef->{'width'});
			
			push @regex, "(.{$lwidth})";
			$totalwidth += $lwidth;
			
			$totalwidth += $cdef->{'width'};
			push @regex, '(.{', $cdef->{'width'}, '})';
		}
		
		# store regex and totalwidth
		push @regex, '$';
		$tdef->{'split_regex'} = join('', @regex);
		$tdef->{'total_width'} = $totalwidth;
	}
	
	# if other_data was sent
	$otherdata = $opts{'other_data'} || {};
	
	# build anon hash of all data
	$all_data = {
		table_defs  => $dbh->{'table_defs'},
		%{$otherdata}
	};
	
	# write out
	print $fh Data::Taxi::freeze($all_data), "\n";
	
	# write position block
	while ( my($tname, $def) = each(%{$dbh->{'table_defs'}}) ) {
		# output table information
		print $fh 
			'tables.',  $tname,  '.first_record:',   DBD::Trini::MTEIGHT, "\n",
			'tables.',  $tname,  '.last_record:',    DBD::Trini::MTEIGHT, "\n",
			'tables.',  $tname,  '.first_deleted:',  DBD::Trini::MTEIGHT, "\n";
	}
	
	# final blank line
	print $fh "\n";
	
	# close filehandle, load database
	$fh->close;
	undef $fh;
	
	if ($opts{'reload'}) {
		load($dbh);
		$dbh->{'in_creation'} = 0;
	}
}
# 
# write_structure
#--------------------------------------------------------------


#--------------------------------------------------------------
# load
# 
# open the data file and load the necessary data
# 
sub load {
	my ($self, %opts) = @_;
	my ($fh, $opentype, @taxi, $dbdesc, $foundfirstline, $cpos, $name, $val, $datalen, $eofiregex, $col_defs);
	
	# regex for end-of-field character
	$eofiregex = quotemeta(DBD::Trini::EOFI);
	
	# determine access type
	if ($self->FETCH('write'))
		{$opentype = '+<'}
	else
		{$opentype = ''}
	
	# open data file
	$fh = FileHandle->new("$opentype " . $self->FETCH('datafile_path') )
		or croak("cannot open database file: $!");
	binmode $fh;
	
	# store filehandle
	$self->STORE('fh', $fh);

	# lop through header lines
	# TODO: this is where we need to check that
	# we're reading a data format we understand
	HEADERS:
	while (my $line = <$fh>) {
		unless ($line =~ m|\S|s)
			{last HEADERS}
	}
	
	# loop through opening lines
	TAXI:
	while (my $line = <$fh>) {
		push @taxi, $line;
		
		# if we've found the end of the Taxi block
		if ($line =~ m|\<\/taxi\s*\>|i)
			{last TAXI}
	}
	
	# get taxi info
	$dbdesc = Data::Taxi::thaw(join('', @taxi));
	$self->STORE('table_defs', $dbdesc->{'table_defs'});
	
	# get current position
	$cpos = $fh->tell;
	
	$self->STORE('pos', SQL::YASP::get_ixhash());
	$self->STORE('pos_pos', {});
	
	
	#-------------------------------------------------------
	# loop through position block
	# 
	POS:
	while (my $line = <$fh>) {
		chomp $line;
		
		# if this line has any data
		if ($line =~ m|\S|) {
			my (@chain, $i, $outer);
			
			# note that we've found data
			$foundfirstline = 1;
			
			($name, $val) = split(':', $line, 2);
			$datalen = length($val);
			$val =~ s/$eofiregex.*//;
			
			@chain = split('\.', $name);
			$ i = 0;
			$outer = $self->FETCH('pos');
			
			while ($i < $#chain) {
					$outer->{$chain[$i]} = $outer->{$chain[$i]} || {};
					$outer = $outer->{$chain[$i]};
					$i++;
			}
			
			$outer->{$chain[$#chain]} = $val;
			
			$self->FETCH('pos_pos')->{$name} = 
				{
				hash     => $outer,
				name     => $chain[$#chain],
				'pos'    => $cpos + length($name) + 1,
				width    => $datalen,
				};
		}
		
		# else empty line
		elsif ($foundfirstline)
			{last POS}
		
		# get current position
		$cpos = $fh->tell;
	}
	# 
	# loop through position block
	#-------------------------------------------------------

}
# 
# load
#--------------------------------------------------------------


#--------------------------------------------------------------
# unload
# 
sub unload {
	my ($self) = @_;
	
	# if no filehandle, nothing to do
	$self->{'fh'} or return;
	
	# write positions
	while(my($n, $h) = each(%{$self->{'pos_pos'}}) ) {
		my $posblock = DBD::Trini::PosBlock->new($self, $h->{'pos'}, $h->{'width'});
		$posblock->{'val'} = $h->{'hash'}->{ $h->{'name'} };
		$posblock->write;
	}
}
# 
# unload
#--------------------------------------------------------------


#--------------------------------------------------------------
# rollback
# 
sub rollback ($) {
	my($dbh) = shift;
	
	if ($dbh->FETCH('Warn'))
		{warn("Rollback ineffective while AutoCommit is on", -1)}
	
    return 0;
}
# 
# rollback
#--------------------------------------------------------------


#--------------------------------------------------------------
# DESTROY
# 
DESTROY {
	my ($dbh) = @_;
	
	if ($dbh->{'in_creation'})
		{write_structure($dbh, reload=>0)}
	
	unload($dbh);
}
# 
# DESTROY
#--------------------------------------------------------------

# 
# DBD::Trini::db
###########################################################################



###########################################################################
# DBD::Trini::st
# 
# ====== STATEMENT ======
# 
package DBD::Trini::st;
use strict;
use Carp 'croak';
# use Debug::ShowStuff ':all';
use SQL::YASP ':all';
use vars qw[$imp_data_size $haveFileSpec];

$imp_data_size = 0;
$haveFileSpec = eval { require File::Spec };


# general
sub bind_param {
	my($sth, $position, $value) = @_;
	$sth->{'param'}->[$position-1] = $value;
	return 1;
}


#-----------------------------------------------------------
# execute
# 
sub execute {
	my($sth, @params) = @_;
	
	
	#-----------------------------------------------------------------------
	# general
	# 
	my ($dir, $param);
	
	# bind params to placeholders
	foreach my $idx (1..@params)
		{$sth->bind_param($idx, $params[$idx-1]) or return}
	
	# initialize param array ref
	$param = $sth->{'param'} || [];
	
	# reset the object
	$sth->finish;
	
	# 
	# general
	#-----------------------------------------------------------------------
	
	
	#-----------------------------------------------------------------------
	# driver specific
	# 
	my $stmt = $sth->{'stmt'};
	my $dbh = $stmt->{'dbh'};
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# CREATE
	# 
	if ($stmt->{'command'} eq 'create') {
		# create a table
		if ($stmt->{'create_type'} eq 'table') {
			my $table = $dbh->func($stmt->{'table_name'}, 'add_table');
			
			# add columns
			while ( my($fieldname,$v) = each(%{$stmt->{'fields'}}) ) {
				my $data = $v->{'data_type'};
				my $modifiers = $v->{'modifiers'};
				my (%def);
				
				# not null
				#if (exists $modifiers->{'not null'})
				#	{$def{'nullable'} = 0}
				#elsif (exists $modifiers->{'undef'})
				#	{$def{'undeffable'} = 1}
				
				# unique
				#if (exists $modifiers->{'unique'})
				#	{$def{'unique'} = 1}
				
				# check
				#if (exists $modifiers->{'check'})
				#	{$def{'check'} = $v->{'arguments'}->{'check'}->{'arguments'}}
				
				# store the data type and the field width
				$def{'data_type'} = $data->{'name'};
				$def{'width'} = $DBD::Trini::datatypes{$def{'data_type'}}->get_db_field_width($data, $modifiers);
				
				$table->add_col($fieldname, %def);
			}
		}
		
		# else don't know how to create this type of object
		else {
			croak qq|Do not know how to create this type of object: "$stmt->{'create_type'}"|;
		}
		
		$sth->{'NUM_OF_ROWS'} = 0;
	}
	# 
	# CREATE
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# INSERT
	# 
	elsif ($stmt->{'command'} eq 'insert') {
		my $table = $dbh->func($stmt->{'table_name'}, 'get_table');
		my $record = $table->get_record();
		my $col_defs = $table->{'db'}->{'table_defs'}->{ $stmt->{'table_name'} }->{'col_defs'};
		my (@errs);
		
		# make sure we got as many params as placeholders
		unless ($stmt->{'placeholder_count'} == @params) {
			$sth->set_err(1, 'Different number of placeholders and bind parameters');
			return 0;
		}
		
		while (my($fn, $expr) = each(%{$stmt->{'set'}})) {
			my $field = $DBD::Trini::datatypes{ $col_defs->{$fn}->{'data_type'} }->new($stmt->{'dbh'});
			$field->set_from_ui( $expr->evalexpr(params=>\@params) );
			$record->{'f'}->{$fn} = $field;
		}

		@errs = $record->save();
		@errs and return sth_err($sth, @errs);
		
		# if succesful, set NUM_OF_ROWS to 1
		$sth->{'NUM_OF_ROWS'} = 1;
	}
	# 
	# INSERT
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# SELECT
	# 
	elsif ($stmt->{'command'} eq 'select') {
		# KLUDGE: for now we assume that the select is from only one table.
		# When we implement the ability to have multi-table selects,
		# this section will need to be radically overhauled.
		
		my $tablename = $stmt->{'from'}->{  (keys(%{$stmt->{'from'}}))[0]  };
		my $table = $dbh->func($tablename, 'get_table');
		$sth->{'recordset'} = $table->get_recordset('where'=>$stmt->{'where'}, params=>\@params);
		
		$sth->{'NUM_OF_FIELDS'} = keys %{$stmt->{'fields'}};
		$sth->{'NUM_OF_ROWS'} = 0;
	}
	# 
	# SELECT
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# UPDATE
	# 
	elsif ($stmt->{'command'} eq 'update') {
		my $tablename = $stmt->{'table_name'};
		my $table = $dbh->func($tablename, 'get_table');
		my $recordset = $table->get_recordset('where'=>$stmt->{'where'}, params=>\@params);
		my $record_count = 0;
		my (@errs, %changed);
		
		@changed{keys %{$stmt->{'set'}}} = ();
		
		while (my $record = $recordset->next) {
			# update the record
			while (my($fn, $expr) = each(%{$stmt->{'set'}}))
				{ $record->{'f'}->{$fn}->set_from_ui($expr->evalexpr(db_record=>$record, params=>\@params)) }
			
			@errs = $record->save( changed=>\%changed );
			@errs and return sth_err($sth, @errs);
			$record_count++;
		}
		
		$sth->{'NUM_OF_ROWS'} = $record_count;
	}
	# 
	# UPDATE
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# DELETE
	# 
	# LEFTOFF: need to implement deletion of memo fields when record is deleted
	# 
	elsif ($stmt->{'command'} eq 'delete') {
		my $tablename = $stmt->{'from'}->{  (keys(%{$stmt->{'from'}}))[0]  };
		my $table = $dbh->func($tablename, 'get_table');
		my $recordset = $table->get_recordset('where'=>$stmt->{'where'}, params=>\@params);
		my $record_count = 0;
		
		RECORDLOOP:
		while (my $record = $recordset->next) {
			$record->delete();
			$record_count++;
		}
		
		$sth->{'NUM_OF_ROWS'} = $record_count;
	}
	# 
	# DELETE
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# unknown
	# 
	else {
		croak qq|Do not know this command: "$stmt->{'command'}"|;
	}
	# 
	# unknown
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - -


	# 
	# driver specific
	#-----------------------------------------------------------------------
	

	# general
	$sth->STORE(Active => 1);
	return 1;
}
# 
# execute
#-----------------------------------------------------------


#---------------------------------------------------------------
# sth_err
# 
sub sth_err {
	my ($sth, @errs) = @_;
	$sth->set_err(1, join("\n", @errs));
	return undef;
}
# 
# sth_err
#---------------------------------------------------------------


#-----------------------------------------------------------
# fetch (driver specific but required)
# 
sub fetch {
	my $sth = shift;
	my $dh  = $sth->{'datahandle'};
	my $dir = $sth->{'dir'};
	my $stmt = $sth->{'stmt'};
	my (%fields);
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# driver specific
	# 
	my ($record);
	
	RECORDLOOP:
	while ($record = $sth->{'recordset'}->next) {
		# if there's a where clause, evaluate it, next loop if it doesn't match
		if ( $stmt->{'where'} ) {

			# objects in {'f'} need to be overloaded so that when tey
			# are called in string or numeric context, they return the
			# results of the get_ui_value function
			
			if ($stmt->{'where'}->evalexpr(db_record=>$record->{'f'}, params=>$sth->{'param'}) )
				{last RECORDLOOP}
		}
		
		# else exit loop
		else
			{last RECORDLOOP}
	}
	
	# if no records
	if (! $record) {
		delete $sth->{'recordset'};
		return undef;
	}
	
	# see above
	
	# evaluate expressions in field list
	while (my($fn, $expr) = each(%{$stmt->{'fields'}}))
		{$fields{$fn} = $expr->evalexpr(db_record=>$record->{'f'}, params=>$sth->{'param'})}
	
	# 
	# driver specific
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# return evaluation of field objects
	# 
	my $rv = $sth->_set_fbav([@fields{ @{$sth->{'fieldnames'}} }]);
	
	foreach my $el (@$rv)
		{ ref($el) and $el = $el->get_ui_value() }
	
	return $rv;
	# 
	# return evaluation of field objects
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
}
# 
# fetch (driver specific but required)
#-----------------------------------------------------------


# ???: not sure if the following code is really needed:
# commenting it out makes no difference, not even for 
# fetchrow_arrayref
# alias fetchrow_arrayref to fetch
# *fetchrow_arrayref = \&fetch;


#-----------------------------------------------------------
# finish
# 
sub finish {
	my $sth = shift;
	
	# driver specific
	$sth->{'datahandle'} and closedir($sth->{'datahandle'});
	delete $sth->{'datahandle'};
	delete $sth->{'dir'};
	
	# call superclass finisher
	$sth->SUPER::finish();
	
	return 1;
}
# 
# finish
#-----------------------------------------------------------


#-----------------------------------------------------------
# STORE (general)
# 
sub STORE {
	my ($sth, $key, $value) = @_;
	
	if (exists $DBD::Trini::dbd_keys{$key}) {
		#- - - - - - - - - - - - - - -
		# driver specific
		#
		return $sth->{$key} = $value;
		#
		# driver specific
		#- - - - - - - - - - - - - - -
	}
	
	return $sth->SUPER::STORE($key, $value);
}
# 
# STORE (general)
#-----------------------------------------------------------


#-----------------------------------------------------------
# FETCH
# 
sub FETCH {
	my ($sth, $key) = @_;
	
	if (exists $DBD::Trini::dbd_keys{$key}) {
		#- - - - - - - - - - - - - - -
		# driver specific
		#
		return $sth->{$key};
		#
		# driver specific
		#- - - - - - - - - - - - - - -
	}
	
	
	# 'NAME'
	elsif ($key eq 'NAME') {
		return [keys %{$sth->{'stmt'}->{'fields'}}];
	}
	
	
	elsif ($key eq 'ParamValues') {
		my $param = $sth->{'param'} || [];
		my %pv = map { $_ => $param->[$_-1] } 1..@$param;
		return \%pv;
	}
	
	# else pass up to DBI to handle
	return $sth->SUPER::FETCH($key);
}
# 
# FETCH
#-----------------------------------------------------------


#-----------------------------------------------------------
# DESTROY (general)
# 
DESTROY {
	my ($self) = @_;
	$self->finish;
	undef;
}
# 
# DESTROY (general)
#-----------------------------------------------------------


# 
# DBD::Trini::st
###########################################################################


####################################################################################
# DBD::Trini::Table
# 
package DBD::Trini::Table;
use strict;
use Carp 'croak';
use vars '@ISA';
# use Debug::ShowStuff ':all';

#--------------------------------------------------------------
# new
#

# =head2 DBD::Trini::Table->new($db, $tablename, %opts)
# 
# 
# Returns a new table object.  If the database is in creation mode then if the
# table already exists it will be returned, and if it does not exist it will be
# created.  If the database is not in creation mode then the table must exist.
# 
# =cut

sub new {
	my ($class, $db, $tablename, %opts) = @_;
	my ($table_def);
	my $self = bless({}, $class);
	
	$self->{'db'} = $db;
	$self->{'name'} = $tablename;
	
	# if existing table
	# for now, do nothing if existing table
	if ($table_def = $db->{'table_defs'}->{$tablename})
		{}
	
	# else if we're creating the table
	elsif($opts{'create'})
		{croak "do not have a table named $tablename"}
	
	return $self;
}
#
# new
#--------------------------------------------------------------


#--------------------------------------------------------------
# row_count
# 
# returns the number of rows in the table
# 
sub row_count {
	my ($self) = @_;
	my $recordset = $self->get_recordset;
	my $count = 0;
	
	# loop through records
	while ($recordset->next)
		{$count++}
	
	return $count;
}
# 
# row_count
#--------------------------------------------------------------


#--------------------------------------------------------------
# add_col
#

# =head2 $table->add_col($colname, %opts)
# 
# Adds a column to the table.  Only use in database creation mode.
# 
# =cut

sub add_col {
	my ($self, $colname, %opts) = @_;
	
	# lowercase, ensure valid name
	$colname = DBD::Trini::db::valid_object_name($colname);
	
	# default length to 8 (yes, I picked that length randomly)
	defined($opts{'width'}) or $opts{'width'} = 8;
	
	$self->{'db'}->{'table_defs'}->{$self->{'name'}}->{'col_defs'}->{$colname} = {%opts};
}
#
# add_col
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_record
# 
sub get_record {
	my ($self, $pos, %opts) = @_;
	return DBD::Trini::Record->new($self, $pos, %opts);
}
# 
# get_record
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_recordset
# 
sub get_recordset {
	my ($self, %opts) = @_;
	return DBD::Trini::Recordset->new($self, %opts);
}
# 
# get_recordset
#--------------------------------------------------------------



# 
# DBD::Trini::Table
###############################################################################


###############################################################################
# Record
# 
# Represents a database record
# 
package DBD::Trini::Record;
use strict;
use Carp 'croak';
# use Debug::ShowStuff ':all';


#------------------------------------------------------------------------------
# new
#
sub new {
	my ($class, $table, $pos, %opts) = @_;
	my $self = bless({}, $class);
	my ($db);
	
	$db = $table->{'db'};
	$self->{'table'} = $table;
	$self->{'f'} = SQL::YASP::get_ixhash();
	
	#----------------------------------------------------
	# if a position was sent, retrieve
	# 
	if ($pos) {
		my $fh = $table->{'db'}->{'fh'};
		my ($raw, $tdef, $cdef, @fields, $eofiregex);
		
		$eofiregex = quotemeta(DBD::Trini::EOFI);
		
		# error checking
		unless ($pos =~ m|^\d+$|s)
			{die 'non-numeric seek'}
		
		$fh->seek($pos, 0);
		
		$tdef = $table->{'db'}->{'table_defs'}->{ $table->{'name'} };
		$fh->read($raw, $tdef->{'total_width'});
		
		# parse fields
		@fields = $raw =~ m/$tdef->{'split_regex'}/s
			or die "cannot split: $raw";
		grep {s/$eofiregex.*//} @fields;
		
		# if active (i.e. not deleted)
		$self->{'active'} = shift @fields;
		
		# previous and next positions
		$self->{'prev_pos'} = crunch(shift @fields);
		$self->{'next_pos'} = crunch(shift @fields);
		
		# loop through field definitions
		while ( my($n, $def) = each( %{$tdef->{'col_defs'}} ) ) {
			my $len = crunch(shift @fields);
			my $val = shift @fields;
			my $class = $DBD::Trini::datatypes{ $def->{'data_type'} };
			
			$class or croak("do not recognize data type: $def->{'data_type'}");
			
			if ($len =~ m|^\-|s)
				{undef $val}
			else
				{$val = substr($val, 0, $len)}
			
			# create new field object
			$self->{'f'}->{$n} = $class->new($db, $val);
		}
		
		$self->{'pos'} = $pos;
	}
	#
	# if a position was sent, retrieve
	#----------------------------------------------------
	
	
	# else just note that this is an active record
	else {
		$self->{'active'} = 1;
	}
	
	return $self;
}
#
# new
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# crunch
# 
sub crunch {
	my ($rv) = @_;
	$rv =~ s|^\s+||s;
	$rv =~ s|\s+$||s;
	$rv =~ s|\s+| |s;
	return $rv;
}
# 
# crunch
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# save
# 
sub save {
	my ($self, %opts) = @_;
	my (@out, @outfinal, $formerlast_pos, $formerlast, @errs, %keys);
	
	# convenience variables
	my $table = $self->{'table'};
	my $tablename = $table->{'name'};
	my $db = $self->{'table'}->{'db'};
	my $fh = $db->{'fh'};
	my $f = $self->{'f'};
	my $new = ! $self->{'pos'};
	my $table_positions = $db->{'pos'}->{'tables'}->{ $tablename };
	my $changed = $opts{'changed'};
	
	@keys{keys %$f} = ();
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# calculate string for data fields
	# 
	while( my($n, $def) = each(%{ $db->{'table_defs'}->{ $tablename }->{'col_defs'} })  ) {
		my ($val, $len);
		
		if (exists $keys{$n})
			{$val = $f->{$n}->get_db_value( unchanged=>( $changed && ! exists($changed->{$n}) ) ) }
		
		if (defined $val)
			{$len = length($val)}
		else
			{$len = '-'}
		
		$len .= (' ' x ( length($def->{'width'}) - length($len)  )  );
		
		push @out, $len, [$val, $def->{'width'}];
	}
	# 
	# calculate string for data fields
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	# stuff to do if this is a new record
	# 
	if ($new) {
		# if there is deleted position we can use
		if ($table_positions->{'first_deleted'}) {
			my $del = $table->get_record($table_positions->{'first_deleted'});
			
			$self->{'pos'} = $table_positions->{'first_deleted'};
			$table_positions->{'first_deleted'} = $del->{'next_pos'};
		}
		
		# else go to end of file, save that position
		else {
			$fh->seek(0,2);
			
			# write table ID
			print $fh $db->{'table_defs'}->{ $tablename }->{'internal_id'};
			
			$self->{'pos'} = $fh->tell;
		}
		
		# set last record pointer to this record's position
		$formerlast_pos = $table_positions->{'last_record'};
		$table_positions->{'last_record'} = $self->{'pos'};
		
		# link to previous record
		if ($formerlast_pos) {
			$formerlast = $self->{'table'}->get_record( $formerlast_pos );
			$formerlast->{'next_pos'} = $self->{'pos'};
			$self->{'prev_pos'} = $formerlast->{'pos'};
			$formerlast && $formerlast->save_pointers;
		}
	}
	# 
	# stuff to do if this is a new record
	#- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	
	
	# set this as the first record if there isn't already a first record
	if ($self->{'active'})
		{ $db->{'pos'}->{'tables'}->{ $tablename }->{'first_record'} ||= $self->{'pos'} }
	
	# save link to next record
	unshift @out, [$self->{'next_pos'}, DBD::Trini::POSWIDTH];
	
	# save link to previous record
	unshift @out, [$self->{'prev_pos'}, DBD::Trini::POSWIDTH];
	
	# fix field widths
	fix_field_widths(@out);
	
	# write record string
	$fh->seek($self->{'pos'}, 0);
	
	print $fh 
		($self->{'active'} ? '1' : '0'),
		@out,
		($new ? DBD::Trini::EOL : '');
	
	return @errs;
}
# 
# save
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# save_pointers
# 
sub save_pointers {
	my ($self, %opts) = @_;
	my $fh = $self->{'table'}->{'db'}->{'fh'};
	my (@out, $writeactive);
	
	$writeactive = $opts{'write_active'};
	$self->{'active'} = $self->{'active'} ? '1' : '0';
	
	# set active field
	if ($writeactive)
		{push @out, $self->{'active'}}
	
	# pointer to previous record
	push @out, [$self->{'prev_pos'}, DBD::Trini::POSWIDTH];
	
	# pointer link to next record
	push @out, [$self->{'next_pos'}, DBD::Trini::POSWIDTH];
	
	# fix field widths
	fix_field_widths(@out);
	
	# write record string
	if ($writeactive)
		{$fh->seek($self->{'pos'}, 0)}
	else
		{$fh->seek($self->{'pos'} + 1, 0)}
	
	print $fh @out;
}
# 
# save_pointers
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# fix_field_widths
# 
sub fix_field_widths {
	foreach my $el (@_) {
		if (ref $el) {
			my ($val, $width) = @{$el};
			defined($val) or $val = '';
			
			if (length($val) < $width)
				{$el = $val . (' ' x ($width - length($val))) }
			
			# the next test should never evaluate to true, but hey,
			# things don't always go as planned.
			elsif (length($val) > $width)
				{die "value ($val) exceeds maximum field width ($width): $val"}
			
			else
				{$el = $val}
		}
	}
}
# 
# fix field widths
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# delete
# 
sub delete {
	my ($self) = @_;
	my (%keys);
	my $table = $self->{'table'};
	my $tablename = $table->{'name'};
	my $db = $table->{'db'};
	my $table_positions = $db->{'pos'}->{'tables'}->{ $tablename };
	my $f = $self->{'f'};
	@keys{keys %$f} = ();	
	
	# run after_record_delete for each field
	while( my($n, $def) = each(%{ $db->{'table_defs'}->{ $tablename }->{'col_defs'} })  ) {
		if ( exists($keys{$n}) && $f->{$n}->can('after_record_delete') )
			{ $f->{$n}->after_record_delete() }
	}
	
	# set next record's pointer to this record to the
	# position of this record's previous record
	if ($self->{'next_pos'}) {
		my $next = $table->get_record( $self->{'next_pos'} );
		$next->{'prev_pos'} = $self->{'prev_pos'};
		$next->save_pointers;
	}
	
	# set previous record pointer to this record to the
	# position of this record's next record
	if ($self->{'prev_pos'}) {
		my $prev = $table->get_record( $self->{'prev_pos'} );
		$prev->{'next_pos'} = $self->{'next_pos'};
		$prev->save_pointers;
	}
	
	# if this is the last record, set pointer to last
	# record to the position of this record's previous
	# record
	if ( $table_positions->{'last_record'} == $self->{'pos'} )
		{ $table_positions->{'last_record'} = $self->{'prev_pos'} }
	
	# if this is the first record, set pointer to
	# the first record to the position of this record's
	# next record
	if ( $table_positions->{'first_record'} == $self->{'pos'} )
		{ $table_positions->{'first_record'} = $self->{'next_pos'} }
	
	# add this record to linked list of deletions
	$self->{'next_pos'} = $table_positions->{'first_deleted'};
	$self->{'prev_pos'} = '';
	$table_positions->{'first_deleted'} = $self->{'pos'};
	
	# set active indicator to 0
	$self->{'active'} = 0;
	
	# save the record
	$self->save_pointers(write_active=>1);
}
# 
# delete
#------------------------------------------------------------------------------


# 
# Record
###############################################################################


###############################################################
# PosBlock
# 
# Represents a single value stored in a fixed location in the file.
# undefs are stored as empty strings.
# 
package DBD::Trini::PosBlock;
use strict;
use Carp 'croak';
# use Debug::ShowStuff ':all';

#--------------------------------------------------------------
# new
# 
sub new {
	my ($class, $db, $pos, $width) = @_;
	my $self = bless({}, $class);
	
	$self->{'db'} = $db;
	$self->{'pos'} = $pos;
	$self->{'width'} = $width;
	
	return $self;
}
# 
# new
#--------------------------------------------------------------


#--------------------------------------------------------------
# write
# 
sub write {
	my ($self) = @_;
	my $fh = $self->{'db'}->{'fh'}
		or croak 'Cannot write to database that does not have an open filehandle';
	my $writeval = $self->{'val'};
	defined($writeval) or $writeval = '';
	
	if (length($writeval) < $self->{'width'})
		{$writeval .= DBD::Trini::EOFI}
	elsif (length($writeval) > $self->{'width'})
		{croak 'data length too long for data block'}
	
	$fh->seek($self->{'pos'}, 0);
	
	print $fh $writeval;
}
# 
# write
#--------------------------------------------------------------


# 
# PosBlock
###############################################################


###############################################################
# Recordset
# 
# 
package DBD::Trini::Recordset;
use strict;
use Carp 'croak';
# use Debug::ShowStuff ':all';


# TODO: implement order clause in recordset object


#--------------------------------------------------------------
# new
# 
sub new {
	my($class, $table, %opts) = @_;
	my $self = bless({}, $class);
	
	# default options
	defined($opts{'forward'}) or $opts{'forward'} = 1;
	
	# store properties
	$self->{'table'} = $table;
	$self->{'where'} = $opts{'where'};
	$self->{'params'} = $opts{'params'};
	
	# get first position for looping
	if ($self->{'forward'} = $opts{'forward'})
		{$self->{'next_pos'} = $table->{'db'}->{'pos'}->{'tables'}->{ $table->{'name'} }->{'first_record'}}
	else
		{$self->{'next_pos'} = $table->{'db'}->{'pos'}->{'tables'}->{ $table->{'name'} }->{'last_record'}}
	
	return $self;
}
# 
# new
#--------------------------------------------------------------


#--------------------------------------------------------------
# next
# 
sub next {
	my ($self) = @_;
	my ($record);
	
	# if there is a next position
	$self->{'next_pos'} or return(undef);
	
	RECORDLOOP:
	while (
		$self->{'next_pos'} &&
		($record = $self->{'table'}->get_record( $self->{'next_pos'} ))
		) {
		$self->{'next_pos'} = $self->{'forward'} ? $record->{'next_pos'} : $record->{'prev_pos'};
		
		# see object overloading comment
		
		if (
			(! $self->{'where'}) ||
			($self->{'where'}->evalexpr(db_record=>$record->{'f'}, params=>$self->{'params'}))
			)
			{last RECORDLOOP}
	}
	
	if (! $record)
		{delete $self->{'next_pos'}}
	
	return $record;
}
# 
# next
#--------------------------------------------------------------


# 
# Recordset
###############################################################


###############################################################
# DBD::Trini::datatypes::VarChar
# 
package DBD::Trini::datatypes::VarChar;
use strict;
use Carp 'croak';
# use Debug::ShowStuff ':all';

# register field type
$DBD::Trini::datatypes{'varchar'} = 'DBD::Trini::datatypes::VarChar';

# object overloading
use overload '""'     => \&get_ui_value, fallback => 1;


#--------------------------------------------------------------
# new
# 
sub new {
	my ($class, $db, $dbval) = @_;
	my $self = bless({}, $class);
	
	$self->{'db'} = $db;
	$self->{'dbval'} = $dbval;
	
	return $self;
}
# 
# new
#--------------------------------------------------------------


#--------------------------------------------------------------
# set_from_db
# 
# Set the object's value using a value from the database
# 
sub set_from_db {
	$_[0]->{'dbval'} = $_[1];
}
# 
# set_from_db
#--------------------------------------------------------------


#--------------------------------------------------------------
# set_from_ui
# 
# Set the object's value using a value from the user interface
# 
sub set_from_ui {
	$_[0]->{'dbval'} = $_[1];
}
# 
# set_from_ui
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_ui_value
# 
# Returns the user interface value
# 
sub get_ui_value {
	return $_[0]->{'dbval'};
}
# 
# get_ui_value
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_db_value
# 
# Returns the value that is stored in the database record.
# May also have the side effect of storing data elsewhere in the database.
# 
sub get_db_value {
	return $_[0]->{'dbval'};
}
# 
# get_db_value
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_db_field_width
# 
# Returns the width the database engine should allocate for
# a field of this type.
# 
sub get_db_field_width {
	my ($class, $data_declaration, $modifiers) = @_;
	return $data_declaration->{'arguments'}->[0];
}
# 
# get_db_field_width
#--------------------------------------------------------------


# 
# DBD::Trini::datatypes::VarChar
###############################################################


###############################################################
# DBD::Trini::datatypes::Memo
# 
package DBD::Trini::datatypes::Memo;
use strict;
use Carp 'croak';
# use Debug::ShowStuff ':all';
use vars '@ISA';
@ISA = 'DBD::Trini::datatypes::VarChar';
use constant VCWIDTH => 128;

# object overloading
use overload '""' => \&get_ui_value, fallback => 1;

# register field type
$DBD::Trini::datatypes{'memo'} = 'DBD::Trini::datatypes::Memo';



#--------------------------------------------------------------
# set_from_ui
# 
# Set the object's value using a value from the user interface
# 
sub set_from_ui {
	$_[0]->{'fullval'} = $_[1];
}
# 
# set_from_ui
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_ui_value
# 
# Returns the user interface value
# 
sub get_ui_value {
	my ($self) = @_;
	my ($memotable, $pos, @rv);
	
	# if we already have the ui value
	exists($self->{'fullval'}) and return $self->{'fullval'};
	
	# if there is no database value
	defined($self->{'dbval'}) or return undef;
	
	$pos = $self->{'dbval'};
	
	# get the table
	$memotable = DBD::Trini::db::get_table($self->{'db'}, '_memo_full');
	
	while ($pos) {
		my $record = $memotable->get_record($pos);
		push @rv, $record->{'f'}->{'data'}->{'dbval'};
		$pos = $record->{'f'}->{'next'}->{'dbval'};
	}
	
	# cache full value
	$self->{'fullval'} = join('', @rv);
	
	return $self->{'fullval'};
}
# 
# get_ui_value
#--------------------------------------------------------------


#--------------------------------------------------------------
# get_db_value
# 
# Returns the value that is stored in the database record.
# May also have the side effect of storing data elsewhere in the database.
# 
sub get_db_value {
	my ($self, %opts) = @_;
	my ($memotable, $prevrec, $splitex, @chunks, $pos);
	
	# if there's been no change, just return the dbval
	$opts{'unchanged'} and return $self->{'dbval'};
	
	$memotable = DBD::Trini::db::get_table($self->{'db'}, '_memo_full');
	
	# clear out existing full-value records
	$self->clear_data($memotable);
	
	# early exit
	defined($self->{'fullval'}) or return undef;
	
	# divide into chunks
	$splitex = '.{' . VCWIDTH . '}';
	@chunks = reverse grep {length($_)} split(m|($splitex)|s, $self->{'fullval'});
	
	foreach my $chunk (@chunks) {
		my ($record, $prev, $data);
		$record = $memotable->get_record();
		
		$data = DBD::Trini::datatypes::VarChar->new($self->{'db'}, $chunk);
		$record->{'f'}->{'data'} = $data;
		
		if ($prevrec) {
			$prev = DBD::Trini::datatypes::VarChar->new($self->{'db'}, $prevrec->{'pos'});
			$record->{'f'}->{'next'} = $prev;
		}
		
		$record->save;
		$prevrec = $record;
	}
	
	return $prevrec->{'pos'};
}
# 
# get_db_value
#--------------------------------------------------------------


#------------------------------------------------------------------------------
# before_db_create
# 
sub before_db_create {
	my ($class, $dbh, $type) = @_;
	my ($table);
	
	$table = DBD::Trini::db::add_table(
		$dbh,
		'_memo_full',
		internal=>1,
	);
	
	# data column
	$table->add_col(
		'data',
		data_type => 'varchar',
		width     => VCWIDTH,
		);
	
	# next location column
	$table->add_col(
		'next',
		data_type => 'varchar',
		width     => DBD::Trini::POSWIDTH,
		);
}
# 
# before_db_create
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# clear_data
# 
# Notice that this subroutine DOES reset the $self->{'dbval'} property, even
# though it renders that property incorrect.  I've found that no routine that
# calls this sub actually relies on the old value of $self->{'dbval'} anymore,
# so it made no sense to waste CPU cycles resetting it.
# 
sub clear_data {
	my ($self, $memotable) = @_;
	my $pos = $self->{'dbval'};
	
	while ($pos) {
		my $record = $memotable->get_record($pos);
		$pos = $record->{'f'}->{'next'};
		$record->delete;
	}
}
# 
# clear_data
#------------------------------------------------------------------------------


#------------------------------------------------------------------------------
# after_record_delete
# 
# Called when the record is definitely going to be deleted.
# 
sub after_record_delete {
	my ($self) = @_;
	$self->clear_data( DBD::Trini::db::get_table($self->{'db'}, '_memo_full') );
}
# 
# after_record_delete
#------------------------------------------------------------------------------


# 
# DBD::Trini::datatypes::Memo
###############################################################


# return true
1;

__END__

=head1 NAME

DBD::Trini - Pure Perl DBMS


=head1 SYNOPSIS

 #!/usr/local/bin/perl -w
 use strict;
 use DBD::Trini;
 my ($path, $sql, $dbh, $sth, $i);
 
 # directory where database is stored
 $path = 'mydb'; 
 
 $sql = <<'(SQL)';
 create table members
    member_pk   VARCHAR(5),
    name_first  VARCHAR(25),
    name_last   VARCHAR(25),
    notes       MEMO
 (SQL)
 
 # create the database
 $dbh = DBI->connect("dbi:Trini:$path", '', '', {'create'=>1});
 $dbh->do($sql) or die $DBI::errstr;
 $dbh->commit();
 
 # insert
 $sql = qq[ insert into members(member_pk, name_first, name_last, notes) values(?,?,?,?) ];
 $sth = $dbh->prepare($sql) or die $DBI::errstr;
 $i=1;
 
 $sth->execute( $i++, 'Starflower', 'Shanti',   'Totally cool chick')       or die $DBI::errstr;
 $sth->execute( $i++, 'Paul',       'Ruggerio', 'Dangerous with a spatula') or die $DBI::errstr;
 $sth->execute( $i++, 'Mary',       'Edwin',    'Star Wars nut')            or die $DBI::errstr;
 $sth->execute( $i++, 'Ryan',       'Ragsdale', 'Loves his daughter')       or die $DBI::errstr;
 $sth->execute( $i++, 'Grady',      'Smith',    'Great sculptor')           or die $DBI::errstr;
 
 # updates
 $sql = qq[ update members set name_first=? where member_pk=2 ];
 $sth = $dbh->prepare($sql) or die $DBI::errstr;
 $sth->execute('Guido') or die $DBI::errstr;
 
 # select
 $sql = qq[ select name_last || ',' ||| name_first as name from members where member_pk=? ];
 $sth = $dbh->prepare($sql) or die $DBI::errstr;
 $sth->execute(2) or die $DBI::errstr;
 
 while (my $row = $sth->fetchrow_hashref)
    { print $row->{'name'}, "\n" }
 
 # delete
 $sql = 'delete from members where member_pk > ?';
 $sth = $dbh->prepare($sql);
 $sth->execute(3);

=head1 INSTALLATION

"Easy Installation" is one of Trini's central goals.  Just copy
Trini.pm into the DBD/ directory of one of your library trees.

Or you can do the traditional routine: 

    perl Makefile.PL
    make
    make test
    make install

You will also need to install the following modules which are also Pure Perl,
are just as easy to install, and are on CPAN:

	Data::Taxi
	SQL::YASP

Finally, you'll need to install the DBI module itself, which may be anywhere
from extremely easy (it's included in later distributions of Perl 5) to
painfully difficult, depending on your skill level.  Be sure to check out
the notes in DBI about the the Pure Perl version of DBI if you find it
difficult to install DBI.

=head1 DESCRIPTION

Trini (pronounced "TRINN-EE") is a Pure Perl DBMS.  Some highlights
of the eventual product:

- Commit/rollback segments

- Journaled data writing for automatic crash recovery

- All data stored in a single data file

- Open architecture for data type definitions (i.e., create your own types
of database fields)

- Enforcement of referential integrity

- Triggers

- Constraints

- Entirely written in the grooviest programming language there is


=head1 So, do the world really need another DBMS?

The creation of "yet another DBMS" requires some justification.  After all, 
there are already several excellent open source DBMS packages, notably MySQL
and PostGreSql.

And yet, despite the availability of those programs, Perl hackers around the
world continue to store data in Unix DB tables, flat files, and other
file-based data structures.  The difficulty in using those data storage
techniques seems less daunting than the difficulty of installing, starting,
and connecting to a true DBMS package.

The problem, I believe, is a simple question of paradigm confusion.
Programmers want to understand how and where their data is being stored.  They
want to have a file where they can see the data, and they want to be able to
install a simple package quickly and begin using it without spending a long
time reading through installation guides. 

Finally, for those of us who prefer Perl to any other language, there is a
need for a DBMS that lets us join in the fun of hacking the code.

Trini was created to fill this niche.  The entirety of Trini's code is
contained in a half dozen Pure Perl modules, all of which can be installed
either through the traditional make/make test/make install dance, or by simply
copying them into your @INC library.  The data is stored in a single data file.
Trini provides a rich (and extensible) set of SQL operators and commands,
compliments of SQL::YASP.  Trini provides commit/rollback segments, and
automatic crash recovery, both compliments of FileHandle::Rollback.  Trini
also provides an extensible field type API, so that if the built-in data types
(NUMBER, VARCHAR, MEMO, others) don't suit you, you can define your own.

Trini is still in its early stages.  I've defined a data structure.  It can do
basic database definition, inserts, deletes, updates and selects.  I invite
all interested parties to join in the fun.

=head1 Similar modules

Trini is hardly the first Pure Perl database manager.  To my knowledge none of
them provide the same set of features as listed above, though many provide
some of those features. Some similar modules are:

=over

=item DBD::Sprite

Sprite is a popular Pure Perl DBMS with many of the features listed above.
AFAIK, however, it does not support the following features, and given how
Sprite stores data, it is not likely to do so in the near future:

- Cannot store undefs, only stores nulls as empty strings.

- Does not have auomatic crash recovery

- Does not have extensible datatype API

- Does not support extensible SQL function and operator definitions

- Does not handle "memo" (i.e. strings of arbitrary length) fields

- Sprite does not bill itself as a production-worthy DBMS.  Trini will.

- Does not have indexing to speed up queries.

BTW, I always get yelled at when I attempt to list the differences in modules.
Jim, I apologize in advance if this list isn't quite right.  Drop me a line
and I'll fix it. :-)

Sprite does support user-ids and passwords, which is a nice feature.  Trini
doesn't support that feature, and for now I don't plan to do so.

=item DBD::SQLite

DBD::SQLite is a very powerful local-file DBMS. It accomplishes many of the
same objectives as Trini.  Is not Pure Perl.  Obviously a much more mature
package than Trini, you should definitely look at SQLite if you need to get
going on a local-file project immediately.

=back


=head1 A note about the state of Trini

This is an early release of Trini.  In the spirit of Eric Raymond's motto
"Release Early, Release Often" I am releasing Trini before it is a fully
working module, or indeed before it is properly documented.  This version does
some very basic databasing operations.  It allows you to create a database,
insert, update, select, and delete records.  It supports two data types:
varchar and memo. See demo.pl for a basic walk through of Trini's current
features.

A few things this module does NOT support are: modifying the structure of a
database, data integrity checks, file locking, rollback segments, and
automatic crash recovery. All of those features are planned.  Indeed, Trini
was designed with those features in mind.

See the TO DO section below for a more detailed list of planned features.

=head1 To Do

=head2 Rollback segments and automatic crash recovery

Rollback segments and automatic crash recovery will both be implemented using
File::Rollback, which encapsulates both of these features.

=head2 Indexing

The open architecture of Trini should support indexing well.  Here's how it
should work:

Each field is an instantiation of a class.  Right now there are only two 
datatype classes: varchar and memo.  One of the methods of a datatype class
is that when the record is modified, the field object can use its
C<get_db_value> method to modify other records.  That's what memo fields
do to store strings of arbitrary length.

So, we need to create an index class that contructs a binary tree based on the
value of some expression that uses the values in the record.  Then, when a
search is done that uses that expression, the search engine will understand
to trace through the binary tree rather than do a brute force search.

There are several tasks that need to be done, however.  First, every datatype
field needs to implement a compare_as method that tells the indexer if the
field value should be compared as a string or as a number.  Then, we need to
augment YASP so that every operator and function does the same thing, with the
addition that they might also return either.  Next, YASP needs to be able to
return information on any given expression about if that expression returns a
string or a number. Still with me?  OK, finally, the indexing class uses all
this string/number information compare values numerically or stringily.

So you see why I haven't gotten around to it just yet.

=head2 Modifying the structure of a database

Trini currently does not support modifying the structure of a database once
it's created.  As Homer would say, "doh".  The structure of a Trini file is 
such that once it's written, you need to rewrite the entire database file to
modify the structure.  I don't think that's such a big deal, but it does mean
writing an entire routine to do it.

Oh, and as long as we're rewriting the entire data file, we can go ahead and...

=head2 Compact the database file

The structure of Trini data files is such that the space of a deleted record
can only be reclaimed by another record of the same type.  Trini is quite good
at that... a new record never increases the size of the file as long as there
is space from a previously deleted record of the same table.  However, it can
still happen that a database becomes bloated.  There needs to be a C<compact>
method that rewrites the data file, removing empty space.

=head2 Data integrity checks

Trini currently stores the information about any data integrity checks, but
does not implement them.  When a record is saved, there needs to be a call
to each field to run its C<validate> method, returning an error if any of the
validations fail.  There also needs to be a check of any check constraints
that were defined for the table.

=head2 File locking

Trini needs to automatically lock the entire database for every call, using
either shared locks for read-only access or exclusive locks for write access.
Because each Trini database has its own directory, it should be relatively
simple to have a file called C<lock> that is opened and locked for each
access.

=head1 TERMS AND CONDITIONS

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
02111-1307  USA

=head1 AUTHOR

Miko O'Sullivan
F<miko@idocs.com>


=head1 VERSION

=over

=item Version 0.01    July 15, 2003

Initial release

=back


=cut
