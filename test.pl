#!/usr/local/bin/perl -w
use strict;
use DBD::Trini;
my ($path, $sql, $dbh, $sth, $i);
use Test;

BEGIN { plan tests => 1 };


# directory where database is stored
$path = 'mydb'; 

# remove exisrting data file
if (-e "$path/data") {unlink "$path/data" or die $!}

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

$sth->execute( $i++, 'Starflower', 'Shanti',   'Totally cool chick' )        or die $DBI::errstr;
$sth->execute( $i++, 'Paul',       'Ruggerio', 'Dangerous with a spatula' )  or die $DBI::errstr;
$sth->execute( $i++, 'Mary',       'Edwin',    'Knows all about Star Wars' ) or die $DBI::errstr;
$sth->execute( $i++, 'Ryan',       'Ragsdale', 'Loves his daughter' )        or die $DBI::errstr;
$sth->execute( $i++, 'Grady',      'Smith',    'Great sculptor' )            or die $DBI::errstr;

# updates
$sql = qq[ update members set name_first=? where member_pk=2 ];
$sth = $dbh->prepare($sql) or die $DBI::errstr;
$sth->execute('Guido') or die $DBI::errstr;

# select
$sql = qq[ select name_last || ',' ||| name_first as name from members where member_pk=? ];
$sth = $dbh->prepare($sql) or die $DBI::errstr;
$sth->execute(2) or die $DBI::errstr;

$i=0;

while (my $row = $sth->fetchrow_hashref){
	print $row->{'name'}, "\n";
	$i++;
}

$i or die 'unable to retrieve stored records';

# delete
$sql = 'delete from members where member_pk > ?';
$sth = $dbh->prepare($sql);
$sth->execute(3);

ok(1);
