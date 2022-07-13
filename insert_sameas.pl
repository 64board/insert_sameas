#!/usr/bin/perl

use strict;
use warnings;

use feature 'say';

use DBI();
use Config::Tiny;
use Getopt::Std;
use File::Basename;

sub print_usage {
    my $script_name = basename($0);
    say "Usage: $script_name -f <DB config filename.";
    say "       $script_name -h Show this usage help.";
}

sub warn_handler {

    my ($signal) = @_;

    chomp $signal;

    say $signal;
    say $DBI::err;
}

sub get_sameas_instruments {

    my $DBH = shift;

    my $query = "SELECT symbol, contract FROM prices " .
        "WHERE symbol COLLATE latin1_general_ci IN " .
        "(SELECT sameas FROM pltracker.instruments WHERE TRIM(sameas) <> '');";

    my $query_handle = $DBH->prepare($query);

    $query_handle->execute();

    if ($DBI::err) {
        say $DBI::errstr;
    }

    my $symbol;
    my $contract;
    my %sameas_instruments = ();

    $query_handle->bind_columns(\$symbol, \$contract);

    while ($query_handle->fetch()) {
        $sameas_instruments{"$symbol|$contract"} = 1;
    }

    $query_handle->finish();

    return %sameas_instruments;
}

sub convert_contract {

    my $contract = shift;
    my $result = $contract;

    if (length($contract) > 6) {
        $result = substr($contract,6, 1) . substr($contract, 9, 2);
    }

    return $result;
}

### Main program ###

my %options = ();

getopts("f:h", \%options);

if (defined($options{'h'})) {
    print_usage();
    exit 1;
}

if (!defined($options{'f'})) {
    print_usage();
    exit 1;
}

my $config_filename = $options{'f'};

my $config = Config::Tiny->read($config_filename);

if (!defined($config)) {
    die "Error reading configuration file: $config_filename";
}

my $DB_HOST = $config->{Database}->{db_host};
my $DB_NAME = $config->{Database}->{db_name};
my $DB_USER = $config->{Database}->{db_user};
my $DB_PASSWD = $config->{Database}->{db_passwd};

$SIG{__WARN__} = 'Warn_Handler';

my $DBH = DBI->connect("DBI:mysql:database=${DB_NAME};host=${DB_HOST}",
    ${DB_USER}, ${DB_PASSWD}, {'RaiseError' => 1, 'PrintError' => 1});

die "Error conecting to MySQL database: $DBI::errstr\n" unless $DBH;

$DBH->{'RaiseError'} = 0;
$DBH->{'PrintError'} = 0;

my %sameas_instruments = get_sameas_instruments($DBH);

my %not_found = ();

my $query = "SELECT DISTINCT f.symbol, contract, sameas FROM pltracker.traders_fills f " .
    "INNER JOIN instruments i ON f.symbol = i.symbol WHERE f.symbol IN " .
    "(SELECT symbol FROM pltracker.instruments WHERE TRIM(sameas) <> '') " .
    "ORDER BY symbol;";

my $query_handle = $DBH->prepare($query);

$query_handle->execute();

if ($DBI::err) {
    say $DBI::errstr;
}

my $symbol;
my $contract;
my $sameas;

$query_handle->bind_columns(\$symbol, \$contract, \$sameas);

my $not_found = 0;

while($query_handle->fetch()) {

    my $cc = convert_contract($contract);

    if (!defined($sameas_instruments{"$sameas|$cc"})) {
        $not_found{"$symbol|$cc"} = $sameas;
        $not_found++;
    }
}

$query_handle->finish();

if ($not_found > 0) {

    say "PROBLEMS: Some symbols were not found!";

    for my $k (sort keys %not_found) {
    
        say "$k -> " . $not_found{$k};
        
        my $symbol = $not_found{$k};
        my $contract = (split /\|/, $k)[1];

        my $sql = "INSERT INTO pltracker.prices(symbol, contract) VALUES ('$symbol', '$contract');";
        say $sql;
        $DBH->do($sql);
    }

} else {
    say "EVERYTHING OK!";
}

$DBH->disconnect();

__END__

