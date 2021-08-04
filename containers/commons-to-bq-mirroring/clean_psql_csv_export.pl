use strict;
use warnings;

use open ':std', ':encoding(UTF-8)';
use Text::CSV_XS;
use Getopt::Long;

# CSV parser initialization
my $csv = Text::CSV_XS->new ({ binary => 1, auto_diag => 1 });

while (my $row = $csv->getline (*STDIN)) {
    # remove embedded newlines for row
    s/(\r\n|\r|\n)/   /g for @$row;

    # write out row to output file
    $csv->combine (@$row);
    my $line = $csv->string ();
    print "$line\n";
}
