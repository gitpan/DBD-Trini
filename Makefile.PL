use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'DBD::Trini',
    'VERSION_FROM'	=> 'Trini.pm', # finds $VERSION
    'PREREQ_PM'		=> {Data::Taxi => 0.01, SQL::YASP => 0.01, },
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM => 'Trini.pm', # retrieve abstract from module
       AUTHOR     => q[Miko O'Sullivan <miko@idocs.com>]) : ()),
);
