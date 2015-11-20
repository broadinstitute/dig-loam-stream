package util;

use warnings;
use strict;
use vars;

use bin::trap_sig;

use Getopt::Long;
Getopt::Long::Configure("pass_through", "no_auto_abbrev");

sub cat_dir_file($$)
{
		my $dir = shift;
		my $file = shift;
		return "$dir/$file";
}

sub get_file_dir($)
{
		my $file = shift;
		if ($file =~ /^(.+\/)[^\/]+$/)
		{
				return $1;
		}
		else
		{
				return undef;
		}
}

sub get_file_file($)
{
		my $file = shift;
		$file =~ s/.*\///g;
		return $file;
}

sub search_for_file($@)
{
		my $file = shift;
		my @dirs = @_;

		#if (-e $file)
		#{
		#		return $file;
		#}
		foreach my $dir (@dirs)
		{
				my $path = cat_dir_file($dir, $file);
				if (-e $path)
				{
						$file = $path;
						last;
				}
		}
		return $file;
}

sub get_args($)
{
		my $value = shift;

		my @to_return = ();

		#my @matches = $value =~ /(^|[^\\])@\{?[a-zA-Z0-9\_]+\}?/g;
		my @matches = $value =~ /.?@\{?[a-zA-Z0-9\_]+\}?/g;
		foreach my $captured (@matches)
		{
				if ($captured =~ /^\\/)
				{
						next;
				}
				elsif ($captured =~ /^[^\@]/)
				{
						$captured = substr($captured, 1);
				}

				if ($captured =~ /^{/ && $captured !~ /}$/)
				{
						$captured = substr($captured, 1);
				}
				elsif ($captured !~ /^{/ && $captured =~ /}$/)
				{
						$captured = substr($captured, 0, length($captured) - 1);
				}

				my $captured_key = $captured;
				$captured_key =~ s/@//g;
				$captured_key =~ s/\{//g;
				$captured_key =~ s/\}//g;
				push @to_return, $captured_key;
		}
		return @to_return;
}

my %subs_cache = ();
sub substitute_args($@)
{
		my $value = shift;
		my %args = @_;
		my $key = join(":", %args);
		unless (exists $subs_cache{$value}{$key})
		{
				my @required_args = sort {length($b) <=> length($a)} get_args($value);
				foreach my $arg (@required_args)
				{
						if (!exists $args{$arg})
						{
								die "Error: value $value requires argument \@$arg\n" unless defined $args{$arg};
						}
						$value = util::substitute_arg($value, $arg, $args{$arg});

				}
				$value = util::unescape_args($value);
				$subs_cache{$value}{$key} = $value;
		}
		return $subs_cache{$value}{$key};
}

my %sub_cache = ();
sub substitute_arg($$$)
{
		my $value = shift;
		my $arg_name = shift;
		my $arg_value = shift;

		if (ref($arg_value) && lc(ref($arg_value)) eq "array")
		{
				$arg_value = join(" ", @{$arg_value});
		}
		if ($sub_cache{$value}{$arg_name})
		{
				my $cache_array = $sub_cache{$value}{$arg_name};
				return "$cache_array->[0]$arg_value$cache_array->[1]";
		}
		else
		{
				if ($value =~ /(.*[^\\]|^)\@(\{$arg_name\}|$arg_name)(.*)/)
				{
						$sub_cache{$value}{$arg_name} = [$1, $3];
						return "$1$arg_value$3";
				}
				else
				{
						$value =~ s/([^\\]|^)\@(\{$arg_name\}|$arg_name)/$1$arg_value/g;
						return $value;
				}
		}
}
sub unescape_args($)
{
		my $value = shift;
		$value =~ s/\\@/\@/g;
		return $value;
}

sub get_col_mappings($@)
{
		my $header_line = shift;
		my $delim = shift || "\t";
		#split it
		chomp($header_line);
		my @cols = split(/$delim/, $header_line);
		my $map = {};
		for (my $i = 0; $i <= $#cols; $i++)
		{
				$cols[$i] =~ s/^\s+//g;
				$cols[$i] =~ s/\s+$//g;

				$map->{$cols[$i]} = $i;
		}
		return $map;
}

sub map_col($$)
{
		my $head = shift;
		my $col_mappings = shift;
		my $col = $head;
		if (defined $col_mappings)
		{
				$head =~ s/^\s+//g;
				$head =~ s/\s+$//g;
				$col = $col_mappings->{$head};
				if (!defined $col)
				{
						die "No header $head\n";
				}
		}
		if ($col !~ /[0-9]+/)
		{
				die "Column $col is not numeric\n";
		}
		return $col;
}

sub fetch_col($$@)
{
		my $head = shift;
		my $col_mappings = shift;
		my @cols = @_;
		
		my $col = map_col($head, $col_mappings);
		if ($col > $#cols)
		{
				die "Line did not have enough columns to fetch $head\n";
		}
		return $cols[$col];
}

1;

