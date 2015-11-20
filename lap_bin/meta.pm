package meta;

use warnings;
use strict;
use vars;

use bin::trap_sig;

use Cache::SizeAwareMemoryCache;

use Time::HiRes qw(gettimeofday);
require bin::config;
require bin::util;

use Getopt::Long;
Getopt::Long::Configure("pass_through", "no_auto_abbrev");
use Storable qw(freeze thaw);

use Devel::StackTrace;

my $meta_file = undef;
my @arg_only_res = ();
my @arg_only_meta_res = ();
my @ignore_prop = ();
my $init = undef;

GetOptions("meta=s" => \$meta_file,
		       "only=s" => \@arg_only_res,
		       "ignore-prop=s" => \@ignore_prop,
		       "only-with-meta=s" => \@arg_only_meta_res,
		       "init" => \$init);

my %ignore_prop = ();
map {$ignore_prop{$_} = 1} @ignore_prop;

if ($init)
{
		push @ARGV, "--init";
}
foreach my $arg_only_meta_re (@arg_only_meta_res)
{
		push @ARGV, "--only-with-meta";
		push @ARGV, $arg_only_meta_re;
}
push @arg_only_res, @arg_only_meta_res;

my $INSTANCE_DELIM = "#";

my $include_prefix = "!";
my $config_keyword = "config";
my $include_keyword = "include";
my $macro_keyword = "macro";
my $title_keyword = "title";
my $select_keyword = "select";
my $file_keyword = "file";
my $key_keyword = "key";
my $end_keyword = "end";
my $expand_all_keyword = "expand_all";

#second keywords
my $class_keyword = "class";
my $parent_keyword = "parent";
my $consistent_keyword = "consistent";
my $disp_keyword = "disp";
my $sort_keyword = "sort";
my $skip_key_keyword = "skip_key";
my $prop_keyword = "prop";
my $key_as_prop_keyword = "key_as_prop";
my $has_keyword = "has";
my $default_keyword = "default";
my $copy_prop_keyword = "copy_prop";

#third keywords
my $list_keyword = "list";
my $scalar_keyword = "scalar";

my %orig_prop_to_type = (
		$class_keyword => $scalar_keyword,
		$disp_keyword => $scalar_keyword,
		$sort_keyword => $scalar_keyword,
		$skip_key_keyword => $list_keyword,
		$parent_keyword => $scalar_keyword,
		$consistent_keyword => $scalar_keyword,
		$prop_keyword => $scalar_keyword,
		);

my %prop_to_type = %orig_prop_to_type;


my %prop_to_default = ();

my %additional_load = ();

my %class_to_objects = ();
my %object_to_instances = ();
my %instance_to_object = ();

my %only_instances = ();
my %only_instances_cache = ();

my %root_instances = ();
my %instance_to_children = ();
my %instance_to_parent = ();

my %instance_props = ();
my %instance_props_rev = ();
my %object_to_class = ();

my %object_to_consistent = ();

my %cached_instances_in_class = ();
my %cached_consistent_instances_in_class = ();

die "Must specify a --meta file" unless $meta_file;

$meta_file = util::search_for_file($meta_file, @INC);
my $meta_file_dir = util::get_file_dir($meta_file);
if ($meta_file_dir)
{
		unshift @INC, $meta_file_dir;
}

my @only_res = ();
my %object_disps = ();

sub get_config_file()
{
		open IN, $meta_file or die "Can't read $meta_file\n";
		while (my $line = <IN>)
		{
				chomp($line);
				if ($line =~ /^[^\#]/ && $line =~ /\S/)
				{
						my @cols = split(" ", $line);
						if (scalar @cols != 2 || $cols[0] ne "$include_prefix$config_keyword")
						{
								die "Error: First line of meta file must be: $include_prefix$config_keyword /path/to/config\n";
						}
						return $cols[1];
				}
		}

}

my @meta_files = ();

sub expand_macros($$)
{
		my $val = shift;
		my $o = $val;
		my $macros = shift;

		my %macros = %{$macros};

		my %matched = ();
		my %last_matched = ();
		while (1)
		{
				%last_matched = %matched;
				%matched = ();
				foreach my $m (keys %macros)
				{
						if ($val =~ s/$m/$macros{$m}/g)
						{
								$matched{$m} = 1;
						}
				}
				foreach my $m (keys %matched)
				{
						if (!exists $last_matched{$m})
						{
								next;
						}
				}
				last;
		}

		return $val;
}

sub clear_caches();

sub init($@)
{
		my $just_keys = shift;
		
		@meta_files = ($meta_file, map {util::search_for_file($_, @INC)} @_);

		%prop_to_type = %orig_prop_to_type;
		%prop_to_default = ();
		%additional_load = ();
		%class_to_objects = ();
		%object_to_instances = ();
		%instance_to_object = ();
		%only_instances = ();
		%only_instances_cache = ();
		%root_instances = ();
		%instance_to_children = ();
		%instance_to_parent = ();
		%instance_props = ();
		%object_to_class = ();
		%object_to_consistent = ();
		%cached_instances_in_class = ();
		%cached_consistent_instances_in_class = ();

		clear_caches();

		my %object_to_copy_prop = ();


		my @classes = config::get_all_classes();
		my %classes = ();

		map {$classes{$_} = 1} @classes;

		my @if_prop_files = ();
		if ($init)
		{
				foreach my $file_key (config::get_all_files())
				{
						die "Can't use $file_key as a key in config file; reserved keyword in meta" if exists $prop_to_type{$file_key};
						$prop_to_type{$file_key} = $scalar_keyword;
				}
		}
		else
		{
				@if_prop_files = config::get_all_files();
		}

		my @config_props = config::get_all_props();

		foreach my $prop (@config_props, @if_prop_files)
		{
				die "Can't use $prop as a prop in config file; reserved keyword in meta" if exists $prop_to_type{$prop};

				$additional_load{$prop} = 1;

				if (config::is_file($prop))
				{
						$prop_to_type{$prop} = $scalar_keyword;
				}
				elsif (config::prop_is_list($prop))
				{
						$prop_to_type{$prop} = $list_keyword;
				}
				else
				{
						$prop_to_type{$prop} = $scalar_keyword;
		 		}
		}


		%cached_instances_in_class = ();
		%cached_consistent_instances_in_class = ();

		my %macros = ();

		my %all_object_props = ();
		my %special_instance_props = ();
		my %special_instance_selects = ();

		my @lines = ();
		my @end_lines = ();
		foreach my $cur_meta_file (@meta_files)
		{
				open IN, $cur_meta_file or die "Can't read $cur_meta_file\n";
				while (my $cur_line = <IN>)
				{
				    if ($cur_line =~ /^$include_prefix$end_keyword\s+(.+)/)
				    {
								push @end_lines, $1;
				    }
				    else
				    {
								push @lines, $cur_line;
				    }
				}
				close IN;
		}
		push @lines, @end_lines;

		#store object to props for later
		#we will convert to instances at the end
		my %object_props = ();
		my %object_props_is_class_level = ();

		my %deferred_parents = ();

		my @select_values = ();
		my $select_text = undef;
		my %seen_lines = ();
		my $l = 0;
		while (@lines)
		{
				$_ = shift @lines;

				chomp;

				s/^\s+//;
				s/\s+$//;
				s/\#.*//;

				next unless $_;

				if (/^$include_prefix$config_keyword/)
				{
						next;
				}

				if (/^$include_prefix$macro_keyword\s+(\S+)\s+(.*)$/)
				{
						my $one = $1;
						my $two = $2;
						$macros{$one} = expand_macros($two, \%macros);
						next;
				}
				$_ = expand_macros($_, \%macros);

				if (/^$include_prefix$include_keyword(\S*)\s+(\S+)\s*(.*)$/)
				{
						my $file_to_include = $2;
						$file_to_include = util::search_for_file($file_to_include, @INC);
						open IN2, $file_to_include or die "Can't read $file_to_include\n";
						my @file_lines = <IN2>;
						close IN2;
						foreach (reverse @file_lines)
						{
								unshift @lines, $_;
						}
						next;
				}

				if (/^$include_prefix$key_keyword\s+(\S+)\s*(.*)$/)
				{
						my $global_prop = $1;
						my $global_value = $2;
						if ($just_keys)
						{
								config::set_initial_value($global_prop, $global_value);
						}
						next;
				}
				if ($just_keys)
				{
						next;
				}

				if (/^$include_prefix$title_keyword\s+(.+)$/)
				{
						next;
				}

				my $stripped = "";

				my $is_file = undef;
				if (/^($include_prefix$file_keyword\s+)(.+)$/)
				{
						$is_file = 1;
						$_ = $2;
						$stripped .= $1;
				}

				my $expand_all = 0;
				if (/(.*)$include_prefix$expand_all_keyword\s+(.+)/)
				{
						$expand_all = 1;
						$_ = "$1$2";
				}

				if (/^($include_prefix$select_keyword(.)(\S+)\s+)(.+)$/)
				{
						my $select_delim = $2;
						$select_text = $3;
						@select_values = split($select_delim, $select_text);
						$select_text = join($select_delim, sort @select_values);
						@{$special_instance_selects{$select_text}} = @select_values;
						$_ = $4;
						$stripped .= $1;
				}
				else
				{
						$select_text = undef;
						@select_values = ();
				}

				my @cols = split(' ');
				my $first_initial = $cols[0];
				my @first = expand_item($first_initial);

				my $second_initial = $cols[1];
				my @second = expand_item($second_initial);

				my $third_initial = join(' ', @cols[2..$#cols]);

				my @third = expand_item($third_initial);

				if ($is_file)
				{
						my @new_third = ();
						foreach my $third (@third)
						{
								my $file = util::search_for_file($third, @INC);
								open IN2, $file or die "Can't read $file";
								while (<IN2>)
								{
										chomp;
										next if /^#/;
										my @cur_entries = split;
										foreach my $entry (@cur_entries)
										{
												push @new_third, $entry;
										}
								}
								close IN2;
						}
						@third = @new_third;
				}
				
				my $first_count = scalar @first;
				my $second_count = scalar @second;
				my $third_count = scalar @third;

				my $max_num = $first_count >= $second_count && $first_count >= $third_count ? $first_count : ($second_count >= $third_count ? $second_count : $third_count);

				my @to_validate = ();
				my $num_its = undef;
				if (($second_count > 1 || $first_count == $third_count) && !$expand_all)
				{
						$num_its = $max_num;
						@to_validate = (["first", \@first], ["second", \@second], ["third", \@third]);
				}
				else
				{
						$num_its = $first_count * $third_count;
						@to_validate = (["second", \@second]);
				}

				foreach my $pos (["first", $first_initial], ["second", $second_initial], ["third", $third_initial])
				{
						die "Need a $pos->[0] value: error parsing\n\t$_\n" unless defined $pos->[1];
				}

				foreach my $pos (@to_validate)
				{
						if (scalar @{$pos->[1]} != 1 && scalar @{$pos->[1]} != $max_num)
						{
								die "$pos->[0] value must parse into same number of objects as max value on line: error parsing\n\t$_\n";
						}
				}

			FIRSTS:

				for (my $f = 0; $f < $num_its; $f++)
				{
						my $first = $first_count == 1 ? $first[0] : ($first_count == $third_count && !$expand_all ? $first[$f] : $first[$f % $first_count]);
						my $second = scalar @second == 1 ? $second[0] : $second[$f];
						my $third = $third_count == 1 ? $third[0] : ($first_count == $third_count && !$expand_all ? $third[$f] : $third[int($f / $first_count)]);

						my $line_key = "$stripped $first $second $third";
						if ($seen_lines{$line_key})
						{
								next;
						}
						$seen_lines{$line_key} = 1;

						eval
						{
								if ($second eq $class_keyword || $second eq $has_keyword)
								{
										die "No class $third" unless exists $classes{$third};
										my @entries = ();
										if ($second eq $class_keyword)
										{
												push @entries, $first;
										}
										else
										{
												my $file = util::search_for_file($first, @INC);
												open IN2, $file or die "Can't read $first";
												while (<IN2>)
												{
														chomp;
														next if /^#/;
														my @cur_entries = split;
														foreach my $entry (@cur_entries)
														{
																push @entries, $entry;
														}
												}
												close IN2;
										}
										foreach my $first (@entries)
										{
												$class_to_objects{$third}{$first} = 1;
												#instantiate a dummy it now
												my $instance = $first;
												$object_to_instances{$first}{$instance} = 1;
												$instance_to_object{$instance} = $first;

												if (has_class($first) && get_class($first) ne $third)
												{
														die "Error: trying to assign class $third to $first when $first already has class " . get_class($first) . "\n";
												}

												set_class($first, $third);
												#Not sure why this is here: never setting this prop, so commenting out
												#unless (has_prop_value($first, $parent_keyword))
												{
														$root_instances{$first} = 1;
												}
										}
								}
								elsif ($second eq $prop_keyword)
								{
										if ($third ne $list_keyword && $third ne $scalar_keyword)
										{
												die "Unrecognized prop class: $third\n";
										}
										#die "Property $first already defined as $prop_to_type{$first}" if exists $prop_to_type{$first} && $prop_to_type{$first} ne $third;
										$prop_to_type{$first} = $third;
								}
								elsif ($second eq $default_keyword)
								{
										if (!exists $prop_to_type{$first})
										{
												die "Unrecognized property: $first\n";
										}
										die "Property $first already has default value defined" if exists $prop_to_default{$first};
										$prop_to_default{$first} = config::expand_value($third);
								}
								elsif ($second eq $copy_prop_keyword)
								{
										$object_to_copy_prop{$first} = $third;
								}
								else
								{
										if ($second eq $parent_keyword)
										{
												my @classes = ();

												my $ignore = 0;
												foreach my $value ($first, $third)
												{
														my $ind = $#classes + 1;

														if (exists $classes{$value})
														{
																map {$classes[$ind]{$_} = $value} keys %{$class_to_objects{$value}};
														}
														elsif (has_class($value))
														{
																$classes[$ind]{$value} = get_class($value);
														}
														else
														{
																if ($value eq $first)
																{
																		$ignore = 1;
																}
																else
																{
																		die "$value has not been defined";
																}
														}
												}

												if (!$ignore)
												{
														foreach my $first (keys %{$classes[0]})
														{
																foreach my $third (keys %{$classes[1]})
																{
																		my $object_class = get_class($first);
																		my $parent_class = get_class($third);
																		if ((config::has_parent_key($object_class) && config::get_parent_key($object_class) ne $parent_class) || @select_values)
																		{
																				my %ancestor_keys = config::get_ancestor_keys($object_class);
																				if (exists $ancestor_keys{$parent_class})
																				{
																						my $num_ancestors = scalar keys %ancestor_keys;
																						#trying to assign to an ancestor; store for later
																						push @{$deferred_parents{$num_ancestors}{$first}{$third}}, [];
																						@{$deferred_parents{$num_ancestors}{$first}{$third}->[$#{$deferred_parents{$num_ancestors}{$first}{$third}}]} = @select_values;
																						next;
																				}
																		}
																		set_parent($first, $third, 0, \@select_values);
																}
														}
												}
										}
										elsif ($second eq $consistent_keyword)
										{
												#store this for later
												my $first_class = get_class($first);
												my $third_class = get_class($third);

												my %first_class_ancestors = config::get_ancestor_keys($first_class);
												my %third_class_ancestors = config::get_ancestor_keys($third_class);

												if ($first_class_ancestors{$third_class} || $third_class_ancestors{$first_class})
												{
														die "Error: Cannot use $consistent_keyword to relate two objects that obey an ancestor/descendent relationship";
												}

												my $good_structure = 0;
												if (config::has_consistent_key($first_class))
												{
														my @consistent_keys = config::get_consistent_key($first_class);
														foreach my $c (@consistent_keys)
														{
																if ($c eq $third_class)
																{
																		$good_structure = 1;
																		last;
																}
														}
												}
												if (!$good_structure && config::has_consistent_key($third_class))
												{
														my @consistent_keys = config::get_consistent_key($third_class);
														foreach my $c (@consistent_keys)
														{
																if ($c eq $first_class)
																{
																		$good_structure = 1;
																		last;
																}
														}
												}

												unless ($good_structure)
												{
														die "Error: To assign $first to be consistent with $third, must define $first_class to be consistent with $third_class in config file";
												}
												$object_to_consistent{$first}{$third_class}{$third} = 1;
												#$object_to_consistent{$third}{$first_class}{$first} = 1;
										}
										elsif ($prop_to_type{$second} || $second eq $key_as_prop_keyword)
										{

												if ($second eq $key_as_prop_keyword)
												{
														$prop_to_type{$third} = $scalar_keyword;
														$second = $third;
														$third = config::get_value($second);
												}

												if ($init || !config::is_file($second) || $additional_load{$second})
												{
														my @classes = ();

														my $ignore = 0;
														my $is_class_level = 0;
														foreach my $value ($first)
														{
																my $ind = $#classes + 1;


																if (exists $classes{$value})
																{
																		$is_class_level = 1;
																		map {$classes[$ind]{$_} = $value} keys %{$class_to_objects{$value}};
																}
																elsif (has_class($value))
																{
																		$classes[$ind]{$value} = get_class($value);
																}
																else
																{
																		#warn "Warning: $value has not been defined (parsing $_)";
																		$ignore = 1;
																}
														}

														if (!$ignore)
														{
																foreach my $first (keys %{$classes[0]})
																{
																		my $prop_type = $prop_to_type{$second};
																		$all_object_props{$first}{$second} = 1;

																		if ($prop_type eq $list_keyword)
																		{
																				if (@select_values)
																				{
																						push @{$special_instance_props{$first}{$second}{$select_text}}, $third;
																				}
																				else
																				{
																						push @{$object_props{$first}{$second}}, $third;
																				}
																		}
																		else
																		{

																				if (@select_values)
																				{
																						$special_instance_props{$first}{$second}{$select_text} = [$third];

																				}
																				else
																				{
																						if (exists $object_props{$first}{$second})
																						{
																								unless ($object_props_is_class_level{$first}{$second} && !$is_class_level)
																								{
																										die "$first scalar $second already has a value: $object_props{$first}{$second}->[0]\n" 
																								}

																						}
																						$object_props_is_class_level{$first}{$second} = $is_class_level;

																						$object_props{$first}{$second} = [$third];
																				}
																		}
																}
														}
												}
										}
										else
										{
												unless (!$init && config::is_file($second))
												{
														die "Don't recognize property $second" unless $ignore_prop{$second};
												}
										}
								}

						};
						if ($@)
						{
								if ($@ eq main::trap_signal())
								{
										die $@;
								}

								die "Error parsing $meta_file: $@\nLine $_\n";
						}
				}
		}
		

		#now resolve deferred parent
		foreach my $num_ancestors (sort {$a <=> $b} keys %deferred_parents)
		{
				foreach my $deferred_child (keys %{$deferred_parents{$num_ancestors}})
				{
						my $deferred_child_class = get_class($deferred_child);
						my $deferred_child_parent_class = config::get_parent_key($deferred_child_class);
						foreach my $deferred_ancestor (keys %{$deferred_parents{$num_ancestors}{$deferred_child}})
						{
								foreach my $select_values_ref (@{$deferred_parents{$num_ancestors}{$deferred_child}{$deferred_ancestor}})
								{
										my @deferred_ancestor_instances = keys %{$object_to_instances{$deferred_ancestor}};
										foreach my $deferred_ancestor_instance (@deferred_ancestor_instances)
										{
												foreach my $parent_instance (@{&get_instances_in_class($deferred_child_parent_class, $deferred_ancestor_instance, 1)})
												{
														set_parent($deferred_child, $parent_instance, 1, $select_values_ref);
												}
										}
								}
						}
				}
		}
		set_only_res(@arg_only_res);
		foreach my $root_instance (get_root_instances())
		{
				my $root_class = get_class($root_instance);
				if (config::has_parent_key($root_class))
				{
						die "Error: Instance $root_instance has class $root_class; it therefore must have a parent with class " . (config::get_parent_key($root_class)) . " but it has not been assigned a parent\n";
				}
		}

		#resolve all properties

		my $skipped = undef;		
		my %skipped = ();
		my $last_skipped = undef;
		while (!defined $skipped || $skipped > 0)
		{
				$last_skipped = $skipped;
				$skipped = 0;
				%skipped = ();

				foreach my $object (keys %all_object_props)
				{
						foreach my $prop (keys %{$all_object_props{$object}})
						{
								foreach my $instance (keys %{$object_to_instances{$object}})
								{
										if (has_prop_value_instance($instance, $prop, "NO_DEFAULT"))
										{
												next;
										}

										my @values = ();
										if ($object_props{$object}{$prop})
										{
												@values = @{$object_props{$object}{$prop}};
										}
										if ($special_instance_props{$object}{$prop})
										{
												my %matched_select = ();
												foreach my $select_text (sort {scalar(@{$special_instance_selects{$a}}) <=> scalar(@{$special_instance_selects{$b}})} keys %{$special_instance_props{$object}{$prop}})
												{
														my @select_values = @{$special_instance_selects{$select_text}};
														my %intersection = ();
														my %ancestors = get_ancestor_objects($instance);
														map {$intersection{$_} = 1} values %ancestors;
														my $okay = 1;
														foreach my $select_value (@select_values)
														{
																if (!$intersection{$select_value})
																{
																		$okay = 0;
																		last;
																}
														}

														if ($okay)
														{
																#check whether the previous matched selects were a subset of this one; if so than clear it
																my $prev_subset = 1;
																my %cur_matched_select = ();
																map {$cur_matched_select{$_}=1} @select_values;
																foreach my $select_value (keys %matched_select)
																{
																		if (!$cur_matched_select{$select_value})
																		{
																				$prev_subset = 0;
																		}
																}


																if ($prev_subset)
																{
																		@values = ();
																}

																map {$matched_select{$_}=1} @select_values;
																push @values, @{$special_instance_props{$object}{$prop}{$select_text}};
														}
												}
										}
										if (!@values)
										{
												next;
										}
										my @new_values = ();
										my $should_skip = 0;
										eval
										{
												foreach my $value (@values)
												{
														my @args = util::get_args($value);
														my $cur_value = $value;
														foreach my $arg (@args)
														{
																#arg can be: a class name, or a property of it or any ancestor
																my $arg_value = undef;
																if (has_prop_value($instance, $arg, "NO_CACHE", "NO_DEFAULT"))
																{
																		$arg_value = get_prop_value($instance, $arg, "NO_CACHE");
																}
																else
																{
																		if (exists $prop_to_type{$arg})
																		{
																				$should_skip = 1;
																				last;
																		}
																		else
																		{
																				die "Error with $value: Instance $instance has no property for argument $arg";
																		}
																}

																unless ($prop eq $disp_keyword || $prop eq $sort_keyword || $prop eq $skip_key_keyword)
																{
																		$cur_value = util::substitute_arg($cur_value, $arg, $arg_value);
																}
														}
														last if $should_skip;
														push @new_values, $cur_value;
												}
										};
										if ($@)
										{
												if ($@ eq main::trap_signal())
												{
														die $@;
												}

												die "Error expanding property $prop for $object: $@\n";
										}
										my $prop_type = $prop_to_type{$prop};

										if ($should_skip)
										{
												$skipped{$prop} = 1;
												$skipped++;
												next;
										}

										if ($prop_to_type{$prop} eq $list_keyword)
										{
												set_prop_value($instance, $prop, \@new_values);												
										}
										else
										{
												die "Bug: Expanding property $prop for $instance yielded " . (scalar @new_values) . " values but $prop is a scalar\n" unless scalar @new_values == 1;

												set_prop_value($instance, $prop, $new_values[0]);
										}

								}
						}
				}

				if (defined $last_skipped && $skipped > 0 && $skipped == $last_skipped)
				{
						my $msg = "Error in expanding properties: There is either a circular dependency among arguments in the properties or an argument in the properties is not defined at any ancestor of the object\n";
						$msg .= "Unresolved properties: " . join(",", keys %skipped) . "\n";
						die $msg;
				}
		}
		#resolve all copy props
		foreach my $object (keys %object_to_copy_prop)
		{
				foreach my $instance (keys %{$object_to_instances{$object}})
				{
						my @instances_to_copy = ();
						foreach my $copy_instance (keys %{$object_to_instances{$object_to_copy_prop{$object}}})
						{
								push @instances_to_copy, $copy_instance;
						}
						#sort
						my %i = get_ancestor_objects($instance);
						@instances_to_copy = sort 
						{
								my $a_count = 0;
								my $b_count = 0;
								my %a = get_ancestor_objects($a); 
								my %b = get_ancestor_objects($b);
								
								foreach my $an (keys %a)
								{
										if (exists $i{$an} && $i{$an} == $a{$an})
										{
												$a_count++;
										}
								}

								foreach my $bn (keys %b)
								{
										if (exists $i{$bn} && $i{$bn} == $a{$bn})
										{
												$b_count++;
										}
								}
								return $a_count <=> $b_count;

						} @instances_to_copy;
						foreach my $instance_to_copy (@instances_to_copy)
						{
								my $object_to_copy = convert_to_object($instance_to_copy);
								foreach my $prop (keys %{$all_object_props{$object_to_copy}})
								{
										if (has_prop_value_instance($instance_to_copy, $prop, "NO_DEFAULT") && !has_prop_value_instance($instance, $prop, "NO_DEFAULT"))
										{
												set_prop_value($instance, $prop, get_prop_value_instance($instance_to_copy, $prop));
										}
								}
						}
				}
		}
		#store object disps
		%object_disps = ();
		foreach my $object (keys %object_props)
		{
				if (exists $object_props{$object}{$disp_keyword})
				{
						$object_disps{$object} = $object_props{$object}{$disp_keyword};
				}
		}

		#remove orphans
		foreach my $object (keys %object_to_instances)
		{
				foreach my $instance (keys %{$object_to_instances{$object}})
				{
						#delete $object_to_instances{$object}{$instance} unless defined get_parent($instance);
				}
		}

		##now add consistent objects based on consistent_props
		#my %prop_value_to_instance = ();
		#foreach my $object (keys %all_object_props)
		#{
		#my $class = get_class($object);
		#if (config::has_consistent_prop($class))
		#{
		#foreach my $instance (keys %{$object_to_instances{$object}})
		#{
		#foreach my $consistent_prop (config::get_consistent_prop($class))
		#{
		#if (has_prop_value_instance($instance, $consistent_prop))
		#{
		#my $value = get_prop_value_instance($instance, $consistent_prop);
		#$prop_value_to_instance{$consistent_prop}{$value}{$instance} = 1;
#}
#}
#}
#}
#}
		#foreach my $object (keys %all_object_props)
		#{
		#my $class = get_class($object);
		#if (config::has_consistent_prop($class))
		#{
		#foreach my $instance (keys %{$object_to_instances{$object}})
		#{
		#my %potential_consistent_instances = ();
		#foreach my $consistent_prop (config::get_consistent_prop($class))								
		#{
		#if (has_prop_value_instance($instance, $consistent_prop))
		#{
		#my $value = get_prop_value_instance($instance, $consistent_prop);
		#
		#foreach my $consistent_instance (keys %{$prop_value_to_instance{$consistent_prop}{$value}})
		#{
		#my $consistent_object = convert_to_object($consistent_instance);
		#if ($consistent_object ne $object)
		#{
		#$potential_consistent_instances{$consistent_instance} = 1;
#}
#}
#}
#}
		#foreach my $consistent_instance (keys %potential_consistent_instances)
		#{
		#foreach my $consistent_prop (config::get_consistent_prop($class))								
		#{
		#my $num_fail = 0;
		#if (has_prop_value_instance($instance, $consistent_prop) && has_prop_value_instance($consistent_instance, $consistent_prop) && get_prop_value_instance($instance, $consistent_prop) ne get_prop_value_instance($consistent_instance, $consistent_prop))
		#{
		#$num_fail++;
		#last;
#}
		#
		#if (!$num_fail)
		#{
		#my $consistent_object = convert_to_object($consistent_instance);
		#my $consistent_class = get_class($consistent_object);
		#$object_to_consistent{$object}{$consistent_class}{$consistent_object} = 1;
		#$object_to_consistent{$consistent_object}{$class}{$object} = 1;
#}
#}
#}
#}
#}
#}
}

sub get_meta_paths()
{
		return @meta_files;
}

sub expand_item($)
{
		my $initial = shift;
		my @final = ($initial);
		if ($initial =~ /([^\{]*)\{([^\}]+)\}(.*)/)
		{
				my $pre = $1;
				my $array = $2;
				my $post = $3;
				
				@final = ();
				my $did_not_match = 0;
				if ($array =~ /^([0-9]+)\.\.([0-9]+)$/)
				{
						my $low = $1;
						my $high = $2;
						if ($low > $high)
						{
								$low = $2;
								$high = $1;
						}
						
						for (my $i = $low; $i <= $high; $i++)
						{
								push @final, $i;
						}
				}
				elsif ($array =~ /,/)
				{
						@final = split(/,/, $array, -1);
				}
				else
				{
						@final = ($initial);
						$did_not_match = 1;
				}
				unless ($did_not_match)
				{
						foreach (my $i = 0; $i <= $#final; $i++)
						{
								if ($pre)
								{
										$final[$i] = "$pre$final[$i]";
								}
								if ($post)
								{
										$final[$i] = "$final[$i]$post";
								}
						}
				}
		}
		return @final;
}


sub get_root_instances()
{
		my @root_instances = ();
		foreach my $instance (keys %root_instances)
		{
				#next if @only_res && !$only_instances{$instance};
				push @root_instances, $instance;
		}
		return @root_instances;
}

sub get_parent($)
{
		my $instance = shift;
		if ($instance_to_parent{$instance})
		{
				return $instance_to_parent{$instance};
		}
		else
		{
				return undef;
		}
}

sub set_parent($$@)
{
		my $object = shift;
		my $parent = shift;
		my $parent_is_instance = shift;
		my $parent_matches = shift;

		my $require_parent_instance = undef;
		if ($parent_is_instance)
		{
				$require_parent_instance = $parent;
				$parent = convert_to_object($parent);
		}

		my $object_class = get_class($object);
		my $parent_class = get_class($parent);
		

		if (!config::has_parent_key($object_class) || config::get_parent_key($object_class) ne $parent_class)
		{
				die "Can't assign $parent_class as parent for $object_class; incompatible with class hierarchy in config file";
		}
		
		my $first = 1;
		foreach my $parent_instance (keys %{$object_to_instances{$parent}})
		{
				if ($require_parent_instance)
				{
						next unless $parent_instance eq $require_parent_instance;
				}

				if ($parent_matches && @{$parent_matches})
				{
						my %intersection = ();
						my %ancestors = get_ancestor_objects($parent_instance);

						map {$intersection{$_} = 1} values %ancestors;
						my $next = 0;
						foreach my $parent_match (@{$parent_matches})
						{
								if (!$intersection{$parent_match})
								{
										$next = 1;
										next;
								}
						}
						next if $next;
				}
				my $instance = convert_to_instance($parent_instance, $object);
				#this object now has at least one instance, so we are good (no longer a root)
				if ($first && exists $root_instances{$object})
				{
						delete $root_instances{$object};
				}
				$first = 0;

				#First, add it's parent information
				$instance_to_parent{$instance} = $parent_instance;
				$instance_to_children{$parent_instance}{$instance} = 1;

				#now, we need to change it and all of its children

				my @mod_info = ([$instance, $object]);
				while (@mod_info)
				{
						my $mod_info = pop @mod_info;
						my $new_instance = $mod_info->[0];
						my $old_instance = $mod_info->[1];
						my $cur_object = convert_to_object($new_instance);

						#get an instance that has already been created at this level
						#this is in case this object already has a parent and children
						#in this case we need a way to find out which children to add
						#all instances of a given object will have the same children 
						#(since parent is defined at the object level)
						#we can't just use the dummy instance $object since if this object
						#already has a parent the dummy instance was deleted
						
						my @reference_instances = keys %{$object_to_instances{$cur_object}};
						die "Bug: No instances for $object" unless scalar @reference_instances > 0;
						my $reference_instance = $reference_instances[0];

						$object_to_instances{$cur_object}{$new_instance} = 1;
						$instance_to_object{$new_instance} = $cur_object;

						foreach my $child (keys %{$instance_to_children{$reference_instance}})
						{
								my $child_object = convert_to_object($child);
								my $new_child = convert_to_instance($new_instance, $child_object);
								$instance_to_parent{$new_child} = $new_instance;
								$instance_to_children{$new_instance}{$new_child} = 1;

								my $old_child = convert_to_instance($old_instance, $child_object);

								push @mod_info, [$new_child, $old_child];
						}
						delete $object_to_instances{$cur_object}{$old_instance};
						delete $instance_to_object{$old_instance};
						delete $instance_to_parent{$old_instance};
						delete $instance_to_children{$old_instance};
				}
		}
}

sub get_ancestor_instances($)
{
		my $instance = shift;
		my $class = get_class($instance);

		my %ancestors = ();
		$ancestors{$class} = $instance;

		my $parent = get_parent($instance);
		while ($parent)
		{
				my $parent_class = get_class($parent);
				$ancestors{$parent_class} = $parent;
				$parent = get_parent($parent);
		}

		return %ancestors;
}

sub get_ancestor_objects($)
{
		my $instance = shift;
		my %ancestors = get_ancestor_instances($instance);
		foreach my $class (keys %ancestors)
		{
				$ancestors{$class} = convert_to_object($ancestors{$class});
		}
		return %ancestors;
}

our $do_print = 1;

#get all objects consistent with $cur_instance at $class; following multiple consistent links
#if $class is null, then check all classes directly consistent (not ancestors or descendents)
#if $class is non-null, obey ancestor/descendent relationships between $class and consistent
#if return value is null, then $class was not in the tree for any consistent
#if return value is empty, $class was in a tree but no instances matched $cur_instance
sub get_all_consistent_non_ancestor_descendents($$@)
{
		my $cur_instance = shift;

		my $class = shift;
		my %class_ancestors = config::get_ancestor_keys($class);

		my $cur_object = convert_to_object($cur_instance);
		my $print = shift;
		my @objects_to_process = ([$cur_instance, 0]);
		my %consistent_objects = ();
		my %processed_classes = ();
		my $consistent_instances = undef;

		my $found_class = undef;
		while (@objects_to_process)
		{
				my $cur_process_array = shift @objects_to_process;
				my $cur_process_instance = $cur_process_array->[0];
				my $cur_process = convert_to_object($cur_process_instance);
				my $cur_process_links = $cur_process_array->[1];
				my $process_class = get_class($cur_process);
				
				my %processed_class_ancestors = config::get_ancestor_keys($process_class);

				if (!defined $consistent_instances && defined $class && ($processed_class_ancestors{$class} || $class_ancestors{$process_class}))
				{
						@{$consistent_instances} = ();
				}

				$processed_classes{$process_class} = $cur_process_links;
				
				my @classes_to_check = keys %{$object_to_consistent{$cur_process}};
				if (defined $class && !exists $object_to_consistent{$cur_process}{$class})
				{
						unshift @classes_to_check, $class;
				}

				foreach my $cur_class (@classes_to_check)
				{
						my @cnads = ();
						$do_print = 1 if $class && $class eq "burden_test";
						my $cnads = get_consistent_non_ancestor_descendents($cur_process_instance, $cur_class);
						$do_print = 0;

						if ($cnads)
						{
								@cnads = @{$cnads};
						}
						foreach my $cnad (@cnads)
						{
								my $cnad_object = convert_to_object($cnad);

								#NEED TO:
								# 1. Record number of links from the initial object of each consistent
								# 2. If multiple instances from same class have different link numbers, take only smallest
								# This is to prevent "doubling back", where A -> B -> C but C is (say) a parent of A
								my $cnad_class = get_class($cnad_object);

								#must not be in the tree of this object
								if (!$consistent_objects{$cnad_object})
								{
										my $okay = 1;
										my %cnad_class_ancestors = config::get_ancestor_keys($cnad_class);
										#only process this one if it is on the shortest path
										foreach my $processed_class (keys %processed_classes)
										{
												if (($cnad_class_ancestors{$processed_class} || $processed_class_ancestors{$cnad_class}) && $processed_classes{$processed_class} < $cur_process_links)
												{
														$okay = 0;
														last;
												}
										}

										if ($okay)
										{
												$consistent_objects{$cnad_object} = $cur_process_links;
												push @objects_to_process, [$cnad, $cur_process_links + 1];

												#only add if two conditions hold:
												#1. This class matches the looked for path
												#2. This class is on the shortest path
												#It's possible that we may try to add via a longer path if A and B both link to $class
												#but A is on a shorter path
												if ((!defined $class || $class eq $cnad_class) && (!defined $found_class || !defined $class || $found_class == $cur_process_links))
												{
														$found_class = $cur_process_links unless defined $found_class;
														@{$consistent_instances} = () unless defined $consistent_instances;
														push @{$consistent_instances}, $cnad;
												}
										}
								}
						}
				}
		}
		return $consistent_instances;

}

sub get_ancestor_object_path($)
{
		my $instance = shift;
		my %ancestor_objects = get_ancestor_objects($instance);
		return  map {$ancestor_objects{$_}} sort keys %ancestor_objects;
}

sub get_children($)
{
		my $instance = shift;

		my @children = ();
		if ($instance_to_children{$instance})
		{
				foreach my $child (keys %{$instance_to_children{$instance}})
				{
						next if @only_res && !$only_instances{$child};
						push @children, $child;
				}
		}
		return @children;
}

sub has_class($)
{
		my $object = shift;
		$object = convert_to_object($object);

		return exists $object_to_class{$object};
}

sub get_class($)
{
		my $object = shift;
		$object = convert_to_object($object);

		die "$object is not an object" unless has_class($object);
		return $object_to_class{$object};
}

sub set_class($$)
{
		my $object = shift;
		$object = convert_to_object($object);
		my $class = shift;

		$object_to_class{$object} = $class;
}

sub get_consistent_non_ancestor_descendents($$@)
{
		my $instance = shift;
		my $object = convert_to_object($instance);

		my %ancestors = get_ancestor_objects($instance);

		my $consistent_class = shift;
		my $consistent = undef;

		if ($object_to_consistent{$object})
		{
				my $orig_consistent_class = $consistent_class;

				my %consistent_class_ancestors = config::get_ancestor_keys($consistent_class);

				my %consistent_intersection = ();
				map {$consistent_intersection{$_} = 1} grep {$consistent_class_ancestors{$_}} keys %{$object_to_consistent{$object}};

				foreach my $cur_consistent_class (keys %{$object_to_consistent{$object}})
				{

						my %cur_consistent_class_ancestors = config::get_ancestor_keys($cur_consistent_class);
						if ($cur_consistent_class_ancestors{$consistent_class})
						{
								$consistent_intersection{$cur_consistent_class} = 1;
						}
				}

				my @consistent_intersection = keys %consistent_intersection;

				if (@consistent_intersection)
				{						
						$consistent_class = shift @consistent_intersection;

						my @consistent_objects = keys %{$object_to_consistent{$object}{$consistent_class}};
						my @consistent_instances = ();
						foreach my $consistent_object (@consistent_objects)
						{
								foreach my $consistent_instance (keys %{$object_to_instances{$consistent_object}})
								{
										my %consistent_ancestors = get_ancestor_objects($consistent_instance);
										my $match = 1;
										foreach my $consistent_level (keys %consistent_ancestors)
										{
												if (exists $ancestors{$consistent_level})
												{
														if ($ancestors{$consistent_level} ne $consistent_ancestors{$consistent_level})
														{
																$match = 0;
																last;
														}
												}
										}
										if ($match)
										{
												push @consistent_instances, $consistent_instance;
										}
								}
						}

						if ($consistent_class ne $orig_consistent_class)
						{
								my @new_consistent_instances = ();
								foreach my $ci (@consistent_instances)
								{
										foreach my $cj (@{&get_instances_in_class($orig_consistent_class, $ci)})
										{

												push @new_consistent_instances, $cj;
										}
								}
								@consistent_instances = @new_consistent_instances;
						}
						$consistent = \@consistent_instances;
				}
				
		}
		return $consistent;
}

sub get_only_res()
{
		return @only_res;
}

sub set_only_res(@)
{
		@only_res = @_;

		my $key = join(":", sort @only_res);
		unless (exists $only_instances_cache{$key})
		{
				my %only_instances_raw = ();
				foreach my $only_re (@only_res)
				{
						#first add instances who have an ancestor matching only
						foreach my $class (keys %class_to_objects)
						{
								foreach my $object (keys %{$class_to_objects{$class}})
								{
										foreach my $instance (keys %{$object_to_instances{$object}})
										{
												last if $only_instances_raw{$instance}{$only_re};
												my %ancestor_objects = get_ancestor_objects($instance);
												foreach my $ancestor_object (values %ancestor_objects)
												{
														my $ancestor_disp = $object_disps{$ancestor_object};
														if ($ancestor_object =~ $only_re || (defined $ancestor_disp && $ancestor_disp->[0] =~ $only_re))
														{
																$only_instances_raw{$instance}{$only_re} = 1;
																last;
														}
												}
										}
								}
						}

						#now add instances who have a descendent matching only
						foreach my $class (keys %class_to_objects)
						{
								foreach my $object (keys %{$class_to_objects{$class}})
								{
										my $object_disp = $object_disps{$object};
										next unless $object =~ $only_re || (defined $object_disp && $object_disp->[0] =~ $only_re);
										foreach my $instance (keys %{$object_to_instances{$object}})
										{
												my %ancestor_instances = get_ancestor_instances($instance);
												foreach my $ancestor_instance (values %ancestor_instances)
												{
														$only_instances_raw{$ancestor_instance}{$only_re} = 1;
												}
										}
								}
						}

				}
				if (@only_res)
				{
						foreach my $instance (keys %only_instances_raw)
						{
								if (scalar keys %{$only_instances_raw{$instance}} == scalar @only_res)
								#if (scalar keys %{$only_instances_raw{$instance}} > 0)
								{
										$only_instances{$instance} = 1;
										my $object = convert_to_object($instance);
										if (exists $object_to_consistent{$object})
										{
												foreach my $consistent_class (keys %{$object_to_consistent{$object}})
												{
														my $consistent_instances = get_consistent_non_ancestor_descendents($instance, $consistent_class);
														if ($consistent_instances)
														{
																my @consistent_instances = @{$consistent_instances};
																foreach my $consistent_instance (@consistent_instances)
																{
																		$only_instances{$consistent_instance} = 1;
																}
														}
												}
										}
								}
						}
				}
				%{$only_instances_cache{$key}} = %only_instances;
		}
		else
		{
				%only_instances = %{$only_instances_cache{$key}};
		}
}

sub sort_instances(@)
{
		my @instances = @_;

		my %sv = ();
		foreach my $instance (@instances)
		{
				if (has_prop_value_instance($instance, $sort_keyword, "NO_DEFAULT"))
				{
						$sv{$instance} = get_prop_value_instance($instance, $sort_keyword, "NO_DEFAULT");
				}
		}
		if (scalar keys %sv)
		{
				my $max_sort = undef;
				foreach my $instance (keys %sv)
				{
						$max_sort = $sv{$instance} unless defined $max_sort && $max_sort > $sv{$instance};
				}
				foreach my $instance (@instances)
				{
						$sv{$instance} = $max_sort unless exists $sv{$instance};
				}

				@instances = sort {$sv{$a} == $sv{$b} ? $a cmp $b : $sv{$a} <=> $sv{$b}} @instances;
		} else
		{
				@instances = sort @instances;
		}
		return @instances;
}

my @empty_instances = ();
sub get_instances_in_class($@)
{
		my $class = shift;

		my %class_ancestors = config::get_ancestor_keys($class);
		my $consistent_with = shift;

		my $no_cache = shift;
		my $consistent_prop = shift;

		my $consistent_prop_from = undef;
		if (defined $consistent_prop)
		{
				$no_cache = 1;
				#consistent prop overrides the instance
				$consistent_prop_from = $consistent_with;
				$consistent_with = undef;
		}

		my $consistent_with_class = undef;
		my %consistent_with_class_ancestors = ();

		my $orig_consistent_with = $consistent_with;

		if ($consistent_with)
		{
				$consistent_with_class = get_class($consistent_with);
				return \@empty_instances unless defined $consistent_with_class;
				%consistent_with_class_ancestors = config::get_ancestor_keys($consistent_with_class);

				#here we handle case where consistent and object are not ancestor/descendent
				while ($consistent_with && !$class_ancestors{$consistent_with_class} && !$consistent_with_class_ancestors{$class} && $consistent_with_class ne $class)
				{
						#first check if the current consistent with explicitly specified who is consistent with it
						my $consistent_instances = get_consistent_non_ancestor_descendents($consistent_with, $class);
						if ($consistent_instances)
						{
								my @consistent_instances = @{$consistent_instances};
								#FIXME
								#TECHNICALLY WE SHOULD CHECK FOR SORTING, BUT NOT IMPLEMENTED RIGHT NOW
								@consistent_instances = sort_instances(@consistent_instances);

								return \@consistent_instances;
						}

						if (!config::has_parent_key($consistent_with_class))
						{
								die "Error: Couldn't find any relationship between $class and $consistent_with_class when trying to get instances for $class consistent with $orig_consistent_with\n";
						}
						$consistent_with_class = config::get_parent_key($consistent_with_class);
						$consistent_with = get_parent($consistent_with);
				}

				if ($consistent_with && exists $cached_consistent_instances_in_class{$class}{$consistent_with} && !$no_cache)
				{
						return $cached_consistent_instances_in_class{$class}{$consistent_with};
				}
		}
		else
		{
				if (exists $cached_instances_in_class{$class} && !$no_cache)
				{
						return $cached_instances_in_class{$class};
				}
		}
		return \@empty_instances unless defined $class_to_objects{$class};

		my @instances = ();

		if ($consistent_with && $consistent_with_class_ancestors{$class})
		{
				my %ancestors = get_ancestor_instances($consistent_with);
				if (defined $ancestors{$class})
				{
						@instances = ($ancestors{$class});
				}
				else
				{
						@instances = ();
				}
		}
		else
		{
				my @to_check_instances = ();
				if (defined $consistent_prop_from)
				{
						if (has_prop_value_instance($consistent_prop_from, $consistent_prop))
						{
								my $consistent_prop_value = get_prop_value_instance($consistent_prop_from, $consistent_prop);
								@to_check_instances = keys %{$instance_props_rev{$consistent_prop}{$consistent_prop_value}{$class}};
						}
				}
				else
				{
						if ($consistent_with && $consistent_with_class_ancestors{$class})
						{
								my %ancestors = get_ancestor_instances($consistent_with);
								@to_check_instances = ($ancestors{$class});
						}
						else
						{
								foreach my $object (keys %{$class_to_objects{$class}})
								{
										my $object_class = get_class($object);
										foreach my $i (keys %{$object_to_instances{$object}})
										{
												push @to_check_instances, $i;
										}
								}
						}
				}

				foreach my $instance (@to_check_instances)
				{
						next if !$consistent_with && @only_res && !$only_instances{$instance};

						if ($consistent_with)
						{
								#if the consistent with object is a descendent
								#this should never actually get triggered, since now we are checking above whether this condition holds
								if ($consistent_with_class_ancestors{$class})
								{
										my %ancestors = get_ancestor_instances($consistent_with);
										next unless $ancestors{$class} && $ancestors{$class} eq $instance;
								}
								#else if the consistent with object is an ancestor
								elsif ($class_ancestors{$consistent_with_class})
								{
										my %ancestors = get_ancestor_instances($instance);
										next unless defined $ancestors{$consistent_with_class} && $ancestors{$consistent_with_class} eq $consistent_with;
								}
								#else if the consistent with object is the same class
								else
								{
										next unless $consistent_with eq $instance;
								}
						}
						push @instances, $instance;
				}
		}

		@instances = sort_instances(@instances);
		#store cache
		if (!$no_cache)
		{
				if ($consistent_with)
				{
						$cached_consistent_instances_in_class{$class}{$consistent_with} = \@instances;
				}
				else
				{
						$cached_instances_in_class{$class} = \@instances;
				}
		}
		return \@instances;
}

sub has_prop_value_instance($$@)
{
		my $instance = shift;
		my $prop = shift;
		my $no_default = shift;

		my $has = exists $instance_props{$instance} && exists $instance_props{$instance}{$prop};
		if (!$has && !$no_default && (exists $prop_to_default{$prop} || config::has_default($prop)))
		{
				$has = 1;
		}
		return $has;
}

sub get_prop_value_instance($$)
{
		my $instance = shift;
		my $prop = shift;
		my $no_default = shift;

		if (has_prop_value_instance($instance, $prop, "NO_DEFAULT"))
		{
				return $instance_props{$instance}{$prop};
		}
		elsif (!$no_default)
		{
				if (exists $prop_to_default{$prop} || config::has_default($prop))
				{
						my $value = undef;
						if (exists ($prop_to_default{$prop}))
						{
								$value = $prop_to_default{$prop};
						}
						elsif (config::has_default($prop))
						{
								$value = config::get_default($prop);
						}

						if ($prop_to_type{$prop} eq $list_keyword)
						{
								my $old_value = $value;
								$value = undef;
								@{$value} = split(' ', $old_value);
						}
						return $value;
				}
		}
		return undef;
}

my %int_cache = ();
my %int_cache2 = ();
my %int_cache3 = ();

my %prop_default_cache = ();

#my %props_as_args_cache = ();
#my %props_as_args_cache2 = ();
#my %props_as_args_cache3 = ();
#my %props_as_args_cache4 = ();
my %cached_convert_to_object = ();

sub clear_caches()
{
		%cached_instances_in_class = ();
		%cached_consistent_instances_in_class = ();

		%int_cache = ();
		%int_cache2 = ();
		%int_cache3 = ();

		%prop_default_cache = ();

		#%props_as_args_cache ();
		#%props_as_args_cache2 = ();
		#%props_as_args_cache3 = ();
		#%props_as_args_cache4 = ();

		%cached_convert_to_object = ();
}


sub get_prop_value_int($$@);
sub get_prop_value_int($$@)
{
		my $instance = shift;
		my $prop = shift;
		my $consistent_with = shift;
		my $no_cache = shift;

		if ($prop eq $disp_keyword)
		{
				my $object = convert_to_object($instance);
				if (exists $instance_props{$instance} && exists $instance_props{$instance}{$prop})
				{
						return $instance_props{$instance}{$prop}
				}
				if (exists $object_disps{$object})
				{
						return $object_disps{$object};
				}
				else
				{
						return $object;
				}
		}

		my $does_not_depend = 0;
		unless ($no_cache)
		{
				if (exists $int_cache3{$instance}{$prop})
				{
						my $value = $int_cache3{$instance}{$prop};
						return $int_cache3{$instance}{$prop};
				}
				if ($consistent_with && exists $int_cache2{$instance}{$prop}{$consistent_with})
				{
						my $value = $int_cache2{$instance}{$prop}{$consistent_with};

						return $int_cache2{$instance}{$prop}{$consistent_with};
				}
				if (!$consistent_with && exists $int_cache{$instance}{$prop})
				{
						my $value = $int_cache{$instance}{$prop};

						return $int_cache{$instance}{$prop};
				}
		}
		my $value = undef;
		if (config::is_class($prop))
		{
				my %ancestor_objects = get_ancestor_objects($instance);
				if ($ancestor_objects{$prop})
				{
						$value = $ancestor_objects{$prop};
						$does_not_depend = 1;
				}
				else
				{
						#otherwise need to find those consistent

						my @consistent_objects = map {convert_to_object($_)} @{&get_instances_in_class($prop, $instance)};

						if ($consistent_with)
						{
								my @consistent_objects2 = map {convert_to_object($_)} @{&get_instances_in_class($prop, $consistent_with)};

								my %consistent_objects = ();
								map {$consistent_objects{$_} = 1} @consistent_objects;
								my %consistent_objects2 = ();
								map {$consistent_objects2{$_} = 1} @consistent_objects2;
								@consistent_objects = grep {$consistent_objects{$_}} @consistent_objects2;
						}
						if (!@consistent_objects)
						{
								$value = undef;
						}
						elsif (scalar @consistent_objects == 1)
						{
								$value = $consistent_objects[0];
						}
						else
						{
								$value = \@consistent_objects;
						}
				}
		}
		if (!defined $value)
		{
				my $cur_instance = $instance;
				while (defined $cur_instance && !defined $value)
				{
						if (exists $instance_props{$cur_instance}{$prop})
						{
								$value = $instance_props{$cur_instance}{$prop};
						}
						else
						{
								#check all instances consistent with this one
								#but DON'T check the parents of the consistent instances (just the children)
								
								my $cur_object = convert_to_object($cur_instance);
								foreach my $consistent_class (keys %{$object_to_consistent{$cur_object}})
								{
										my $consistent_objects = get_consistent_non_ancestor_descendents($cur_instance, $consistent_class);
										if ($consistent_objects)
										{
												my @consistent_objects = @{$consistent_objects};
												foreach my $cnad (@consistent_objects)
												{
														if (exists $instance_props{$cnad}{$prop})
														{
																$value = $instance_props{$cnad}{$prop};
																last;
														}
												}
										}
								}

								last if defined $value;
						}
						if (!defined $value)
						{
								$cur_instance = get_parent($cur_instance);
						}
				}
				if (!defined $value && $consistent_with)
				{
						#my %consistent_ancestor_instances = get_ancestor_instances($consistent_with);
						#my @intersection = grep {$_ eq $instance} values %consistent_ancestor_instances;

						#if consistent is a descendent of instance, try getting its prop value
						#if (@intersection)
						#{
						#		$value = get_prop_value_int($consistent_with, $prop);
						#}
				}
				elsif (defined $value)
				{
						$does_not_depend = 1;
				}
		}
		unless ($no_cache)
		{
				if ($does_not_depend)
				{
						$int_cache3{$instance}{$prop} = $value;
				}
				elsif ($consistent_with)
				{
						#$int_cache2{$instance}{$prop}{$consistent_with} = $value;
				}
				else
				{
						$int_cache{$instance}{$prop} = $value;
				}
		}
		return $value;
}

sub has_prop_value($$)
{
		my $instance = shift;
		my $prop = shift;
		my $no_cache = shift;
		my $no_default = shift;

		my $has = defined get_prop_value_int($instance, $prop, undef, $no_cache);
		if (!$has && !$no_default && (exists $prop_to_default{$prop} || config::has_default($prop)))
		{
				$has = 1;
		}
		return $has
}

sub get_prop_value($$)
{
		my $instance = shift;
		my $prop = shift;
		my $no_cache = shift;

		my $value = get_prop_value_int($instance, $prop, undef, $no_cache);
		unless (defined $value)
		{
				if (exists $prop_to_default{$prop} || config::has_default($prop))
				{
						if (exists ($prop_to_default{$prop}))
						{
								$value = $prop_to_default{$prop};
						}
						elsif (config::has_default($prop))
						{
								$value = config::get_default($prop);
						}
						if ($prop_to_type{$prop} eq $list_keyword)
						{
								my $old_value = $value;
								$value = undef;
								@{$value} = split(' ', $old_value);
						}
				}
				else
				{
						die "Instance $instance has no property $prop\n";
				}
		}

		return $value;
}

sub should_skip_key($$)
{
		my $instance = shift;
		my $key = shift;
		if (has_prop_value($instance, $skip_key_keyword))
		{
				my @skip_keys = @{&get_prop_value($instance, $skip_key_keyword)};
				foreach my $skip_key (@skip_keys)
				{
						return 1 if $skip_key eq $key;
				}
		}
		return 0;
}

my %seen = ();

sub get_props_as_args($)
{
		my $instance = shift;
		my $consistent_with = shift;
		my $needed_props = shift;

#		if (!defined $needed_props)
#		{
#				if (defined $consistent_with && $props_as_args_cache2{$instance}{$consistent_with})
#				{
#						return $props_as_args_cache2{$instance}{$consistent_with};
#				}
#				elsif (!defined $consistent_with && $props_as_args_cache{$instance})
#				{
#						return $props_as_args_cache{$instance};
#				}
#		}

		my %args = ();
		my @props = ();

		#my @old_props = config::get_all_classes();
		#push @old_props, keys %prop_to_type;
		#my $props_key = join("#", sort(@props));


		if (defined $needed_props)
		{
				@props = @{$needed_props};

				#LOG
				#my $temp = join(":", sort(@props));
				#$seen{$instance}{$temp}++;

		}
		else
		{
				@props = config::get_all_classes();
				push @props, keys %prop_to_type;
		}

		foreach my $prop (@props)
		{
				my $value = get_prop_value_int($instance, $prop, $consistent_with);

				if (defined $value)
				{
						$args{$prop} = $value;
				}
				elsif (exists $prop_to_default{$prop} || config::has_default($prop))
				{
						if (exists ($prop_to_default{$prop}))
						{
								$args{$prop} = $prop_to_default{$prop};
						}
						else
						{
								$args{$prop} = config::get_default($prop);
						}

						if ($prop_to_type{$prop} eq $list_keyword)
						{
								my $old_value = $args{$prop};
								delete $args{$prop};
								if (index($old_value, ' ') >= 0)
								{
										@{$args{$prop}} = split(' ', $old_value);
								}
								else
								{
										@{$args{$prop}} = ($old_value);
								}
						}
				}
		}

#		if (!defined $needed_props)
#		{
#				if (defined $consistent_with)
#				{
#						%{$props_as_args_cache2{$instance}{$consistent_with}} = %args;
#				}
#				elsif (!defined $consistent_with && $props_as_args_cache{$instance})
#				{
#						%{$props_as_args_cache{$instance}} = %args;
#				}
#		}
#		if (defined $needed_props)
#		{
#				if (defined $consistent_with)
#				{
#						%{$props_as_args_cache3{$instance}{$consistent_with}{$props_key}} = %args;
#				}
#				else
#				{
#						%{$props_as_args_cache4{$instance}{$props_key}} = %args;
#				}
#
#		}


		return \%args;
}

sub set_prop_value($)
{
		my $instance = shift;
		my $prop = shift;
		my $value = shift;

		my $class = get_class($instance);
		if (exists $instance_props{$instance}{$prop})
		{
				my $old_value = $instance_props{$instance}{$prop};
				delete $instance_props_rev{$prop}{$old_value}{$class}{$instance};
		}

		$instance_props{$instance}{$prop} = $value;
		$instance_props_rev{$prop}{$value}{$class}{$instance} = 1;
}

sub has_default_file($$)
{
		my $instance = shift;
		my $key = shift;
		return has_prop_value($instance, $key);
}

sub get_default_file($$)
{
		my $instance = shift;
		my $key = shift;
		return get_prop_value($instance, $key);
}

#By definition, last item in key is the object of this instance
#Should probably do this more robustly
sub convert_to_object($)
{
		my $instance = shift;
		if (!exists $cached_convert_to_object{$instance})
		{
				my @object = split($INSTANCE_DELIM, $instance);
				$cached_convert_to_object{$instance} = $object[$#object];
		}
		return $cached_convert_to_object{$instance};
}

sub convert_to_instance($$)
{
		my $parent_instance = shift;
		my $child_object = shift;
		my @ancestors = get_ancestor_object_path($parent_instance);
		push @ancestors, $child_object;
		return join($INSTANCE_DELIM, @ancestors);
}

#END {foreach my $u (keys %seen) {foreach my $v (keys %{$seen{$u}}) { print "$u, $v, $seen{$u}{$v}\n"} } }

1;
