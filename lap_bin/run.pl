use FindBin;
use lib "$FindBin::Bin/..";

use strict;
use vars;
use warnings;


use Time::HiRes qw(gettimeofday);
use Getopt::Long;
use Date::Format;
use Storable qw(freeze thaw);
use Digest::MD5 qw(md5);
#use bin::cache;

my $overall_started_time = gettimeofday();

#DEBUG INFO
#In case bug where project_ibd_initial_pdf_file is not rebuilt
my %debug_cmd_key = ();
#DEBUG INFO

print STDERR "PID: $$\n";
print STDERR "Arguments: @ARGV\n";
print STDERR "Initializing...";


require bin::common;
require bin::config;
require bin::meta;
use bin::trap_sig;

meta::init("just_keys");
my $config_file = meta::get_config_file();
config::init($config_file);

print STDERR "...";
meta::init();

print STDERR "done\n";
print STDERR "Using " . common::get_tmp_dir() . " as temporary directory\n";

sub mkdir_at_level($$@);
sub convert_meta_files_to_meta_paths(@);
sub query_props($$$$$);
sub eval_if_value($$);
sub get_bsub_batch_size($);

my $initialize = undef;
my $mkdir = undef;
my $copy_max = 0;
my $check = undef;
my $debug = undef;
my $debug2 = undef;
my @debug_cmd_key = ();
my $force = undef;
my $force_started = undef;
my $bsub = undef;
my $ignore_bsub = undef;
my $min_num_bsub = 1;
my @only_bsub = ();
my @skip_bsub = ();
my @only_cmd = ();
my @skip_cmd = ();
my @force_cmd_texts = ();
my @skip_cmd_texts = ();
my @force_keys = ();
my @only_keys = ();
my @skip_keys = ();
my @stop_after_keys = ();
my @touch_keys = ();
my $touch_mod_cmd = ();
my @preserve_time_keys = ();
my $reset_time_to = undef;
my @only_level = ();
my @skip_level = ();
my @skip_with_input_level = ();
my $max_try_it = 1;
my $restart = 2;
if (common::get_use_sge())
{
		$restart = 4;
}
my $scale_restart_mem = 2;
my @restart_long_cmd = ();
my @restart_mem_cmd = ();
my @restart_long_code = (134,140,137);
my %restart_long_code = ();
my @restart_mem_code = (130, 143);
my %restart_mem_code = ();
my $no_short = undef;
my $all_short = undef;
my $no_with = undef;
my $no_batch = undef;
my $error_if_not_generated = undef;

my $skip_meta = undef;
my $skip_non_meta = undef;
my $follow_cmd = undef;
my $dynamic_mkdir = 1;
my $skip_custom_meta = 0;
my @skip_custom_meta_level = ();
my @only_custom_meta_level = ();
my $suppress_warnings = 0;
my $ignore_cmd_different = 0;
my @update_cmd_key_different = ();
my @update_cmd_text_different = ();
my $ignore_input_list = 0;
my @only_meta = ();
GetOptions(
		'init' => \$initialize,
		'mkdir' => \$mkdir,
		'copy-max=s' => \$copy_max,
		'tries=i' => \$restart,
		'restart=i' => \$restart,
		'restart-long-cmd=s' => \@restart_long_cmd,
		'restart-long-code=s' => \@restart_long_code,
		'no-short' => \$no_short,
		'force-long' => \$no_short,
		'force-short' => \$all_short,
		'all-short' => \$all_short,
		'skip-with' => \$no_with,
		'no-with' => \$no_with,
		'no-batch' => \$no_batch,
		'no-bsub-batch' => \$no_batch,
		'restart-mem-cmd=s' => \@restart_mem_cmd,
		'restart-mem-code=s' => \@restart_mem_code,
		'scale-restart-mem=f' => \$scale_restart_mem,
		'only-level=s' => \@only_level,
		'skip-level=s' => \@skip_level,
		'skip-with-input-level=s' => \@skip_with_input_level,
		'only-bsub=s' => \@only_bsub,
		'skip-bsub=s' => \@skip_bsub,
		'only-cmd=s' => \@only_cmd,
		'skip-cmd=s' => \@skip_cmd,
		'only-key=s' => \@only_keys,
		'preserve-time-key=s' => \@preserve_time_keys,
		'preserve-mod-key=s' => \@preserve_time_keys,
		'reset-time-to=s' => \$reset_time_to,
		'touch-key=s' => \@touch_keys,
		'touch-mod-cmd' => \$touch_mod_cmd,
		'force-cmd-text=s' => \@force_cmd_texts,
		'skip-cmd-text=s' => \@skip_cmd_texts,
		'force-key=s' => \@force_keys,
		'skip-key=s' => \@skip_keys,
		'stop-after-key=s' => \@stop_after_keys,
		'check' => \$check,
		'force' => \$force,
		'debug' => \$debug,
		'debug2' => \$debug2,
		'debug-cmd-key=s' => \@debug_cmd_key,
		'force-started' => \$force_started,
		'error-if-not-generated' => \$error_if_not_generated,
		'bsub' => \$bsub,
		'ignore-bsub' => \$ignore_bsub,
		'skip-custom-meta' => \$skip_custom_meta,
		'skip-custom-meta-level=s' => \@skip_custom_meta_level,
		'only-custom-meta-level=s' => \@only_custom_meta_level,
		'min-num-bsub=i' => \$min_num_bsub,
		'skip-meta' => \$skip_meta,
		'skip-non-meta' => \$skip_non_meta,
	  'follow-cmd'=> \$follow_cmd,
	  'suppress-warnings'=> \$suppress_warnings,
	  'ignore-cmd-different'=> \$ignore_cmd_different,
	  'update-cmd-key-different=s'=> \@update_cmd_key_different,
	  'update-cmd-key=s'=> \@update_cmd_key_different,
	  'update-cmd-text-different=s'=> \@update_cmd_text_different,
	  'update-cmd-text=s'=> \@update_cmd_text_different,
	  'ignore-input-list'=> \$ignore_input_list,
	  'only-meta=s'=> \@only_meta,
	  'only-with-meta=s'=> \@only_meta,
		);

map {$debug_cmd_key{$_}=1} @debug_cmd_key;

map {$restart_long_code{$_}=1} @restart_long_code;
map {$restart_mem_code{$_}=1} @restart_mem_code;

my %update_cmd_key_different = ();
map {$update_cmd_key_different{$_}=1} @update_cmd_key_different;

my %specific_only_meta = ();
my @all_only_meta = ();
foreach my $only_meta (@only_meta)
{
		my @cur_only_meta = split(":", $only_meta);
		if (scalar @cur_only_meta == 2)
		{
				push @{$specific_only_meta{$cur_only_meta[0]}}, $cur_only_meta[1];
		}
		else
		{
				push @all_only_meta, $only_meta;
		}
}


sub should_force($);
sub should_skip($);
sub should_stop_after($);
sub should_only($);
sub should_touch($);
sub should_preserve($);
								 
my %only_level;
my %skip_level;
my %skip_with_input_level;
map {$only_level{$_} = 1} @only_level;

foreach my $only_level (@only_level)
{
		if (!$no_with && config::has_with($only_level))
		{
				foreach my $with (config::get_with($only_level))
				{
						push @only_level, $with if !exists $only_level{$with};
						$only_level{$with} = 1;
				}
		}
}
@only_level = keys %only_level;

map {$skip_level{$_} = 1} @skip_level;
foreach my $skip_level (@skip_level)
{
		if (config::has_with($skip_level))
		{
				foreach my $with (config::get_with($skip_level))
				{
						push @skip_level, $with if !exists $skip_level{$with};
						$skip_level{$with} = 1;
				}
		}
}
@skip_level = keys %skip_level;

map {$skip_with_input_level{$_} = 1} @skip_with_input_level;

my %skip_custom_meta_level;
map {$skip_custom_meta_level{$_} = 1} @skip_custom_meta_level;

my %only_custom_meta_level;
map {$only_custom_meta_level{$_} = 1} @only_custom_meta_level;

my %need_but_cant = ();

my %cached_skip_command = ();

sub skip_command($)
{
		my $cmd_key = shift;

		until (exists $cached_skip_command{$cmd_key})
		{
				my $class_level = config::get_class_level($cmd_key);

				my %all_class_levels = ($class_level=>1);
				if (config::has_with($cmd_key) && !$no_with)
				{
						foreach my $with (config::get_with($cmd_key))
						{
								$all_class_levels{$with} = 1;
						}
				}

				if (@only_level)
				{
						my $any_matches = 0;
						foreach my $cur_only_level (@only_level)
						{
								if ($all_class_levels{$cur_only_level})
								{
										$any_matches = 1;
										last;
								}
						}
						unless ($any_matches)
						{
								$cached_skip_command{$cmd_key} = 1;
								last;
						}
				}

				if (@skip_level)
				{
						my $all_matches = 1;
						foreach my $cur_class_level (keys %all_class_levels)
						{
								if (!$skip_level{$cur_class_level})
								{
										$all_matches = 0;
										last;
								}
						}

						if ($all_matches)
						{
								$cached_skip_command{$cmd_key} = 1;
								last;
						}
				}

				if (@only_keys || @skip_keys)
				{
						my $all_outputs_skipped = 1;

						my %output_keys = config::get_output_keys_for_cmd($cmd_key);

						foreach my $output_key (keys %output_keys)
						{
								next if @only_keys && !should_only($output_key);
								next if should_skip($output_key);
								$all_outputs_skipped = 0;
						}
						if ($all_outputs_skipped)
						{
								$cached_skip_command{$cmd_key} = 1;
								last;
						}
				}

				my $skip_this_cmd = 0;
				my $do_this_cmd = 0;

				foreach my $cmd_info ([\@only_cmd, 0], [\@skip_cmd, 1])
				{
						my @matches_cmd = @{$cmd_info->[0]};
						my $matches = 1;
						my $cur_skip = undef;
						if (@matches_cmd)
						{
								$matches = 0;
								foreach my $matches_cmd (@matches_cmd)
								{
										if ($cmd_key =~ $matches_cmd)
										{
												$matches = 1;
												last;
										}
								}
								if ($cmd_info->[1])
								{
										$cur_skip = $matches;
								}
								else
								{
										$cur_skip = !$matches;
										$do_this_cmd = 1 if $matches;
								}
								$skip_this_cmd = 1 if $cur_skip;
						}
				}


				if ($skip_this_cmd)
				{
						$cached_skip_command{$cmd_key} = 1;
						last;
				}

				if (config::is_meta_table($cmd_key) && $skip_meta)
				{
						$cached_skip_command{$cmd_key} = 1;
						last;
				}
				if (!config::is_meta_table($cmd_key) && $skip_non_meta)
				{
						$cached_skip_command{$cmd_key} = 1;
						last;
				}

				if (!$do_this_cmd)
				{
						if (@skip_with_input_level)
						{
								my %needed_classes = config::get_needed_classes_for_cmd($cmd_key);
								my $should_skip = 0;
								foreach my $needed_class (keys %needed_classes)
								{
										if ($skip_with_input_level{$needed_class})
										{
												$should_skip = 1;
										}
								}
								if ($should_skip)
								{
										$cached_skip_command{$cmd_key} = 1;
										last;
								}
						}
				}
				$cached_skip_command{$cmd_key} = 0;
				last;
		}
		return $cached_skip_command{$cmd_key};
}

sub eval_if_value($$)
{
		my $text = shift;
		my $to_run_instance = shift;

		my $return_val = config::parse_if_value_clauses($text);
		(my $complement, my $do_and, my $clauses, my $is_clause) = @{$return_val};
		my @clauses = @{$clauses};
		my @is_clause = @{$is_clause};

		my $num_has = 0;
		my $num_no_has = 0;

		for (my $i = 0; $i <= $#clauses; $i++)
		{
				my $check_value = $clauses[$i];

				my $has_value = undef;
				if ($is_clause[$i])
				{
						if ($check_value eq $text)
						{
								die "Error parsing clause: $check_value\n";
						}
						$has_value = eval_if_value($check_value, $to_run_instance);
				} 
				else
				{
						(my $prop, my $op, my $operand) = (undef, undef, undef);
						eval
						{
								($prop, $op, $operand, undef) = config::parse_prop_comp($check_value, "", $check_value);
						};
						if ($@)
						{
								if ($@ =~ /^Bad content/)
								{
										print STDERR "Error processing $text for $to_run_instance\n";
								}
								die $@;
						}

						#here, parse prop (if has ;), get instances, use those going forward rather than to_run_instance
						my @to_run_instances = ($to_run_instance);

						my @parsed_prop = split(";", $prop);

						if (scalar @parsed_prop == 2 && config::is_class($parsed_prop[0]))
						{
								@to_run_instances = @{&meta::get_instances_in_class($parsed_prop[0], $to_run_instance, undef, undef)};
								$prop = $parsed_prop[1];
						}

						foreach my $instance_to_check (@to_run_instances)
						{
								my $cur_has_value = meta::has_prop_value($instance_to_check, $prop);
								if ($cur_has_value)
								{
										$cur_has_value = meta::get_prop_value($instance_to_check, $prop);

										my @cur_has_value = ();
										if (defined $cur_has_value && ref($cur_has_value) && lc(ref($cur_has_value)) eq "array")
										{
												@cur_has_value = @{$cur_has_value};
										}
										else
										{
												@cur_has_value = ($cur_has_value);
										}

										if (defined $op && defined $operand)
										{
												$cur_has_value = undef;
												foreach my $hv (@cur_has_value)
												{
														$cur_has_value |= config::check_prop_comp($hv, $op, $operand);
														last if $cur_has_value;
												}
										}
										else
										{
												$cur_has_value = undef;
												foreach my $hv (@cur_has_value)
												{
														$cur_has_value |= $hv;
														last if $cur_has_value;
												}
										}
								}
								$has_value |= $cur_has_value;
						}
				}

				if ($complement)
				{
						$has_value = !$has_value;
				}

				if ($has_value)
				{
						$num_has++;
				}
				else
				{
						$num_no_has++;
				}
				if (($do_and && $num_no_has > 0)  || (!$do_and && $num_has > 0))
				{
						last;
				}

		}
		my $has_value = $do_and ? $num_no_has == 0 && $num_has > 0 : $num_has > 0;

		$has_value = 0 unless $has_value;

		return $has_value;
}

for (my $try_it = 0; $try_it < $max_try_it; $try_it++)
{
		if ($try_it > 0)
		{
				$force = 0;
		}

		%need_but_cant = ();

		my @meta_table_files = ();
		my %levels_from_meta_tables = ();
		foreach my $file_key (config::get_all_files())
		{
				if (config::is_meta_table($file_key))
				{
						push @meta_table_files, $file_key;
						foreach my $meta_level (config::get_meta_levels($file_key))
						{
								$levels_from_meta_tables{$meta_level} = 1;
						}
				}
		}

		my %paths_will_build = ();

		#list of levels needed for execution
		my %levels_to_run = ();
		foreach my $cmd_key (config::get_all_cmds())
		{
				if (!skip_command($cmd_key))
				{
						my %needed_classes = config::get_needed_classes_for_cmd($cmd_key, 1);

						#need to load all meta files that contain classes used in this command
						foreach my $needed_class (keys %needed_classes)
						{
								my %needed_ancestor_classes = config::get_ancestor_keys($needed_class);
								foreach my $needed_ancestor_class (keys %needed_ancestor_classes)
								{
										$levels_to_run{$needed_ancestor_class}=1 if $levels_from_meta_tables{$needed_ancestor_class};
								}
						}

				}
		}

		while (1)
		{
				my $num_levels = scalar keys %levels_to_run;
				foreach my $level_to_run (keys %levels_to_run)
				{
						if (config::has_parent_key($level_to_run))
						{
								my $parent_level = config::get_parent_key($level_to_run);
								#load any meta files that build parents
								$levels_to_run{$parent_level} = 1 if $levels_from_meta_tables{$parent_level};
						}
						if (config::has_consistent_key($level_to_run))
						{
								my @consistent_keys = config::get_consistent_key($level_to_run);
								foreach my $consistent_key (@consistent_keys)
								{
										#load any meta files that build consistent instances
										$levels_to_run{$consistent_key} = 1 if $levels_from_meta_tables{$consistent_key};
								}
						}
				}

				foreach my $file_key (@meta_table_files)
				{
						foreach my $meta_level (config::get_meta_levels($file_key))
						{
								if ($levels_to_run{$meta_level})
								{
										#this meta file (A) will be loaded
										#load any meta files that create instances with (A) meta files
										my $file_class_level = config::get_class_level($file_key);
										$levels_to_run{$file_class_level} = 1;
								}

						}
				}
				if ($num_levels == scalar keys %levels_to_run)
				{
						last;
				}
		}

		#store keys that contain meta files for reading in later
		my %meta_file_to_level = ();
		my %level_to_meta_file = ();
		foreach my $file_key (@meta_table_files)
		{
				foreach my $meta_level (config::get_meta_levels($file_key))
				{
						#next unless ($levels_to_run{$meta_level} || $initialize || $mkdir);
						next unless $levels_to_run{$meta_level};

						push @{$meta_file_to_level{$file_key}}, $meta_level;
						$level_to_meta_file{$meta_level}{$file_key} = 1;
				}
		}

		my $sortcrit = sub
		{
				my %a_ancestor_keys = config::get_ancestor_keys($a);
				my %b_ancestor_keys = config::get_ancestor_keys($b); 

				my %a_consistent_ancestor_keys = ();
				if (config::has_consistent_key($a))
				{
						foreach my $c (config::get_consistent_key($a))
						{
								my %d = config::get_ancestor_keys($c);
								foreach my $e (keys %d)
								{
										$a_consistent_ancestor_keys{$e} = $d{$e};
								}
						}
				}
				my %b_consistent_ancestor_keys = ();
				if (config::has_consistent_key($b))
				{
						foreach my $c (config::get_consistent_key($b))
						{
								my %d = config::get_ancestor_keys($c);
								foreach my $e (keys %d)
								{
										$b_consistent_ancestor_keys{$e} = $d{$e};
								}
						}
				}

				my $a_scalar_keys = scalar(keys(%a_ancestor_keys)) > scalar(keys(%a_consistent_ancestor_keys)) + 1 ? scalar(keys(%a_ancestor_keys)) : scalar(keys(%a_consistent_ancestor_keys)) + 1;
				my $b_scalar_keys = scalar(keys(%b_ancestor_keys)) > scalar(keys(%b_consistent_ancestor_keys)) + 1 ? scalar(keys(%b_ancestor_keys)) : scalar(keys(%b_consistent_ancestor_keys)) + 1;

				return $a_scalar_keys <=> $b_scalar_keys;
		};

		my @meta_file_levels = sort $sortcrit (keys %level_to_meta_file);

		my $had_an_error = 0;
		if ($initialize || $mkdir)
		{
				if (keys %meta_file_to_level)
				{
						my @meta_files_to_load = ();
						foreach my $meta_file_level (@meta_file_levels)
						{
								foreach my $meta_file_to_load (keys %{$level_to_meta_file{$meta_file_level}})
								{
										push @meta_files_to_load, $meta_file_to_load;
								}
						}
						print STDERR "Converting @meta_files_to_load\n" if $debug;
						my @meta_paths_to_load = convert_meta_files_to_meta_paths(@meta_files_to_load);
						
						print STDERR "Loading @meta_paths_to_load\n" if $debug;
						meta::init(undef, @meta_paths_to_load);
						print STDERR "Done\n" if $debug;

				}

				if ($mkdir)
				{
						#go through and make all directories
						mkdir_at_level(undef, undef, ());
				}
				if ($initialize)
				{
						if ($copy_max =~ /^([0-9]+)(G|K|M)$/)
						{
								my $mult = 1;
								if ($2 eq "G")
								{
										$mult = 1e9
								}
								elsif ($2 eq "M")
								{
										$mult = 1e6
								}
								elsif ($2 eq "K")
								{
										$mult = 1e3
								}
								$copy_max = $1 * $mult;
						}

						#now copy over all of the initial files
						my %file_to_level = ();
						foreach my $file_key (config::get_all_files())
						{
								next if @only_keys && !should_only($file_key);
								next if should_skip($file_key);

								print STDERR "Doing $file_key\n" if $debug;
								if (!config::has_class_level($file_key))
								{
										die "Error: Must specify a class level for all files (none for $file_key)\n";
								}

								my $class_level = config::get_class_level($file_key);

								next if @only_level && !$only_level{$class_level};
								next if @skip_level && $skip_level{$class_level};

								my $instances = meta::get_instances_in_class($class_level);
								foreach my $instance (@{$instances})
								{
										my @needed_props = config::get_args($file_key, undef, 1);
										my %args = %{&meta::get_props_as_args($instance)};
										my $path = config::get_path($file_key, %args);
										if (meta::has_default_file($instance, $file_key))
										{
												my $orig_path = meta::get_default_file($instance, $file_key);

												if ($dynamic_mkdir)
												{
														my $output_dir_key = config::get_dir_key($file_key);
														my $output_dir = config::get_dir($file_key, %args);
														if (!-e $output_dir || (config::is_sortable($output_dir_key) && !-e config::get_value('sort_dir', (dir=>$output_dir))))
														{
																common::run_mkdir_chmod($output_dir_key, $check, %args);
														}
												}

												my $should_force = $force || should_force($file_key);
												if ($try_it > 0)
												{
														$should_force = 0;
												}
												if ($should_force || !common::path_exists($path) || common::check_key_error($file_key, %args) || common::get_mod_time($orig_path) > common::get_mod_time($path))
												{
														common::record_key_started($file_key, %args) unless $check;
														my $status = undef;
														my $msg = "";
														if (!common::path_exists($orig_path))
														{
																$status = 1;
																$msg = "No file $orig_path";
														}
														else
														{
																my $cp_cmd = "ln -s";
																if ($copy_max < 0 || -s $orig_path < $copy_max)
																{
																		$cp_cmd = "cp";
																}
																foreach my $cmd ("rm -f $path", "$cp_cmd $orig_path $path")
																{
																		($status, $msg) = common::run_cmd($cmd, $check);
																		common::record_key_cmd($cmd, $file_key, %args);
																		if ($status)
																		{
																				last;
																		}
																}
														}
														common::clear_key_started($file_key, %args) unless $check;
														if ($status)
														{
																if ($check)
																{
																		print STDERR "Won't copy $orig_path; doesn't exist\n";
																}
																else
																{
																		print STDERR "Error: $msg\n";
																		common::record_key_error($file_key, $status, $msg, %args);
																}
														}
												}
										}
								}
						}
				}
		}
		else
		{
				my %cmd_keys_processed = (); 
				my %prev_cmds_to_defer = ();
				my %prev_meta_files_loaded = ();
				my $first_run = 1;
				my %level_to_cmd_key = ();
				my %cmd_key_to_level = ();
				foreach my $cmd_key (config::get_all_cmds())
				{
						if (skip_command($cmd_key))
						{
								next;
						}

						my $class_level = config::get_class_level($cmd_key);
						foreach my $ancestor_class_level (config::get_ancestor_keys($class_level))
						{
								$level_to_cmd_key{$ancestor_class_level}{$cmd_key} = 1;
								$cmd_key_to_level{$cmd_key}{$ancestor_class_level} = 1;
						}

						if (config::has_with($cmd_key))
						{
								foreach my $with (config::get_with($cmd_key))
								{
										#$level_to_cmd_key{$with}{$cmd_key} = 1;
								}
						}

						unless (config::is_no_custom($cmd_key))
						{
								my %needed_classes = config::get_needed_classes_for_cmd($cmd_key, 1);
								foreach my $needed_class (keys %needed_classes)
								{
										$level_to_cmd_key{$needed_class}{$cmd_key} = 1;
										$cmd_key_to_level{$cmd_key}{$needed_class} = 1;
								}
						}
				}

				my %cant_rebuild = ();
				my @meta_files_to_load = ();
				my %meta_files_to_load = ();

				while (1)
				{
						print STDERR "Starting\n" if $debug;

						my %cmds_to_defer = ();
						#determine levels to skip because cmds that are yet to run might alter meta file
						
						my %skip_meta_files = ();
						my %skip_meta_levels = ();
						my %skip_meta_load_levels = ();

						#get all to defer after some level
						my %level_to_with = ();
						foreach my $cmd_key (config::get_all_cmds())
						{
								if (config::has_run_with($cmd_key))
								{
										foreach my $with (config::get_run_with($cmd_key))
										{
												$level_to_with{$with}{$cmd_key} = 1;
										}
								}
						}

						foreach my $cmd_key (config::get_all_cmds())
						{

								if (skip_command($cmd_key))
								{
										next;
								}

								next if exists $cmd_keys_processed{$cmd_key};
								my %output_keys = config::get_output_keys_for_cmd($cmd_key);
								foreach my $output_key (keys %output_keys)
								{
										$skip_meta_files{$output_key} = 1;
										if (exists $meta_file_to_level{$output_key})
										{
												foreach my $meta_level (@{$meta_file_to_level{$output_key}})
												{
														foreach my $cmd_to_defer (keys %{$level_to_cmd_key{$meta_level}})
														{
																$cmds_to_defer{$cmd_to_defer} = 1;
														}
														#Commands explicitly told to defer if this level is deferred
														foreach my $cmd_to_defer (keys %{$level_to_with{$meta_level}})
														{
																$cmds_to_defer{$cmd_to_defer} = 1;
														}
												}
										}
								}
						}

						#if we had no levels to defer, and not the first run, then done
						if (!$first_run && scalar keys %prev_cmds_to_defer == 0)
						{
								last;
						}
						
						#see if made no progress
						my $made_progress = undef;

						print STDERR "Done getting defer\n" if $debug;
						#load in the rest of the meta files
						for (my $i = 0; $i <= $#meta_file_levels; $i++)
						{
								#the level for which the meta file loads instances
								my $meta_file_load_level = $meta_file_levels[$i];
								foreach my $meta_file_to_load (keys %{$level_to_meta_file{$meta_file_load_level}})
								{
										#the level at which the meta file is declared
										my $meta_file_level = config::get_class_level($meta_file_to_load);

										#We may need to skip:
										# 1. Loading a meta file if it loads instances that descend or are consistent with a skipped level
										# 2. Loading a meta file if it's level has been skipped
										# 3. Loading a meta file if it has been told to skip (a command may run that builds it)
										# 4. Commands if they have any dependencies at a level that loaded by a skipped meta file

										#skip it if any ancestor or consistent key is at skipped level
										my $skip_meta_level = 0;

										my @m = ($meta_file_level, $meta_file_load_level);
										my @s = (\%skip_meta_levels, \%skip_meta_load_levels);

										for (my $i = 0; $i <= $#m; $i++)
										{
												my $level_type = $m[$i];
												my %meta_file_load_ancestors = config::get_ancestor_keys($level_type);
												foreach my $ancestor_key (keys %meta_file_load_ancestors)
												{
														if ($s[$i]->{$ancestor_key})
														{

																$skip_meta_level = 1;
																last;
														}
												}
												if (config::has_consistent_key($level_type))
												{
														foreach my $consistent_key (config::get_consistent_key($level_type))
														{
																if ($s[$i]->{$consistent_key})
																{
																		$skip_meta_level = 1;
																		last;
																}
														}
												}
										}
										
										#okay to load
										if (!$skip_meta_files{$meta_file_to_load} && !$skip_meta_level)
										{
												next if $meta_files_to_load{$meta_file_to_load};
												#did something this iteration
												$made_progress = 1;
												$meta_files_to_load{$meta_file_to_load} = 1;
												push @meta_files_to_load, $meta_file_to_load;
												if (!$prev_meta_files_loaded{$meta_file_to_load})
												{
														$skip_meta_levels{$meta_file_load_level} = 1;
												}
										}
										else
										{
												#defer all of the commands that run at the level that was skipped
												foreach my $cmd_to_defer (keys %{$level_to_cmd_key{$meta_file_load_level}})
												{
														$cmds_to_defer{$cmd_to_defer} = 1;
												}
												#don't load any meta files that load at the level loaded by this file
												$skip_meta_levels{$meta_file_load_level} = 1;
												$skip_meta_load_levels{$meta_file_load_level} = 1;
										}
								}
						}
						print STDERR "Done getting meta files\n" if $debug;
						#defer all commands that depend on a deferred command
						if (%cmds_to_defer)
						{
								my %input_key_to_cmd_key = ();
								foreach my $cmd_key (config::get_all_cmds())
								{
										my %input_keys = config::get_input_keys_for_cmd($cmd_key);
										foreach my $input_key (keys %input_keys)
										{
												$input_key_to_cmd_key{$input_key}{$cmd_key} = 1;
										}
								}

								my @need_to_process = keys %cmds_to_defer;
								my %seen_cmds = ();
								while (@need_to_process)
								{
										my $cur_process_cmd = shift @need_to_process;
										my $cur_process_md5 = md5($cur_process_cmd);

										if ($seen_cmds{$cur_process_md5} || skip_command($cur_process_cmd))
										{
												next;
										}
										$seen_cmds{$cur_process_md5} = 1;

										my %output_keys = config::get_output_keys_for_cmd($cur_process_cmd);
										foreach my $output_key (keys %output_keys)
										{
												foreach my $cmd_key (keys %{$input_key_to_cmd_key{$output_key}})
												{
														push @need_to_process, $cmd_key;
														$cmds_to_defer{$cmd_key} = 1;
												}
										}
								}
						}

						print STDERR "Done deferring meta files\n" if $debug;

						foreach my $prev_cmd (keys %prev_cmds_to_defer)
						{
								$made_progress = 0 unless defined $made_progress;
								if (!exists $cmds_to_defer{$prev_cmd})
								{
										$made_progress = 1;
								}
						}

						if (defined $made_progress && !$made_progress)
						{
								last;
						}

						if (@meta_files_to_load)
						{
								print STDERR "Reinitializing...\n";
								print STDERR "Meta files: @meta_files_to_load\n" if $debug;
								my @meta_paths_to_load = convert_meta_files_to_meta_paths(@meta_files_to_load);
								print STDERR "Meta paths: @meta_paths_to_load\n" if $debug;
								meta::init(undef, @meta_paths_to_load);
						}
						print STDERR "Done initializing meta files\n" if $debug;
						#map from integer id to text cmd
						my @cmd_id_to_cmd = ();

						#get all of the commands
						my %cmd_ids = ();
						my %cmd_ids_reverse = ();

						#flip if any inputs errored out this round
						my %cmd_ids_with_input_errors = ();

						#map input_key to an array of bound commands
						my %input_key_to_cmd_id = ();

						#map commands to bound inputs
						my %cmd_id_to_inputs = ();

						#store these for later
						my %cmd_id_to_cmd_key = ();
						my %cmd_key_to_cmd_id = ();
						my %cmd_id_to_class_to_instances = ();
						my %cmd_id_to_instance_to_args = ();
						my %cmd_id_to_outputs = ();

						my $instance_to_object = \&meta::convert_to_object;
						my $instance_to_ancestors = \&meta::get_ancestor_instances;

						if (!$first_run)
						{
								print STDERR "Processing deferred cmds...\n";
						}

						my %cached_class_to_instances = ();
						my %cached_instance_to_args = ();

						foreach my $do_edges (0, 1)
						{
								my %seen_cmds = ();
								my %seen_meta_tables = ();

								if ($do_edges)
								{
										print STDERR "Building dependencies among commands...\n";
								}
								else
								{
										print STDERR "Gathering list of commands...\n";
								}


								my $cur_cmd_id = 0;
								my $start_time = undef;

								my @all_cmds = config::get_all_cmds();

								my $num_processed = -1;
								my $prev_percent_done = 0;

								foreach my $cmd_key (@all_cmds)
								{
										$num_processed++;

										my $percent_done = int(100 * $num_processed / scalar(@all_cmds));
										if (int($percent_done / 10) != int($prev_percent_done / 10))
										{
												print STDERR "$percent_done%.."
										}
										$prev_percent_done = $percent_done;

										my $class_level = config::get_class_level($cmd_key);

										if (exists $cmds_to_defer{$cmd_key})
										{
												next;
										}

										if ($cmd_keys_processed{$cmd_key})
										{
												print STDERR "Processed $cmd_key\n" if $debug_cmd_key{$cmd_key};
												next;
										}

										if ($do_edges)
										{
												$cmd_keys_processed{$cmd_key} = 1;
										}
										print STDERR "Gathering $cmd_key\n" if $debug;

										next if skip_command($cmd_key);

										$start_time = gettimeofday();
										my %needed_classes = config::get_needed_classes_for_cmd($cmd_key);

										my $query_prop_result = query_props($cmd_key, undef, undef, $instance_to_object, $instance_to_ancestors);
                    my $good_query = $query_prop_result->[0];
                    my $needed_props = $query_prop_result->[1];

									INSTANCE:
										foreach my $to_run_instance (@{&meta::get_instances_in_class($class_level)})
										{
												print STDERR "Instance $to_run_instance\n" if $debug_cmd_key{$cmd_key};
												my $cur_time = undef;
												my $class_to_instances = {};
												my $instance_to_args = {};

												my %output_key_to_optional = config::get_output_keys_for_cmd($cmd_key);
												foreach my $output_key (keys %output_key_to_optional)
												{
														if (meta::has_prop_value($to_run_instance, $output_key) || (meta::should_skip_key($to_run_instance, $output_key)))
														{
																next INSTANCE;
														}
												}

												foreach my $do_skip (1, 0)
												{
														if ($do_skip && config::has_skip_if_value($cmd_key))
														{
																my $has_value = eval_if_value(config::get_skip_if_value($cmd_key), $to_run_instance);
																print STDERR "Skipping due to " . (config::get_skip_if_value($cmd_key)) . "\n" if $has_value && $debug_cmd_key{$cmd_key};
																next INSTANCE if $has_value;
														}
														elsif (!$do_skip && config::has_run_if_value($cmd_key))
														{
																my $has_value = eval_if_value(config::get_run_if_value($cmd_key), $to_run_instance);
																print STDERR "Skipping due to no " . (config::get_run_if_value($cmd_key)) . "\n" if !$has_value && $debug_cmd_key{$cmd_key};
																next INSTANCE unless $has_value;
														}
												}

												my $cmd = undef;
												my $cmd_id = $cur_cmd_id++;

												if ($do_edges)
												{
														$class_to_instances = thaw($cmd_id_to_class_to_instances{$cmd_id});
														$instance_to_args = thaw($cmd_id_to_instance_to_args{$cmd_id});
												}
												else
												{
														my $cache_key = join("#", ($cmd_id,$to_run_instance));
														my $cur_good_query = $good_query;
														my %cur_needed_props = %{$needed_props};
														my $cur_needed_props = \%cur_needed_props;

														while (1)
														{
																my %last_needed_props = ();
																if (defined $cur_needed_props)
																{
																		%last_needed_props = %{$cur_needed_props};
																}
																$class_to_instances = {};
																$instance_to_args = {};

																my $consistent_prop = undef;
																if (config::has_consistent_prop_key($cmd_key))
																{
																		$consistent_prop = config::get_consistent_prop_key($cmd_key);
																}

																foreach my $class (keys %needed_classes)
																{
																		my @consistent_with = ([$to_run_instance, 1, undef]);
																		if ($needed_classes{$class})
																		{
																				push @consistent_with, [defined $consistent_prop ? $to_run_instance : undef, 0, $consistent_prop];
																		}

																		print STDERR "Needed class $class\n" if $debug_cmd_key{$cmd_key};
																		foreach my $consistent_with_ref (@consistent_with)
																		{
																				my $consistent_with = $consistent_with_ref->[0];
																				my $use_consistent = $consistent_with_ref->[1];
																				my $cur_consistent_prop = $consistent_with_ref->[2];

																				my @old_only_res = meta::get_only_res();
																				if (!$use_consistent)
																				{
																						#technically, for this class/cmd combo everything is consistent with the current instance
																						#because meta.pm filters out instances not an ancestor or descendent of this class at the start
																						#it could have filtered out instances consistent with this class/cmd combo
																						meta::set_only_res();
																				}

																				$class_to_instances->{$use_consistent}->{$class} = meta::get_instances_in_class($class, $consistent_with, !$use_consistent, $cur_consistent_prop);

																				foreach my $instance (@{$class_to_instances->{$use_consistent}->{$class}})
																				{
																						print STDERR "Instance $instance, needed " . (defined $cur_needed_props->{$class} ? (join (",", @{$cur_needed_props->{$class}})) : "undef") . "\n" if $debug_cmd_key{$cmd_key};

																						$instance_to_args->{$use_consistent}->{$instance} = meta::get_props_as_args($instance, $consistent_with, $cur_needed_props->{$class});
																				}
																				if (!$use_consistent)
																				{
																						meta::set_only_res(@old_only_res);
																				}
																		}
																}

																if ($cur_good_query)
																{
																		last;
																}
																else
																{
																		my $cur_query_result = query_props($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object, $instance_to_ancestors);
																		$cur_good_query = $cur_query_result->[0];
																		$cur_needed_props = $cur_query_result->[1];
																		if (!$cur_good_query)
																		{
																				my $match = 0;
																				if (scalar keys %{$cur_needed_props} == scalar keys %last_needed_props)
																				{
																						$match = 1;
																						foreach my $cur_needed_class (keys %{$cur_needed_props})
																						{
																								my $cur_match = 0;
																								if (scalar @{$cur_needed_props->{$cur_needed_class}} == scalar @{$last_needed_props{$cur_needed_class}})
																								{
																										$cur_match = 1;
																										my @sorted_needed_props = @{$cur_needed_props->{$cur_needed_class}};
																										my @sorted_last_needed_props = @{$last_needed_props{$cur_needed_class}};
																										for (my $i = 0; $i <= $#sorted_needed_props; $i++)
																										{
																												$cur_match = 0 if $sorted_needed_props[$i] ne $sorted_last_needed_props[$i];
																										}
																										if (!$cur_match)
																										{
																												#each call should be superset of previous
																												die "Bug: successive calls to query needed props resulted in inconsistency\n";
																										}
																								}
																								$match = $match && $cur_match;
																						}
																				}
																				if ($match)
																				{
																						#the query engine had some undefined values, but 
																						#it did not request any additional props
																						#so the values it needs are actually undefined
																						#(rather than not passed into it)
																						last;
																				}
																		}
																}
														} 
														
														#$cached_class_to_instances{$cache_key} = $class_to_instances;
												}

												if ($do_edges)
												{
														next unless defined $cmd_id_to_cmd[$cmd_id];
														$cmd = $cmd_id_to_cmd[$cmd_id];
												}
												else
												{
														eval
														{
																if (config::is_meta_table($cmd_key))
																{
																		$cmd = config::get_meta_table_string($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object, $instance_to_ancestors);
																		#cmd is seen for meta table only if its text and output matches
																		my %outputs = config::get_outputs_for_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object);
																		my %output_paths = ();
																		foreach my $output_key (keys %outputs)
																		{
																				foreach my $args_ref (@{$outputs{$output_key}})
																				{
																						my $output_path  = config::get_path($output_key, %{$args_ref});
																						$output_paths{$output_path} = 1
																				}
																		}													
																		if (scalar keys %output_paths != 1)

																		{
																				die "Error: Meta table cmd $cmd_key does not have exactly one output\n";
																		}
																		my @output_paths = keys %output_paths;
																		my $cmd_md5 = md5($cmd);
																		if ($seen_meta_tables{$cmd_md5}{$output_paths[0]})
																		{
																				warn "Warning: Processing same cmd twice ($cmd_key):\t$cmd\n";
																		}
																		$seen_meta_tables{$cmd_md5}{$output_paths[0]} = 1;
																}
																else
																{
																		$cmd = config::bind_instances_to_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object);
																		my $cmd_md5 = md5($cmd);
																		if ($seen_cmds{$cmd_md5})
																		{
																				warn "Warning: Processing same cmd twice ($cmd_key same as $seen_cmds{$cmd}):\t$cmd\n";
																		}
																		$seen_cmds{$cmd_md5} = $cmd_key;
																}
														};
														if ($@)
														{
																if ($@ eq main::trap_signal())
																{
																		die $@;
																}
																warn "Warning: Skipping $cmd_key for $to_run_instance: $@\n";
																next;
														}

														$cmd_id_to_cmd[$cmd_id] = $cmd;
												}

												#may need a more efficient way of checking for intersection
												#for example, may not need to iterate over all arguments

												if ($do_edges)
												{
														my %outputs = config::get_outputs_for_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object);

														#first get all of the commands with the same key
														foreach my $output_key (keys %outputs)
														{
																if (exists $input_key_to_cmd_id{$output_key})
																{
																		foreach my $cur_cmd_id (keys %{$input_key_to_cmd_id{$output_key}})
																		{
																				next if $cmd_ids{$cmd_id}{$cur_cmd_id};
																				#check all of the outputs

																				foreach my $args_ref (@{$outputs{$output_key}})
																				{
																						my %args = %{$args_ref};
																						my $output_path = config::get_path($output_key, %args);
																						if ($cmd_id_to_inputs{$cur_cmd_id}{$output_path})
																						{
																								#cmd_id must run before cur_cmd_id
																								my $is_optional = $input_key_to_cmd_id{$output_key}{$cur_cmd_id};
																								$cmd_ids{$cmd_id}{$cur_cmd_id} = $is_optional;
																								$cmd_ids_reverse{$cur_cmd_id}{$cmd_id} = $is_optional;
																								last;
																						}
																				}

																		}
																}
														}

												}
												else
												{
														my %inputs = config::get_inputs_for_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object);
														my %input_key_to_optional = config::get_input_keys_for_cmd($cmd_key);
														foreach my $input_key (keys %inputs)
														{
																$input_key_to_cmd_id{$input_key}{$cmd_id} = $input_key_to_optional{$input_key};

																foreach my $args_ref (@{$inputs{$input_key}})
																{
																		my %args = %{$args_ref};
																		my $input_path = config::get_path($input_key, %args);
																		$cmd_id_to_inputs{$cmd_id}{$input_path} = 1;
																		
																}
														}

														$cmd_ids{$cmd_id} = {};
														$cmd_id_to_cmd_key{$cmd_id} = $cmd_key;
														$cmd_key_to_cmd_id{$cmd_key} = $cmd_id;

														$cmd_id_to_class_to_instances{$cmd_id} = freeze($class_to_instances);
														$cmd_id_to_instance_to_args{$cmd_id} = freeze($instance_to_args);
												}
										}
										printf STDERR ("Total Elapsed Time: %.3g seconds\n", gettimeofday() - $start_time) if $debug;
										$start_time = gettimeofday();

								}
								print STDERR "done\n";
						}

						my @cmd_id_list = ();
						print STDERR "Determining order of commands...\n";
						foreach my $cur_cmd_id (keys %cmd_ids)
						{
								my $num_dependencies = 0;
								if (exists $cmd_ids_reverse{$cur_cmd_id})
								{
										$num_dependencies = scalar keys %{$cmd_ids_reverse{$cur_cmd_id}};
								}

								$cmd_id_list[$num_dependencies]->{$cur_cmd_id} = 1;
						}

						my $round = 1;

						my %running_cmd_ids = ();
						my %failed_submit_cmd_ids = ();
						my %times_restarted_cmd_ids = ();
						my %restarted_long_cmd_ids = ();
						my %times_restarted_mem_cmd_ids = ();
						my %last_exit_status_cmd_ids = ();
						my %all_started_cmd_ids = ();

						my %original_mod_times = ();

						my $max_jobs_allowed = ($bsub && !$check) ? common::get_max_jobs_allowed() : undef;
						my $max_long_jobs_allowed = ($bsub && !$check) ? common::get_max_jobs_allowed("long") : undef;
						my $max_short_jobs_allowed = ($bsub && !$check) ? common::get_max_jobs_allowed("short") : undef;

						print STDERR "Starting execution...\n";

						while (@cmd_id_list)
						{
								unless ($cmd_id_list[0])
								{
										my $err = "Have unrun commands but none with zero dependencies\nThis could be a bug in the code but it is more likely that you have defined command with circular dependencies:\n";
										foreach (my $i = 0; $i <= $#cmd_id_list; $i++)
										{
												$err .= "$i:";
												my @cur_cmd_ids = keys %{$cmd_id_list[$i]};

												$err .= join("\n", map {my $o = $_; $cmd_id_to_cmd_key{$o} . ": " . join(" ", map {$cmd_id_to_cmd_key{$_}} keys %{$cmd_ids_reverse{$o}})} @cur_cmd_ids);
												$err .= "\n";
										}
										die $err;
								}
								my @cur_cmd_ids = keys %{$cmd_id_list[0]};

								my $num_started = 0;

								my %started_cmd_ids = ();
								my %touch_cmd_ids = ();
								my %finished_cmd_ids = ();

								my $submitted_this_round = 0;

								#check how many jobs are already running
								my $already_running = 0;
								my $already_short_running = 0;
								my $already_long_running = 0;
								if (defined $max_jobs_allowed || defined $max_short_jobs_allowed || defined $max_long_jobs_allowed)
								{
										eval
										{
												$already_running = common::get_num_running_jobs();
												$already_short_running = common::get_num_running_jobs("short");
												$already_long_running = common::get_num_running_jobs("long");
										};
										if ($@)
										{
												my $to_match = "^" . main::trap_signal();
												if ($@ =~ $to_match)
												{
														die $@;
												}
												warn "Warning: Can't check how many jobs are running; going ahead with submission: get running jobs returned $@";
										}
								}
								foreach my $cur_cmd_id (@cur_cmd_ids)
								{
										die "Bug: Error in internal cmd_id to cmd mapping: no cmd for $cur_cmd_id" unless $cur_cmd_id >= 0 && $cur_cmd_id <= $#cmd_id_to_cmd;
										my $cur_cmd = $cmd_id_to_cmd[$cur_cmd_id];

										my $cmd_key = $cmd_id_to_cmd_key{$cur_cmd_id};
										my $batch_size = get_bsub_batch_size($cmd_key);

										if ((defined $max_jobs_allowed && ($num_started / $batch_size + $already_running) >= $max_jobs_allowed) || 
												(defined $max_short_jobs_allowed && ($num_started / $batch_size + $already_short_running) >= $max_short_jobs_allowed) || 
												(defined $max_long_jobs_allowed && ($num_started / $batch_size + $already_long_running) >= $max_long_jobs_allowed))
										{
												#stop trying to do anything more if we're going to start more than allowed
												#this is conservative; we may not bsub all of these but if that is the case
												#then on the next iteration we will just start them then
												last;
										}

										next if $running_cmd_ids{$cur_cmd_id};

										#now run it
										print STDERR "Running $cmd_key...\n" if $debug;

										my $should_bsub = $bsub && !config::is_local($cmd_key);

										die "Bug: No cmd_key for cmd_id $cur_cmd_id\n" unless defined $cmd_key;
										
										my $class_to_instances = thaw($cmd_id_to_class_to_instances{$cur_cmd_id});
										my $instance_to_args = thaw($cmd_id_to_instance_to_args{$cur_cmd_id});

										my %input_key_to_optional = config::get_input_keys_for_cmd($cmd_key);
										my %output_key_to_optional = config::get_output_keys_for_cmd($cmd_key);

										my %input_key_to_is_list = config::get_input_keys_for_cmd($cmd_key, "do_is_list");

										my %inputs = config::get_inputs_for_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object);
										my %outputs = config::get_outputs_for_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object);

										#Changing this to default to 1: Changed because if no inputs want to be able to run by default

										#my $can_run = undef;
										my $can_run = $cmd_ids_with_input_errors{$cur_cmd_id} ? 0 : 1;
										my $needs_run = $force && scalar keys %outputs > 0 ? 1 : 0;

										my $reason_cant = undef;
										my $upstream_cant = undef;
										if (!$can_run)
										{
												$upstream_cant = 1;
												$reason_cant = "Dependent command had an input error:$cmd_ids_with_input_errors{$cur_cmd_id}";
										}

										print STDERR "DEBUG: Cmd key $cmd_key initial status: CAN $can_run, NEEDS $needs_run\n" if $debug_cmd_key{$cmd_key};

										#set if an input to this cmd failed during this run
										#or, if any of its inputs failed during this run
										my $cant_rebuild_input = undef;


										if ($try_it == 0)
										{
												foreach my $force_cmd_text (@force_cmd_texts)
												{
														if ($cur_cmd =~ /$force_cmd_text/)
														{
																$needs_run = 1;
																last;
														}
												}
										}

										#see whether cmd needs to be updated
										my $update_cmd_different = 0;
										foreach my $update_cmd_text_different (@update_cmd_text_different)
										{
												if ($cur_cmd =~ /$update_cmd_text_different/)
												{
														$update_cmd_different = 1;
														last;
												}
										}
										foreach my $update_cmd_key_different (@update_cmd_key_different)
										{
												if ($cmd_key =~ /$update_cmd_key_different/)
												{
														$update_cmd_different = 1;
														last;
												}
										}

										my $most_recent_input = undef;
										my %input_to_mod_times = ();
										my %input_path_to_is_list = ();
										my %input_path_to_input_key = ();
										my $will_build_an_input = undef;

										my $all_outputs_skipped = 0;
										foreach my $skip_cmd_text (@skip_cmd_texts)
										{
												if ($cur_cmd =~ /$skip_cmd_text/)
												{
														$all_outputs_skipped = 1;
														last;
												}
										}

										if ($all_outputs_skipped)
										{
												print STDERR "DEBUG: All outputs skipped for $cmd_key\n" if $debug_cmd_key{$cmd_key};
												$needs_run = 0;
										}
										else
										{
												foreach my $input_key (keys %inputs)
												{
														foreach my $args_ref (@{$inputs{$input_key}})								
														{
																my %args = %{$args_ref};
																my $input_path = config::get_path($input_key, %args);
																my $is_optional = config::optional_input($input_key) || $input_key_to_optional{$input_key};
																if ($cant_rebuild{$input_path})
																{
																		unless ($is_optional)
																		{
																				#an input upstream failed
																				#so this needs rebuilding
																				$needs_run = 1;
																				$can_run = 0;
																				$upstream_cant = 1;
																				$reason_cant = "Couldn't rebuild $input_path";
																				print STDERR "DEBUG: Can't rebuild input $input_path for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																				$cant_rebuild_input = 1;
																		}
																}

																if ($check && $paths_will_build{$input_path})
																{
																		$will_build_an_input = 1;
																}

																if (common::path_exists($input_path))
																{
																		my $mod_time = common::get_mod_time($input_path);
																		$input_to_mod_times{$input_path} = $mod_time;
																		$input_path_to_input_key{$input_path} = $input_key;
																		if (!defined $most_recent_input || $mod_time > $most_recent_input)
																		{
																				print STDERR "DEBUG: $input_path most recent for $cmd_key\n" if $debug_cmd_key{$cmd_key};																		
																				$most_recent_input = $mod_time;

																		}

																		if ($input_key_to_is_list{$input_key} && !$ignore_input_list)
																		{
																				$input_path_to_is_list{$input_path} = 1;
																				if (open(IN, $input_path))
																				{
																						while (<IN>)
																						{
																								chomp;
																								my @potential_paths = split;
																								foreach my $potential_path (@potential_paths)
																								{
																										if (!common::path_exists($potential_path))
																										{
																												$can_run = 0;
																												$reason_cant = "$potential_path did not exist";
																										}
																										elsif (common::check_path_error($potential_path))
																										{
																												$can_run = 0;
																												$reason_cant = "$potential_path had an error";
																										}
																										elsif (common::check_path_started($potential_path))
																										{
																												$can_run = 0;
																												$reason_cant = "$potential_path was running";
																										}
																										elsif ($cant_rebuild{$potential_path})
																										{
																												$upstream_cant = 1;
																												$reason_cant .= "$potential_path was not ready: earlier error upstream";
																										}
																										else
																										{
																												my $cur_mod_time = common::get_mod_time($_);
																												$input_to_mod_times{$potential_path} = $cur_mod_time;
																												if ($cur_mod_time > $most_recent_input)
																												{
																														print STDERR "DEBUG: $potential_path most recent for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																														$most_recent_input = $cur_mod_time;
																												}
																										}
																								}
																						}
																						close IN;
																				}
																		}

																}

																if (common::check_key_started($input_key, %args))
																{
																		#input is currently running, or started path did not get removed
																		#so this one needs to run but can't, and can't because the input couldn't be rebuilt (because it was already running)
																		$can_run = 0;
																		$reason_cant = "Input $input_path was running";
																		$needs_run = 1;
																		print STDERR "DEBUG: Input still started for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																		$cant_rebuild_input = 1;
																}
																elsif (should_stop_after($input_key) || ((common::check_key_error($input_key, %args) || !common::path_exists($input_path) || $cant_rebuild{$input_path}) && (!$check || !$paths_will_build{$input_path})))
																{
																		unless ($is_optional)
																		{
																				print STDERR "DEBUG: Input $input_path not ready for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																				$can_run = 0;
																				$reason_cant = "Input $input_path was not ready: ";
																				if (should_stop_after($input_key))
																				{
																						$reason_cant .= "stopping after building";
																				}
																				elsif (common::check_key_error($input_key, %args))
																				{
																						$reason_cant .= "had error";
																				}
																				elsif (!common::path_exists($input_path))
																				{
																						$reason_cant .= "did not exist";
																				}
																				elsif ($cant_rebuild{$input_path})
																				{
																						$upstream_cant = 1;
																						$reason_cant .= "earlier error upstream";
																				}

																				if ($cant_rebuild{$input_path})
																				{
																						$cant_rebuild_input = 1;
																						$needs_run = 1;
																				}
																		}
																}
																else
																{
																		print STDERR "DEBUG: Good to go for for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																		$can_run = 1 unless defined $can_run;
																}
														}
												}
												my $should_touch = scalar @touch_keys;

												foreach my $output_key (keys %outputs)
												{
														if ($should_touch && !should_touch($output_key))
														{
																$should_touch = 0;
														}

														if ($try_it == 0 && should_force($output_key))
														{
																$needs_run = 1;
														}

														my $is_optional = config::optional_output($output_key) || $output_key_to_optional{$output_key};
														foreach my $args_ref (@{$outputs{$output_key}})								
														{
																my %args = %{$args_ref};
																my $output_path = config::get_path($output_key, %args);
																push @{$cmd_id_to_outputs{$cur_cmd_id}}, $output_path;


																#if the output is a symlink, want time of symlink
																#in contrast, for inputs take maximum modification time
																#we are assuming that if the output is a symlink, the command created the symlink last time it was run
																#therefore, date of symlink signifies last time command was run
																my $output_mod_time = common::get_mod_time($output_path, 1);
																$original_mod_times{$output_path} = $output_mod_time;

																next if $is_optional;
																next if ($needs_run && !config::is_meta_table($cmd_key));

																#see if directory exists
																unless ($dynamic_mkdir || -e config::get_dir($output_key, %args))
																{
																		print STDERR "DEBUG: No dir for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																		$reason_cant = "Output directory was not available";
																		$can_run = 0;
																}

																if (common::check_key_error($output_key, %args))
																{
																		my $mod_time = common::check_key_error_time($output_key, %args);

																		print STDERR "DEBUG: Output had error for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																		if (!defined $most_recent_input || $most_recent_input > $mod_time)
																		{
																				$needs_run = 1;
																		}
																		else
																		{
																				$needs_run = 1;
																		}
																		$should_touch = 0;
																}
																elsif (common::check_key_started($output_key, %args))
																{
																		print STDERR "DEBUG: Output started for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																		if ($force_started)
																		{
																				$needs_run = 1;
																		}
																		else
																		{
																				$needs_run = 1;
																				$can_run = 0;
																				$reason_cant = "Output $output_path was running";
																		}
																		$should_touch = 0;
																}
																elsif (!common::path_exists($output_path))
																{
																		print STDERR "DEBUG: Output $output_path does not exist for $cmd_key\n" if $debug_cmd_key{$cmd_key};
																		$needs_run = 1;
																		$should_touch = 0;
																}
																elsif ($check && $will_build_an_input)
																{
																		print STDERR "DEBUG: Will build an input\n" if $debug_cmd_key{$cmd_key};
																		$needs_run = 1;
																}
																else
																{
																		my $mod_time = $output_mod_time;
																		if (defined $most_recent_input && $most_recent_input > $mod_time)
																		{
																				print STDERR "DEBUG: Input more recent for $cmd_key\n" if $debug_cmd_key{$cmd_key};																		
																				$needs_run = 1;
																				#check if inputs are identical

																				my $input_key_to_input_paths = {};
																				my $has_a_list = 0;
																				foreach my $input_to_check (keys %input_to_mod_times)
																				{
																						my $cur_input_key = $input_path_to_input_key{$input_to_check};
																						unless (defined $cur_input_key)
																						{
																								$has_a_list = 1;
																						}
																						elsif ($input_to_mod_times{$input_to_check} > $mod_time)
																						{
																								push @{$input_key_to_input_paths->{$cur_input_key}}, $input_to_check;
																						}
																						if ($input_path_to_is_list{$input_to_check} && !$ignore_input_list)
																						{
																								if (open(IN, $input_to_check))
																								{
																										while (<IN>)
																										{
																												chomp;
																												my @potential_paths = split;
																												foreach my $potential_path (@potential_paths)
																												{
																														if ($input_to_mod_times{$potential_path} > $mod_time)
																														{
																																#if any file in the list is newer, then assume it is different (not implemented right now to record md5s for these)
																																$has_a_list = 1;
																														}
																												}
																										}
																								}
																						}
																				}

																				unless ($has_a_list || common::check_key_inputs_different($output_key, $input_key_to_input_paths, %args))
																				{
																						$should_touch = 1;
																				}
																		}

																		if (config::is_meta_table($cmd_key))
																		{
																				$should_touch = 0;
																				#compare the strings
																				my $verified_same = 0;
																				if (open IN, $output_path)
																				{
																						my @lines = <IN>;
																						my $old_text = join("", sort @lines);
																						$old_text =~ s/^\s*//;
																						$old_text =~ s/\s*$//;
																						my $new_text = join("\n", sort split("\n", $cur_cmd));
																						$new_text =~ s/^\s*//;
																						$new_text =~ s/\s*$//;
																						if ($old_text eq $new_text)
																						{
																								$verified_same = 1;
																						}
																						else
																						{

																						}
																				}
																				$needs_run = 1 unless $verified_same;
																		}
																}

																if (!$ignore_cmd_different && !$update_cmd_different && !config::is_meta_table($cmd_key) && common::check_key_cmd_different($cur_cmd, $output_key, %args))
																{
																		if ($debug_cmd_key{$cmd_key})
																		{
																				print STDERR "DEBUG: Cmd different for for $cmd_key\n";
																		}
																		$needs_run = 1;
																		$should_touch = 0;
																}																

																last if $needs_run;
														}
														last if $needs_run;
												}
												if ($should_touch)
												{
														$touch_cmd_ids{$cur_cmd_id} = 1;
												}

										}

										print STDERR "Needs: $needs_run. Can: $can_run.\n" if $debug || $debug_cmd_key{$cmd_key};
										if ($needs_run)
										{
												if (!config::is_local($cmd_key) && $ignore_bsub)
												{
														$can_run = 0;
														$reason_cant = "Ignored $bsub commands";
														$cant_rebuild_input = 1;
												}

												foreach my $output_key (keys %outputs)
												{
														foreach my $args_ref (@{$outputs{$output_key}})
														{

																my $output_path = config::get_path($output_key, %{$args_ref});
																if ($can_run)
																{
																		unless (should_preserve($output_key) && exists $original_mod_times{$output_path} && defined $original_mod_times{$output_path})
																		{
																				$paths_will_build{$output_path} = 1;
																		}
																		push @{$all_started_cmd_ids{$cur_cmd_id}{$output_key}}, $args_ref;
																		
																		$started_cmd_ids{$cmd_key}{$cur_cmd_id} = 1;
																		$num_started++;
																}

																if (!$can_run)
																{
																		$cant_rebuild{$output_path} = 1;
																		if (!$upstream_cant)
																		{
																				$need_but_cant{$output_path} = $reason_cant;
																		}
																}
														}
												}
												last if $started_cmd_ids{$cmd_key}{$cur_cmd_id} && !$check && !$bsub;
										}
										else
										{
												if ($update_cmd_different && (!$needs_run || !$can_run))
												{
														foreach my $output_key (keys %outputs)
														{
																foreach my $args_ref (@{$outputs{$output_key}})
																{
																		if (common::check_key_cmd_different($cur_cmd, $output_key, %{$args_ref}))
																		{
																				print STDERR "Updating command for $cmd_key\n";
																				unless ($check)
																				{
																						common::record_key_cmd($cur_cmd, $output_key, %{$args_ref});
																				}
																		}
																}
														}
												}
												if ($will_build_an_input)
												{
														print STDERR "Warning: it does not appear like cmd key $cmd_key needs to run even though it had an input built during this run\n";
												}
										}
										#if we didn't start it, it must have either
										# 1. !$needs_run
										# 2. !$can_run
										# 3. $needs_run && $can_run but with no outputs
										if (!$started_cmd_ids{$cmd_key}{$cur_cmd_id})
										{
												$finished_cmd_ids{$cur_cmd_id} = [0, ""];
										}
								}
								print STDERR "Done all cmds\n" if $debug;

								#record all of those that started
								my $num_this_round = $num_started;

								my @to_run_local = ();

								foreach my $cmd_key (keys %started_cmd_ids)
								{
										my %to_run_bsub = ();

										foreach my $cur_cmd_id (keys %{$started_cmd_ids{$cmd_key}})
										{
												die "Bug: Error in internal cmd_id to cmd mapping: no cmd for $cur_cmd_id" unless $cur_cmd_id >= 0 && $cur_cmd_id <= $#cmd_id_to_cmd;
												my $cur_cmd = $cmd_id_to_cmd[$cur_cmd_id];

												my $should_bsub = $bsub && !config::is_local($cmd_key);

												if ($touch_cmd_ids{$cur_cmd_id})
												{
														$should_bsub = 0;
														$cur_cmd = "";
														my @hidden_output_classes = config::get_all_hidden_output_class();
														foreach my $output_key (keys %{$all_started_cmd_ids{$cur_cmd_id}})
														{
																foreach my $args_ref (@{$all_started_cmd_ids{$cur_cmd_id}{$output_key}})
																{
																		$cur_cmd .= " && " if $cur_cmd;
																		my $output_path = config::get_path($output_key, %{$args_ref});
																		if (-l $output_path)
																		{
																				$cur_cmd .= "rm -f $output_path && ln -s " . readlink($output_path) . " $output_path";
																		}
																		else
																		{
																				$cur_cmd .= "touch -c $output_path";
																		}
																		foreach my $hidden_output_key (@hidden_output_classes)
																		{
																				my $hidden_output_ext = config::get_value($hidden_output_key);
																				if ($output_path =~ /$hidden_output_ext$/ && config::has_update_ext_value($hidden_output_key))
																				{
																						my $hidden_output_path = "$output_path." . config::get_update_ext_value($hidden_output_key);
																						if (-e $hidden_output_path)
																						{
																								$cur_cmd .= " && " if $cur_cmd;
																								if (-l $hidden_output_path)
																								{
																										$cur_cmd .= "rm -f $hidden_output_path && ln -s " . readlink($hidden_output_path) . " $hidden_output_path";
																								}
																								else
																								{
																										$cur_cmd .= "touch -c $hidden_output_path";
																								}
																						}
																				}
																		}
																}
														}
														$cmd_id_to_cmd[$cur_cmd_id] = $cur_cmd;
												}

												if (config::is_meta_table($cmd_key))
												{
														my @outputs = @{$cmd_id_to_outputs{$cur_cmd_id}};
														if (scalar @outputs > 1)
														{
																die "Error: more than one output for $cmd_key\n";
														}
														print STDERR "Building $outputs[0]\n";
												}

												if ($check)
												{
														unless (config::is_meta_table($cmd_key))
														{
																common::run_cmd($cur_cmd, $check, undef, undef, $cmd_key);
														}
														$finished_cmd_ids{$cur_cmd_id} = [0, ""];
												}
												else
												{
														foreach my $output_key (keys %{$all_started_cmd_ids{$cur_cmd_id}})
														{
																foreach my $args_ref (@{$all_started_cmd_ids{$cur_cmd_id}{$output_key}})
																{
																		if ($dynamic_mkdir)
																		{
																				my $output_dir_key = config::get_dir_key($output_key);
																				my $output_dir = config::get_dir($output_key, %{$args_ref});
																				if (!-e $output_dir)
																				{
																						common::run_mkdir_chmod($output_dir_key, $check, %{$args_ref});
																				}
																		}
																		common::record_key_started($output_key, %{$args_ref});
																		my $output_path = config::get_path($output_key, %{$args_ref});
																		if ($touch_cmd_ids{$cur_cmd_id} && !$touch_mod_cmd)
																		{
																				common::append_key_cmd($cur_cmd, $output_key, %{$args_ref});
																		}
																		else
																		{
																				if (-l $output_path || -e $output_path)
																				{
																						unlink($output_path) unless $check;
																				}
																				common::record_key_cmd($cur_cmd, $output_key, %{$args_ref});
																		}
																}
														}

														if ($num_this_round < $min_num_bsub && $already_running == 0)
														{
																$should_bsub = 0;
														}
														if ($should_bsub)
														{
																if (@only_bsub)
																{
																		$should_bsub = 0;
																		foreach my $only_bsub (@only_bsub)
																		{
																				if ($cmd_key =~ $only_bsub)
																				{
																						$should_bsub = 1;
																						last;
																				}
																		}
																}
																if (@skip_bsub && $should_bsub)
																{
																		foreach my $skip_bsub (@skip_bsub)
																		{
																				if ($cmd_key =~ $skip_bsub)
																				{
																						$should_bsub = 0;
																						last;
																				}
																		}
																}
														}

														if (config::is_meta_table($cmd_key))
														{
																my $status = 0;
																my $msg = "";
																eval
																{
																		my @outputs = @{$cmd_id_to_outputs{$cur_cmd_id}};
																		if (scalar @outputs > 1)
																		{
																				die "Error: more than one output for $cmd_key\n";
																		}
																		my $output_path = $outputs[0];
																		open OUT, ">$output_path" or die "Can't write to $output_path\n";
																		print OUT $cur_cmd;
																		close OUT;

																};
																if ($@)
																{
																		if ($@ eq main::trap_signal())
																		{
																				die $@;
																		}
																		$status = 1;
																		$msg = $@;
																}
																$finished_cmd_ids{$cur_cmd_id} = [$status, $msg];
														}
														elsif (!$should_bsub)
														{
																push @to_run_local, $cur_cmd_id;
														}
														else
														{
																my $force_long = $no_short;
																my $mem_expand = undef;
																if ($times_restarted_cmd_ids{$cur_cmd_id})
																{
																		print STDERR "Restarting failed command...\n";
																		#check to increase memory

																		#does current error require scaling?
																		my $do_expand = undef;
																		if (config::get_restart_mem($cmd_key))
																		{
																				$do_expand = 1;
																		}
																		foreach my $restart_mem (@restart_mem_cmd)
																		{
																				if ($cmd_key =~ $restart_mem)
																				{
																						$do_expand = 1;
																				}
																				last if $do_expand;
																		}
																		if (!$do_expand && exists $last_exit_status_cmd_ids{$cur_cmd_id} && $restart_mem_code{$last_exit_status_cmd_ids{$cur_cmd_id}})
																		{
																				$do_expand = 1;
																		}

																		#how many times did we expand?
																		if ($do_expand)
																		{
																				$times_restarted_mem_cmd_ids{$cur_cmd_id} = exists $times_restarted_mem_cmd_ids{$cur_cmd_id} ? $times_restarted_mem_cmd_ids{$cur_cmd_id} + 1 : 1;
																		}
																		if (exists $times_restarted_mem_cmd_ids{$cur_cmd_id})
																		{
																				$mem_expand = $scale_restart_mem ** $times_restarted_mem_cmd_ids{$cur_cmd_id};
																		}

																		#check to restart long

																		#does current error require restart long?
																		$force_long = 1 if config::get_restart_long($cmd_key);
																		foreach my $restart_long (@restart_long_cmd)
																		{
																				if ($cmd_key =~ $restart_long)
																				{
																						$force_long = 1;
																						last;
																				}
																		}
																		if (!$force_long && exists $last_exit_status_cmd_ids{$cur_cmd_id} && $restart_long_code{$last_exit_status_cmd_ids{$cur_cmd_id}})
																		{
																				$force_long = 1;
																		}
																		if ($force_long)
																		{
																				$restarted_long_cmd_ids{$cur_cmd_id} = 1;
																		}

																		#restart long if ever had to
																		if ($restarted_long_cmd_ids{$cur_cmd_id})
																		{
																				$force_long = 1;
																		}
																}
																my $long_type = $force_long ? "LONG" : ($all_short ? "SHORT" : undef);
																push @{$to_run_bsub{$cmd_key}}, [$cur_cmd_id, $long_type, $mem_expand];

														}
												}
										}

										#determine whether we need to batch any

										foreach my $to_run_cmd_key (keys %to_run_bsub)
										{
												my @to_run_refs = @{$to_run_bsub{$to_run_cmd_key}};

												#the cmd_ids that must be pulled into this one
												my %cmd_id_to_batch_cmd_ids = ();
												#the cmd_id that this one was batched with
												my %batch_cmd_id_to_cmd_id = ();
												my $batch_size = get_bsub_batch_size($to_run_cmd_key);
												if ($batch_size > 1 && !$no_batch)
												{
														foreach (my $i = 0; $i <= $#to_run_refs; $i += $batch_size)
														{
																my $to_run_ref = $to_run_refs[$i];
																my $cur_cmd_id = $to_run_ref->[0];
																for (my $j = $i+1; $j < $i + $batch_size && $j <= $#to_run_refs; $j++)
																{
																		my $to_batch_ref = $to_run_refs[$j];
																		my $cur_to_batch_cmd_id = $to_batch_ref->[0];
																		push @{$cmd_id_to_batch_cmd_ids{$cur_cmd_id}}, $cur_to_batch_cmd_id;
																		$batch_cmd_id_to_cmd_id{$cur_to_batch_cmd_id} = $cur_cmd_id;
																		if (defined $to_batch_ref->[1] && $to_batch_ref->[1] eq "LONG")
																		{
																				$to_run_ref->[1] = "LONG";
																		}
																		if (defined $to_batch_ref->[2] && (!defined $to_run_ref->[2] || $to_run_ref->[2] < $to_batch_ref->[2]))
																		{
																				$to_run_ref->[2] = $to_batch_ref->[2];
																		}

																}
														}
												}

												foreach my $to_run_ref (@to_run_refs)
												{
														my $cur_cmd_id = $to_run_ref->[0];
														my $long_type = $to_run_ref->[1];
														my $mem_expand = $to_run_ref->[2];

														my $cur_cmd = $cmd_id_to_cmd[$cur_cmd_id];
														my $cmd_key = $cmd_id_to_cmd_key{$cur_cmd_id};

														die "Something went wrong; bug here" unless $cmd_key eq $to_run_cmd_key;

														foreach my $output_key (keys %{$all_started_cmd_ids{$cur_cmd_id}})
														{
																foreach my $args_ref (@{$all_started_cmd_ids{$cur_cmd_id}{$output_key}})
																{
																		common::record_key_started($output_key, %{$args_ref});
																}
														}
														if ($batch_cmd_id_to_cmd_id{$cur_cmd_id})
														{
																next;
														}

														my @submitted_cmd_ids = $cur_cmd_id;

														my @cur_cmds = ($cur_cmd);
														if ($cmd_id_to_batch_cmd_ids{$cur_cmd_id})
														{
																foreach my $to_batch_cmd_id (@{$cmd_id_to_batch_cmd_ids{$cur_cmd_id}})
																{
																		push @cur_cmds, $cmd_id_to_cmd[$to_batch_cmd_id];
																		#$cur_cmd .= " && $cmd_id_to_cmd[$to_batch_cmd_id]";
																		push @submitted_cmd_ids, $to_batch_cmd_id;
																}
														}

														(my $job_id_ref, my $error_code) = common::run_bsub(\@cur_cmds, 0, $cmd_key, $long_type, $mem_expand);
														for (my $i = 0; $i <= $#submitted_cmd_ids; $i++)
														{
																my $submitted_cmd_id = $submitted_cmd_ids[$i];
																if ($error_code)
																{
																		#print STDERR "$cur_cmd finished: bsub error\n";
																		$finished_cmd_ids{$submitted_cmd_id} = [$error_code, "bsub failed"];
																		$failed_submit_cmd_ids{$submitted_cmd_id} = 1;
																}
																else
																{
																		$running_cmd_ids{$submitted_cmd_id} = $job_id_ref->[$i];
																		$submitted_this_round = 1;
																}
														}
												}
										}
								}
								#Run the local commands last
								foreach my $to_run_cmd_id (@to_run_local)
								{
										my $cur_cmd = $cmd_id_to_cmd[$to_run_cmd_id];
										my $cmd_key = $cmd_id_to_cmd_key{$to_run_cmd_id};

										my $started_time = gettimeofday();
										(my $status, my $msg) = common::run_cmd($cur_cmd, 0, 0, $follow_cmd, $cmd_key);
										printf STDERR ("Elapsed Time: %.3f seconds\n", gettimeofday() - $started_time);
										$finished_cmd_ids{$to_run_cmd_id} = [$status, $msg];
								}

								print STDERR "Done started\n" if $debug;


								#get jobs that completed this round
								my %running_job_ids = ();
								if (!$check && $bsub)
								{
										%running_job_ids = common::get_running_jobs();
								}

								print STDERR "Got running\n" if $debug;

								foreach my $running_cmd_id (keys %running_cmd_ids)
								{
										if (!$running_job_ids{$running_cmd_ids{$running_cmd_id}})
										{
												#print STDERR "$running_cmd finished: no longer running\n";

												$finished_cmd_ids{$running_cmd_id} = [0, ""];
										}
								}

								#get those that finished that were running
								my @finished_job_ids = ();
								foreach my $finished_cmd_id (keys %finished_cmd_ids)
								{
										if ($running_cmd_ids{$finished_cmd_id})
										{
												push @finished_job_ids, $running_cmd_ids{$finished_cmd_id};
										}
								}

								#get the status for finished running
								my %error_status = ();
								my %error_code = ();
								my $completion_error_code = 0;
								if (@finished_job_ids)
								{
										my ($error_status_ref, $error_code_ref) = common::job_statuses(\@finished_job_ids);
										%error_status = %{$error_status_ref};
										%error_code = %{$error_code_ref};
										if (scalar keys %error_status)
										{
												$completion_error_code = 1;
										}
								}

								print STDERR "Got job statuses\n" if $debug;
								#clear out each finished job
								my $cmd_start_time = gettimeofday();
								foreach my $finished_cmd_id (keys %finished_cmd_ids)
								{
										die "Bug: Error in internal cmd_id to cmd mapping: no cmd for $finished_cmd_id" unless $finished_cmd_id >= 0 && $finished_cmd_id <= $#cmd_id_to_cmd;
										my $finished_cmd = $cmd_id_to_cmd[$finished_cmd_id];
										my $finished_cmd_key = $cmd_id_to_cmd_key{$finished_cmd_id};

										my $status = $finished_cmd_ids{$finished_cmd_id}->[0];
										my $msg = $finished_cmd_ids{$finished_cmd_id}->[1];

										my $do_restart = 0;

										#if was running, update status from bsub log
										my $was_running = 0;
										if ($running_cmd_ids{$finished_cmd_id} || $failed_submit_cmd_ids{$finished_cmd_id})
										{
												if ($debug2) {printf STDERR ("DEBUG: Finished $finished_cmd_id: %.3g seconds\n", gettimeofday() - $cmd_start_time); $cmd_start_time = gettimeofday();}

												#now clean up
												if (!$status && exists $running_cmd_ids{$finished_cmd_id})
												{
														if (exists $error_status{$running_cmd_ids{$finished_cmd_id}})
														{
																$status = $error_code{$running_cmd_ids{$finished_cmd_id}};
																$msg = $error_status{$running_cmd_ids{$finished_cmd_id}};
														}
												}
												$was_running = 1;
												if ($status && $restart > 0 && (!exists $times_restarted_cmd_ids{$finished_cmd_id} || $times_restarted_cmd_ids{$finished_cmd_id} < $restart))
												{
														$last_exit_status_cmd_ids{$finished_cmd_id} = $status;
														$times_restarted_cmd_ids{$finished_cmd_id} = exists $times_restarted_cmd_ids{$finished_cmd_id} ? $times_restarted_cmd_ids{$finished_cmd_id} + 1 : 1;
														$do_restart = 1;
														delete $finished_cmd_ids{$finished_cmd_id};
														delete $started_cmd_ids{$finished_cmd_key}{$finished_cmd_id};
														delete $started_cmd_ids{$finished_cmd_key} unless scalar keys %{$started_cmd_ids{$finished_cmd_key}};
												}
												else
												{
														#print STDERR "$finished_cmd finished: recorded from log\n";
														$finished_cmd_ids{$finished_cmd_id}->[0] = $status;
														$finished_cmd_ids{$finished_cmd_id}->[1] = $msg;
												}
												delete $running_cmd_ids{$finished_cmd_id};

												delete $failed_submit_cmd_ids{$finished_cmd_id};
												if ($debug2) {printf STDERR ("DEBUG: Done cleaning up: %.3g seconds\n", gettimeofday() - $cmd_start_time); $cmd_start_time = gettimeofday();}
										}
										#record errors and record finished for all output files
										if ($status)
										{
												my $msg_to_print = length($msg) > 10000 ? "End of output: " . substr($msg, length($msg) - 1000) : $msg;
												print STDERR "Error:\n\t$finished_cmd\n\tExit code $status\n\t$msg_to_print\n";
										}

										unless ($check)
										{
												foreach my $output_key (keys %{$all_started_cmd_ids{$finished_cmd_id}})
												{
														if ($debug2) {printf STDERR ("DEBUG: Starting $output_key: %.3g seconds\n", gettimeofday() - $cmd_start_time); $cmd_start_time = gettimeofday();}
														my %seen_output_paths = ();

														foreach my $args_ref (@{$all_started_cmd_ids{$finished_cmd_id}{$output_key}})
														{
																my $output_path = config::get_path($output_key, %{$args_ref});
																next if $seen_output_paths{$output_path};
																$seen_output_paths{$output_path} = 1;

																my $started_time = undef;
																if (common::check_key_started($output_key, %{$args_ref}))
																{
																		if (!$status && !common::path_exists($output_path, 10))
																		{
																				print STDERR "Warning: $output_path not generated by command $finished_cmd_key\n";
																				if ($error_if_not_generated)
																				{
																						$status = 1;
																						$msg = "$output_path not generated by command $finished_cmd_key\n";
																				}
																		}
																		if ($was_running)
																		{
																				print STDERR "Finished: $output_path\n";
																				printf STDERR ("Elapsed Time: %d seconds\n", gettimeofday() - common::check_key_started_time($output_key, %{$args_ref}));
																		}
																		$started_time = common::check_key_started_time($output_key, %{$args_ref});
																		common::clear_key_started($output_key, %{$args_ref});
																}
																elsif ($status)
																{
																		warn "Warning: cmd $finished_cmd had non-zero status ($status, $msg) but $output_key ($output_path) had not started\n";
																}
																if ($debug2) {printf STDERR ("DEBUG: Cleared started: %.3g seconds\n", gettimeofday() - $cmd_start_time); $cmd_start_time = gettimeofday();}

																if ($status)
																{
																		unless ($do_restart)
																		{
																				$cant_rebuild{$output_path} = 1;
																				$had_an_error = 1;
																		}
																		common::record_key_error($output_key, $status, $msg, %{$args_ref});
																}
																else
																{
																		my $input_paths = [];
																		#if any of these keys are lists of keys, we aren't going to record them
																		push @{$input_paths}, keys %{$cmd_id_to_inputs{$finished_cmd_id}};
																		common::record_key_finished($output_key, $input_paths, %{$args_ref});
																		if ($debug2) {printf STDERR ("DEBUG: Recorded finished: %.3g seconds\n", gettimeofday() - $cmd_start_time); $cmd_start_time = gettimeofday();}
																		if (defined $started_time && -e $output_path)
																		{
																				#if the modification time of the output path is less than that
																				#of the started time, wait to see if the time refreshes
																				my $max_sleep_time = 120;
																				my $sleep_inc = 10;
																				for (my $i = 0; $i <= $max_sleep_time; $i += $sleep_inc)
																				{
																						if (common::get_mod_time($output_path) < $started_time)
																						{
																								print STDERR "Waiting: Mod time of $output_path may be stale...\n";
																								#trying to see if we don't need this below
																								#hopefully waiting will be enough to make it refresh
																								#common::run_cmd("touch $output_path")
																								sleep $sleep_inc;
																						}
																						else
																						{
																								last;
																						}
																				}
																		}
																		#preserve if need be
																		if (should_preserve($output_key))
																		{
																				my $time_to_reset = $reset_time_to;
																				if (!defined $time_to_reset && exists $original_mod_times{$output_path} && defined $original_mod_times{$output_path})
																				{
																						$time_to_reset = (time2str("%b %d %H:%m %Y", $original_mod_times{$output_path}));
																				}
																				if (defined $time_to_reset)
																				{
																						common::run_cmd("touch -d '" . $time_to_reset . "' $output_path");
																				}
																		}

																}																		
																if ($debug2) {printf STDERR ("DEBUG: Done: %.3g seconds\n", gettimeofday() - $cmd_start_time); $cmd_start_time = gettimeofday();}

														}
												}
										}
										if ($do_restart)
										{
												delete $all_started_cmd_ids{$finished_cmd_id};
												next;
										}

										#finally, update dependencies
										my @dependent_cmd_ids = keys %{$cmd_ids{$finished_cmd_id}};

										foreach my $dependent_cmd_id (@dependent_cmd_ids)
										{
												unless ($dependent_cmd_id >= 0 && $dependent_cmd_id <= $#cmd_id_to_cmd)
												{
														die "Bug: Error in internal cmd_id to cmd mapping: no cmd for $dependent_cmd_id";
												}
												my $dependent_cmd = $cmd_id_to_cmd[$dependent_cmd_id];

												my $is_optional = $cmd_ids{$finished_cmd_id}{$dependent_cmd_id};

												if ($status && !$is_optional)
												{
														$cmd_ids_with_input_errors{$dependent_cmd_id} = $finished_cmd_key;
												}

												delete $cmd_ids{$finished_cmd_id};
												my $old_num = scalar keys %{$cmd_ids_reverse{$dependent_cmd_id}};
												die "Bug: Didn't find reverse map for $dependent_cmd, $finished_cmd\n" unless exists $cmd_ids_reverse{$dependent_cmd_id}{$finished_cmd_id};
												die "Bug: cmd_list incorrect" unless $cmd_id_list[$old_num]->{$dependent_cmd_id};
												delete $cmd_id_list[$old_num]->{$dependent_cmd_id};
												delete $cmd_id_list[$old_num] unless scalar keys %{$cmd_id_list[$old_num]};
												delete $cmd_ids_reverse{$dependent_cmd_id}{$finished_cmd_id};
												my $new_num = scalar keys %{$cmd_ids_reverse{$dependent_cmd_id}};
												die "Bug: After deleting didn't have one fewer" unless $new_num == $old_num - 1;
												$cmd_id_list[$new_num]->{$dependent_cmd_id} = 1;
										}
										delete $cmd_id_list[0]->{$finished_cmd_id};

										unless (scalar keys %{$cmd_id_list[0]})
										{
												delete $cmd_id_list[0];
										}

								}
								print STDERR "Done finished\n" if $debug;
								sleep 15 if ($submitted_this_round || $already_running > 0) && !$check;
								$round++;
						}
						%prev_cmds_to_defer = %cmds_to_defer;
						%prev_meta_files_loaded = %meta_files_to_load;

						$first_run = 0;
				}

				unless ($had_an_error)
				{
						last;
				}
		}
}

if (%need_but_cant && !$suppress_warnings)
{
		print STDERR "Warning: the following output paths could not be rebuilt:\n";
		foreach my $need_but_cant (keys %need_but_cant)
		{
				print STDERR "$need_but_cant: $need_but_cant{$need_but_cant}\n";
		}
}

printf STDERR ("Total Elapsed Time: %.3f seconds\n", gettimeofday() - $overall_started_time);

sub mkdir_at_level($$@)
{
		my $cur_instance = shift;
		my $class_level = shift;
		my %args = @_;

		if (@only_level)
		{
				my $matches = 0;
				foreach my $only_level (@only_level)
				{
						my %ancestors = config::get_ancestor_keys($only_level);
						if (!defined $class_level || $ancestors{$class_level})
						{
								$matches = 1;
								last;
						}
				}
				return unless $matches;
		}

		my $should_mkdir = 1;
		if (@skip_level)
		{
				if ($class_level && $skip_level{$class_level})
				{
						$should_mkdir = 0;
				}
		}

		if ($should_mkdir)
		{
				my @mkdir_keys = config::get_all_mkdir($class_level);
				foreach my $mkdir_key (@mkdir_keys)
				{
						common::run_mkdir_chmod($mkdir_key, $check, %args);
				}
		}

		#call for each child
		my @children = ();
		if (!defined $cur_instance)
		{
				@children = meta::get_root_instances()
		}
		else
		{
				@children = meta::get_children($cur_instance)
		}

		foreach my $child (@children)
		{
				my $child_args = meta::get_props_as_args($child);
				mkdir_at_level($child, meta::get_class($child), %{$child_args});
		}
}

sub get_bsub_batch_size($)
{
		my $cmd_key = shift;
		my $batch_size = common::get_bsub_batch($cmd_key);
		if ($batch_size < 1 || $no_batch)
		{
				$batch_size = 1;
		}
		if (!$no_batch && (common::get_use_uger() || common::get_use_sge()))
		{
				$batch_size = config::get_value("max_sge_batch");
		}
		return $batch_size;
}

sub should_force($)
{
		my $key = shift;
		return should_helper($key, @force_keys);
}
sub should_skip($)
{
		my $key = shift;
		return should_helper($key, @skip_keys);
}
sub should_stop_after($)
{
		my $key = shift;
		return should_helper($key, @stop_after_keys);
}
sub should_only($)
{
		my $key = shift;
		return should_helper($key, @only_keys);
}

sub should_touch($)
{
		my $key = shift;
		return should_helper($key, @touch_keys);
}

sub should_preserve($)
{
		my $key = shift;
		return should_helper($key, @preserve_time_keys);
}


sub should_helper($@)
{
		my $key = shift;
		my @should_keys = @_;

		foreach my $ancestor_key (config::get_ancestor_keys($key))
		{
				foreach my $should_key (@should_keys)
				{
						if ($ancestor_key =~ $should_key)
						{
								return 1;
						}
				}
		}
		return 0;
}

sub convert_meta_files_to_meta_paths(@)
{
		my @meta_files = @_;
		my @meta_paths = ();
		my @old_only_res = meta::get_only_res();
		if ($skip_custom_meta)
		{
				return ();
		}
		foreach my $meta_file (@meta_files)
		{
				print STDERR "Converting $meta_file\n" if $debug;
				my $class_level = config::get_class_level($meta_file);

				my @cur_only_meta = @all_only_meta;
				if ($specific_only_meta{$class_level})
				{
						push @cur_only_meta, @{$specific_only_meta{$class_level}};
				}

				if (@cur_only_meta)
				{
						meta::set_only_res(@cur_only_meta);
				}
				else
				{
						meta::set_only_res();
				}

				if ($skip_custom_meta_level{$class_level})
				{
						next;
				}
				if (@only_custom_meta_level && !$only_custom_meta_level{$class_level})
				{
						next;
				}

				my $class_instances = meta::get_instances_in_class($class_level, undef, 1);
				foreach my $instance (@{$class_instances})
				{
						my @needed_props = config::get_args($meta_file, 1);
						my $cur_args = meta::get_props_as_args($instance, undef, \@needed_props);
						my $meta_path = config::get_path($meta_file, %{$cur_args});
						if (!common::check_key_error($meta_file, %{$cur_args}) && !common::check_key_started($meta_file, %{$cur_args}) && -e $meta_path)
						{
								push @meta_paths, $meta_path;
						}
				}
		}
		meta::set_only_res(@old_only_res);
		return @meta_paths;
}

#query to find out which props are needed
sub query_props($$$$$)
{
		my $cmd_key = shift;
		my $class_to_instances = shift;
		my $instance_to_args = shift;
		my $instance_to_object = shift;
		my $instance_to_ancestors = shift;
		my $needed_props = {};
		my $good_query = 1;
		eval
		{
				my $output_query_result_ref = config::get_outputs_for_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object, "query");
				my $input_query_result_ref = config::get_inputs_for_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object, "query");

				my $cmd_query_result_ref = [];

				if (config::is_meta_table($cmd_key))
				{
						$cmd_query_result_ref = config::get_meta_table_string($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object, $instance_to_ancestors, "query");
				}
				else
				{
						$cmd_query_result_ref = config::bind_instances_to_cmd($cmd_key, $class_to_instances, $instance_to_args, $instance_to_object, "query");
				}

				foreach my $query_result_ref ($output_query_result_ref, $input_query_result_ref, $cmd_query_result_ref)
				{
						my $query_good = $query_result_ref->[0];
						my $query_result = $query_result_ref->[1];
						if (!$query_good)
						{
								$good_query = 0;
						}


						foreach my $needed_class (keys %{$query_result})
						{						
								foreach my $needed_prop (keys %{$query_result->{$needed_class}})
								{
										$needed_props->{$needed_class}->{$needed_prop} = 1;
								}
						}
				}

		};
		if ($@)
		{
				$good_query = 0;
				die $@;
		}
		my %needed_props = ();
		foreach my $needed_class (keys %{$needed_props})
		{
				@{$needed_props{$needed_class}} = keys %{$needed_props->{$needed_class}};
		}
		my $return_needed_props = \%needed_props;
		return [$good_query, $return_needed_props]
}
