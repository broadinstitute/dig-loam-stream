#Must include common.cfg in your config file to use this

package common;

use warnings;
use strict;
use vars;

use bin::trap_sig;
require bin::config;
require bin::util;
use bin::cache;

use Getopt::Long;
use File::Basename;
use File::Path qw(make_path);
use File::Temp qw/ tempfile tempdir /;
Getopt::Long::Configure("pass_through", "no_auto_abbrev");

sub preface_cmd($@);
sub get_rusage_mod($@);

sub get_tmp_dir()
{
		my $t = config::get_value("tmp_dir");
		my $v = `echo $t`;
		chomp($v);
		run_mkdir($v);
		return $v;
}

sub get_max_jobs_allowed(@)
{
		my $type = shift;
		my $max_jobs = config::get_value("max_jobs");
		if ($type)
		{
				if ($type eq "short")
				{
						$max_jobs = config::get_value("max_short_jobs");
				} 
				elsif ($type eq "long")
				{
						$max_jobs = config::get_value("max_long_jobs");
				}
		}
		if ($max_jobs > 0)
		{
				return $max_jobs;
		}
		else
		{
				return undef;
		}

}

sub kill_jobs(@)
{
		my @jobs = @_;
		my $use_lsf = get_use_lsf();
		if (scalar @jobs > 0)
		{
				if ($use_lsf)
				{
						run_cmd("bkill @jobs");
				}
				else
				{
						my %unique_jobs = ();
						map {$unique_jobs{$_}=1} @jobs;
						run_cmd("qdel " . join(",", map {get_job($_) . (is_task($_) ? "." . get_task($_) : "")} keys %unique_jobs));
				}
		}
}

sub kill_job($)
{
		my $job = shift;
		kill_jobs(($job));
}

our $bsub_log_path = undef;
sub get_bsub_log_path()
{
		if (!defined $bsub_log_path)
		{
				my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
				$year += 1900;
				$mon++;
				if ($mon < 10)
				{
						$mon = "0$mon";
				}
				if ($mday < 10)
				{
						$mday = "0$mday";
				}
				$bsub_log_path = config::get_value("bsub_log_path") . ".$year.$mon.$mday." . get_bsub_project_name();
		}
		my $umask = config::get_value("default_umask");
		unless (path_exists($bsub_log_path))
		{
				run_cmd("umask $umask; touch $bsub_log_path", 0, 1);				
		} 
		return $bsub_log_path;
}

sub get_sge_log_path($$)
{
		my $log_path = shift;
		my $script = shift;
		$script = util::get_file_file($script);
		return "$log_path.$script";
}

sub get_sge_task_log_path($$)
{
		my $log_path = shift;
		my $task = shift;
		return "$log_path.$task";
}

sub get_bsub_project_name()
{
		return "lap.$$";
}

our $use_lsf = undef;
sub get_use_sge()
{
		return !get_use_lsf() && !get_use_uger();
}

our $use_uger = undef;
sub get_use_uger()
{
		if (!defined $use_uger)
		{
				my $qacct_output = `qstat -help 2>& 1`;
				my @lines = split(/\n/, $qacct_output);
				$use_uger = 0;
				if (scalar @lines > 0)
				{
						my @type = split(/\s+/, $lines[0]);
						if (scalar @type > 0)
						{
								if ($type[0] =~ /UGE/)
								{
										$use_uger = 1;
								}
						}
				}
		}
		return !get_use_lsf() && $use_uger;
}

sub get_use_lsf()
{
		if (!defined $use_lsf)
		{
				my $qacct_output = `bjobs 2>& 1`;
				unless ($?)
				{
						$use_lsf = 1
				}
				else
				{
						$use_lsf = 0
				}
		}
		return $use_lsf;
}

our $task_delim = ":";
sub is_task($)
{
		my $id = shift;
		return $id =~ /$task_delim/;
}

sub create_task_id($$)
{
		my $job = shift;
		my $task = shift;
		return "$job$task_delim$task";
}

sub get_job($)
{
		my $id = shift;
		if (is_task($id))
		{
				(my $job, my $task) = split(/:/, $id);
				return $job;
		}
		else
		{
				return $id;
		}
}
sub get_task($)
{
		my $id = shift;
		if (is_task($id))
		{
				(my $job, my $task) = split(/:/, $id);
				return $task;
		}
		else
		{
				return undef;
		}
}

sub get_short_queue()
{
		if (get_use_lsf())
		{
				return config::get_value("lsf_short_queue");
		}
		elsif (get_use_uger())
		{
				return config::get_value("uger_short_queue");
		} 
		else
		{
				return config::get_value("sge_short_queue");
		}
}

sub get_queue()
{
		if (get_use_lsf())
		{
				return config::get_value("lsf_queue");
		}
		elsif (get_use_uger())
		{
				return config::get_value("uger_queue");
		}
		else
		{
				return config::get_value("sge_queue");
		}
}

our %job_id_to_script = ();
#CHANGE THIS SO JOB_ID IS PRINTED AT TOP, AND TASK_ID prefaces each line of output
sub run_bsub($$)
{
		my $cmds = shift;
		my @cmds = ();
		foreach my $cmd (@{$cmds})
		{
				push @cmds, $cmd if defined $cmd;
		}
		if (scalar @cmds < 1) { return wantarray ? (0, 1) : 1 }
		my $check = shift;
		my $cmd_key = shift;

		#specify short or long
		my $long_type = shift;
		my $mem_expand = shift;

		my $use_lsf = get_use_lsf();

		my $bsub_cmd = $use_lsf ? sprintf("bsub -P %s ", get_bsub_project_name()) : sprintf("qsub -cwd -V -N %s ", get_bsub_project_name());
		my $queue = get_queue();
		my $bsub_opts = config::get_value("bsub_opts");

		my $force_long = (defined $long_type && $long_type =~ /long/i);
		my $force_short = (defined $long_type && $long_type =~ /short/i);

		my $short_overrides = config::get_value("short_overrides");
		my $long_overrides = config::get_value("long_overrides");

		my $mem = config::get_value("default_mem");
		my $orig_mem = $mem;

		my $bjobs_wait = config::get_value("bjobs_wait");

		my $run_short = 0;

		for (my $i = 0; $i <= $#cmds; $i++)
		{
				my $cmd = $cmds[$i];

				if ($short_overrides)
				{
						my @short_overrides = split(' ', $short_overrides);
						foreach my $short_override (@short_overrides)
						{
								(my $o_cmd_key, my $regex) = split(":", $short_override);
								if ($cmd_key =~ /$o_cmd_key/ && $cmd =~ /$regex/)
								{
										$force_short = 1;
								}
						}
				}
				if ($long_overrides)
				{
						my @long_overrides = split(' ', $long_overrides);
						foreach my $long_override (@long_overrides)
						{
								(my $o_cmd_key, my $regex) = split(":", $long_override);
								if ($cmd_key =~ /$o_cmd_key/ && $cmd =~ /$regex/)
								{
										$force_long = 1;
								}
						}
				}

				if (defined $cmd_key && ((config::is_short($cmd_key) && !$force_long) || $force_short))
				{
						$run_short = 1;
						$queue = get_short_queue();
						$bsub_opts = config::get_value("bsub_short_opts");
						my $bsub_short_time = "";
						if ($use_lsf)
						{
								$bsub_short_time = config::get_value("bsub_short_time");
						}
						elsif (get_use_uger())
						{
								$bsub_short_time = config::get_value("uger_short_time");
						}
						else
						{
								$bsub_short_time = config::get_value("sge_short_time");
						}
						$bsub_opts .= " " if $bsub_opts;
						if ($use_lsf)
						{
								$bsub_opts .= "-W " . $bsub_short_time;
						}
						else
						{
								if ($bsub_short_time =~ /^[0-9]+$/)
								{
										$bsub_short_time .= ":00"
								}
								if ($bsub_short_time =~ /^[0-9]+:[0-9]+$/)
								{
										$bsub_short_time .= ":00"
								}
								$bsub_opts .= "-l h_rt=" . $bsub_short_time;
						}

				}

				my $rusage_mod = get_rusage_mod($cmd, $cmd_key);
				if (defined $rusage_mod && $rusage_mod > $mem)
				{
						$mem = $rusage_mod;
				}

				if ($cmd =~ /java.*-Xmx(.\S+)/)
				{
						my $java_mem = $1;
						if ($java_mem =~ /([0-9]+)([g|m])/)
						{
								my $needed_mem = 0;
								my $size = $1;
								my $unit = $2;
								if ($unit eq "g")
								{
										$needed_mem = $size * 1000;
								}
								else
								{
										$needed_mem = $size
								}
								if ($mem_expand)
								{
										$needed_mem *= $mem_expand;
										$cmds[$i] =~ s/(java.*-Xmx)(.\S+)/$1$needed_mem$unit/;
								}
								if ($needed_mem > $mem)
								{
										$mem = $needed_mem;
								}
						}
				}
				elsif ($mem_expand)
				{
						$mem *= $mem_expand;
						$mem = int($mem);
				}
		}

		if ($queue)
		{
				$bsub_cmd .= " -q $queue"
		}
		if ($use_lsf)
		{
				$bsub_cmd .= " -R rusage[mem=$mem]";
		}
		else
		{
				if (get_use_uger())
				{
						$bsub_cmd .= " -j y -l m_mem_free=${mem}M";
				}
				else
				{
						$bsub_cmd .= " -j y -l mem_free=${mem}M";
				}
				if (scalar @cmds > 1)
				{
						$bsub_cmd .= " -t 1-" . scalar(@cmds) . " -tc " . config::get_value("max_sge_batch");
				}
		}

		my $timeout = undef;
		if (defined $cmd_key && config::has_timeout($cmd_key))
		{
				$timeout = config::get_timeout($cmd_key);
				if ($timeout !~ /^\d+$/)
				{
						warn "Warning: Bad timeout value $timeout for $cmd_key; submitting with no timeout";
						$timeout = undef;
				}
				else
				{
						$timeout /= 60;
				}

		}

		if (defined $timeout)
		{
				if ($use_lsf)
				{
						$bsub_opts =~ s/-W\s*\S*//;
						$bsub_opts .= "$bsub_opts -W $timeout";
				}
				else
				{
						$bsub_opts =~ s/-l h_rt=\S*//;
						$bsub_opts .= "$bsub_opts -l h_rt=$timeout";
				}
		}

		my $job_id = undef;
		my @job_ids = ();
		my $error_code = 0;

		#construct the command from the set of commands that may have been submitted
		my $first_cmd = $cmds[0];
		my $original_cmd = undef;

		my $script = undef;
		my $log_path = get_bsub_log_path();

		my $cmd = "";
		if ($use_lsf)
		{
				shift @cmds;
				$cmd = $first_cmd;
				foreach (@cmds)
				{
						$cmd .= " && $_";
				}
				$cmd = preface_cmd($cmd, $cmd_key, 1);
				$original_cmd = $cmd;
				$cmd = check_cmd_length($cmd, $check);
		}
		else
		{
				my $preface_qsub = config::get_value("preface_qsub");
				$cmd = "";
				if (scalar(@cmds) == 1)
				{
						$cmd = $first_cmd;
						$cmd = preface_cmd($cmd, $cmd_key, 1);
						$original_cmd = $cmd;
						$cmd = "(echo JobId: \$JOB_ID; $preface_qsub; $cmd) 2>&1";
				}
				else
				{
						$cmd .= "#!/bin/sh\n";
						$cmd .= "LOG_PATH=" . get_sge_task_log_path("\$SGE_STDOUT_PATH", "\$SGE_TASK_ID") . "\n";
						$cmd .= "echo JobId: " . create_task_id("\$JOB_ID", "\$SGE_TASK_ID") . " 2>&1 >> \$LOG_PATH\n";
						$cmd .= "$preface_qsub\n";
						$cmd .= "INDEX=\$SGE_TASK_ID\n";
						
						for (my $i = 0; $i <= $#cmds; $i++)
						{
								if ($i > 0)
								{
										$cmd .= "el";
								}
								$cmd .= "if [[ \$INDEX == " . ($i + 1) . " ]]; then\n";
								$cmd .= "  " . preface_cmd("($cmds[$i]) 2>&1 | cat >> \$LOG_PATH", $cmd_key, 1) . "\n";
						}
						$cmd .= "fi\n";
						$original_cmd = "";
						for (my $i = 0; $i <= $#cmds; $i++)
						{
								$original_cmd .= "(Task " . ($i+1) . ") " . $cmds[$i] . "\n";
						}
						$original_cmd =~ s/\n$//;
						$original_cmd = preface_cmd($original_cmd, $cmd_key, 1);
				}
				$cmd = generate_script($cmd, 1);
				$script = $cmd;
				$log_path = get_sge_log_path($log_path, $script);
		}
		$bsub_cmd .= " -o $log_path";
		if ($bsub_opts)
		{
				$bsub_cmd .= " $bsub_opts";
		}

		$cmd =~ s/(\\*)/$1$1/g;
		$cmd =~ s/\`/\\\`/g;
		$cmd =~ s/\"/\\\"/g;
		$cmd =~ s/\$/\\\$/g;

		if ($mem ne $orig_mem)
		{
				print STDERR "(mem=$mem) ";
		}
		if ($bsub_opts)
		{
				print STDERR "($bsub_opts) "
		}

		if (!$check)
		{
				my $max_total_jobs = get_max_jobs_allowed();

				my @to_check = (undef);
				if ($run_short)
				{
						push @to_check, "short";
				}
				else
				{
						push @to_check, "long";
				}

				foreach my $to_check (@to_check)
				{
						my $max_jobs = get_max_jobs_allowed($to_check);
						next if (defined $to_check && $max_jobs > $max_total_jobs);
						if (defined $max_jobs)
						{
								while (1)
								{
										my %running_jobs = ();
										eval
										{
												my %all_running_jobs = get_running_jobs($to_check);
												foreach my $running_job (keys %all_running_jobs)
												{
														$running_jobs{get_job($running_job)} = 1;
												}
										};
										if ($@)
										{
												if ($@ eq main::trap_signal())
												{
														die $@;
												}

												return wantarray ? (0, 1) : 1;
										}
										if (scalar keys %running_jobs < $max_jobs)
										{
												last;
										}
										sleep($bjobs_wait);
								}
						}
				}

				my $output = "";
				if ($use_lsf)
				{
						$cmd = "\"$cmd\"";
						$cmd = "$cmd 2>& 1";
				}

				print STDERR "$original_cmd\n";
				#print STDERR "$bsub_cmd \"$cmd\"\n";


				my $bsub_failure_wait = config::get_value("bsub_fail_wait");
				while (1)
				{
						$output = `$bsub_cmd $cmd 2>& 1`;
						$error_code = $?;

						if (!$error_code)
						{
								if (($use_lsf && $output =~ /Job <([0-9]+)>/) || (!$use_lsf && scalar(@cmds) == 1 && $output =~ /job ([0-9]+)/) || (!$use_lsf && scalar(@cmds) > 1 && $output =~ /job-array ([0-9]+)/))
								{
										$job_id = $1;
								}
								else
								{
										$error_code = -1;
								}
						}
						if (!wantarray)
						{
								print STDERR "$output";
						}

						print STDERR "Submitted via " . ($use_lsf ? "LSF" : (get_use_uger() ? "UGE" : "SGE")) . ($queue ? " to $queue" : "") . " (JobId " . (defined $job_id ? $job_id : "?") . ")\n";
						if (!$use_lsf)
						{
								$job_id_to_script{$job_id} = $script;
						}
						if (wantarray)
						{
								if ($error_code)
								{
										warn "Warning: bsub failed with error $error_code\n\t$output\n";
								}
								else
								{
										foreach (my $i = 0; $i <= $#cmds; $i++)
										{
												my $cur_job_id = $job_id;
												if (!$use_lsf && scalar(@cmds) > 1)
												{
														$cur_job_id = create_task_id($cur_job_id, $i + 1);
												}
												push @job_ids, $cur_job_id;
										}
										return (\@job_ids, $error_code);
								}
						}
						else
						{
								if (!$error_code) 
								{
										return $error_code;
								}
						}
						sleep $bsub_failure_wait
				}
		}
		else
		{
				return wantarray ? (0, 0) : 0;
		}
}

sub get_running_jobs(@)
{
		my $max_failures = 10;
		if (!config::is_key("bjobs_fail_wait"))
		{
				return ();
		}

		my $type = shift;

		my $use_lsf = get_use_lsf();
		my $failure_wait = config::get_value("bjobs_fail_wait");
		my %queues = (get_queue()=>1,
									get_short_queue()=>1);

		if ($type)
		{
				if ($type eq "short")
				{
						%queues = (get_short_queue()=>1);
				}
				elsif ($type eq "long")
				{
						%queues = (get_queue()=>1);
				}
		}

		my $failure_num = 0;
		my $project_name = get_bsub_project_name();
		
		my $out = "";
		my @lines = ();
		while (1)
		{
				#get list of jobs
				#my $bjobs_cmd = sprintf("bjobs -P %s -q $queue 2>& 1", get_bsub_project_name());
				my $bjobs_cmd = "";
				if ($use_lsf)
				{
						$bjobs_cmd = sprintf("bjobs -P %s 2>& 1", $project_name);
				}
				else
				{
						$bjobs_cmd = sprintf("qstat 2>& 1")
				}

				$out = `$bjobs_cmd`;
				my $error_code = $?;
				if ($error_code)
				{
						$failure_num++;
						if ($failure_num == $max_failures)
						{
								die "bjobs failed:\n$out";
						}
						else
						{
								sleep $failure_wait;
								next;
						}
				}
				@lines = split /\n/, $out;
				if ($use_lsf && $#lines < 0)
				{
						$failure_num++;
						if ($failure_num == $max_failures)
						{
								die "No output from bjobs";
						}
						else
						{
								sleep $failure_wait;
								next;
						}

				}
				else
				{
						last;
				}
		}

		my $found_no_job_output = undef;
		#one must have column JOBID
		my $line_no = 0;
		my $id_col = -1;
		my $queue_col = -1;
		my $name_col = -1;
		my $task_col = -1;
		if (scalar @lines == 0)
		{
				$found_no_job_output = 1;
		}

		my $job_id_colname = $use_lsf ? "JOBID" : "job-ID";
		my $queue_colname = $use_lsf ? "QUEUE" : "queue";
		my $name_colname = $use_lsf ? "" : "name";
		my $task_colname = $use_lsf ? "" : "ja-task-ID";
		my $sge_template = undef;
		while ($id_col < 0 && $line_no <= $#lines)
		{
				if ($lines[$line_no] =~ /no\s*.*\s+job\s+.*\s*found/i)
				{
						$found_no_job_output = 1;
				}
				my @cols = split /\s+/, $lines[$line_no];
				for (my $i = 0; $i <= $#cols; $i++)
				{
						if ($cols[$i] eq $job_id_colname)
						{
								$id_col = $i;
						}
						elsif ($cols[$i] eq $queue_colname)
						{
								$queue_col = $i;
						}
						elsif ($cols[$i] eq $name_colname)
						{
								$name_col = $i;
						}
						elsif ($cols[$i] eq $task_colname)
						{
								$task_col = $i;
						}
				}
				if ($id_col >= 0)
				{
						chomp($lines[$line_no]);
						my @sge_template = map {'A' . length($_)} $lines[$line_no] =~ /(\S+\s*)/g;
						$sge_template[-1] = 'A*';
						$sge_template = "@sge_template";
				}
				$line_no++;
		}

		if ($id_col < 0 || $queue_col < 0)
		{
				if ($found_no_job_output)
				{
						return ();
				}
				else
				{
						if ($id_col < 0)
						{
								die "Couldn't identify column with ". $job_id_colname . " in output of bjobs: $out";
						}
						elsif (!$use_lsf && $name_col < 0)
						{
								die "Couldn't identify column with ". $name_colname . " in output of bjobs: $out";
						}
						else
						{
								die "Couldn't identify column with " . $queue_colname . " in output of bjobs: $out";
						}
				}
		}

		my %running_jobs = ();
		for (my $j = $line_no; $j <= $#lines; $j++)
		{
				next if !$use_lsf && $lines[$j] =~ /^\-+$/;
				my @cols = ();
				if ($use_lsf)
				{
						$lines[$j] =~ s/^\s*//g;
						@cols = split /\s+/, $lines[$j];
				}
				else
				{
						chomp($lines[$j]);
						@cols = unpack($sge_template, $lines[$j]); 
						@cols = map {s/\s*//; $_} @cols;
						@cols = map {s/\s*$//; $_} @cols;
				}

				die unless $id_col <= $#cols;
				die if $use_lsf && $queue_col > $#cols;
				die if !$use_lsf && $name_col > $#cols;
				my $cur_queue = $cols[$queue_col];
				#FIXME Right now don't now how to figure out which queue a job is assigned to from qstat
				if ($use_lsf && !$queues{$cur_queue})
				{
						next;
				}
				if (!$use_lsf && $cols[$name_col] ne $project_name)
				{
						next;
				}
				my $id = $cols[$id_col];
				if (!$use_lsf && $task_col >= 0 && $task_col <= $#cols)
				{
						my $all_task = $cols[$task_col];
						my @tasks = split(",", $all_task);
						my $added_task = 0;
						foreach my $task (@tasks)
						{
								if ($task =~ /^[0-9]+$/)
								{
										my $cur_id = create_task_id($id, $task);
										$running_jobs{$cur_id} = 1;
										$added_task = 1;
								}
								elsif ($task =~ /^([0-9]+)-([0-9]+):([0-9]+)$/)
								{
										for (my $cur_task = $1; $cur_task <= $2; $cur_task += $3)
										{
												my $cur_id = create_task_id($id, $cur_task);
												$running_jobs{$cur_id} = 1;
												$added_task = 1;
										}
								}
						}
						if (!$added_task)
						{
								$running_jobs{$id} = 1;
						}								
				}
				else
				{
						$running_jobs{$id} = 1;
				}
		}
		return %running_jobs;
}

sub get_num_running_jobs(@)
{
		my $type = shift;
		my %running_jobs = get_running_jobs($type);
		my @running_jobs = keys %running_jobs;
		%running_jobs = ();
		map {$running_jobs{get_job($_)}=1} @running_jobs;
		return scalar keys %running_jobs;
}

#return 1 if there was an error
sub wait_on_jobs(@)
{
		my @jobs = @_;

		if (scalar @jobs == 0)
		{
				return wantarray ? (0, "") : 0;
		}

		my $bjobs_wait = config::get_value("bjobs_wait");

		while (1)
		{
				my %running_jobs = ();
				eval
				{
						%running_jobs = get_running_jobs();
				};
				if ($@)
				{
						if ($@ eq main::trap_signal())
						{
								die $@;
						}

						return wantarray ? (1, $@) : 1;
				}

				my $wait = undef;
				foreach my $job (@jobs)
				{
						next unless defined $job && $job ne "";
						if ($running_jobs{$job})
						{
								#wait
								sleep $bjobs_wait;
								$wait = 1;
								last;
						}
				}
				if ($wait)
				{
						next;
				}
				else
				{
						last;
				}
		}		
		return wantarray ? (0, "") : 0;
}

sub append_with_trim($$$)
{
		my $cur_output = shift;
		my $to_add = shift;
		my $max_bytes = shift;
		my $overwrote = 0;

		my $new_length = length($cur_output) + length($to_add);

		if ($new_length > $max_bytes)
		{
				$overwrote = 1;
				my $amount_to_trim = $new_length - $max_bytes;
				if ($amount_to_trim > length($cur_output))
				{
						$cur_output = "";
						$to_add = substr($to_add, 0, $amount_to_trim - length($cur_output));
				}
				else
				{
						$cur_output = substr($cur_output, $amount_to_trim);
				}
		}
		$cur_output .= $to_add;

		return ($cur_output, $overwrote);
}

sub process_lsf_log_line(@)
{
		my $line = shift;
		my $max_bytes = shift;
		my $log_output_ref = shift;
		my $exit_codes_ref = shift;
		my $jobs_ref = shift;
		my $cur_job_ref = shift;
		my $cur_output_ref = shift;
		my $in_output_ref = shift;
		my $overwrote_ref = shift;

		if ($line =~ /Subject:\s+Job\s+(\S+):/)
		{
				my $job = $1;

				$log_output_ref->{$$cur_job_ref} = $$cur_output_ref if ($$cur_job_ref && $$cur_output_ref && $jobs_ref->{$$cur_job_ref});
				$$cur_job_ref = $job;
		}
		elsif ($line =~ /Successfully completed/)
		{
				if (!exists $exit_codes_ref->{$$cur_job_ref})
				{
						$exit_codes_ref->{$$cur_job_ref} = 0 if ($$cur_job_ref && $jobs_ref->{$$cur_job_ref});
				}
				else
				{
						$$in_output_ref = undef;
				}
		}
		elsif ($line =~ /Exited with exit code ([0-9]+)/)
		{
				if (!exists $exit_codes_ref->{$$cur_job_ref})
				{
						$exit_codes_ref->{$$cur_job_ref} = $1 if ($$cur_job_ref && $jobs_ref->{$$cur_job_ref} );
				}
				else
				{
						$$in_output_ref = undef;
				}
		}
		elsif ($line =~ /^Sender:/)
		{
				$$in_output_ref = undef;
		}
		elsif ($line =~ /^The output.+follows/)
		{
				if (!$$in_output_ref)
				{
						$$in_output_ref = 1;
						$$cur_output_ref = "";
				}
				else
				{
						$$in_output_ref = 0;
				}
		}
		elsif ($$in_output_ref)
		{
				($$cur_output_ref, my $cur_overwrote) = append_with_trim($$cur_output_ref, $line, $max_bytes);
				$$overwrote_ref = 1 if $cur_overwrote;
		}
}

sub process_sge_log_line(@)
{
		my $line = shift;
		my $max_bytes = shift;
		my $log_output_ref = shift;
		my $exit_codes_ref = shift;
		my $jobs_ref = shift;
		my $cur_job_ref = shift;
		my $cur_output_ref = shift;
		my $in_output_ref = shift;
		my $overwrote_ref = shift;
		my $qacct_cache_ref = shift;

		if ($line =~ /^JobId:\s+(\S+)/)
		{
				my $job = $1;

				if ($$cur_job_ref && $jobs_ref->{$$cur_job_ref})
				{
			  	$log_output_ref->{$$cur_job_ref} = $$cur_output_ref;
				}
				$$cur_job_ref = $job;
				$$cur_output_ref = "";

				my $exit_status = undef;
				my $num_qacct_tries = 5;
				while (1)
				{
						my $qacct_output = "";
						if ($qacct_cache_ref->{get_job($job)})
						{
								$qacct_output = $qacct_cache_ref->{get_job($job)};
						}
						else
						{
								my $add_job = "-j " . get_job($job);
								#my $add_task = is_task($job) ? "-t " . get_task($job)  : "";
								$qacct_output = `qacct $add_job 2>&1`;
						}
						unless ($?)
						{
								$qacct_cache_ref->{get_job($job)} = $qacct_output unless $qacct_cache_ref->{get_job($job)};
								my @lines = split /\n/, $qacct_output;
								my $in_correct_task = 0;
								foreach (@lines)
								{
										chomp;
										my @cols = split(/\s+/, $_);
										if (is_task($job) && $cols[0] eq "taskid")
										{
												if ($cols[1] eq get_task($job))
												{
														$in_correct_task = 1;
												}
												else
												{
														$in_correct_task = 0;
												}
										}
										if ((!is_task($job) || $in_correct_task) && $cols[0] eq "exit_status")
										{
												if (!$exit_status)
												{
														$exit_status = $cols[1];
												}
												last if $exit_status;
										}
								}
								last;
						}
						sleep 5;
				}
				if (!defined $exit_status)
				{
						$$cur_job_ref = undef;
				}
				if (defined $exit_status)
				{
						$exit_codes_ref->{$$cur_job_ref} = $exit_status if ($$cur_job_ref && $jobs_ref->{$$cur_job_ref} );
				}
		}
		else
		{
				($$cur_output_ref, my $cur_overwrote) = append_with_trim($$cur_output_ref, $line, $max_bytes);
				$$overwrote_ref = 1 if $cur_overwrote;
		}
}

sub get_log_output($@)
{
		my $jobs = shift;
		my @jobs = @{$jobs};

		my %log_output = ();
		my %exit_codes = ();
		
		my $bsub_log_path = get_bsub_log_path();
		my %log_paths = ();
		my $use_lsf = get_use_lsf();

		my @additional_log_paths_to_delete = ();

		my %qacct_cache = ();
		if ($use_lsf)
		{
				$log_paths{$bsub_log_path} = \@jobs;
		}
		else
		{
				for my $job (@jobs)
				{
						if (exists $job_id_to_script{get_job($job)})
						{
								my $cur_log_path = get_sge_log_path($bsub_log_path, $job_id_to_script{get_job($job)});
								if (is_task($job))
								{
										push @additional_log_paths_to_delete, $cur_log_path;
										$cur_log_path = get_sge_task_log_path($cur_log_path, get_task($job));
								}
								$log_paths{$cur_log_path} = [] unless defined $log_paths{$cur_log_path};
								push @{$log_paths{$cur_log_path}}, $job;
						}
				}
		}

		my $failure_wait = config::get_value("bjobs_fail_wait");
		my $max_tries = 15;
		my $max_bytes = config::get_value("max_bytes_output");

		for my $log_path (keys %log_paths)
		{
				my %cur_log_output = ();
				my %jobs = ();
				map {$jobs{"$_"} = 1} @{$log_paths{$log_path}};
				for (my $i = 0; $i < $max_tries; $i++)
				{
						%cur_log_output = ();
						open IN, "$log_path" or last;

						my $cur_job = undef;
						my $cur_output = "";
						my $in_output = undef;
						my $overwrote = 0;

						while (my $line = <IN>)
						{
								if (scalar keys %cur_log_output == scalar keys %jobs)
								{
										last;
								}

								if ($use_lsf)
								{
										process_lsf_log_line($line, $max_bytes, \%cur_log_output, \%exit_codes, \%jobs, \$cur_job, \$cur_output, \$in_output, \$overwrote);
								}
								else
								{
										process_sge_log_line($line, $max_bytes, \%cur_log_output, \%exit_codes, \%jobs, \$cur_job, \$cur_output, \$in_output, \$overwrote, \%qacct_cache);
								}
						}
						close IN;

						if ($cur_output && $overwrote)
						{
								$cur_output = "Last $max_bytes bytes of output:\n$cur_output";
						}

						$cur_log_output{$cur_job} = $cur_output if ($cur_job && $jobs{$cur_job});

						if (scalar keys %cur_log_output == scalar keys %jobs)
						{
								if (!$use_lsf)
								{
										foreach my $job (keys %jobs)
										{
												$job = get_job($job);
												if (-e $job_id_to_script{$job})
												{
														#unlink($job_id_to_script{$job});
												}
												if (-e $log_path)
												{
														system("cat $log_path >> $bsub_log_path");
														unlink($log_path);
												}
										}
								}
								last;
						}
						sleep $failure_wait;
				}
				map {$log_output{$_} = $cur_log_output{$_}} keys %cur_log_output;
		}
		foreach my $log_path (@additional_log_paths_to_delete)
		{
				unlink $log_path if -e $log_path;
		}

		my @jobs_with_no_output = ();
		foreach my $cur_job (@jobs)
		{
				unless (exists $log_output{$cur_job})
				{
						$log_output{$cur_job} = "No logged output available";
						push @jobs_with_no_output, $cur_job
				}
		}
		if (scalar @jobs_with_no_output)
		{
				#delete it just to be sure
				my %running_jobs = get_running_jobs();
				foreach my $job_with_no_output (@jobs_with_no_output)
				{
						if ($running_jobs{$job_with_no_output})
						{
								kill_job($job_with_no_output);
						}
				}
		}
		#no need to use default exit code because caller will handle this
		
		return (\%log_output, \%exit_codes);
}

sub job_statuses($@)
{
		my $jobs = shift;
		my @jobs = @{$jobs};

		if (scalar @jobs == 0)
		{
				return ();
		}

		#default: error out all jobs
		my $unknown_error_status = "Couldn't find error information";
		my %error_status = ();
		my %error_code = ();
		map {$error_status{$_} = "$unknown_error_status (Job $_)"} @jobs;
		map {$error_code{$_} = 1} @jobs;

		#check for log output
		my @cur_jobs = keys %error_status;
		my ($log_output_ref, $log_exit_status_ref) = get_log_output(\@cur_jobs);
		my %log_output = %{$log_output_ref};
		my %log_exit_status = %{$log_exit_status_ref};
		foreach my $job_id (keys %log_exit_status)
		{
				if ($error_status{$job_id} =~ /^$unknown_error_status/)
				{
						if ($log_exit_status{$job_id})
						{
								$error_status{$job_id} = $log_exit_status{$job_id};
								$error_code{$job_id} = $log_exit_status{$job_id};
						}
						else
						{
								delete $error_status{$job_id};
								delete $error_code{$job_id};
						}
				}
		}

		foreach my $job_id (keys %log_output)
		{
				#It must have errored, so swap in correct error status
				if (exists $error_status{$job_id})
				{
						$error_status{$job_id} = "Output:\n$log_output{$job_id}\n";
				}
		}

		return (\%error_status, \%error_code);
}

sub files_same_num_lines($$@)
{
		my $path1 = shift;
		my $path2 = shift;
		my $check = shift;


		#get the word count
		(my $code1, my $msg1) = run_cmd("cat $path1 | wc -l", $check, 1);
		(my $code2, my $msg2) = run_cmd("cat $path2 | wc -l", $check, 1);

		if ($check)
		{
				return (0, "");
		}

		if ($code1)
		{
				return ($code1, "Couldn't count lines of $path1");
		}
		if ($code2)
		{
				return ($code2, "Couldn't count lines of $path2");
		}

		my $lines1 = $msg1;
		my $lines2 = $msg2;
		chomp $lines1;
		chomp $lines2;

		if ($lines1 != $lines2)
		{
				return (1, "$path1 has $lines1 lines but $path2 has $lines2 lines");
		}

		return (0, "");
}


sub run_mkdir($$)
{
		my $dir = shift;
		my $check = shift;
		my $hide_write = -e $dir;
		if ($check)
		{
				return run_cmd("mkdir -p $dir", $check, $hide_write);
		}
		else
		{
				my $status = make_path($dir, {error => \my $err});
				return scalar @{$err} ? 1 : 0;
		}

}

sub run_mkdir_chmod($$@);

sub run_mkdir_chmod($$@)
{
		my $dir_key = shift;
		my $check = shift;
		my @args = @_;

		my $sort_dir_key = "sort_dir";
		my $filter_dir_key = "filter_dir";
		my $did_something = 0;

		my $dir = config::get_value($dir_key, @args);

		if (!-e $dir)
		{
				$did_something = 1;
				run_mkdir($dir, $check);
		}

		if (config::has_chmod_value($dir_key))
		{
				my $chmod_value = config::get_chmod_value($dir_key);
				if ($check)
				{
						run_cmd("chmod $chmod_value $dir", $check, 1);
				}
				else
				{
						chmod oct("0$chmod_value"), $dir;
				}
		}

		if ($sort_dir_key ne $dir_key && config::is_sortable($dir_key))
		{
				my $cur_did_something = run_mkdir_chmod($sort_dir_key, $check, (dir=>$dir));
				$did_something ||= $cur_did_something;
				if ($filter_dir_key ne $dir_key)
				{
						my $cur_did_something = run_mkdir_chmod($filter_dir_key, $check, (dir=>$dir));
						$did_something ||= $cur_did_something;
				}
		}
		return $did_something;
}

sub preface_cmd($@)
{
		my $cmd = shift;
		my $cmd_key = shift;
		my $is_bsub = shift;

		my @cmd_classes = config::get_all_cmd_class();

		my @all_env_mod_keys = ();
		my @all_env_mod_values = ();

		if (defined $cmd_key && config::has_env_mod_value($cmd_key))
		{
				my @env_mod_values = config::get_env_mod_values($cmd_key);
				foreach my $env_mod_value (@env_mod_values)
				{
						my @cur_env_mod_value = @{$env_mod_value};
						push @all_env_mod_keys, $cur_env_mod_value[0];
						push @all_env_mod_values, join(":", @cur_env_mod_value[1..$#cur_env_mod_value]);
				}
		}
		foreach my $cmd_class_key (@cmd_classes)
		{
				my $cmd_class = config::get_value($cmd_class_key);
				if ($cmd =~ /$cmd_class\s+/)
				{
						if (config::has_env_mod_value($cmd_class_key))
						{
								my @env_mod_values = config::get_env_mod_values($cmd_class_key);
								foreach my $env_mod_value (@env_mod_values)
								{
										my @cur_env_mod_value = @{$env_mod_value};
										push @all_env_mod_keys, $cur_env_mod_value[0];
										push @all_env_mod_values, join(":", @cur_env_mod_value[1..$#cur_env_mod_value]);
								}
						}
				}
		}
		for (my $i = 0; $i <= $#all_env_mod_keys; $i++)
		{
				my $env_value = "$all_env_mod_values[$i]";
				if ($ENV{$all_env_mod_keys[$i]})
				{
						$env_value = "$env_value:\$$all_env_mod_keys[$i]";
				}

				$cmd = "export $all_env_mod_keys[$i]=$env_value; $cmd";
		}

		my $umask = config::get_value("default_umask");
		my $found = 0;
		if (defined $cmd_key && config::has_umask_mod_value($cmd_key))
		{
				return config::get_umask_mod_value($cmd_key);
		}
		else
		{
				foreach my $cmd_class_key (@cmd_classes)
				{
						my $cmd_class = config::get_value($cmd_class_key);
						if ($cmd =~ /$cmd_class\s+/)
						{
								if (config::has_umask_mod_value($cmd_class_key))
								{
										$found = 1;
										if ($found)
										{
												die "Cmd matched two different umask_mod values: $cmd\n";
										}

										$umask = config::get_umask_mod_value($cmd_class_key);
								}
						}
				}
		}

		$cmd = "umask $umask; $cmd";

		my $preface_pipe_status = config::get_value("preface_pipe_status");
		if ($preface_pipe_status)
		{
				$cmd = "$preface_pipe_status; $cmd";
		}

		return $cmd;
}

sub get_rusage_mod($@)
{
		my $cmd = shift;
		my $cmd_key = shift;
		my @cmd_classes = config::get_all_cmd_class();

		my $found = 0;
		my $rusage = undef;

		my $overrides = config::get_value("rusage_mod_overrides");
		if ($overrides)
		{
				my @overrides = split(' ', $overrides);
				foreach my $override (@overrides)
				{
						(my $o_cmd_key, my $regex, my $value) = split(":", $override);
						if ($cmd_key =~ /$o_cmd_key/ && $cmd =~ /$regex/)
						{
								return $value;
						}
				}
		}

		if (defined $cmd_key && config::has_rusage_mod_value($cmd_key))
		{
				return config::get_rusage_mod_value($cmd_key);
		}


		foreach my $cmd_class_key (@cmd_classes)
		{
				my $cmd_class = config::get_value($cmd_class_key);
				if ($cmd =~ /$cmd_class\s+/)
				{
						if (config::has_rusage_mod_value($cmd_class_key))
						{
								if ($found)
								{
										die "Cmd matched two different rusage_mod values: $cmd\n";
								}
								$rusage = config::get_rusage_mod_value($cmd_class_key);

								$found = 1;
						}
				}
		}

		return $rusage;
}

sub get_bsub_batch($)
{
		my $key = shift;
		my $key_batch = 1;
		if (config::has_bsub_batch_value($key))
		{
				$key_batch = config::get_bsub_batch_value($key);
		}
		my $scale_batch = 1;
		if (config::is_key("bsub_batch_scale"))
		{
				$scale_batch = config::get_value("bsub_batch_scale");
		}
		return int($key_batch * $scale_batch);
}

sub generate_script($)
{
		my $cmd = shift;
		my $skip_preface_pipe = shift;
		my ($fh, $filename) = tempfile( "lapcmd.XXXX", DIR => get_tmp_dir);
		my $preface_pipe_status = config::get_value("preface_pipe_status");
		if ($preface_pipe_status && !$skip_preface_pipe)
		{
				$cmd = "$preface_pipe_status; $cmd";
		}
		print $fh $cmd;
		close $fh;
		return $filename;
}

sub check_cmd_length($$)
{
		my $cmd = shift;
		my $check = shift;
		if (!$check && length($cmd) > config::get_value("max_cmd_length"))
		{
				my $filename = generate_script($cmd);
				return "sh $filename";
		}
		else
		{
				return $cmd;
		}
}

sub run_cmd($@)
{
		my $cmd = shift;
		my $check = shift;
		my $suppress = shift;
		my $follow_cmd = shift;
		my $cmd_key = shift;
		my $error_code = 0;
		my $output = "";

		my $alarm_sig = "alarm\n";

		$cmd = preface_cmd($cmd, $cmd_key);

		my $timeout = undef;
		if (defined $cmd_key && config::has_timeout($cmd_key))
		{
				$timeout = config::get_timeout($cmd_key);
		}

		#for readability, remove cmds
		my $cmd_to_print = $cmd;

		foreach my $cmd_to_remove (config::get_value("return_bash_pipe_status"))
		{
				while (1)
				{
						my $index = index($cmd_to_print, $cmd_to_remove);
						if ($index > 0)
						{
								$cmd_to_print = substr($cmd_to_print, 0, $index) . substr($cmd_to_print, $index + length($cmd_to_remove));
						}
						else
						{
								last;
						}
				}
		}

		my $unchecked_cmd = $cmd;
		$cmd = check_cmd_length($cmd, $check);
		unless (defined $cmd)
		{
				return wantarray ? (1, "Couldn't write to " . get_tmp_dir()) : 1;
		}

		print STDERR "Running $cmd_to_print\n" unless $suppress;
		if ($cmd ne $unchecked_cmd)
		{
				print STDERR "Running as $cmd\n";
		}


		my $max_bytes = config::get_value("max_bytes_output");
		my $read_at_a_time = $max_bytes;
		if ($follow_cmd)
		{
				$read_at_a_time = 1;
		}
		my $pid = undef;
		if (!$check)
		{
				eval
				{
						local $SIG{ALRM} = sub { die $alarm_sig };
						if (defined $timeout)
						{
								alarm $timeout;
						}
						$cmd = "($cmd) 2>&1";
						$output = "";
						my $overwrote = 0;
						if ($pid = open (CMD, "$cmd |"))
						{
								my $cur_output = "";
								while (my $cur_length = read(CMD, $cur_output, $read_at_a_time))
								{
										my $new_length = length($output) + $cur_length;

										if ($new_length > $max_bytes)
										{
												$overwrote = 1;
												my $amount_to_trim = $new_length - $max_bytes;
												if ($amount_to_trim > length($output))
												{
														$output = "";
														$cur_output = substr($cur_output, 0, $amount_to_trim - length($output));
												}
												else
												{
														$output = substr($output, $amount_to_trim);
												}
										}
										$output .= $cur_output;

										if ($follow_cmd)
										{
												print STDERR $cur_output;
										}

								}
								close CMD;
						}
						else
						{
								$error_code = 1;
								$output = "Couldn't run $cmd";
						}
						$error_code = $?;
						if ($output && $overwrote)
						{
								$output = "Last $max_bytes bytes of output:\n$output";
						}
						if (defined $timeout)
						{
								alarm 0;
						}

				};
				if ($@)
				{
						if (defined $pid)
						{
								system("kill $pid");
								close CMD;
						}
						die $@ unless $@ eq $alarm_sig;
						$error_code = 256;
						$output = "Exceeded specified timeout $timeout\n";
				}
				if (!wantarray && !$follow_cmd)
				{
						print STDERR $output;
				}
		}
		$error_code = $error_code >> 8;
		return wantarray ? ($error_code, $output) : $error_code;
}

sub path_exists($@)
{
		my $path = shift;
		my $tries = shift || 1;
		while (1)
		{
				$tries--;
				if ($tries <= 0)
				{
						return -e $path;
				}
				else
				{
						return -e $path if -e $path;
				}
				sleep 5;
		}
}


sub get_mod_time($@)
{
		my $path  = shift;
		my $take_link_time = shift;

		my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size, $atime,$mtime,$ctime,$blksize,$blocks) = lstat($path);
		my $ltime = $mtime;
		my $time = $mtime;
		if (-l $path)
		{
				($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size, $atime,$mtime,$ctime,$blksize,$blocks) = stat($path);
				$time = $mtime;
		}


		if ($ltime && $time)
		{
				if ($take_link_time)
				{
						return $ltime;
				}
				else
				{
						return $time > $ltime ? $time : $ltime;
				}
		}
		elsif ($time)
		{
				return $time;
		}
		elsif ($ltime)
		{
				return $ltime;
		}
		else
		{
				return 0;
		}
}

sub trim($)
{
		my $val = shift;
		$val =~ s/^\s+//;
		$val =~ s/\s+$//;
		return $val;
}

sub listify(@)
{
		my @args = sort @_;
		my $list_start_char = config::get_value("list_start_char");
		my $list_end_char = config::get_value("list_end_char");
		my $list_delim_char = get_value("list_delim_char");

		return $list_start_char . join($list_delim_char, @args) . $list_end_char;
}


sub delistify(@)
{
		my $arg = shift;
		my $list_start_char = config::get_value("list_start_char");
		my $list_end_char = config::get_value("list_end_char");
		my $list_delim_char = config::get_value("list_delim_char");

		$arg =~ s/^\s+//g;
		$arg =~ s/\s+$//g;
		if (substr($arg, 0, 1) ne $list_start_char || substr($arg, length($arg) - 1, 1) ne $list_end_char)
		{
				return $arg;
		}
		else
		{
				return split($list_delim_char, substr($arg, 1, length($arg) - 2));
		}
}

sub guess_delim($)
{
		my $file = shift;
		my $delim = '\s+';
		my %delim_to_return = ('\s+'=>' ', '\t'=>"\t", ","=>',');

		if (open IN, $file)
		{
				my @orig_lines = <IN>;
				foreach my $trim (0, 1)
				{
						my @lines = @orig_lines;
						if ($trim)
						{
								@lines = map {s/^\s*//; $_} @orig_lines;
						}
						foreach my $cur_delim ('\s+', '\t', ",")
						{
								my $max = 1;
								my %counts = ();
								map {$counts{$_}++} map {my @a = split($cur_delim, $_); scalar @a} @lines;
								my @count_keys = keys %counts;

								if ($#count_keys == 0 && $count_keys[0] > $max)
								{
										$max = $count_keys[0];
										$delim = $cur_delim;
								}
						}
				}
		}
		return $delim_to_return{$delim};
}

sub chomp_header($$)
{
		my $fh = shift;
		my $key = shift;

		my $match_ignore = undef;
		if ($key && config::has_skip_re_value($key))
		{
				$match_ignore = config::get_skip_re_value($key);
		}

		my $double_comment = undef;

		if ($key)
		{
				$double_comment = config::has_double_comment($key);
		}

		my $header = undef;
		while (<$fh>)
		{
				if (defined $match_ignore)
				{
						next if /$match_ignore/;
				}

				next if /^\s*$/;
				if ($double_comment)
				{
						s/^\#//;
				}
				next if /^\#/;
				chomp;
				$header = $_;
				last;
		}
		die "No header!" unless $header;
		return $header;
}

sub chomp_col_mappings($$)
{
		my $fh = shift;
		my $delim = shift;
		my $key = shift;
		my $header = chomp_header($fh, $key);
		return get_col_mappings($header, $delim);
}

sub round($)
{
		my $num = shift;
		return int($num + .5);
}

sub get_error_file($@)
{
		my $file = shift;
		return config::get_value("error_file", (file=>$file));
}

sub get_started_file($@)
{
		my $file = shift;
		return config::get_value("started_file", (file=>$file));
}

sub get_error_path($@)
{
		my $key = shift;
		my %args = @_;
		my $file = config::get_value($key, %args);
		my $error_file = get_error_file($file);
		my $dir = config::get_dir($key, %args);

		return util::cat_dir_file($dir, $error_file);
}

sub get_error_path_from_path($@)
{
		my $path = shift;
		my $base = basename($path);
		my $dir = dirname($path);
		return util::cat_dir_file($dir, get_error_file($base));
}

sub get_started_path($@)
{
		my $key = shift;
		my %args = @_;
		my $file = config::get_value($key, %args);
		my $started_file = get_started_file($file);
		my $dir = config::get_dir($key, %args);

		return util::cat_dir_file($dir, $started_file);
}

sub get_started_path_from_path($@)
{
		my $path = shift;
		my $base = basename($path);
		my $dir = dirname($path);
		return util::cat_dir_file($dir, get_started_file($base));
}

sub get_cmd_path($@)
{
		my $key = shift;
		my %args = @_;
		my $file = config::get_value($key, %args);
		my $cmd_file = config::get_value("cmd_file", (file=>$file));
		my $dir = config::get_dir($key, %args);

		return util::cat_dir_file($dir, $cmd_file);
}

sub get_finished_path($@)
{
		my $key = shift;
		my %args = @_;
		my $file = config::get_value($key, %args);
		my $finished_file = config::get_value("finished_file", (file=>$file));
		my $dir = config::get_dir($key, %args);

		return util::cat_dir_file($dir, $finished_file);
}

sub record_error($$@)
{
		my $error_path = shift;
		my $error_code = shift;
		my $error_msg = shift;
		my $cmd = shift;

		if (!$error_code)
		{
				return;
		}

		open OUT, ">$error_path" or return;

		print OUT "$error_code\n";
		if ($cmd)
		{
				$cmd =~ s/\"/\\\"/g;
				$cmd =~ s/\`/\\\`/g;
				$cmd = "Failed on command: $cmd";
				print OUT "$cmd\n";
		}

		print OUT "$error_msg";
		close OUT;
}

sub check_error($)
{
		my $error_path = shift;

		open IN, $error_path or return 0;

		if ($_ = <IN>)
		{
				chomp;
				return $_;
		}
		else
		{
				return 1;
		}
}

sub check_key_error($%)
{
		my $key = shift;
		my %args = @_;
		my $error_path = get_error_path($key, %args);
		return check_error($error_path);
}

sub check_path_error($)
{
		my $path = shift;
		my $error_path = get_error_path_from_path($path);
		return check_error($error_path);
}


sub check_key_error_time($%)
{
		my $key = shift;
		my %args = @_;
		if (!check_key_error($key, %args))
		{
				return undef;
		}
		my $error_path = get_error_path($key, %args);
		return get_mod_time($error_path);
}

sub check_key_inputs_different($$@)
{
		my $key = shift;
		my $input_key_to_input_paths = shift;
		my %args = @_;
		my $finished_path = get_finished_path($key, %args);
		return check_cmd_inputs_different($finished_path, $input_key_to_input_paths);
}


sub clear_error($)
{
		my $error_path = shift;
		unlink($error_path);
}

sub clear_finished($)
{
		my $finished_path = shift;
		unlink($finished_path);
}


sub record_key_finished($%)
{
		my $key = shift;
		my $input_paths = shift;
		my %args = @_;
		my $finished_path = get_finished_path($key, %args);

		record_finished($finished_path, $input_paths);
}

sub record_finished($$@)
{
		my $finished_path = shift;
		my $input_paths = shift || [];

		open OUT, ">$finished_path" or return;

		foreach my $input_path (@{$input_paths})
		{
				my $md5 = get_md5($input_path) || "";
				print OUT "$input_path $md5\n";
		}

		close OUT;
}

sub record_keys_started($%)
{
		my $keys = shift;
		my %args = @_;
		foreach my $key (@{$keys})
		{
				record_key_started($key, %args);
		}
}

sub clear_keys_started($%)
{
		my $keys = shift;
		my %args = @_;
		foreach my $key (@{$keys})
		{
				clear_key_started($key, %args);
		}
}

sub record_keys_error($%)
{
		my $keys = shift;
		my $status = shift;
		my $msg = shift;
		my %args = @_;
		foreach my $key (@{$keys})
		{
				record_key_error($key, $status, $msg, %args);
		}
}


sub record_key_started($%)
{
		my $key = shift;
		my %args = @_;
		my $started_path = get_started_path($key, %args);
		my $error_path = get_error_path($key, %args);
		my $finished_path = get_finished_path($key, %args);
		record_started($started_path, $error_path, $finished_path);
}

sub update_key_started($%)
{
		my $key = shift;
		my %args = @_;
		my $started_path = get_started_path($key, %args);
		utime(undef, undef, $started_path);
}

sub clear_key_started($%)
{
		my $key = shift;
		my %args = @_;
		my $started_path = get_started_path($key, %args);
		clear_started($started_path);
}

sub check_key_started($%)
{
		my $key = shift;
		my %args = @_;
		my $started_path = get_started_path($key, %args);
		return check_started($started_path);
}

sub check_path_started($)
{
		my $path = shift;
		my $started_path = get_started_path_from_path($path);
		return check_started($started_path);
}


sub check_key_started_time($%)
{
		my $key = shift;
		my %args = @_;
		if (!check_key_started($key, %args))
		{
				return undef;
		}
		my $started_path = get_started_path($key, %args);
		return get_mod_time($started_path);
}

sub record_key_cmd($$%)
{
		my $cmd = shift;
		my $key = shift;
		my %args = @_;
		my $cmd_path = get_cmd_path($key, %args);
		record_cmd($cmd, $cmd_path);
}

sub append_key_cmd($$%)
{
		my $cmd = shift;
		my $key = shift;
		my %args = @_;
		my $cmd_path = get_cmd_path($key, %args);
		record_cmd($cmd, $cmd_path, 1);
}

sub check_key_cmd_different($$%)
{
		my $cmd = shift;
		my $key = shift;
		my %args = @_;
		my $cmd_path = get_cmd_path($key, %args);
		return check_cmd_different($cmd, $cmd_path);
}

sub record_key_error($%)
{
		my $key = shift;
		my $status = shift;
		my $msg = shift;
		my %args = @_;
		my $error_path = get_error_path($key, %args);

		record_error($error_path, $status, $msg);
}


my %active_started_paths = ();
sub record_started($$)
{
		my $started_path = shift;
		my $error_path = shift;
		my $finished_path = shift;
		#print STDERR "Recording $started_path\n";
		#common::run_cmd("touch $started_path", 0, 1);
		open OUT, ">$started_path";
		close OUT;
		$active_started_paths{$started_path} = $error_path;
		clear_error($error_path);
		clear_finished($finished_path);
}

sub check_started($)
{
		my $started_path = shift;
		return path_exists($started_path);
}
sub clear_started($)
{
		my $started_path = shift;

		#print STDERR "Clearing $started_path\n";
		my $try_it = 1;
		while ($started_path && path_exists($started_path))
		{
				if ($try_it > 1)
				{
						sleep 5;
				}
				#common::run_cmd("rm -f $started_path", 0, 1);
				unlink($started_path);
				$try_it++;
				if ($try_it > 10)
				{
						last;
				}
		}
}

sub cleanup_started()
{
		foreach my $started_path (keys %active_started_paths)
		{
				my $error_path = $active_started_paths{$started_path};
				if (check_started($started_path) && !check_error($error_path))
				{
						record_error($error_path, 1, "Execution abnormally stopped");
				}
				clear_started($started_path);
		}
}

sub clean_cmd($@)
{
		my $cmd = shift;
		my $do_strip = shift;

		my $new_cmd = "";
		my $offset = 0;

		#$cmd =~ s/^\s*//;
		#$cmd =~ s/\s*$//;
		#$cmd =~ s/[^\S\t]+/ /g;

		my $strip_diff = config::get_value("strip_from_cmd_different");
		if ($strip_diff)
		{
				$cmd =~ s/$strip_diff//g;
		}

		my @cmd = split(/[^\S\t]+/, $cmd);
		$cmd = join(' ', @cmd);

		my $start = 0;
		my $end = -1;

		my $first = substr($cmd, 0, 1);
		if ($first eq ' ')
		{
				$cmd = substr($cmd, 1);
		}
		return $cmd;
}

sub record_cmd($$)
{
		my $cmd = shift;
		my $cmd_path = shift;
		my $append = shift;
		my $mode = ">";
		if ($append)
		{
		    $mode = ">>";
		}

		if (open OUT, "$mode$cmd_path")
		{
				$cmd = clean_cmd($cmd);
				print OUT "$cmd\n";
				close OUT;
		}
}

sub check_cmd_different($)
{
		my $cmd = shift;
		my $cmd_path = shift;
		my $check_cmd_different = config::get_value("check_cmd_different");
		if ($check_cmd_different && open IN, $cmd_path)
		{
				my $stored_cmd = <IN>;
				close IN;
				if ($stored_cmd)
				{
						chomp($stored_cmd);
						$stored_cmd = clean_cmd($stored_cmd, 1);
						$cmd = clean_cmd($cmd, 1);
						return $cmd ne $stored_cmd;
				}
		}
		return 0;
}

our $md5_cache = undef;

sub get_md5($)
{
		my $path = shift;

		if (!defined $md5_cache)
		{
				$md5_cache = cache->new(100);
		}
		my $cache_data = $md5_cache->get($path);
		if (defined $cache_data)
		{
				my $mod_time = $cache_data->[0];
				my $md5 = $cache_data->[1];
				if (defined $md5)
				{
						my $cur_mod_time = common::get_mod_time($path);
						if ($cur_mod_time == $mod_time)
						{
								return $md5;
						}
						else
						{
								$md5_cache->del($path);
						}
				}
		}

		my $max_bytes_md5 = config::get_value("max_bytes_md5");
		if (-e $path && -s $path <= $max_bytes_md5)
		{
				(my $status, my $new_md5_out) = run_cmd("md5sum $path", 0, 1);
				if ($new_md5_out =~ /^([0-9a-f]+)/)
				{
						$new_md5_out = $1;
						$cache_data = [common::get_mod_time($path), $new_md5_out];
						$md5_cache->ins($path, $cache_data);
						return $new_md5_out;
				}
		}

		return undef;
}

sub check_cmd_inputs_different($$@)
{
		my $finished_path = shift;
		my $input_key_to_input_paths = shift || {};

		my $different = 0;

		if (open IN, $finished_path)
		{
				my %old_md5s = ();
				while (my $line = <IN>)
				{
						chomp($line);
						my @cols = split(' ', $line);
						if (scalar(@cols) != 2)
						{
								next;
						}
						my $path = $cols[0];
						my $md5 = $cols[1];
						if (defined $md5)
						{
								$old_md5s{$path} = $md5;
						}
				}

				foreach my $input_key (keys %{$input_key_to_input_paths})
				{
						if (config::get_ignore_md5($input_key))
						{
								$different = 1;
								last;
						}
						my $input_paths = $input_key_to_input_paths->{$input_key};
						foreach my $new_input_path (@{$input_paths})
						{
								if ($old_md5s{$new_input_path})
								{
										my $new_md5 = get_md5($new_input_path);
										if (!defined $new_md5 || $new_md5 ne $old_md5s{$new_input_path})
										{
												$different = 1;
												last;
										}
								}
								else
								{
										$different = 1;
										last;
								}
						}
						last if $different;
				}
		}
		else
		{
				$different = 1;
		}
		return $different;
}


END
{
		cleanup_started();
		my %all_running_jobs = get_running_jobs();
		my %running_jobs = ();
		map {$running_jobs{get_job($_)} = 1} keys %all_running_jobs;
		my @running_jobs = keys %running_jobs;
		foreach my $running_job (@running_jobs)
		{
				print STDERR "Killing $running_job...\n";
				kill_job($running_job);
		}
}


1;
