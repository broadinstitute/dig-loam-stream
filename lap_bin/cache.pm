package cache;

use strict;

## okay, a second shot at this

sub new {
		my $self=bless {},shift;

		## we keep track of lots of things
		$self->{size}=shift || 30;
		$self->{count}=0;
		$self->{head}=undef;
		$self->{tail}=undef;
		$self->{hash}={};

		$self;
}

sub ins {
		my $self=shift;
		my $key=shift;
		my $val=shift;

		## put it at the head (we walk down -- next is further down the chain)
		## create the hash ref
		my $new={
				prev => undef,
				next => $self->{head},
				key => $key,
				value => $val,
		};

		## tack it on
		$self->{head}=$new;
		## and the hash for quick lookup
		$self->{hash}->{$key}=$new;
		$self->{count}++;

		## do our size checking
		if ($self->{count}==$self->{size}+1) {
				$self->del($self->{tail}->{key});
		}
}

sub del {
		my $self=shift;
		my $key=shift;
		my $item=$self->{hash}->{$key};

		return unless defined $item;

		## reattach it's prev/next
		$item->{prev}->{next}=$item->{next} if defined $item->{prev};
		$item->{next}->{prev}=$item->{prev} if defined $item->{next};
		## head/tail handling
		$self->{head}=$item->{next} if $self->{head}==$item;
		$self->{tail}=$item->{prev} if $self->{tail}==$item;
		## and the hash
		delete $self->{hash}->{$key};
		## and the count
		$self->{count}--;
}

sub get {
		my $self=shift;
		my $key=shift;
		my $item=$self->{hash}->{$key};

		return unless defined $item;
		## move it to the front by
		## delete it
		$self->del($key);
		## and add it
		$self->ins($key,$item->{value});

		$item->{value};
}

sub show {
		my $self=shift;
		my $item=$self->{head};

		while (defined $item) {
				print "$item: ";
				print "key $item->{key} " if defined $item->{key};
				print "value $item->{value} " if defined $item->{value};
				print "\n";
				$item=$item->{next};
		}
}

1;
