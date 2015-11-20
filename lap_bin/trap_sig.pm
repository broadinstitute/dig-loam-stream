sub trap_signal() {return "Execution interrupted";}
$SIG{TERM} = $SIG{INT} = $SIG{QUIT} = $SIG{HUP} = sub { die trap_signal(); };

1;
