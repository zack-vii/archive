#!/bin/sh
#
# /etc/init.d/transientserver.sh
# Subsystem file for "transient" server
#
# chkconfig: 2345 95 05
# description: transient server daemon
#
# processname: transientserver
# config: /etc/sysconfig/transientserver
# pidfile: /var/run/transientserver.pid

RETVAL=0
prog="transientServer"


start() {
	echo -n $"Starting $prog:"
	python /git/archive/python/scripts/transientserver.py 60
	RETVAL=$?
	[ "$RETVAL" = 0 ] && touch /var/lock/subsys/$prog
	echo
}

stop() {
	echo -n $"Stopping $prog:"
	killproc $prog -TERM
	RETVAL=$?
	[ "$RETVAL" = 0 ] && rm -f /var/lock/subsys/$prog
	echo
}

reload() {
	echo -n $"Reloading $prog:"
	killproc $prog -HUP
	RETVAL=$?
	echo
}

case "$1" in
	start)
		start
		;;
	stop)
		stop
		;;
	restart)
		stop
		start
		;;
	reload)
		reload
		;;
	condrestart)
		if [ -f /var/lock/subsys/$prog ] ; then
			stop
			# avoid race
			sleep 3
			start
		fi
		;;
	status)
		status $prog
		RETVAL=$?
		;;
	*)
		echo $"Usage: $0 {start|stop|restart|reload|condrestart|status}"
		RETVAL=1
esac
exit $RETVAL
