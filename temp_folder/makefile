.PHONY: pin_mysql check_mysql_cpu

server_compile:
	gcc -g -Wall kv-server-civetweb.c ./civetweb/libcivetweb.a -I./civetweb/include -I. -lpthread -lmysqlclient -o kvserver

server_start:
	sudo taskset -c 0,1 ./kvserver

load_gen_compile:
	gcc kv-client-raw.c -o load_test -lcurl -lpthread


pin_mysql:
	@PID=$$(pgrep mysqld); \
	echo "MySQL PID = $$PID"; \
	sudo taskset -pc 9-11 $$PID

check_mysql_cpu:
	@PID=$$(pgrep mysqld); \
	sudo taskset -pc $$PID
