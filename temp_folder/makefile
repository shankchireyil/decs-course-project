.PHONY: pin_mysql check_mysql_cpu load_data server_compile server_start

server_compile:
	gcc -g -Wall kv-server-civetweb.c ./civetweb/libcivetweb.a -I./civetweb/include -I. -lpthread -lmysqlclient -o kvserver

server_start:
	sudo taskset -c 0,1 ./kvserver

load_gen_compile:
	gcc kv-client-raw.c -o load_test -lcurl -lpthread

load_data:
	taskset -c 2-8 ./load_test -d 200 -t 15 -o post

pin_mysql:
	@PID=$$(pgrep mysqld); \
	echo "MySQL PID = $$PID"; \
	sudo taskset -pc 9-11 $$PID

check_mysql_cpu:
	@PID=$$(pgrep mysqld); \
	sudo taskset -pc $$PID
