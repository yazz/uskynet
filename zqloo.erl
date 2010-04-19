-module(zqloo,[Conn]).
-compile(export_all).

get(Key) -> zql:get(Conn,Key).
set(Key,Value) -> zql:set(Conn, Key,Value).
ls( ) -> zql:ls(Conn).
print_all( ) -> zql:print_all( Conn ).
count( ) -> zql:count(Conn).


get_record(Key) -> recordoo:new(Conn,Key).