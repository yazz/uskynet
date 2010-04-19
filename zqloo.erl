-module(zqloo,[Conn]).
-compile(export_all).
-import(zprint,[println/1,p/1,q/1,print_number/1]).
-import(zutils,[uuid/0]).

help() -> 
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("-                                MODULE zqloo                               -"),
p("-                                                                           -"),
p("-                 OO Erlang interface to the ZQL system                     -"),
p("-                                                                           -"),
p("-----------------------------------------------------------------------------"),
p("                                                                             "),
p("help (Command)                                   get help on a command       "),
p("test ()                                          run the self tests for zql  "),
p("                                                                             "),
p("ls ()                                                                        "),
p("set (Key, Value)                                 set a value                 "),
p("put (Key, Value)                                 set a value                 "),
p("get (Key)                                        get the value of Key        "),
p("get_record( Key )                                get the record stored at Key"),
p("exists (Key)                                     does this record/Key exist  "),
p("                                                                             "),
p("create_record ( )                                create and return a record  "),
p("delete_record ( Key )                            delete a record             "),
p("                                                                             "),
p("print_all ( )                                    print all records           "),
p("count( )                                         count the number of records "),
p("                                                                             "),
p(" Example:                                                                    "),
p("C = [{driver,db_riak_driver}, {hostname,'riak@127.0.0.1'},{bucket,<<\"default\">>}]."),
p("DB = zql:session(C).                                                         "),
q('DB:set("Name", "Scott").                                                     '),
q('DB:get("Name" ).                                                             '),
p("-----------------------------------------------------------------------------"),
ok.

get(Key) -> zql:get(Conn,Key).
set(Key,Value) -> zql:set(Conn, Key,Value).
ls( ) -> zql:ls(Conn).
print_all( ) -> zql:print_all( Conn ).
count( ) -> zql:count(Conn).


get_record(Key) -> recordoo:new(Conn,Key).

create_record( ) -> Key = zql:create_record( Conn ),
                    Record = recordoo:new( Conn, Key ),
                    Record.