-module(zql_oo_session,[Conn]).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help() -> 
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("-                                MODULE zql_oo_session                      -"),
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
p("get_number_or_nil (Key)                                        get the value of Key        "),
p("get_record( Key )                                get the record stored at Key"),
p("exists (Key)                                     does this record/Key exist  "),
p("                                                                             "),
p("create_record ( )                                create and return a record  "),
p("delete_record ( Key )                            delete a record             "),
p("                                                                             "),
p("print_all ( )                                    print all records           "),
p("count( )                                         count the number of records "),
p("                                                                             "),
p("incr( Key )                                      increment this key by one   "),
p("                                                                             "),
p("                                                                             "),

p(" Example:                                                                    "),
p("C = [{driver,db_riak_driver}, {hostname,'riak@127.0.0.1'},{bucket,<<\"default\">>}]."),
p("DB = session(C).                                                             "),
q('DB:set("Name", "Scott").                                                     '),
q('DB:get("Name" ).                                                             '),
p("-----------------------------------------------------------------------------"),
ok.

get(Key) -> zql:get( Conn, Key ).

get(Key,Prop) -> zql:get_property( Conn, Key, Prop).

get_or_nil(Key) -> zql:get_or_nil( Conn, Key ).


set( Key, Value ) -> zql:set(Conn, Key, Value).

ls( ) -> zql:ls( Conn ).

print_all( ) -> zql:print_all( Conn ).

count( ) -> zql:count(Conn).

connection() -> Conn.

get_record( Key ) -> zql_oo_record:new(Conn,Key).

create_record_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    create_record( ConnectionArgs )                -"),
p("-                                                                   -"),
p("-                       This creates a new record                   -"),
p("-                                                                   -"),
p("         The unique ID of the record is returned as a HEX string    -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("---------------------------------------------------------------------").

create_record( ) -> Key = uuid(),
                    Record = create_record( Key ),
                    Record.

create_record( Key ) -> Record = zql_oo_record:new( Conn, Key ),
                        Record:set( Key,"" ),
                        Record.


incr( Key ) -> Res = get_number_or_nil( Key ),
               Y = case Res of
                    nil -> set( Key , "1" ),1;
                    N -> X = N + 1,set(Key, to_string( X )),X
               end,
               Y.


set( Key, PropName, Value ) -> zql:set_property(Conn, Key, PropName,Value).

decr( Key ) -> Res = get_number_or_nil( Key ),
               Y = case Res of
                    nil -> set( Key , "-1" ),-1;
                    N -> X = N - 1,set(Key, to_string( X )),X
               end,
               Y.

list( ListName ) -> Oodb = zql_oo_helper:create_oo_session( Conn ),
                    List = zql_oo_list:new( Oodb, ListName ),
                    List.


get_number_or_nil( Key ) -> Entry = get_or_nil( Key ),
                            X = case Entry of
                                nil -> nil;
                                N -> I = to_integer( N ), I
                            end,
                            X.

exists(Key) -> zql:exists(Conn,Key).

last() ->      LastUsed = get_record("last_used"),

               Id = LastUsed:get("last"),
               Record = get_record( Id ),

               Record.

last2() ->     LastUsed = get_record("last_used"),

               Id = LastUsed:get("last2"),
               Record = get_record( Id ),

               Record.
