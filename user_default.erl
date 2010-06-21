-module(user_default).
-compile(export_all).
-include_lib("zql_imports.hrl").


zhelp() -> 
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("-               default commands from the erlang shell                      -"),
p("-                                                                           -"),
p("-----------------------------------------------------------------------------"),
p("                                                                             "),
p(" shell( )                                  starts a ZQL shell                "),
p(" test ()                                   run the self tests for zql        "),
p(" test_connection( ConnectionArgs )         tests a connection                "),
p(" oo( )                                     returns an OO session to ZQL      "),
p(" c( )                                      compile all                       "),
p(" compile_all( )                            compile all                       "),
p("                                                                             "),
p(" Example:                                                                    "),
q(' C = [ {driver,db_cassandra_driver}, {hostname,"127.0.0.1"}].                '),
q(' test_connection( C).                                                        '),
q(' DB = oo( ).                                                                 '),
q(' Record = DB:get("key234").                                                  '),
p("-----------------------------------------------------------------------------"),
ok.

           
c() -> compile_all().  
compile_all() -> zql_compiler:compile_all().



sh() -> shell().
shell() -> zql_compiler:compile_all(),
           p(" start( )                                  starts mnesia database            "),
           zql_shell:init(),
           zql_shell:start().

test() -> zql:test().

db() -> sys_connection().

count() -> (oodb()):count().

oodb( ) -> DB = zql_oo_helper:create_oo_session( db() ),
           DB.

db2() ->  WhichConnectionToUseResult = zql:get(sys_connection(), "conn_name"),

                    Conn = case WhichConnectionToUseResult of

                        [ok, ConnName] -> get_connection( ConnName );
                        [_,_] -> zql_connections:local_mnesia_connection()

                    end,
                    Conn.


whichdb( ) -> zql:whichdb( db() ).


mnesia() -> zql:get_connection(local_mnesia_connection).

start() -> init(),
           zql_server:start().
init() -> zql_shell:init().

stop() -> zql_server:stop().

gpn(Key)->zql:get_property_names(mnesia(),Key).
set(Key,PropName,Value) ->  Db = oodb(),
                            Db:set( Key, PropName, Value ).



incr( Key ) -> Db = oodb(),
               Db:incr( Key ).

new_record() -> Db = oodb(),
                Record = Db:create_record(),
                Record.

add_code(Code) -> 

    CodeRecord = new_record(),
    CodeRecord:set(code,Code),
    CodeRecord:set(type,code),
    ok.

ask( [ Query | MoreQueries  ] ) ->  ask_one( Query ),
                                    ask( MoreQueries ).

ask_one( Query ) -> {Left, Operator,Right} = Query,
                    ask( Left, Operator, Right ).

ask( Left, equals, Right ) -> QueryIndex = "index_" ++ to_string(Left) ++ "_equals_" ++ to_string( Right ),
                              X = incr( QueryIndex ),
                              set( QueryIndex, type, index_count ),
                              Indexes = list("indexes"),
                              Indexes:add( QueryIndex ),
                              [{QueryIndex},{count,X}].

list(ListName) -> Db = oodb(),
                  Db:list(ListName).

get( Key, Prop ) -> Db = oodb(),
                    Db:get( Key, Prop ).


foreach(List, Fn) -> ok.

lists() -> Db = oodb(),
                Lists = Db:list("lists"),
                Lists:ls().