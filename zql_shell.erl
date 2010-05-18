-module(zql_shell).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help() -> 

p("help - this command"),
p("connections - lists the possible connections"),
p("use connection name"),
p("connection - shows the current connection"),
p("start - starts the mnesia database"),
p("it - the last thing created"),
p("quit - exits to the command line").



help(Args) -> Num = length(Args),
              
              case Num of 
                   0 -> help();

                   _ -> Command = nth(1,Args),
		     	HelpFunctionName = Command ++ "_help",
                     	apply(sh, list_to_atom(HelpFunctionName), [ ])
              end.


continue() -> start().

start() -> 
        p(""),
     	p("---------------------------------------------------------------------------------------------"),
        InputWithReturn = io:get_line(">"),
	Input = remove_newline(InputWithReturn),
        
        Tokens = string:tokens(Input, " \n"),
        [Command | Args] = Tokens,

	CommandAtom = list_to_atom(Command),

        case CommandAtom of
             it -> it(), continue();
             get -> read(Args), continue();
             help -> help(Args), continue();
	     use -> use_connection(Args), continue();
	     start -> start_mnesia(),continue();

	     connection -> connection(), continue();
             connections -> connections(), continue();

	     q -> finished;
             quit -> finished;

             _UnknownCommand -> process(Input), continue()
        end.

start_mnesia() -> mnesia:start().

connections() -> Conns = zql:list_connections(),
                 for_each_item( Conns, fun(C) -> zql_shell:test_connection(C) end ).

connection() -> C = zql:get( zql:get_connection(system), "conn_name"),
	     	p( to_string(C) ).

use_connection(Args) -> ConnectionName = nth(1,Args),
                        % SKey = to_string(Key),
                        zql:set( zql:get_connection(system), "conn_name", ConnectionName).

test_connection(Conn) -> p(Conn).
                         %apply(zql,test_connection,[Conn]).

it_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                           it                                      -"),
p("-                                                                   -"),
p("-     Used to refer to the last used object                         -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > get person_ref						       -"),
p("- > delete it                                                       -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

it() -> R = last(),
        R:print().





read(Args) -> p("get called with arg count of:"),
              Num = length(Args),
              
              case Num of 
                   1 -> Key = nth(1,Args),
                        SKey = to_string(Key),
                        V = zql:get( db_conn_args(), SKey),
                        p(V);
                   X -> print_number(X)
              end.





process_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                      process( InputText )                         -"),
p("-                                                                   -"),
p("-    processes free text       				       -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > 						       		       -"),
p("- >                                                        	       -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

process( InputText ) -> p("Searching for '" ++ InputText ++ "'"),
	 	     	V =  zql:get( db_conn_args(), InputText ),
	 	     	p(V).



hello( ) -> p("Hello. System is available").







oo_db( ) -> DB = zql:create_oo_session( db_conn_args() ),
            DB.

db_conn_args () -> ExistsConnSpec = zql:exists( zql:get_connection(system), "conn_name"),
	     	   Conn = case ExistsConnSpec of
		   	true ->  zql:get_connection(to_atom(zql:get(zql:get_connection(system), "conn_name")));
 			false -> zql_connections:local_cassandra_connection()
		end,
		Conn.

whichdb( ) -> zql:whichdb( db_conn_args() ).

test( ) -> zql:test_with_connection( db_conn_args() ).



lsdb( ) -> zql:ls( db_conn_args() ).

count( ) -> zql:count( db_conn_args() ).


find( ) -> count( ).

add(Type) -> DB = oo_db(),
             Record = DB:create_record( ),
             Record:set(type,Type),
             Record.



lp() ->  Last = last(),
         Last:print().

lp2() -> Last2 = last2(),
         Last2:print().

history() -> lp2(),
             lp().


add_code(TriggerRule, Code) -> DB = oo_db(),
                               Record = DB:create_record( ),
                               Record:set(type,"code"),
                               Record:set(trigger, TriggerRule),
                               Record:set(code, Code),
                               Id = Record:id(),
                               Db = db_conn_args(),
                               CodeRecord = zql_oo_code_record:new( Db, Id ),
                               CodeRecord.

add_code( ) ->                 add_code("", "").



last( Record )      -> Oodb = oo_db(), 
                       RecentlyUsed = Oodb:create_record("last_used"),
                       LastId = RecentlyUsed:get("last"),
                       RecentlyUsed:set( "last2", LastId ),
                       RecentlyUsed:set( "last", Record:id() ).

last() ->      Oodb = oo_db(),
               LastUsed = Oodb:last(),
               LastUsed.

last2() ->     Oodb = oo_db(),
               LastUsed2 = Oodb:last2(),
               LastUsed2.

new( ) -> DB = oo_db(),
          Record = DB:create_record( ),
          Record.

%ContentKey = sha1:hexstring( Text ),

store(Text) -> new( Text ).
new( Text ) -> 
                 Oodb = oo_db(),
                 Record = Oodb:create_record( ),
                 Record:set( Text ),
                 last(Record),
                 Record.

%it_is_a( Type ) -> add_relationship( X, "is a", Y ).

add_relationship( X, Relationship, Y ) ->

                 Oodb = oo_db(),
                 Record = Oodb:create_record( ),
                 Record:set( "type", Relationship ),
                 Record:set( "first", X:id() ),
                 Record:set( "second", Y:id() ),
                 last(Record),
                 Record.

r( Relationship ) -> add_relationship( last() , Relationship, last2() ).
print(X) -> zprint:p(X).
