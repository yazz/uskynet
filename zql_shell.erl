-module(zql_shell).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help() -> 

p("help - this command"),
p("connections - lists the possible connections"),
p("use connection name"),
p("connection - shows the current connection"),
p("count/size - number of items in the system"),
p("it - the last thing created"),
p("create it - make it into an item"),
p("quit - exits to the command line").







connections() -> Conns = zql:list_connections(),
                 for_each_item( Conns, fun(C) -> zql_shell:test_connection(C) end ).

use_connection(Args) -> ConnectionName = nth(1,Args),
                        % SKey = to_string(Key),
                        zql:set( zql:get_connection(system), "conn_name", ConnectionName).

connection() -> C = zql:get( zql:get_connection(system), "conn_name"),
	     	    p( to_string(C) ).







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
p("- > person_ref						                               -"),
p("- > delete it                                                       -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

it() -> R = last(),
        R:print().




create_it() ->  p("Attempting to create an item"),
                ItemText = get_value(last_used_text),
                Id = store( ItemText ),
                p(Id).
                


get_value( Key ) -> zql:get_or_nil( db_conn_args(), Key).



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
p("-                       processes free text       				   -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > 						       		                               -"),
p("- >                                                        	       -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

process( InputText ) -> 

                        InputTextIsAKey = zql:get( db_conn_args(), InputText ),
                        case InputTextIsAKey of
                            [ok,V] ->   p("Searching for '" ++ InputText ++ "'"),
                                        p(V);

                            [_,_V] ->   p(" '" ++ InputText ++ "' doesn't exist (type 'create it' to it turn into an item)"),
                                        zql:set(db(), "last_input", InputText)
                                        
                                        
                        end.



hello( ) -> p("Hello. System is available").






oodb() -> oo_db().
oo_db( ) -> DB = zql:create_oo_session( db_conn_args() ),
            DB.

db() -> db_conn_args().
db_conn_args () ->  WhichConnectionToUseResult = zql:get(sys_connection(), "conn_name"),
                    Conn = case WhichConnectionToUseResult of
                        [ok, ConnName] -> zql:get_connection( ConnName );
                        [_,_] -> zql_connections:local_mnesia_connection()
                    end,
                    Conn.

sys_connection() -> zql:get_connection(system).

whichdb( ) -> zql:whichdb( db_conn_args() ).

test( ) -> zql:test_with_connection( db_conn_args() ).



lsdb( ) -> zql:ls( db_conn_args() ).


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

count() ->  Count = zql:count(db()),
            p("Number of items in database"),
            print_number(Count).


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

help(Args) ->   Num = length(Args),
              
                case Num of 
                    0 -> help();

                    _ -> Command = nth(1,Args),

                    HelpFunctionName = Command ++ "_help",
                    apply( sh, list_to_atom( HelpFunctionName ), [ ] )
              end.

continue() -> start().


start_mnesia() -> mnesia:create_schema( [ node() ] ),
                  mnesia:start(),
                  ok.

init() -> start_mnesia().

start() -> 
        p(""),
     	p("--------------------------------------------------------------"),
        InputWithReturn = io:get_line(">"),
	Input = remove_newline(InputWithReturn),
        
        Tokens = string:tokens(Input, " \n"),
        [Command | Args] = Tokens,

	CommandAtom = list_to_atom(Command),

        case CommandAtom of
            it -> it(), continue();
            get -> read(Args), continue();
            create -> create_it(), continue();
            help -> help(Args), continue();
            count -> count(), continue();
            size -> count(), continue();
	     use -> use_connection(Args), continue();
	     start -> start_mnesia(),continue();

	     connection -> connection(), continue();
             connections -> connections(), continue();

	     q -> finished;
             quit -> finished;

             _UnknownCommand -> process(Input), continue()
        end.


