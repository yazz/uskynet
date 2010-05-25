-module(zql_shell).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help() -> 

p("help - this command"),

p("connections - lists the possible connections"),
p("use connection name"),
p("current connection - shows the current connection"),
p("test connection - tests the named connection"),

p("count/size - number of items in the system"),
p("it - the last thing created"),
p("create it - make it into an item"),
p("ls - list files"),
p("pwd - print current directory"),
p("quit - exits to the command line").







connections_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                       connections                                 -"),
p("-                                                                   -"),
p("-          show a list of the connections available                 -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > connections( )                                                  -"),
p("-                                                                   -"),
p("- Used by shell as:                                                 -"),
p("-                                                                   -"),
p("- > connections                                                     -"),
p("- > get connections                                                 -"),
p("---------------------------------------------------------------------").
connections() -> Conns = zql:list_connections(),
                 for_each_item( Conns, fun(C) -> zql_shell:test_connection(C) end ).




use_connections_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                       use_connections                             -"),
p("-                                                                   -"),
p("-                 use a specific connection                         -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > use_connection( oracle )                                        -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("- Used by shell as:                                                 -"),
p("-                                                                   -"),
p("- > use connection oracle                                           -"),
p("---------------------------------------------------------------------").
use_connection(Args) -> ConnectionName = nth(1,Args),
                        zql:set( zql:get_connection(system), "conn_name", ConnectionName).






show_current_connection_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                  show_current_connection                          -"),
p("-                                                                   -"),
p("-                shows the current connection in use                -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > show_current_connection( )                                      -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("- Used by shell as:                                                 -"),
p("-                                                                   -"),
p("- > show current connection                                         -"),
p("---------------------------------------------------------------------").

show_current_connection() ->    ConnectionName = zql:get( sys_connection(), "conn_name"),
	     	                    p( ConnectionName ).







test_connection_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                       test_connection(Conn)                       -"),
p("-                                                                   -"),
p("-          show a list of the connections available                 -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > connections( )                                                  -"),
p("-                                                                   -"),
p("- Used by shell as:                                                 -"),
p("-                                                                   -"),
p("- > connections                                                     -"),
p("- > get connections                                                 -"),
p("---------------------------------------------------------------------").

test( ) -> test_with_connection( db() ).

test_with_connection( Conn ) -> p( Conn ),
                                apply(zql,test_connection,[ Conn ]).





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
                ItemText = get_value( last_typed_text ),
                Record = store( ItemText ),
                Record:print().
                


get_value( Key ) -> zql:get_or_nil( db(), Key).



read(Args) -> p("get called with arg count of:"),
              Num = length(Args),
              
              case Num of 
                   1 -> Key = nth(1,Args),
                        SKey = to_string(Key),
                        V = zql:get( db(), SKey),
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

                        InputTextIsAKey = zql:get( db(), InputText ),

                        case InputTextIsAKey of

                            [ ok, Value ] ->   p( "Searching for '" ++ InputText ++ "'" ),
                                               p( Value );

                            [ _, _V ] ->    TrackingToken = content_key( InputText ),
                                            TrackingTokenResult = zql:get_property( db(), TrackingToken, tracking_id ),

                                            case TrackingTokenResult of
                                                [ ok, Value2 ] -> p( "Found '" ++ InputText ++ "' as ID " ++ Value2 );

                                                [ _, _Value2 ] -> p( " '" ++ InputText ++ "' doesn't exist (type 'create it' to it turn into an item)" ),
                                                       	       	  zql:set(db(), "last_typed_text", InputText)
                                            end
                        end.



hello( ) -> p("Hello. System is available").







oodb( ) -> DB = zql:create_oo_session( db() ),
           DB.

db() ->  WhichConnectionToUseResult = zql:get(sys_connection(), "conn_name"),

                    Conn = case WhichConnectionToUseResult of

                        [ok, ConnName] -> zql:get_connection( ConnName );
                        [_,_] -> zql_connections:local_mnesia_connection()

                    end,
                    Conn.

sys_connection() -> zql:get_connection(system).

whichdb( ) -> zql:whichdb( db() ).





lsdb( ) -> zql:ls( db() ).


find( ) -> count( ).

add(Type) -> DB = oodb(),
             Record = DB:create_record( ),
             Record:set(type,Type),
             Record.



lp() ->  Last = last(),
         Last:print().

lp2() -> Last2 = last2(),
         Last2:print().

history() -> lp2(),
             lp().


add_code(TriggerRule, Code) -> DB = oodb(),
                               Record = DB:create_record( ),
                               Record:set(type,"code"),
                               Record:set(trigger, TriggerRule),
                               Record:set(code, Code),
                               Id = Record:id(),
                               Db = db(),
                               CodeRecord = zql_oo_code_record:new( Db, Id ),
                               CodeRecord.

add_code( ) ->                 add_code("", "").



last( Record )      -> Oodb = oodb(), 
                       RecentlyUsed = Oodb:create_record("last_used"),
                       LastId = RecentlyUsed:get("last"),
                       RecentlyUsed:set( "last2", LastId ),
                       RecentlyUsed:set( "last", Record:id() ).

last() ->      Oodb = oodb(),
               LastUsed = Oodb:last(),
               LastUsed.

last2() ->     Oodb = oodb(),
               LastUsed2 = Oodb:last2(),
               LastUsed2.

new( ) -> DB = oodb(),
          Record = DB:create_record( ),
          Record.



store( Text) -> Oodb = oodb(),
                Record = Oodb:create_record( ),
                Record:set( Text ),

                ContentKey = content_key( Text ),
                ContentTrackingRecord = Oodb:create_record(ContentKey),
                ContentTrackingRecord:set( tracking_id, Record:id() ),
                 
                %last(Record),
                Record.

%it_is_a( Type ) -> add_relationship( X, "is a", Y ).

count() ->  Count = zql:count(db()),
            p("Number of items in database"),
            print_number(Count).


add_relationship( X, Relationship, Y ) ->

                 Oodb = oodb(),
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




start_mnesia() -> mnesia:create_schema( [ node() ] ),
                  mnesia:start(),
                  ok.

init() -> start_mnesia().

continue() -> start().
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

	            current -> show_current_connection(), continue();
                connections -> connections(), continue();

	            q -> finished;
                quit -> finished;

                _UnknownCommand -> process(Input), continue()
            end.
