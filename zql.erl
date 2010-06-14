-module(zql).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help() -> 
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("-                                MODULE zql                                 -"),
p("-                                                                           -"),
p("-                   Erlang interface to the ZQL system                      -"),
p("-                                                                           -"),
p("-----------------------------------------------------------------------------"),
p("                                                                             "),
p("help (Command)                                   get help on a command       "),
p("test ()                                          run the self tests for zql  "),
p("local ()                                         returns a local connection  "),
p("test_connection( ConnectionArgs )                tests a connection          "),
p("create_oo_session( ConnectionArgs )              returns a zqloo OO instance "),
p("whichdb( ConnectionArgs )                        returns the DB name         "),
p("                                                                             "),
p("ls (ConnectionArgs)                              lists all keys              "),
p("set (ConnectionArgs, Key, Value)                 set a value                 "),
p("get (ConnectionArgs, Key)                        get the value of Key        "),
p("exists (ConnectionArgs, Key)                     does this record/Key exist  "),
p("                                                                             "),
p("create_record (ConnectionArgs )                  create a record             "),
p("delete_record (ConnectionArgs )                  delete a record             "),
p("                                                                             "),
p("get_property_names (C, Key)                      gets the property names     "),
p("has_property (ConnectionArgs, Key, PropertyName) check for a property        "),
p("set_property (C, Key, PropertyName, Value)       set a property              "),
p("get_property (C, Key, PropertyName)              get a property              "),
p("delete_property (C, Key, Property)               delete a property           "),
p("                                                                             "),
p("print (ConnectionArgs, Key)                      print record                "),
p("print_all (ConnectionArgs)                       print all records           "),
p("                                                                             "),
p(" Example:                                                                    "),
q(' C = [ {driver,zql_cassandra_driver}, {hostname,"127.0.0.1"}].               '),
q(' zql:set(C, "Name", "Scott").                                                '),
q(' zql:get(C, "Name" ).                                                        '),
p("-----------------------------------------------------------------------------"),
ok.

help(Command)       ->  HelpFunctionName = atom_to_list(Command) ++ "_help",
                        apply(zql, list_to_atom(HelpFunctionName), [ ]).






















test_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                           test()                                  -"),
p("-                                                                   -"),
p("-          This runs basic tests on the ZQL system by creating      -"),
p("-         and performing some actions on records                    -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

test() -> ConnectionArgs = local(),
          test_with_connection( ConnectionArgs ).


test_with_connection(C) ->

                p("Number of records in datastore:"),
                Count = count(C),
                print_number(Count),

                delete_all(C,yes_im_sure),                

                set(C, "boy", "Is here"),
                p("Saved 'boy' as 'is here'"),
                [ok,Value] = get(C, "boy"),
                p("got value of boy as : "),
                println(Value),
                println("Check 'boy' exists :"),
                Exists = exists(C, "boy"),
                println(Exists),
                delete(C,"boy"),
                println("deleted 'boy'"),
                println("Check 'boy' exists :"),
                Exists2 = exists(C, "boy"),
                println(Exists2),
                println("-----------------------").












local_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                           local( )                                -"),
p("-                                                                   -"),
p("-             returns a local database connection object            -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs1 = local().                                        -"),
p("-                                                                   -"),
p("- ConnectionArgs2 = [{driver,zql_riak_driver}, {hostname,'riak@127.0.0.1'},{bucket,<<\"default\">>}]."),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

local() -> ConnectionArgs = zql_connections:local_mnesia_connection(),
           ConnectionArgs.
                
test_conection_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    test_connection( ConnectionArgs )              -"),
p("-                                                                   -"),
p("-                    This tests the database connection             -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- test_connection( ConnectionArgs ).                                -"),
p("- >> {ok, connection_fine}                                          -"),
p("-                                                                   -"),
p("- test_connection( bad_data ).                                      -"),
p("- >> {error, bad_connection}                                        _"),
p("---------------------------------------------------------------------").

test_connection( ConnectionArgs ) -> try ( 
                                         test_conn( ConnectionArgs ) 
                                     ) of
                                         _ -> {ok, connection_fine}
                                     catch
                                         _Exception:_Reason -> {error, bad_connection}
                                     end.


test_conn( ConnectionArgs ) -> Driver = get_zql_driver_name( ConnectionArgs ),
                               apply( Driver , connect, [ ConnectionArgs ]),
                               ok.




whichdb( ConnectionArgs ) -> Driver = get_zql_driver_name( ConnectionArgs ),
                             Name = apply( Driver , name, [ ]),
                             Name.






print_all_help()  -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    print_all(Connection)                          -"),
p("-                                                                   -"),
p("-          Prints all the records to the console                    -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

print_all(C) -> PrintRecord = fun(Key) -> print(C,Key) end,
                lists:foreach(PrintRecord, zql:ls(C)).


print_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    print(Connection, Key)                         -"),
p("-                                                                   -"),
p("-             Prints the record with ID of RecordID                 -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print( ConnectionArgs, Key ).                                     -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

print( ConnectionArgs, Key ) -> 
    p("--------------------------------------------------------------"),
    io:format("ID:~s~n", [Key]),
    p("--------------------------------------------------------------"),

    try ( print_fn( ConnectionArgs, Key )) of 
      ok -> ok
    catch
        _:_ -> p("error")
    end,
    p("--------------------------------------------------------------"),
    p(""),
    ok.

print_fn( Conn, Key ) ->

    PropertyNames = get_property_names( Conn, Key ),

    lists:foreach(
             fun( PropertyName ) ->

                  [ok,PropValue] = get_property( Conn , Key, PropertyName ), 

                  io:format( "~s:~s~n", [ to_string(PropertyName), to_string(PropValue) ]) 
             end,

             PropertyNames
    ),
    ok.











match_help() -> 
p("---------------------------------------------------------------------"),
p("-                    match(Connection)                              -"),
p("-                                                                   -"),
p("- This should match a set of records. Not implemented yet though    -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- match( ConnectionArgs ).                                          -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

%match(RecordData,Queries) -> 
%lists:foreach( fun(Property) -> match_property(Property,Queries) end , RecordData ).
%match_property(Property, Queries) -> lists:foreach( fun(Query) -> 
match(Value, equals, Value) -> true;
match(_Value, equals, _ExpectedValue) -> false.







get_zql_driver_name_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-               get_zql_driver_name(ConnectionArgs)                  -"),
p("-                                                                   -"),
p("-              gets the name of the database driver                 -"),
p("-                                                                   -"),
p("-  This is only used internally to get the database driver name     -"),
p("  from the connection parameters                                    -"),
p("                                                                    -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- get_zql_driver_name( ConnectionArgs ).                             -"),
p("                                                                    -"),
p("---------------------------------------------------------------------").

get_zql_driver_name( ConnectionArgs ) -> Driver = proplists:get_value( driver, ConnectionArgs ),
                                         Driver.










get_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                get( ConnectionArgs, Key )                         -"),
p("-                                                                   -"),
p("-           gets the value from the database for Key                -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > ConnectionArgs = zql:local( ).                                  -"),
q('- > zql:set( ConnectionArgs, "Name", "Bob").                        -'),
q('- > zql:get( ConnectionArgs, "Name").                               -'),
q('- [ok,"Bob"]                                                        -'),
q('                                                                    -'),
q('- > zql:get_property( ConnectionArgs, "Name", value).               -'),
q('- [ok,"Bob"]                                                        -'),
q('                                                                    -'),
q('- > zql:get( ConnectionArgs, "UndefinedThing" ).                    -'),
q('- [not_found, item]                                                 -'),
p("---------------------------------------------------------------------").

get( ConnectionArgs, Key ) -> BKey = to_binary(Key),
                              Result = get_property( ConnectionArgs, BKey, value ),
                              Result.



get_or_nil( ConnectionArgs, Key ) ->    Result = get( ConnectionArgs, Key),
                                        ReturnValue = case Result of
                                            [ok, Value] -> Value;
                                            [_X,_Y] -> nil
                                        end,
                                        ReturnValue.





get_property_names_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                 get_property_names(ConnectionArgs,Key)            -"),
p("-                                                                   -"),
p("-     gets all the properties for the record identified by Key      -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

get_property_names( ConnectionArgs, Key) -> Driver = get_zql_driver_name(ConnectionArgs),
                                            BKey = to_binary(Key),
                                            PropertyNames = apply(Driver, get_property_names, [ConnectionArgs, BKey]),
                                            PropertyNames.






get_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-          get_property( ConnectionArgs, Key, PropertyName)         -"),
p("-                                                                   -"),
p("- gets the value of the named property for record identified by Key -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

get_property( ConnectionArgs, Key, PropertyName) ->  BKey = to_binary(Key),
                                                     SPropertyName = to_string(PropertyName),
                                                     Driver = get_zql_driver_name( ConnectionArgs ),
                                                     Value = apply(Driver, get_property, [ConnectionArgs, BKey, SPropertyName]),
                                                     Value.




set_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    set(Connection,Key,Value)                      -"),
p("-                                                                   -"),
p("-      sets the value for a record identified by Key                -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
q('- set( ConnectionArgs, "system", "windows" ).                       -'),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

set( ConnectionArgs, Key, Value ) -> set_property( ConnectionArgs, Key, value, Value ).























has_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-      has_property(ConnectionArgs, Record, PropertyName)           -"),
p("-                                                                   -"),
p("-       Tests to see whether a record has a particular property     -"),
p("-      and returns either true or false                             -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

has_property( ConnectionArgs, Key, PropertyName ) -> BKey = to_binary(Key),
                                                     SPropertyName = to_string(PropertyName),
                                                     PropertyNames = get_property_names( ConnectionArgs, BKey ),
                                                     ContainsKey = lists:member( SPropertyName, PropertyNames ),
                                                     ContainsKey.








set_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                  set_property(ConnArgs, Key, Col, Value)          -"),
p("-                                                                   -"),
p("-                      Sets a property of a record                  -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
q('- set_property( ConnectionArgs, "Jonny",type,"Person" ).            -'),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

set_property( ConnArgs, Key, PropertyName, Value ) -> 
                                             Driver = get_zql_driver_name(ConnArgs),
                                             BKey = to_binary(Key),
                                             SPropertyName = to_string(PropertyName),
                                             apply(Driver, set_property, [ConnArgs, BKey, SPropertyName, Value]),

                                             ok.






delete_property_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    connect(ConnectionArgs)                        -"),
p("-                                                                   -"),
p("-                    connects to the database                       -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

delete_property(C, Key, PropertyName) ->

                             Driver = get_zql_driver_name(C),
                             BKey = to_binary(Key),
                             SPropertyName = to_string(PropertyName),
                             apply(Driver, delete_property, [C, BKey, SPropertyName]),
                             ok.



exists_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    connect(ConnectionArgs)                        -"),
p("-                                                                   -"),
p("-                    connects to the database                       -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
q('- exists( ConnectionArgs, "ABC" ).                                  -'),
q('- >> false                                                          -'),
q('- set( ConnectionArgs, "ABC", "Hello world").                       -'),
q('- exists( ConnectionArgs, "ABC" ).                                  -'),
q('- >> true                                                           -'),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

exists( Connection, Key ) -> Driver = get_zql_driver_name( Connection ),
                             BKey = to_binary(Key),
                             Exists = apply( Driver, exists, [ Connection, BKey ] ),
                             Exists.











delete_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                      delete(Connection,Key)                       -"),
p("-                                                                   -"),
p("-                  deletes a record from the database               -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

delete(Connection,Key) -> Driver = get_zql_driver_name(Connection),
                          BKey = to_binary(Key),
                          apply(Driver, delete, [Connection, BKey]),
                          ok.








ls_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                        ls(ConnectionArgs)                         -"),
p("-                                                                   -"),
p("-          Returns a list of objects in the database for            -"),
p("-         in Erlang list format                                     -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- ls( ConnectionArgs ).                                             -"),
p("---------------------------------------------------------------------").

ls(Connection) -> Driver = get_zql_driver_name(Connection),
                  Ls = apply(Driver, ls, [Connection]),
                  Ls.










count_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    count(ConnectionArgs)                          -"),
p("-                                                                   -"),
p("-               counts the number of records in the database        -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- count( ConnectionArgs ).                                          -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

count(Connection) ->  Driver = get_zql_driver_name(Connection),
                      Count = apply(Driver, count, [Connection]),
                      Count.








delete_all_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-           delete_all(ConnectionArgs, yes_im_sure)                 -"),
p("-                                                                   -"),
p("-                       deletes all records                         -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- delete_all( ConnectionArgs, yes_im_sure ).                        -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

delete_all( Connection , yes_im_sure ) -> Driver = get_zql_driver_name(Connection),
                                          apply( Driver , delete_all , [ Connection ,  yes_im_sure ] ),
                                          ok.









