-module(zql).
-compile(export_all).
-import(zprint,[println/1,p/1,q/1,print_number/1]).
-import(zutils,[uuid/0]).

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
p("                                                                             "),
p("ls (ConnectionArgs)                                                          "),
p("set (ConnectionArgs, Key, Value)                 set a value                 "),
p("put (ConnectionArgs, Key, Value)                 set a value                 "),
p("get (ConnectionArgs, Key)                        get the value of Key        "),
p("exists (ConnectionArgs, Key)                     does this record/Key exist  "),
p("                                                                             "),
p("create_record (ConnectionArgs )                  create a record             "),
p("delete_record (ConnectionArgs )                  delete a record             "),
p("                                                                             "),
p("has_property (ConnectionArgs, Key, PropertyName) check for a property        "),
p("set_property (C, Key, PropertyName, Value)       set a property              "),
p("delete_property (C, Key, Property)               delete a property           "),
p("add_property (C, Key, PropertyName, Value)       add to a property list      "),  
p("delete_property_name_value (C, Key, P, Value)    delete a property value     "),
p("                                                                             "),
p("print (ConnectionArgs, ID)                       print record with key ID    "),
p("print_all (ConnectionArgs)                       print all records           "),
p("connect (ConnectionArgs)                         connect to the database     "),
p("get (ConnectionArgs, Key)                        get the value of Key        "),
p("                                                                             "),
p(" Example:                                                                    "),
p("C = [{driver,db_riak_driver}, {hostname,'riak@127.0.0.1'},{bucket,<<\"default\">>}]."),
q('zql:set(C, "Name", "Scott").                                                 '),
q('zql:get(C, "Name" ).                                                         '),
p("-----------------------------------------------------------------------------"),
ok.

help(Command)       ->  HelpFunctionName = atom_to_list(Command) ++ "_help",
                        apply(zql, list_to_atom(HelpFunctionName), []).






















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

local_riak_connection() -> 
         RiakConnection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
         RiakConnection.

local_mnesia_connection() -> 
         MnesiaConnection = [{driver,db_mnesia_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
         MnesiaConnection.

test_with_connection(C) ->

                println("Number of records in datastore:"),
                Count = count(C),
                print_number(Count),

                delete_all(C,yes_im_sure),                

                set(C, "boy", "Is here"),
                println("\nSaved 'boy' as 'is here'"),
                Value = get(C, "boy"),
                println("got value of boy as : "),               
                println(Value),
                println("Check 'boy' exists :"),
                Exists = exists(C, "boy"),
                println(Exists),
                delete(C,"boy"),
                println("deleted 'boy'"),
                println("Check 'boy' exists :"),
                Exists2 = exists(C, "boy"),
                println(Exists2),
                println("-----------------------"),

                LogEntry = create_record(C),
                set_property(C,LogEntry,"type","log").












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
p("- ConnectionArgs2 = [{driver,db_riak_driver}, {hostname,'riak@127.0.0.1'},{bucket,<<\"default\">>}]."),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

local() -> %ConnectionArgs = local_riak_connection(),
           ConnectionArgs = local_mnesia_connection(),
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

test_connection( ConnectionArgs ) -> try ( test_conn( ConnectionArgs ) ) of
                                     _ -> {ok, connection_fine}

                                     catch
                                        _Exception:_Reason -> {error, bad_connection}
                                     end.


test_conn(ConnectionArgs) -> Driver = get_db_driver_name( ConnectionArgs ),
                             apply( Driver , connect, [ ConnectionArgs ]),
                             ok.








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

print( ConnectionArgs, Key) -> 
    p("--------------------------------------------------------------"),
    io:format("ID:~s~n", [Key]),
    p("--------------------------------------------------------------"),

    try ( print_fn(ConnectionArgs,Key )) of 
      ok -> ok
    catch
        _:_ -> error
    end,
    p("--------------------------------------------------------------"),
    p(""),
    ok.

print_fn(ConnectionArgs,Key) ->
    Value = get( ConnectionArgs, Key),
    PropertyNames = get_property_names( ConnectionArgs, Key),
    lists:foreach(
             fun( PropertyName ) ->
                  PropValue = get_property( ConnectionArgs, Key, PropertyName), 
                  io:format( "~s:~s~n", [ PropertyName, PropValue ]) 
             end,
             PropertyNames),
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







get_db_driver_name_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-               get_db_driver_name(ConnectionArgs)                  -"),
p("-                                                                   -"),
p("-              gets the name of the database driver                 -"),
p("-                                                                   -"),
p("-  This is only used internally to get the database driver name     -"),
p("  from the connection parameters                                    -"),
p("                                                                    -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- get_db_driver_name( ConnectionArgs ).                             -"),
p("                                                                    -"),
p("---------------------------------------------------------------------").

get_db_driver_name(ConnectionArgs) -> Driver = proplists:get_value(driver,ConnectionArgs),
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
p("- ConnectionArgs = local( ).                                        -"),
q('- set( ConnectionArgs, "Name", "Bob").                              -'),
q('- get( ConnectionArgs, "Name").                                     -'),
q('- >> "Bob"                                                          -'),
q('- get_property( ConnectionArgs, "Name", value).                     -'),
q('- >> "Bob"                                                          -'),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

get( ConnectionArgs, Key ) -> Value = get_property( ConnectionArgs, Key, value ),
                              Value.




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

get_property_names( ConnectionArgs, Key) -> Driver = get_db_driver_name(ConnectionArgs),
                                            PropertyNames = apply(Driver, get_property_names, [ConnectionArgs, Key]),
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

get_property( ConnectionArgs, Key, PropertyName) -> Driver = get_db_driver_name( ConnectionArgs ),
                                                    Value = apply(Driver, get_property, [ConnectionArgs, Key, PropertyName]),
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

put( ConnectionArgs, Key, Value ) -> set_property( ConnectionArgs, Key, value, Value ). 






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

create_record(Connection) -> Driver = get_db_driver_name(Connection),
                             Key = apply(Driver, create_record, [Connection]),
                             Key.

create_record(Connection,Id) -> Driver = get_db_driver_name(Connection),
                                Key = apply(Driver, create_record, [Connection, Id]),
                                Key.








add_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-        add_property(ConnArgs, Key, PropertyName, Value)           -"),
p("-                                                                   -"),
p("-   This adds a named property to a record identified by Key        -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
q('- add_property( ConnectionArgs, "Peter", type, "Person" ).          -'),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

add_property( Connection, Key, PropertyName, Value) -> 

                          Driver = get_db_driver_name(Connection),
                          apply(Driver, add_property, [Connection, Key, PropertyName, Value]),
                          ok.









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

has_property(C,Record,PropertyName) -> Driver = get_db_driver_name(C),
                                       Ret = apply(Driver, has_property, [C, Record, PropertyName]),
                                       Ret.







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
                                             DoesRecordExist = exists( ConnArgs, Key),
                                             case DoesRecordExist of
                                                false -> create_record( ConnArgs, Key );
                                                true -> do_nothing
                                             end,
                                             
                                             case has_property( ConnArgs, Key, PropertyName ) of
                                                true -> delete_property( ConnArgs, Key, PropertyName);
                                             
                                                _ -> add_property( ConnArgs, Key, PropertyName, Value)
                                             end,
                                             ok.





delete_property_value_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-               delete_property_value(C,Key,PropertyName,Value)     -"),
p("-                                                                   -"),
p("-              Deletes a specific property value from a record      -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- ConnectionArgs = local( ).                                        -"),
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

delete_property_value(C, Key, PropertyName, Value) ->  
                             Driver = get_db_driver_name(C),
                             apply(Driver, delete_property, [C, Key, PropertyName, Value]),
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

                             Driver = get_db_driver_name(C),
                             apply(Driver, delete_property, [C, Key, PropertyName]),
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

exists(Connection,Key) -> Driver = get_db_driver_name(Connection),
                          Exists = apply(Driver, exists, [Connection, Key]),
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

delete(Connection,Key) -> Driver = get_db_driver_name(Connection),
                          apply(Driver, delete, [Connection, Key]),
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

ls(Connection) -> Driver = get_db_driver_name(Connection),
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
p("- print_all( ConnectionArgs ).                                      -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

count(Connection) ->  Driver = get_db_driver_name(Connection),
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

delete_all( Connection , yes_im_sure ) -> Driver = get_db_driver_name(Connection),
                                          apply( Driver , delete_all , [ Connection ,  yes_im_sure ] ),
                                          ok.









