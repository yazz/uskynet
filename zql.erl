-module(zql).
-compile(export_all).
-import(zprint,[println/1,p/1,print_number/1]).
-import(zutils,[uuid/0]).

help() -> 
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("-                                MODULE zql                                 -"),
p("-                                                                           -"),
p("-                   Erlang interface to the ZQL system                      -"),
p("-                                                                           -"),
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("help(Command)                                     get help on a command      "),
p("print(Connection, ID)                             print record with key ID   "),
p("print_all(Connection)                             print all records          "),


p("C = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<\"default\">>}],"),
p(""),
p("zql:set(C, \"boy\", \"Is here\"),"),
p("Value = zql:get(C, \"boy\")."),
p(""),
p(">> \"Is here\"\n\n"),
p("C = zql:local().                     -- gets a connection locally"),
p("zql:set(C, Key, Value ).             -- set a key / value"),
p("zql:get(C, Key).                     -- get a value"),
p("zql:exists(C, Key).                  -- true / false"),
p("zql:delete(C, Key).                  -- ok"),
p("zql:delete_all(C, yes_im_sure).      -- ok"),
p("zql:ls(C).                           -- get all keys as a list"),
p("R = zql:create(C).                   -- create a record and return it's unique ID"),
p("zql:print(C,R).                      -- prints the record in a screen friendly format"),
p("zql:set(C,R,type,person).            -- sets the type of the record to a person"),
p("-                                                                   -"),
p("-------------------------------------------------------------------------"),
ok.





help(Command)       ->  HelpFunctionName = atom_to_list(Command) ++ "_help",
                        apply(zql, list_to_atom(HelpFunctionName), []).









print_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    print(Connection, RecordID)                    -"),
p("-                                                                   -"),
p("-             Prints the record with ID of RecordID                 -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

print(Connection, RecordId) -> 

    Record = get(Connection, RecordId),
    println("-------------------------"),
    io:format("ID:~s~n", [RecordId]),
    case is_list(Record) of
        true -> lists:foreach(
             fun({PropertyName,Value}) -> io:format("~s:~s~n", [PropertyName,Value]) end,Record);

        false -> io:format("~s~n", [Record])
    end,
    println("").






print_all_help()  -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    print_all(Connection)                          -"),
p("-                                                                   -"),
p("-          Prints all the records to the console                    -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

print_all(C) -> PrintRecord = fun(RecordId) -> print(C,RecordId) end,
                lists:foreach(PrintRecord, zql:ls(C)).




match_help() -> 
p("---------------------------------------------------------------------"),
p("-                    match(Connection)                              -"),
p("-                                                                   -"),
p("- This should match a set of records. Not implemented yet though    -"),
p("-                                                                   -"),
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
p("-                                                                   -"),
p("---------------------------------------------------------------------").

get_property_names( ConnectionArgs, Key) -> Driver = get_db_driver_name(ConnectionArgs),
                                            PropertyNames = apply(Driver, get_properties, [ConnectionArgs, Key]),
                                            PropertyNames.






get_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-          get_property( ConnectionArgs, Key, PropertyName)         -"),
p("-                                                                   -"),
p("- gets the value of the named property for record identified by Key -"),
p("-                                                                   -"),
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
p("-                                                                   -"),
p("---------------------------------------------------------------------").

set(Connection,Key,Value) -> Driver = get_db_driver_name(Connection),
                             apply(Driver, set, [Connection, Key, Value]),                                  
                             ok.





create_record_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                    create_record( ConnectionArgs )                -"),
p("-                                                                   -"),
p("-                       This creates a new record                   -"),
p("-                                                                   -"),
p("         The unique ID of the record is returned as a HEX string    -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

create_record(Connection) -> Driver = get_db_driver_name(Connection),
                             Key = apply(Driver, create, [Connection]),
                             Key.








add_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-        add_property(ConnArgs, Key, PropertyName, Value)           -"),
p("-                                                                   -"),
p("-   This adds a named property to a record identified by Key        -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

add_property( Connection, Key, PropertyName, Value) -> 

                          Driver = get_db_driver_name(Connection),
                          apply(Driver, add_property, [Connection, Key,PropertyName, Value]),
                          ok.









has_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-      has_property(ConnectionArgs, Record, PropertyName)           -"),
p("-                                                                   -"),
p("-       Tests to see whether a record has a particular property     -"),
p("-      and returns either true or false                             -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

has_property(C,Record,PropertyName) -> Driver = get_db_driver_name(C),
                                       apply(Driver, has_property, [C, Record, PropertyName]),
                                       ok.







set_property_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                  set_property(ConnArgs, Key, Col, Value)          -"),
p("-                                                                   -"),
p("-                      Sets a property of a record                  -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

set_property(C, Key,Col,Value) -> set_property(C,Key,Col,Value).





delete_property_value_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-               delete_property_value(C,Key,PropertyName,Value)     -"),
p("-                                                                   -"),
p("-              Deletes a specific property value from a record      -"),
p("-                                                                   -"),
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
p("-                                                                   -"),
p("---------------------------------------------------------------------").

delete_all( Connection , yes_im_sure ) -> Driver = get_db_driver_name(Connection),
                                          apply( Driver , delete_all , [ Connection ,  yes_im_sure ] ),
                                          ok.









test_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                           test()                                  -"),
p("-                                                                   -"),
p("-                   tests the connection                            -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

test() -> test_riak().










test_riak_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                         test_riak()                               -"),
p("-                                                                   -"),
p("-         run the test suite on the local riak database             -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

test_riak() ->
               RiakConnection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
               test_with_connection(RiakConnection).










local_riak_connection_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                  local_riak_connection(Connection)                -"),
p("-                                                                   -"),
p("-              gets a connection to a local version of riak         -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

local_riak_connection() -> 

             RiakConnection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
             RiakConnection.










test_with_connection_help() -> 
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                       test(ConnectionArgs)                        -"),
p("-                                                                   -"),
p("-                 Tests the basic features of this module           -"),
p("-                                                                   -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

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



                