-module(zql).
-compile(export_all).
-import(zprint,[println/1,print_number/1]).
-import(zutils,[uuid/0]).

test() -> ok.

h() -> zhelp:help(zql).

print(Connection, RecordId) -> 
                  Record = get(Connection, RecordId),
                  println("-------------------------"),
                  io:format("ID:~s~n", [RecordId]),
                  case is_list(Record) of
                       true -> lists:foreach(fun({PropertyName,Value}) -> io:format("~s:~s~n", [PropertyName,Value]) end,Record);
                       false -> io:format("~s~n", [Record])
                  end,
                  println("").

print_all(C) -> PrintRecord = fun(RecordId) -> print(C,RecordId) end,
                lists:foreach(PrintRecord, zql:ls(C)).



%match(RecordData,Queries) -> lists:foreach( fun(Property) -> match_property(Property,Queries) end , RecordData ).

%match_property(Property, Queries) -> lists:foreach( fun(Query) -> 

match(Value, equals, Value) -> true;
match(_Value, equals, _ExpectedValue) -> false.


connect(ConnectionArgs) -> Driver =
                           proplists:get_value(driver,ConnectionArgs),
                           Driver.

get(Connection,Key) ->                     Driver = connect(Connection),
                                           Value = apply(Driver, get, [Connection, Key]),
                                           Value.

get_property_names(Connection,Key) ->      Driver = connect(Connection),
                                           PropertyNames = apply(Driver, get_properties, [Connection, Key]),
                                           PropertyNames.

get_property(C,Key,PropertyName) ->        Driver = connect(C),
                                           Data = get(C,Key),
                                           Value = [ {Prop,Value} || {Prop,Value} <- Data, Prop == PropertyName ],
                                           [{PN, V} | _] = Value,
                                           V.


set(Connection,Key,Value) -> Driver = connect(Connection),
                             apply(Driver, set, [Connection, Key, Value]),                                  
                             ok.

set([{connection,Connection},{key,Key}],Value) -> set(Connection,Key,Value).


create(Connection) -> Driver = connect(Connection),
                      Key = apply(Driver, create, [Connection]),
                      Key.

add_property( Connection, Key, PropertyName, Value) -> Driver = connect(Connection),
                                                       apply(Driver, add_property, [Connection, Key,PropertyName, Value]),
                                                       ok.


has_property(C,Record,PropertyName) -> Driver = connect(C),
                                       apply(Driver, has_property, [C, Record, PropertyName]),
                                       ok.





set_property(C, Key,Col,Value) -> update_property(C,Key,Col,Value).
set(C, Key,Col,Value) -> update_property(C,Key,Col,Value).



update_property(C, Key,Col,Value) -> Driver = connect(C),
                                     apply(Driver, update_property, [C, Key,Col,Value]),
                                     ok.





delete_property(C, Key, Property) -> delete_property(C, [{key,Key}, {property,Property}]).

delete_property(C, Key, Property, Value) -> delete_property(C, [{key,Key}, {property,Property}, {value,Value}]).

delete_property(C, Args) ->  Driver = connect(C),
                             Key = proplists:get_value(key,Args),
                             Property = proplists:get_value(property,Args),
                             Value = proplists:get_value(value,Args),
                             case Value of 
                                 undefined -> apply(Driver, delete_property, [C, Key, Property]);
                                 _ -> apply(Driver, delete_property, [C, Key, Property, Value])
                             end,
                             ok.



exists(Connection,Key) -> Driver = connect(Connection),
                          Exists = apply(Driver, exists, [Connection, Key]),
                          Exists.




delete(Connection,Key) -> Driver = connect(Connection),
                          apply(Driver, delete, [Connection, Key]),
                          ok.



ls(Connection) ->           Driver = connect(Connection),
                            Ls = apply(Driver, ls, [Connection]),
                            Ls.


count(Connection) ->  Driver = connect(Connection),
                      Count = apply(Driver, count, [Connection]),
                      Count.


delete_all( Connection , yes_im_sure ) -> Driver = connect( Connection ),
                                          apply( Driver , delete_all , [ Connection ,  yes_im_sure ] ),
                                          ok.


test() -> test_riak().
          % test_mnesia().
%----------------------------------------------------------------------------------

test_riak() ->
                RiakConnection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
                test(RiakConnection).
%----------------------------------------------------------------------------------


local() -> RiakConnection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
           RiakConnection.

%----------------------------------------------------------------------------------
test(C) ->      println("Number of records in datastore:"),
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

                LogEntry = create(C),
                set(C,LogEntry,"type","log").

%----------------------------------------------------------------------------------

                