-module(db_cassandra_driver).
-compile(export_all).
-import(zprint,[println/1,p/1,q/1,print_number/1]).
-import(zutils,[uuid/0]).
-include_lib("cassandra_types.hrl").

test() -> ConnectionArgs = local_cassandra_connection(),
          test_with_connection( ConnectionArgs ).
test_with_connection(C) ->

                println("Number of records in datastore:"),
                Count = count(C),
                print_number(Count),

                zql:delete_all(C,yes_im_sure),                

                zql:set(C, "boy", "Is here"),
                println("\nSaved 'boy' as 'is here'").

test_with_connection2(C) ->
                println("Number of records in datastore:"),
                Count = count(C),
                print_number(Count),

                zql:delete_all(C,yes_im_sure),                

                zql:set(C, "boy", "Is here"),
                println("\nSaved 'boy' as 'is here'"),
                Value = zql:get(C, "boy"),
                println("got value of boy as : "),               
                println(Value),
                println("Check 'boy' exists :"),
                Exists = zql:exists(C, "boy"),
                println(Exists),
                zql:delete(C,"boy"),
                println("deleted 'boy'"),
                println("Check 'boy' exists :"),
                Exists2 = zql:exists(C, "boy"),
                println(Exists2),
                println("-----------------------"),

                LogEntry = zql:create_record(C),
                zql:set_property(C,LogEntry,"type","log").


lc() -> local_cassandra_connection().
local_cassandra_connection() -> 
         CassandraConnection = [{driver,db_cassandra_driver},{hostname,'127.0.0.1'}],
         CassandraConnection.

connect(Connection) -> Hostname = proplists:get_value(hostname, Connection),
                       {ok, C} = thrift_client:start_link(Hostname,9160, cassandra_thrift),
                       C.






to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> list_to_binary(Value).





get_property_name2s( ConnectionArgs, Key ) -> 

                                 Data = get( ConnectionArgs, Key ),
                                 NamesWithDuplicates = [ Prop || {Prop,Value} <- Data ],
                                 NoDuplicatesSet = sets:from_list(NamesWithDuplicates),
                                 UniqueList = sets:to_list(NoDuplicatesSet),
                                 UniqueList.



get_property( ConnectionArgs, Key, PropertyName) -> 

                                 Data = get( ConnectionArgs, Key ),
                                 Value = [ {Prop,Value} || {Prop,Value} <- Data, Prop == PropertyName ],
                                 [{_PN, V} | _] = Value,
                                 V.





has_property( ConnectionArgs, Key, PropertyName) -> 

                                               PropertyNames = get_property_names( ConnectionArgs, Key),
                                               ContainsKey = lists:member( PropertyName, PropertyNames),
                                               ContainsKey.





get( ConnectionArgs, Key ) -> RiakClient = connect( ConnectionArgs ),
                              BinaryKey = to_binary( Key ),
                              Bucket = proplists:get_value( bucket, ConnectionArgs ),
                              { ok, Item } = RiakClient:get(
                                  Bucket,
                                  BinaryKey,
                                  1),

                              Value = riak_object:get_value( Item ),
                              Value.










create_record(ConnectionArgs) -> UUID = uuid(),
                                 create_record(ConnectionArgs, UUID).


create_record(ConnectionArgs, Id) -> 
                                 Key = list_to_binary(Id),
                                 CassandraClient = connect( ConnectionArgs ),
                                 put(Key,""),
                                 Key.








add_property(C, Key, PropertyName, Value) ->    RiakClient = connect( C ),
                                                Bucket = proplists:get_value(bucket, C),
                                                BinaryKey = to_binary(Key),

                                                { ok, Item } = RiakClient:get(
                                                    Bucket,
                                                    BinaryKey,
                                                    1),
                                                CurrentValues = riak_object:get_value( Item ),
                                                UpdatedValue = [{PropertyName,Value} | CurrentValues],

                                                UpdatedItem = riak_object:update_value(
                                                    Item,
                                                    UpdatedValue),
                                                RiakClient:put( UpdatedItem, 1).










update_property(Connection, Key,Property,Value) -> 
                            RiakClient = connect( Connection ),
                            Bucket = proplists:get_value(bucket, Connection),

                            { ok, Item } = RiakClient:get(
                                 Bucket,
                                 Key,
                                 1),

                            CurrentValues = riak_object:get_value( Item ),
                            DeletedPropertyList = delete_property_list( Property, CurrentValues),
                            UpdatedValue = [{Property,Value} | DeletedPropertyList],

                            UpdatedItem = riak_object:update_value(
                                                 Item,
                                                 UpdatedValue),

                            RiakClient:put( UpdatedItem, 1).                                       






delete_property(Connection, Key, Property) -> RiakClient = connect( Connection ),
                                       Bucket = proplists:get_value(bucket, Connection),
                                       { ok, Item } = RiakClient:get(
                                           Bucket,
                                           Key,
                                           1),

                                       CurrentValues = riak_object:get_value( Item ),
                                       UpdatedValue = delete_property_list( Property, CurrentValues ),

                                       UpdatedItem = riak_object:update_value(
                                           Item,
                                           UpdatedValue),

                                       RiakClient:put( UpdatedItem, 1),
                                       ok.








delete_property(ConnectionArgs, Key,Property, Value) -> 

                                RiakClient = connect( ConnectionArgs ),
                                Bucket = proplists:get_value(bucket, ConnectionArgs),

                                { ok, Item } = RiakClient:get(
                                     Bucket,
                                     Key,
                                1),
                                CurrentValues = riak_object:get_value( Item ),
                                UpdatedValue = lists:delete( { Property, Value },  CurrentValues ),

                                UpdatedItem = riak_object:update_value(
                                    Item,
                                    UpdatedValue),

                                RiakClient:put( UpdatedItem, 1).
                                       

delete_property_list( _Col, [] ) -> [];
delete_property_list( Col, [{Col,_AnyValue} | T ]) -> delete_property_list( Col, T);
delete_property_list( Col, [H | T]) -> [ H | delete_property_list(Col, T) ].






exists(Connection, Key) ->  CassandraClient = connect( Connection ),
                            BinaryKey = to_binary(Key),
                            Bucket = proplists:get_value(bucket, Connection),

                            try
                                get(Connection, Key)
                            of
                                _ -> true
                            catch

                                error:_Reason -> false
		            end.






delete(Key) -> delete(lc(),Key).
delete(Connection,Key) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

               S = #sliceRange{start="",finish="",reversed=false,count=100},

               List = thrift_client:call( C,
                   'remove',
                   [ "Keyspace1",
                     Key,
                     #columnPath{column_family="KeyValue"},
                     1,
                     1
                     ] ),
               List.

from_keyslice2( [] ) -> [];
from_keyslice2( [{keySlice, Key, Values} | Tail] ) -> [Key | from_keyslice(Tail) ].







count(Connection) -> Keys = ls(Connection),
                     Count = length(Keys),
                     Count.







delete_all( ConnectionArgs , yes_im_sure ) ->  Keys = ls( ConnectionArgs ),
                                               DeleteFunction = fun(Key) -> delete( ConnectionArgs , Key ) end,
                                               lists:map( DeleteFunction , Keys),
                                               ok.



set(K,V) -> set(lc(),K,V).
set(Conn,K,V) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            thrift_client:call( C,
                   'insert',
                   [ "Keyspace1",
                     K,
                     #columnPath{column_family="KeyValue", column="value"},
                     V,
                     1,
                     1
                     ] ).

get(K) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            X = thrift_client:call( C,
                   'get',
                   [ "Keyspace1",
                     K,
                     #columnPath{column_family="KeyValue", column="value"},
                     
                     1
                     ] ),
            X.


names() -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),
thrift_client:call(C, getTableNames, []).






set_property( Key, PropertyName, Value ) -> set_property( lc(), Key, PropertyName, Value ).

set_property( Conn, K, P, V ) -> 

  {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),
         
  MutationMap = 
  {
    <<"i2">>, 
    {
      <<"KeyValue">>, 
      [
        #mutation{ 
          column_or_supercolumn = #column{ name = <<"value">> , value = <<"value">> , timestamp = 1 } 
        }
      ]
    }
  },

  thrift_client:call( C,
    'batch_mutate',
    [ "Keyspace1",
       MutationMap,
       1
    ] ).








getv(K,P) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            X = thrift_client:call( C,
                   'get',
                   [ "Keyspace1",
                     K,
                     #columnPath{column_family="KeyValue", column=P},

                     1
                     ] ),
            X.


get_property_names(Conn,K) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            S = #sliceRange{start="",finish="",reversed=false,count=100},
            CassandraReturn = thrift_client:call( C,
                   'get_slice',
                   [ "Keyspace1",
                     K,
                     #columnParent{column_family="KeyValue"},
                     #slicePredicate{slice_range=S},
                     1
                     ] ),
            {ok, ReturnList} = CassandraReturn,
            ReturnList,
            List = from_returnlist(ReturnList),

            NoDuplicatesSet = sets:from_list(List),
                                 UniqueList = sets:to_list(NoDuplicatesSet),
                                 UniqueList.



from_returnlist( [] ) -> [];

from_returnlist( [{columnOrSuperColumn, {column,PropName, Value,Count},undefined} | Tail] ) 
                 -> 
                    [PropName | from_returnlist(Tail) ].





v() -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),
        thrift_client:call(C, 'describe_version',[]).


ls() -> ls(lc()).
ls(Co) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            S = #sliceRange{start="",finish="",reversed=false,count=100},

            List = thrift_client:call( C,
                   'get_range_slices',
                   [ "Keyspace1",
                     #columnParent{column_family="KeyValue"},
                     #slicePredicate{slice_range=S},
                     #keyRange{start_key="",end_key="",count=100},
                     1
                     ] ),
            {ok,SlicedList} = List,
            Keys = from_keyslice(SlicedList),
            Keys.

from_keyslice( [] ) -> [];
from_keyslice( [{keySlice, Key, Values} | Tail] ) -> [Key | from_keyslice(Tail) ].


