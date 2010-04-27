-module(db_cassandra_driver).
-compile(export_all).
-import(zutils,[uuid/0]).

-include_lib("cassandra_types.hrl").

test() -> ConnectionArgs = local_cassandra_connection(),
          zql:test_with_connection( ConnectionArgs ).

local_cassandra_connection() -> 
         CassandraConnection = [{driver,db_cassandra_driver},{hostname,'127.0.0.1'}],
         CassandraConnection.

connect(Connection) -> Hostname = proplists:get_value(hostname, Connection),
                       {ok, C} = thrift_client:start_link(Hostname,9160, cassandra_thrift),
                       C.






to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> list_to_binary(Value).





get_property_names( ConnectionArgs, Key ) -> 

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








set( ConnectionArgs, Key, Value) -> RiakClient = connect( ConnectionArgs ),
                                    BinaryKey = to_binary(Key),
                                    Bucket = proplists:get_value( bucket, ConnectionArgs ),
                                    Item = riak_object:new( Bucket, BinaryKey, Value ),
                                    RiakClient:put( Item , 1),
                                    ok.









create_record(ConnectionArgs) -> UUID = uuid(),
                                 create_record(ConnectionArgs, UUID).


create_record(ConnectionArgs, Id) -> 

                                 Key = list_to_binary(Id),
                                 RiakClient = connect( ConnectionArgs ),
                                 Bucket = proplists:get_value( bucket, ConnectionArgs ),
                                 Item = riak_object:new( Bucket, Key, [] ),
                                 RiakClient:put( Item , 1),
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






exists(Connection, Key) ->  RiakClient = connect( Connection ),
                            BinaryKey = to_binary(Key),
                            Bucket = proplists:get_value(bucket, Connection),

                            try
                                { ok, _Item } = RiakClient:get(
                                    Bucket,
                                    BinaryKey,
                                    1)
                            of
                                _ -> true
                            catch
                                error:_Reason -> false
		            end.






delete(Connection, Key) ->      RiakClient = connect( Connection ),
                                BinaryKey = to_binary(Key),
                                Bucket = proplists:get_value(bucket, Connection),

                                RiakClient:delete( Bucket, BinaryKey, 1 ),
 		       	    	ok.

delete(Key) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

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




set(K,V) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

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

setv(K,P,V) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            thrift_client:call( C,
                   'insert',
                   [ "Keyspace1",
                     K,
                     #columnPath{column_family="KeyValue", column=P},
                     V,
                     1,
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


get_props(K) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

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
            List.

from_returnlist( [] ) -> [];

from_returnlist( [{columnOrSuperColumn, {column,PropName, Value,Count},undefined} | Tail] ) 
-> 
[PropName | from_returnlist(Tail) ].





v() -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),
        thrift_client:call(C, 'describe_version',[]).


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


