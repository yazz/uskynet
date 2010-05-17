-module(zql_cassandra_driver).
-compile(export_all).
-include_lib("zql_all_imports.hrl").
-include_lib("cassandra_types.hrl").

name() -> "Cassandra database".

connect( ConnectionArgs ) -> Hostname = proplists:get_value( hostname, ConnectionArgs ),
                             { ok, Conn } = thrift_client:start_link( Hostname , 9160 , cassandra_thrift ),
                             Conn.



has_property( ConnArgs, Key, PropertyName) -> PropertyNames = get_property_names( ConnArgs, Key),
                                              ContainsKey = lists:member( PropertyName, PropertyNames),
                                              ContainsKey.




create_record(ConnectionArgs) -> UUID = uuid(),
                                 create_record(ConnectionArgs, UUID).




create_record( Conn, Id ) -> Key = to_binary(Id),
                             set_property( Conn , Key , "value", "" ),
                             Key.







add_property( Conn, Key, PropertyName, Value ) -> 

    C = connect( Conn ),

    thrift_client:call( 
                        C,
                        'insert',
                        [ 
                          "Keyspace1",
                          Key,
                          #columnPath{ column_family = "KeyValue", column = PropertyName },
                          Value,
                          get_timestamp_microseconds(),
                          1
                        ] 
                      ),
    ok.










update_property( Conn, Key, Property, Value ) -> set_property( Conn, Key, Property, Value ).






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






exists(Connection, Key) ->  BinaryKey = to_binary(Key),

                            try
                                get(Connection, BinaryKey)
                            of
                                _ -> true
                            catch

                                _:_Reason -> false
		            end.







delete(Conn,Key) -> C = connect( Conn ),

                    List = thrift_client:call( C,
                                               'remove',
                                               [ "Keyspace1",
                                                 Key,
                                                 #columnPath{column_family="KeyValue"},
                                                 get_timestamp_microseconds(),
                                                 1
                                               ] ),
                    List.









count( Conn ) -> Keys = ls( Conn ),
                 Count = length(Keys),
                 Count.







delete_all( Conn , yes_im_sure ) ->  Keys = ls( Conn ),
                                     DeleteFunction = fun(Key) -> delete( Conn , Key ) end,
                                     lists:map( DeleteFunction , Keys),
                                     ok.




set(ConnArgs,K,V) -> 
               C=connect(ConnArgs),  
               thrift_client:call( C,
                   'insert',
                   [ "Keyspace1",
                     K,
                     #columnPath{column_family="KeyValue", column="value"},
                     V,
                     get_timestamp_microseconds(),
                     1
                     ] ).


get(C,K) -> get_property(C,K,"value").






set_property( ConnArgs, K, P, V ) -> 
               C=connect(ConnArgs),  

    thrift_client:call( C,
                   'insert',
                   [ "Keyspace1",
                     K,
                     #columnPath{column_family="KeyValue", column=P},
                     V,
                     get_timestamp_microseconds(),
                     1
                     ] ),
    ok.









get_property( Conn, Key, P) -> 

            C=connect(Conn),  

            X = thrift_client:call( C,
                   'get',
                   [ "Keyspace1",
                     Key,
                     #columnPath{column_family="KeyValue", column=P},
                     1
                     ] ),
            {ok,{columnOrSuperColumn,Col,_}} = X,
            {_,_,Val,_} = Col,
            Val.


from_return_values( [] ) -> [];

from_return_values( [{columnOrSuperColumn, {column,PropName, _Value, _Count},undefined} | Tail] ) 
                 -> 
                    [PropName | from_returnlist(Tail) ].




get_property_names(Conn,K) -> 
            C=connect(Conn), 
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

            List = from_returnlist(ReturnList),

            NoDuplicatesSet = sets:from_list(List),
            UniqueList = sets:to_list(NoDuplicatesSet),
            UniqueList.



from_returnlist( [] ) -> [];

from_returnlist( [{columnOrSuperColumn, {column,PropName, _Value,_Count},undefined} | Tail] ) 
                 -> 
                    [PropName | from_returnlist(Tail) ].





v() -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),
        thrift_client:call(C, 'describe_version',[]).


ls(Conn) -> C = connect( Conn),

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
from_keyslice( [{keySlice, Key, _Values} | Tail] ) -> [Key | from_keyslice(Tail) ].





test_mutate() ->

  {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

  Key = "Key3",  

  %
  % set first property
  %        
  thrift_client:call( C,
                   'insert',
                   [ "Keyspace1",
                     Key,
                     #columnPath{column_family="KeyValue", column="value"},
                     "value1",
                     get_timestamp_microseconds(),
                     1
                     ] ),
  thrift_client:call( C,
                   'insert',
                   [ "Keyspace1",
                     Key,
                     #columnPath{column_family="KeyValue", column="value2"},
                     "value1",
                     1,
                     1
                     ] ),

  %
  % set second property ( fails! - why? )
  %
  MutationMap = 
  {
    Key, 
    {
      "KeyValue", 
      [
        #mutation{ 
          column_or_supercolumn = #column{ name = "property" , value = "value" , timestamp = get_timestamp_microseconds() } 
        }
      ]
    }
  },

  thrift_client:call( C,
    'batch_mutate',
    [ "Keyspace1",
       MutationMap,
       1
    ] )

  .


