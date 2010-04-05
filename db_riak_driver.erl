-module(db_riak_driver).
-compile(export_all).
-import(zutils,[uuid/0]).

%------------------------------------------------------------------------------------
connect(Connection) -> Hostname = proplists:get_value(hostname, Connection),
                       {ok, C} = riak:client_connect(Hostname),
                       C.

to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> list_to_binary(Value).

%------------------------------------------------------------------------------------
get(Connection, Key) -> RiakClient = connect(Connection),
                        BinaryKey = to_binary(Key),
                        Bucket = proplists:get_value(bucket, Connection),
                        { ok, Item } = RiakClient:get(
                                 Bucket,
                                 BinaryKey,
                                 1),

                        Value = riak_object:get_value( Item ),
                        Value.
%------------------------------------------------------------------------------------
set(Connection, Key, Value) ->      RiakClient = connect( Connection ),
                                    BinaryKey = to_binary(Key),
                                    Bucket = proplists:get_value( bucket, Connection ),
                                    Item = riak_object:new( Bucket, BinaryKey, Value ),
                                    RiakClient:put( Item , 1),
                                    ok.
%------------------------------------------------------------------------------------
create(Connection)            -> UUID = uuid(),
                                 Key = list_to_binary(UUID),
                                 RiakClient = connect( Connection ),
                                 Bucket = proplists:get_value( bucket, Connection ),
                                 Item = riak_object:new( Bucket, Key, [] ),
                                 RiakClient:put( Item , 1),
                                 Key.
%------------------------------------------------------------------------------------
add_property(C, Key, PropertyName, Value) ->    RiakClient = connect( C ),
                                                Bucket = proplists:get_value(bucket, C),

                                                { ok, Item } = RiakClient:get(
                                                    Bucket,
                                                    Key,
                                                    1),
                                                CurrentValues = riak_object:get_value( Item ),
                                                UpdatedValue = [{PropertyName,Value} | CurrentValues],

                                                UpdatedItem = riak_object:update_value(
                                                    Item,
                                                    UpdatedValue),
                                                RiakClient:put( UpdatedItem, 1).
%------------------------------------------------------------------------------------

update_property(Connection, Key,Property,Value)     -> RiakClient = connect( Connection ),
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
%------------------------------------------------------------------------------------
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
%------------------------------------------------------------------------------------
delete_property(ConnectionArgs, Key,Property, Value) -> RiakClient = connect( ConnectionArgs ),
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
                                       

delete_property_list( Col, [] ) -> [];
delete_property_list( Col, [{Col,_AnyValue} | T ]) -> delete_property_list( Col, T);
delete_property_list( Col, [H | T]) -> [ H | delete_property_list(Col, T) ].

%------------------------------------------------------------------------------------
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
%------------------------------------------------------------------------------------
delete(Connection, Key) ->      RiakClient = connect( Connection ),
                                BinaryKey = to_binary(Key),
                                Bucket = proplists:get_value(bucket, Connection),

                                RiakClient:delete( Bucket, BinaryKey, 1 ),
 		       	    	ok.
%------------------------------------------------------------------------------------
ls(Connection) -> RiakClient = connect( Connection ),
                  Bucket = proplists:get_value(bucket, Connection),
                  {ok,Keys} = RiakClient:list_keys( Bucket ),
                  Keys.

%------------------------------------------------------------------------------------
count(Connection) -> Keys = ls(Connection),
                     Count = length(Keys),
                     Count.
%------------------------------------------------------------------------------------
delete_all( Connection , yes_im_sure ) ->  Keys = ls(Connection),
                                           DeleteFunction = fun(Key) -> delete( Connection , Key ) end,
                                           lists:map( DeleteFunction , Keys),
                                           ok.

%------------------------------------------------------------------------------------
