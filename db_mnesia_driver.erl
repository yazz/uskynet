-module(db_riak_driver).
-compile(export_all).


connect(ConnectionArgs) -> Hostname = proplists:get_value(hostname, ConnectionArgs),
                           {ok, C} = riak:client_connect(Hostname),
                           C.


get(ConnectionArgs, Key) ->  RiakClient = connect(ConnectionArgs),
                             Bucket = proplists:get_value(bucket, ConnectionArgs),
                             { ok, Item } = RiakClient:get(
                                 Bucket,
                                 Key,
                                 1),

                             Value = riak_object:get_value( Item ),
                             Value.

set(ConnectionArgs, Key, Value) ->  RiakClient = connect( ConnectionArgs ),
                                    BinaryKey = list_to_binary(Key),
                                    Bucket = proplists:get_value( bucket, ConnectionArgs ),
                                    Item = riak_object:new( Bucket, BinaryKey, Value ),
                                    RiakClient:put( Item , 1),
                                    ok.
