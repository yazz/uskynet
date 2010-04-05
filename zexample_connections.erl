-module(zexample_connections).
-compile(export_all).

local_riak() -> RiakConnection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
                RiakConnection.
