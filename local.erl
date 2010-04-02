-module(local).
-compile(export_all).

db() ->         Connection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],

                C = edb:new(Connection),

                C.