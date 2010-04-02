-module(test).
-compile(export_all).

t1() ->   C = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
          
          Db = uskynet:new(C),
          Db:ls().