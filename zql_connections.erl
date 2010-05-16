-module(zql_connections).
-compile(export_all).
-include_lib("zql_all_imports.hrl").




local_riak_connection() -> 
         RiakConnection = [{driver,zql_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
         RiakConnection.

local_mnesia_connection() -> 
         MnesiaConnection = [{driver,zql_mnesia_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
         MnesiaConnection.

local_cassandra_connection() -> 
         Connection = [{driver,zql_cassandra_driver},{hostname,'127.0.0.1'}],
         Connection.