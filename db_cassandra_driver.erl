-module(t).
-compile(export_all).

-include_lib("cassandra_types.hrl").

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

set(K,P,V) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            thrift_client:call( C,
                   'insert',
                   [ "Keyspace1",
                     K,
                     #columnPath{column_family="KeyValue", column=P},
                     V,
                     1,
                     1
                     ] ).

get(K,P) -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

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
            X = thrift_client:call( C,
                   'get_slice',
                   [ "Keyspace1",
                     K,
                     #columnParent{column_family="KeyValue"},
                     #slicePredicate{slice_range=S},
                     1
                     ] ),
            X.


v() -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),
        thrift_client:call(C, 'describe_version',[]).


count() -> {ok, C} = thrift_client:start_link("127.0.0.1",9160, cassandra_thrift),

            S = #sliceRange{start="",finish="",reversed=false,count=100},

            List = thrift_client:call( C,
                   'get_range_slices',
                   [ "Keyspace1",
                     #columnParent{column_family="KeyValue"},
                     #slicePredicate{slice_range=S},
                     #keyRange{start_key="",end_key="",count=100},
                     1
                     ] ),
            List.

