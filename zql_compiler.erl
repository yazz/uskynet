-module(zql_compiler).
-compile(export_all).

compile_all() -> compile(zql_utils),
                 compile(zql_shell),
                 compile(zql_help),
                 compile(zql_connections),
                 compile(zql),

                 compile(zql_oo_session),
                 compile(zql_oo_record),
                 compile(zql_oo_code_record),

                 compile(zql_mnesia_driver),
                 compile(zql_riak_driver),
                 compile(zql_cassandra_driver),

                 compile(zql_server),

                 compile(user_default),
                 compile(zql_compiler).

compile(SourceFile) -> c:c(SourceFile).


