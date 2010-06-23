-module(zql_compiler).
-compile(export_all).

compile_all() -> 
                 compile(sha1),
                 compile(zql_utils),
                 compile(zql_shell),
                 compile(zql_help),
                 compile(zql_connections),
                 compile(zql),


                 compile(zql_platform),
                 compile(zql_platform_driver_for_cygwin),
                 compile(zql_platform_driver_for_cygwin_dos_shell),
                 compile(zql_platform_driver_for_osx),
                 compile(zql_platform_driver_for_n900),
                 compile(zql_platform_driver_for_linux),

                 compile(zql_context_helper),

                 compile(zql_oo_helper),
                 compile(zql_oo_session),
                 compile(zql_oo_record),
                 compile(zql_oo_code_record),
                 compile(zql_oo_list),

                 compile(zql_mnesia_driver),
                 compile(zql_riak_driver),
                 compile(zql_cassandra_driver),

                 compile(zql_server),

                 compile(user_default),
                 compile(zql_compiler).

compile(SourceFile) -> c:c(SourceFile, [debug_info]).


