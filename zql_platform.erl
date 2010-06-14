-module(zql_platform).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

os() -> [OS | _ ] = zql_utils:readlines("os.txt"),
        to_atom(OS).

hex_uuid() -> call_function_on_driver( hex_uuid ).


call_function_on_driver( FunctionName ) -> DriverName = "zql_platform_driver_for_" ++ atom_to_list(os()),
                                           apply( to_atom( DriverName ), to_atom(FunctionName) , [ ]).
