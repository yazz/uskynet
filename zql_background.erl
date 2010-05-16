-module(background).
-compile(export_all).

-include_lib("zql_all_imports.hrl").
-include_lib("stdlib/include/qlc.hrl").

-record(usage, {zql, count}).
