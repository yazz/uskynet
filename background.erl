-module(background).
-compile(export_all).
-import(zprint,[println/1,p/1,q/1,print_number/1]).
-import(zutils,[uuid/0]).

-include_lib("stdlib/include/qlc.hrl").

-record(usage, {zql, count}).
