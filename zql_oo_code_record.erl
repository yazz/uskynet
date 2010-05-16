-module(zql_oo_code_record,[Conn, Key]).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help( ) -> 
p("help for code").

get_property_names( ) -> zql:get_property_names( Conn, Key ).

print( ) -> zql:print( Conn, Key).

get(PropertyName) -> zql:get_property( Conn, Key , PropertyName).

set(PropertyName, Value) -> zql:set_property(Conn, Key, PropertyName, Value).

add(PropertyName, Value) -> zql:set_property(Conn, Key, PropertyName, Value).

id( ) -> Key.