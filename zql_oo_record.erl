-module(zql_oo_record,[Conn, Key]).
-compile(export_all).

get_property_names( ) -> zql:get_property_names( Conn, Key ).

print( ) -> zql:print( Conn, Key ).

get(PropertyName) -> zql:get_property( Conn, Key , PropertyName).
get_or_nil(PropertyName) -> zql:get_property_or_nil( Conn, Key , PropertyName).
get_or_nil() -> zql:get_property_or_nil( Conn, Key ).
get() -> zql:get_property( Conn, Key , value).

set(Value) -> zql:set_property( Conn , Key, "value", Value).

set(PropertyName, Value) -> zql:set_property( Conn , Key, PropertyName, Value).

add(PropertyName, Value) -> zql:set_property( Conn, Key, PropertyName, Value).

has(PropertyName) -> zql:has_property( Conn, Key, PropertyName).

id( ) -> Key.