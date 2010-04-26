-module(codeoo,[Conn, Key]).
-compile(export_all).
-import(zprint,[println/1,p/1,q/1,print_number/1]).

help( ) -> 
p("help for code").

get_property_names( ) -> zql:get_property_names( Conn, Key ).

print( ) -> zql:print( Conn, Key).

get(PropertyName) -> zql:get_property( Conn, Key , PropertyName).

set(PropertyName, Value) -> zql:set_property(Conn, Key, PropertyName, Value).

add(PropertyName, Value) -> zql:set_property(Conn, Key, PropertyName, Value).

id( ) -> Key.