% ZQL

-module(edb,[Connection]).

-compile(export_all).
to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> list_to_binary(Value).

set(Key,Value) -> db:set(Connection, [{key,to_binary(Key)},{value,Value}]).
get(Key) -> db:get(Connection, [{key,to_binary(Key)}]).
exists(Key) -> db:exists(Connection, [{key,to_binary(Key)}]).
delete(Key) -> db:delete(Connection,[{key,to_binary(Key)}]).

ls() -> db:ls(Connection,[{}]).
count() -> db:count(Connection,[{}]).
delete_all(yes_im_sure) -> db:delete_all( Connection , [{}] , yes_im_sure ).
create_record() -> db:create_record( Connection, [{}] ). 

add_col(Key, Col, Value) -> db:add_col(Connection, [{key,Key}, {col,Col}, {value,Value}]).

update_col(Key,Col,Value) -> db:update_col(Connection, [{key,Key}, {col,Col}, {value,Value}]).

delete_col(Key,Col) -> db:delete_col(Connection, [{key,Key}, {col,Col}]).
delete_col(Key,Col,Value) -> db:delete_col(Connection, [{key,Key}, {col,Col}, {value,Value}]).

test() -> "test the connection to the database".

h() -> help().
help() -> ok.



