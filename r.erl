-module(r).
-compile(export_all).




connect() -> {ok, C} = riak:client_connect('riak@192.168.1.4'),
             C.

default_bucket_name() -> "default".
default_bucket() -> list_to_binary(default_bucket_name()).
table_names_bucket() -> list_to_binary(table_names_bucket_name()).
table_names_bucket_name() -> "table_names".

lb() -> C = connect(),
        C:list_buckets().

store(Key,Value) -> store_key_value_in_bucket( Key, Value, default_bucket() ). 

store_key_value_in_bucket(Key,Value,Bucket) -> C = connect(),
                                               Object = riak_object:new( Bucket, list_to_binary(Key), Value),
                                               C:put(Object,1).
                    



ls(BucketName) ->   C = connect(),
                    C:list_keys(list_to_binary(BucketName)).

ls()           ->   C =	connect(),
                    C:list_keys(list_to_binary(default_bucket_name())).

h()            ->   help().
help()         ->   println("---------------------------------- Help ---------------------------------"),
                    println("r:lb().                       -- lists the buckets"),
                    println("r:ls().                       -- lists the keys in the default bucket"),
                    println("r:ls(BucketName).             -- lists the keys in the named bucket"),
                    println("r:store(Key,Value).           -- stores a value in the default bucket"),
                    println("r:get(Key).                   -- gets the value for Key from the default bucket"),
                    println("r:create_table(TableName).    -- Creates a table called TableName"),
     		    println("-------------------------------------------------------------------------"),
                    ok.

print(Line)  ->   io:fwrite(Line).

println(Line)  ->   io:fwrite(Line),
                    io:fwrite("~n").

create_table(TableName) -> 
                           CurrentTableEntry = get_key_from_bucket( TableName, table_names_bucket() ),
                           
                           case CurrentTableEntry of
                               doesnt_exist -> store_key_value_in_bucket( TableName, <<"table">>, table_names_bucket() );

                               _Table_Already_Exists -> println("Table already exists")
                           end.



get(Key) -> get_key_from_bucket( Key, default_bucket() ).

get_key_from_bucket( Key, Bucket) ->
            C = connect(),
            try            
                {ok, Result} = C:get( Bucket, list_to_binary(Key), 1),
                V = riak_object:get_value( Result ),
                V
            catch
                error:_ -> doesnt_exist
            end.

test()       ->
                RecordId = record:new_record(),
                record:set(RecordId,"type","user"),
                record:set(RecordId,"age","51")
                .

