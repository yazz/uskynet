-module(zql_mnesia_driver).
-compile(export_all).
-include_lib("zql_all_imports.hrl").
-include_lib("stdlib/include/qlc.hrl").

-record(data, {key, value}).

name( ) -> "Mnesia".



connect(_Connection) -> start(),
                        ok.



start() -> mnesia:create_schema([node()]),
           mnesia:start(),
           ok.


get_property_names( ConnectionArgs, Key ) -> 

                                 Data = get( ConnectionArgs, Key ),
                                 NamesWithDuplicates = [ Prop || {Prop,_Value} <- Data ],
                                 NoDuplicatesSet = sets:from_list(NamesWithDuplicates),
                                 UniqueList = sets:to_list(NoDuplicatesSet),
                                 UniqueList.



get_property( ConnectionArgs, Key, PropertyName) -> 

                                 Data = get( ConnectionArgs, Key ),
                                 Value = [ {Prop,Value} || {Prop,Value} <- Data, Prop == PropertyName ],
                                 [{_PN, V} | _] = Value,
                                 V.





has_property( ConnectionArgs, Key, PropertyName) -> 

                                               PropertyNames = get_property_names( ConnectionArgs, Key),
                                               ContainsKey = lists:member( PropertyName, PropertyNames),
                                               ContainsKey.





get( _ConnectionArgs, Key ) -> 
                              BinaryKey = to_binary( Key ),
                              
                              Value  = get_val( BinaryKey ),


                              Value.








set( _ConnectionArgs, Key, Value) -> 
                                    BinaryKey = to_binary(Key),
                                    
                                    put_val( BinaryKey, Value ),
                                    ok.









create_record(ConnectionArgs) -> UUID = uuid(),
                                 create_record(ConnectionArgs, UUID).


create_record(_ConnectionArgs, Id) -> 

                                 Key = list_to_binary(Id),
                                 
                                 put_val( Key , []),
                                 Key.








add_property(_C, Key, PropertyName, Value) ->                                                    
                                                BinaryKey = to_binary(Key),

                                                CurrentValues  = get_val( BinaryKey ),
                                                
                                                UpdatedValue = [{PropertyName,Value} | CurrentValues],


                                                put_val( BinaryKey, UpdatedValue).










update_property(Connection, Key,Property,Value) -> 
                            RiakClient = connect,
                            Bucket = proplists:get_value(bucket, Connection),

                            { ok, Item } = RiakClient:get(
                                 Bucket,
                                 Key,
                                 1),

                            CurrentValues = riak_object:get_value( Item ),
                            DeletedPropertyList = delete_property_list( Property, CurrentValues),
                            UpdatedValue = [{Property,Value} | DeletedPropertyList],

                            UpdatedItem = riak_object:update_value(
                                                 Item,
                                                 UpdatedValue),

                            RiakClient:put( UpdatedItem, 1).                                       






delete_property(Connection, Key, Property) -> RiakClient = connect,
                                       Bucket = proplists:get_value(bucket, Connection),
                                       { ok, Item } = RiakClient:get(
                                           Bucket,
                                           Key,
                                           1),

                                       CurrentValues = riak_object:get_value( Item ),
                                       UpdatedValue = delete_property_list( Property, CurrentValues ),

                                       UpdatedItem = riak_object:update_value(
                                           Item,
                                           UpdatedValue),

                                       RiakClient:put( UpdatedItem, 1),
                                       ok.








delete_property(ConnectionArgs, Key,Property, Value) -> 

                                RiakClient = connect,
                                Bucket = proplists:get_value(bucket, ConnectionArgs),

                                { ok, Item } = RiakClient:get(
                                     Bucket,
                                     Key,
                                1),
                                CurrentValues = riak_object:get_value( Item ),
                                UpdatedValue = lists:delete( { Property, Value },  CurrentValues ),

                                UpdatedItem = riak_object:update_value(
                                    Item,
                                    UpdatedValue),

                                RiakClient:put( UpdatedItem, 1).
                                       

delete_property_list( _Col, [] ) -> [];
delete_property_list( Col, [{Col,_AnyValue} | T ]) -> delete_property_list( Col, T);
delete_property_list( Col, [H | T]) -> [ H | delete_property_list(Col, T) ].






exists(_Connection, Key) ->  
                            BinaryKey = to_binary(Key),

                            try
                                get_val( BinaryKey )
                            of
                                _ -> true
                            catch
                                error:_Reason -> false
		            end.






delete(_Connection, Key) ->      
                                BinaryKey = to_binary(Key),
                                

                                delete_key( BinaryKey ),
 		       	    	ok.






ls(_Connection) -> 
                  Keys = list_keys( ),
                  Keys.








count(Connection) -> Keys = ls(Connection),
                     Count = length(Keys),
                     Count.







delete_all( ConnectionArgs , yes_im_sure ) ->  Keys = ls( ConnectionArgs ),
                                               DeleteFunction = fun(Key) -> delete( ConnectionArgs , Key ) end,
                                               lists:map( DeleteFunction , Keys),
                                               ok.








do() -> mnesia:create_table(data, [{disc_copies, [node()]}, {attributes, record_info(fields, data)}]).

put_val(Key, Val) -> Record = #data{key = Key, value = Val},
	    	    F = fun() ->
				mnesia:write(Record)
				end,
		    mnesia:transaction(F).

get_val(Key) ->
		F = fun() ->
				mnesia:read({data, Key})
		  		end,
		{atomic, Data} = mnesia:transaction(F),
		[{data,Key,Value}] = Data,
                Value.





search_qlc(Val) ->
			F = fun() ->
			qlc:eval(
				qlc:q(
					[X || X <- mnesia:table(data), X#data.value == Val]
					))
					end,
			{atomic, Data} = mnesia:transaction(F),
			Data.

select_all() ->
    F = fun() ->
        qlc:eval( qlc:q(
            [ X || X <- mnesia:table(data) ] 
        )) 
    end, 
    Res = mnesia:transaction( F     ),
    {atomic,More} = Res,
    More.

getkeys([]) -> [];
getkeys([{data,Key,_Value}|Extra]) -> [Key|getkeys(Extra)].


list_keys() ->
                All = select_all(),
                Keys = getkeys(All),
                Keys.


delete_key(Key) ->
Delete=#data{key = Key, _ = '_'},
Fun = fun() ->
              List = mnesia:match_object(Delete),
              lists:foreach(fun(X) ->
                                    mnesia:delete_object(X)
                            end, List)
      end,
      mnesia:transaction(Fun).