-module(zql_mnesia_driver).
-compile(export_all).
-include_lib("zql_all_imports.hrl").
-include_lib("stdlib/include/qlc.hrl").

-record(data, {key, value}).

name( ) -> "Mnesia".

connect(_Connection) -> ok.

setup() -> mnesia:create_table(
                data, 
                [
                        {disc_copies, [node()]}, 
                        {attributes, record_info(fields, data)}
                ]
          ).

get_property_names( _ConnectionArgs, Key ) -> 

                        ReturnValue = case mnesia_get( Key ) of
                                [ok, Data] ->  NamesWithDuplicates = [ Prop || {Prop,_Value} <- Data ],
                                               NoDuplicatesSet = sets:from_list(NamesWithDuplicates),
                                               UniqueList = sets:to_list(NoDuplicatesSet),
                                               UniqueList;

                                _          ->  not_found
                        end,
                        ReturnValue.


get_property( _ConnectionArgs, Key, PropertyName ) -> 

                                 [ok,Data] = mnesia_get( Key ),
                                 Value = [ {Prop,Value} || {Prop,Value} <- Data, Prop == PropertyName ],
                                 [{_PN, V} | _] = Value,
                                 V.










get( _ConnectionArgs, Key ) -> get_property( x, Key, value).








set( _ConnectionArgs, Key, Value) -> set_property( x, Key, value, Value ).














set_property(C, Key, PropertyName, Value) ->    BinaryKey = to_binary(Key),
                                                StringPropertyName = to_string(PropertyName),

                                                case exists(C, Key) of

                                                        false -> mnesia_set( BinaryKey, [ { StringPropertyName , Value } ] );

                                                        _ -> [ ok , CurrentValues ]  = mnesia_get( BinaryKey ),
                                                             UpdatedValue = [ { StringPropertyName , Value } | CurrentValues ],
                                                             mnesia_set( BinaryKey, UpdatedValue )
                                                end.


delete_property(Connection, Key, Property) ->   BinaryKey = to_binary(Key),
                                                StringPropertyName = to_string(PropertyName),

                                                case exists(C, Key) of

                                                        true -> [ok, CurrentValues ] = mnesia_get( BinaryKey ),
                                                        UpdatedValue = delete_property_list( StringPropertyName, CurrentValues ),
                                                        mnesia_set( BinaryKey, UpdatedValue );

                                                        _ -> ok
                                                end.

delete_property_list( _Col, [] ) -> [];
delete_property_list( Col, [{Col,_AnyValue} | T ]) -> delete_property_list( Col, T);
delete_property_list( Col, [H | T]) -> [ H | delete_property_list(Col, T) ].




exists(_Connection, Key) -> BinaryKey = to_binary(Key),

                            try
                                mnesia_get( BinaryKey )
                            of
                                [ok, _] -> true;
                                [not_found, _] -> false
                            catch
                                error:_Reason -> false
		            end.






delete(_Connection, Key) ->      
                                BinaryKey = to_binary(Key),
                                delete_key( BinaryKey ),
 		       	    	ok.






ls(_Connection) -> Keys = list_keys( ),
                   Keys.








count(Connection) -> Keys = ls(Connection),
                     Count = length(Keys),
                     Count.







delete_all( ConnectionArgs , yes_im_sure ) ->  Keys = ls( ConnectionArgs ),
                                               DeleteFunction = fun(Key) -> delete( ConnectionArgs , Key ) end,
                                               lists:map( DeleteFunction , Keys),
                                               ok.









mnesia_set(Key, Val) -> BKey = to_binary(Key),
                        Record = #data{key = BKey, value = Val},

	    	        F = fun() ->
				mnesia:write(Record)
				end,
		        mnesia:transaction(F).

mnesia_get(Key) -> BKey = to_binary(Key),
                   Result = try

		   F = fun() ->
				mnesia:read({data, BKey})
		  		end,

		   {atomic, Data} = mnesia:transaction(F),
		   [{data,BKey,Value}] = Data,
                   Value

                   of
                     Val -> [ok,Val]
                catch
                     _:_Reason -> [ not_found , value ]
		end,
                Result.







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

getkeys( [] ) -> [];
getkeys( [{data,Key,_Value}|Extra] ) -> [ Key | getkeys(Extra) ].


list_keys() ->  All = select_all(),
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





