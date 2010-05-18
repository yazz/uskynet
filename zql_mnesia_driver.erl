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

                        ReturnValue = case get_val( Key ) of
                                [ok, Data] ->  NamesWithDuplicates = [ Prop || {Prop,_Value} <- Data ],
                                               NoDuplicatesSet = sets:from_list(NamesWithDuplicates),
                                               UniqueList = sets:to_list(NoDuplicatesSet),
                                               UniqueList;

                                _          ->  not_found
                        end,
                        ReturnValue.


get_property( ConnectionArgs, Key, PropertyName) -> 

                                 Data = get( ConnectionArgs, Key ),
                                 Value = [ {Prop,Value} || {Prop,Value} <- Data, Prop == PropertyName ],
                                 [{_PN, V} | _] = Value,
                                 V.










get( _ConnectionArgs, Key ) -> get_property( x, Key, value).








set( _ConnectionArgs, Key, Value) -> set_property( x, Key, value, Value ).














set_property(_C, Key, PropertyName, Value) -> BinaryKey = to_binary(Key),

                                              [ok,CurrentValues]  = get_val( BinaryKey ),
                                                
                                              UpdatedValue = [{PropertyName,Value} | CurrentValues],

                                              put_val( BinaryKey, UpdatedValue).






exists(_Connection, Key) -> BinaryKey = to_binary(Key),

                            try
                                get_val( BinaryKey )
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









put_val(Key, Val) ->    BKey = to_binary(Key),
                        Record = #data{key = BKey, value = Val},

	    	        F = fun() ->
				mnesia:write(Record)
				end,
		        mnesia:transaction(F).

get_val(Key) -> BKey = to_binary(Key),
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