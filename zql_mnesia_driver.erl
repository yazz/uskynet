-module(zql_mnesia_driver).
-compile(export_all).
-include_lib("zql_all_imports.hrl").
-include_lib("stdlib/include/qlc.hrl").

-record(data, {key, value}).

name( ) -> "Mnesia".

cognnect(_Connection) -> ok.

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
                                               NoDuplicatesSet = sets:from_list( NamesWithDuplicates ),
                                               UniqueList = sets:to_list( NoDuplicatesSet ),
                                               UniqueList;

                                _          ->  not_found
                        end,
                        ReturnValue.


get_property( _Conn, Key, PropertyName ) -> 

                                 [ ok , Data ] = mnesia_get( Key ),
                                 Value = [ {Prop,Value} || {Prop,Value} <- Data, Prop == PropertyName ],
                                 [ {_PN, V} | _] = Value,

                                 [ ok, V ].










get( _ConnectionArgs, Key ) -> get_property( x, Key, value ).








set( _ConnectionArgs, Key, Value) -> set_property( x, Key, value, Value ).














set_property(C, Key, PropertyName, Value) ->    case exists(C, Key) of

                                                        false -> mnesia_set( Key, [ { PropertyName , Value } ] );

                                                        true -> [ ok , CurrentValues ]  = mnesia_get( Key ),
                                                             RemovedValue = delete_property_list( PropertyName, CurrentValues ),
                                                             AddedValue = [ { PropertyName , Value } | RemovedValue ],
                                                             mnesia_set( Key, AddedValue )
                                                end.


delete_property(Connection, Key, PropertyName) -> case exists(Connection, Key) of

                                                        true -> [ok, CurrentValues ] = mnesia_get( Key ),
                                                        UpdatedValue = delete_property_list( PropertyName, CurrentValues ),
                                                        mnesia_set( Key, UpdatedValue );

                                                        _ -> ok
                                                  end.

delete_property_list( _Col, [] ) -> [];
delete_property_list( Col, [{Col,_AnyValue} | T ]) -> delete_property_list( Col, T);
delete_property_list( Col, [H | T]) -> [ H | delete_property_list(Col, T) ].




exists(_Connection, Key) -> try
                                mnesia_get( Key )
                            of
                                [ok, _] -> true;
                                [not_found, _] -> false
                            catch
                                error:_Reason -> false
		            end.






delete( _Connection , Key ) -> delete_key( Key ),
                               ok.






ls( _Connection) -> Keys = list_keys( ),
                    Keys.








count( Connection ) -> Keys = ls( Connection ),
                       Count = length( Keys ),
                       Count.







delete_all( ConnectionArgs , yes_im_sure ) ->  Keys = ls( ConnectionArgs ),
                                               DeleteFunction = fun(Key) -> delete( ConnectionArgs , Key ) end,
                                               lists:map( DeleteFunction , Keys),
                                               ok.









mnesia_set(Key, Val) -> Record = #data{key = Key, value = Val},

	    	        F = fun() ->
				mnesia:write(Record)
				end,
		        mnesia:transaction(F).

mnesia_get(Key) -> Result = try

		   F = fun() ->
				mnesia:read({data, Key})
		  		end,

		   {atomic, Data} = mnesia:transaction(F),
		   [{data,Key,Value}] = Data,
                   Value

                   of
                     Val -> [ok,Val]
                catch
                     _:_Reason -> [ not_found , value ]
		end,
                Result.







search_qlc(Val) ->      F = fun() ->
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

getkeys( [ ] ) -> [ ];
getkeys( [ { data , Key , _Value } | Extra ] ) -> [ Key | getkeys( Extra ) ].


list_keys() ->  All = select_all( ),
                Keys = getkeys( All ),
                Keys.


delete_key( Key ) ->
        Delete=#data{ key = Key, _ = '_' },

        Fun = fun() ->
              List = mnesia:match_object(Delete),

              lists:foreach(fun(X) ->
                                    mnesia:delete_object(X)
                            end, List)
        end,
        mnesia:transaction(Fun).





