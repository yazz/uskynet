-module(mnes).
-compile(export_all).

-record(data, {key, value}).

start() ->
               mnesia:create_schema([node()]),
               mnesia:start(),
               ok.

do() -> mnesia:create_table(data, [{disc_copies, [node()]}, {attributes, record_info(fields, data)}]).

put(Key, Val) -> Record = #data{key = Key, value = Val},
	    	    F = fun() ->
				mnesia:write(Record)
				end,
		    mnesia:transaction(F).

get(Key) ->
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
