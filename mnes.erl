-module(mnes).
-compile(export_all).

-record(data, {key, value}).

do() -> mnesia:create_table(data, [{disc_copies, [node()]}, {attributes, record_info(fields, data)}]).

insert(Key, Val) -> Record = #data{key = Key, value = Val},
	    	    F = fun() ->
				mnesia:write(Record)
				end,
		    mnesia:transaction(F).

retrieve(Key) ->
		F = fun() ->
				mnesia:read({data, Key})
		  		end,
		{atomic, Data} = mnesia:transaction(F),
		Data.





search_qlc(Val) ->
			F = fun() ->
			qlc:eval(
				qlc:q(
					[X || X <- mnesia:table(data), X#data.value == Val]
					))
					end,
			{atomic, Data} = mnesia:transaction(F),
			Data.
