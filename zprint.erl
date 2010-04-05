-module(zprint).
-compile(export_all).

nl() -> println("").

p(Line) -> println(Line).

print(Line)  ->   io:fwrite(Line).


print_number(N) ->   io:format("~w~n", [N]).

println(Line)  ->   io:fwrite(Line),
                    io:fwrite("~n").