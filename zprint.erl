-module(zprint).
-compile(export_all).

help() -> println("----------------------------------------------------------"),
          println("-                     MODULE zprint                      -"),
          println("- Handles all printing issues to the console             -"),
          println("----------------------------------------------------------"),
 
          println("nl                    newline"),
          println("println(Text)         prints Text plus a linefeed"),
          println("p(Text)                     "),
          println("print_number(N)       print number N plus a linefeed"),
          println("----------------------------------------------------------").

nl() -> println("").

p(Line) -> println(Line).

print(Line)  ->   io:fwrite(Line).


print_number(N) ->   io:format("~w~n", [N]).

println(Line)  ->   io:fwrite(Line),
                    io:fwrite("~n").