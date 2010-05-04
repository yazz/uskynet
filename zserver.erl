-module(zserver).
-compile(export_all).

-import(zprint,[println/1,p/1,q/1,print_number/1]).

start() -> loop2().

loop2() ->
            p("called"),
            timer:sleep(5000),
            loop2().

loop() ->

  receive
      {From, Message} -> 
         io:format("Received: ~w~n",[Message]),
         loop();
      stop ->
         true

  end.