-module(zserver).
-compile(export_all).

-import(zprint,[println/1,p/1,q/1,print_number/1]).

start() -> Location = global:whereis_name(server),
           case Location of
                undefined -> Pid = spawn(zserver,loop2,[]),
                             global:register_name(server,Pid),
                             p("Server started");

                _ ->         p("Server already started")
           end.


stop() -> Location = global:whereis_name(server),
           case Location of

                undefined -> p("Server already stopped");

                Pid -> erlang:exit(Pid,kill),
                p("Stopped")
           end.


loop2() ->  U=user_default,
            lists:foreach(fun(X) -> zprint:p(X) end, U:lsdb()),
%            p(X),
            timer:sleep(15000),
            loop2().

loop() ->

  receive
      start -> 
         p("starting server"),
         loop();
      stop ->
         p("Stopping server"),
         true

  end.