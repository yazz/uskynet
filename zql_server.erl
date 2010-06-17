-module(zql_server).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

start() -> Location = global:whereis_name(server),
           case Location of
                undefined -> Pid = spawn( zql_server, loop, []),
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


loop() ->   scan(),
            p("looped"),
            timer:sleep(60*1000),
            loop().

scan() ->   Db = oodb(),
            
            lists:foreach(
                fun(Record) -> protect( Record,
                                        fun(X) -> check_number(X) 
                                        end
                               ) 
                end, 

                Db:ls()
            ).



protect(X,Fun) -> try(Fun(X)) of 
                     _ -> ok
                catch
                     _:_ -> ok
                end.
              

check_number( RecordId ) -> 
                            Db=oodb(),
                            R=Db:get_record(RecordId),
                            
                            Type = R:has(type),

                            case Type of 
                                true -> p( to_string(R:get_or_nil(type)) );
                                _ -> ok
                            end,
                            ok.

find_code() -> 
    Db = oodb(),
    AllCode = Db:list("code"),

    lists:foreach(

                fun(RecordId) -> 
                                Record = Db:get_record(RecordId),
                                p(to_string(Record:id())),
                                X = eval(to_string(Record:get_or_nil(code)),nil),
                                p("result:" ++to_string(X))

                end, 

                AllCode:ls()
            ).
