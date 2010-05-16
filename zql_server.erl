-module(zql_server).
-compile(export_all).

-include_lib("zql_all_imports.hrl").

start() -> Location = global:whereis_name(server),
           case Location of
                undefined -> Pid = spawn( zserver, loop, []),
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
            timer:sleep(500),
            loop().

scan() ->   %DB=user_default:getdb(),

            user_default:s("count","0"),
            user_default:s("countdone","false"),

            U=user_default,
            
            lists:foreach(
                fun(Record) -> protect( Record,
                                        fun(X) -> check_number(X) 
                                        end
                               ) 
                end, 

                U:lsdb()).



protect(X,Fun) -> try(Fun(X)) of 
                     _ -> ok
                catch
                     _:_ -> ok
                end.
              

check_number(RecordId) ->  
                           R = user_default:get_record(RecordId),
                           Type = R:get("type"),
                           case (Type) of
                                <<"number">> -> user_default:print(RecordId),
                           %                     DB=user_default:getdb(),
                                                C=user_default:g("count"),
                                                Value = zutils:to_integer(C),
                                                C2 = Value + 1,
                                                user_default:s("count",zutils:to_string(C2)),

                                                ok;
                                _ -> ok
                            end.
