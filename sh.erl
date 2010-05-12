-module(sh).
-compile(export_all).
-include_lib("c:/usn/zql_all_imports.hrl").


sh() -> p("------------------------------------"),
        Input = io:get_line(">"),
        
        Tokens = string:tokens(Input, " \n"),
        [Command | Args] = Tokens,


        case Command of
             "it" -> it(),sh();
             "get" -> read(Args),sh();
             "help" -> help(),sh();
             "quit" -> finished;
             X -> store(Input),sh()
        end.

help() -> 
p("help - this command"),
p("it - the last thing created"),
p("quit - exits to the command line").


it() -> R = last(),
        R:print().

read(Args) -> p("get called with arg count of:"),
              Num = length(Args),
              
              case Num of 
                   1 -> Key = nth(1,Args),
                        SKey = to_string(Key),
                        V = g(SKey),
                        p(V);
                   X -> print_number(X)
              end.
