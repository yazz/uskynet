-module(sh).
-compile(export_all).
-import(zprint,[println/1,p/1,q/1,print_number/1]).
-import(zutils,[uuid/0]).

sh() -> p("------------------------------------"),
        Input = io:get_line(">"),
        
        Tokens = string:tokens(Input, " \n"),
        [Command | _] = Tokens,


        case Command of
             "it" -> it(),sh();
             "help" -> help(),sh();
             "quit" -> finished;
             X -> store(Input),sh()
        end.

help() -> 
p("help - this command"),
p("it - the last thing created"),
p("quit - exits to the command line").

store(Text) -> 
               user_default:store(Text).

it() -> R = user_default:last(),
        R:print().