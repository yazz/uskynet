-module(sh).
-compile(export_all).
-import(zprint,[println/1,p/1,q/1,print_number/1]).
-import(zutils,[uuid/0]).

sh() -> 
        Input = io:get_line(">"),
        
        Tokens = string:tokens(Input, " \n"),
        [Command | _] = Tokens,


        case Command of
             "help" -> help(),sh();
             "quit" -> finished;
             X -> store(Input)
        end.

help() -> 
p("help - this command"),
p("quit - exits to the command line").

store(Text) -> 
               user_default:store(Text).