-module(sh).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help() -> 
p("help - this command"),
p("it - the last thing created"),
p("quit - exits to the command line").



help(Args) -> Num = length(Args),
              
              case Num of 
                   0 -> help();

                   _ -> Command = nth(1,Args),
		     	HelpFunctionName = Command ++ "_help",
                     	apply(sh, list_to_atom(HelpFunctionName), [ ])
              end.




sh() -> p(""),
     	p("---------------------------------------------------------------------------------------------"),
        InputWithReturn = io:get_line(">"),
	Input = remove_newline(InputWithReturn),
        
        Tokens = string:tokens(Input, " \n"),
        [Command | Args] = Tokens,

	CommandAtom = list_to_atom(Command),

        case CommandAtom of
             it -> it(),sh();
             get -> read(Args),sh();
             help -> help(Args),sh();

	     q -> finished;
             quit -> finished;

             _UnknownCommand -> process(Input),sh()
        end.





it_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                           it                                      -"),
p("-                                                                   -"),
p("-     Used to refer to the last used object                         -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > get person_ref						       -"),
p("- > delete it                                                       -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

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





process_help() ->
p("---------------------------------------------------------------------"),
p("-                                                                   -"),
p("-                      process( InputText )                         -"),
p("-                                                                   -"),
p("-    processes free text       				       -"),
p("-                                                                   -"),
p("- Example:                                                          -"),
p("-                                                                   -"),
p("- > 						       		       -"),
p("- >                                                        	       -"),
p("-                                                                   -"),
p("---------------------------------------------------------------------").

process( InputText ) -> p("Searching for '" ++ InputText ++ "'"),
	 	     	V = g( InputText ),
	 	     	p(V).
