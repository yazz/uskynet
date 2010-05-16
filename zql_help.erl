-module(zql_help).
-compile(export_all).
-include_lib("zql_imports.hrl").

help()         ->   p("-----------------------------------------------------------------"),
                    p("- To get help on a module use MODULE_NAME:help()                -"),
                    p("- eg: zprint:help().                                            -"),
                    p("-                                                               -"),      
                    p("-                        LIST OF MODULES                        -"),
                    p("-----------------------------------------------------------------"),
                    p("zql                Erlang based interface to system              "),
                    p("zql_help           Help system                                   "),
                    p("zql_oo_session     OO style parameterised module interface to zql"),
                    p("-----------------------------------------------------------------").

help(zql)      ->   println("---------------------------------- Help ---------------------------------"),

	       	    println(" This is the pure functional interface to the ZQL system"),

     		    println("-------------------------------------------------------------------------"),
                    ok.

