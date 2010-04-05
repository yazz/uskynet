-module(zhelp).
-compile(export_all).
-import(zprint,[println/1]).

help()         ->   println("Try one of the following:"),
                    println(""),
                    println("zhelp:help(zql)."),
                    println("zhelp:help(zqloo).").    

help(zql)      ->   println("---------------------------------- Help ---------------------------------"),

	       	    println("C = [{driver,db_riak_driver},{hostname,'riak@192.168.1.4'},{bucket,<<\"default\">>}],"),
		    println(""),
		    println("zql:set(C, \"boy\", \"Is here\"),"),
		    println("Value = zql:get(C, \"boy\")."),
		    println(""),
                    println(">> \"Is here\"\n\n"),

                    println("C = zql:local().                     -- gets a connection locally"),
                    println("zql:set(C, Key, Value ).             -- set a key / value"),

                    println("zql:get(C, Key).                     -- get a value"),
                    println("zql:exists(C, Key).                  -- true / false"),
                    println("zql:delete(C, Key).                  -- ok"),
                    println("zql:delete_all(C, yes_im_sure).      -- ok"),
                    println("zql:ls(C).                           -- get all keys as a list"),

                    println("R = zql:create(C).                   -- create a record and return it's unique ID"),
                    println("zql:print(C,R).                      -- prints the record in a screen friendly format"),
                    println("zql:set(C,R,type,person).            -- sets the type of the record to a person"),

     		    println("-------------------------------------------------------------------------"),
                    ok.

