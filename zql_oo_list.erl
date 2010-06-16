-module(zql_oo_list,[Db, ListName]).
-compile(export_all).
-include_lib("zql_all_imports.hrl").

help() -> 
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("-                           MODULE zql_oo_list                              -"),
p("-                                                                           -"),
p("-                 OO Erlang interface to the ZQL list type                  -"),
p("-                                                                           -"),
p("-----------------------------------------------------------------------------"),
p("                                                                             "),
p("help (Command)                                   get help on a command       "),
p("                                                                             "),
p("ls ()                                            show all items in this list "),
p("add (Value)                                      set a value                 "),
p("remove (Value)                                   set a value                 "),
p("has (Value)                                      get the value of Key        "),
p("                                                                             "),
p("print_all ( )                                    print all records           "),
p("count( )                                         count the number of records "),
p("                                                                             "),
p("                                                                             "),
p("                                                                             "),
p(" Example:                                                                    "),
p("DB = session([{driver,db_riak_driver}, {hostname,'riak@127.0.0.1'}).         "),
q('L = DB:list("stuff").                                                        '),
q('L:add("apples" ).                                                             '),
q('L:add("oranges" ).                                                             '),
p("-----------------------------------------------------------------------------"),
ok.


id( ) -> ListName.
connection( ) -> Db.

add( Item  ) -> AlreadyExists = Db:exists("list_" ++ to_string( ListName ) ++ "_item_" ++ to_string( Item )),
                R = case AlreadyExists of
                    false -> Next = Db:incr( "list_index_" ++ to_string( ListName ) ),
                             ItemStoreName = "list_item_" ++ to_string(Next),
                             Db:set( ItemStoreName, Item ),
                             Db:set( "list_" ++ to_string( ListName ) ++ "_item_" ++ to_string( Item ), to_string(Next-1)),
                             (Next - 1);
                    true -> already_exists
                end,
                R.

count( ) -> C = Db:get_number_or_nil("list_index_" ++ to_string(ListName) ),
            X = case C of
                nil -> 0;
                N -> N
            end,
            X.

remove( Item ) -> Item.

has( Item ) -> N = "list_" ++ to_string( ListName ) ++ "_item_" ++ to_string( Item ),
               Exists = Db:exists( N ),
               Exists.



ls() -> X = THIS:count(),
        L = case X of
            0 -> [];
            N -> ls(N)
        end,
        L.

ls(-1) -> [];
ls(N) -> [THIS:get(N) | ls( N - 1) ].

get(N) -> Db:get_or_nil( "list_item_" ++ to_string(N)).