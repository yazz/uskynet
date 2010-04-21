-module(user_default).

-compile(export_all).

-import(zprint,[println/1,p/1,q/1,print_number/1]).

hello( ) -> zprint:p("Hello SI").
db( ) -> zql:local().

set(Key,Value) -> zql:set(db(),Key,Value).
lsdb( ) -> zql:ls( db()).
count( ) -> zql:count( db() ).
find( ) -> count( ).

add(Type) -> DB = get_db(),
             Record = DB:create_record( ),
             Record:set(type,Type),
             Record.

get_db( ) -> DB = zql:session(db()),
             DB.

last_added( ) -> zprint:p("Show the last added record").
             



start() -> db_mnesia_driver:start().

add_code(TriggerRule, Code) -> DB = get_db(),
                               Record = DB:create_record( ),
                               Record:set(type,"code"),
                               Record:set(trigger, TriggerRule),
                               Record:set(code, Code),
                               ok.