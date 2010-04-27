-module(user_default).
-compile(export_all).

-import(zprint,[println/1,p/1,q/1,print_number/1]).

zhelp() -> 
p("-----------------------------------------------------------------------------"),
p("-                                                                           -"),
p("-                           default commands                                -"),
p("-                                                                           -"),
p("-----------------------------------------------------------------------------"),
p("                                                                             "),
p("hello( )                                  basic check of system              "),
p("whichdb( )                                which database are we using        "),
p("lsdb ( )                                  list contents of the database      "),
p("start( )                                  starts mnesia database             "),
p("                                                                             "),
p("R=new( )                                  creates a new record               "),

p("test ()                                          run the self tests for zql  "),
p("local ()                                         returns a local connection  "),
p("test_connection( ConnectionArgs )                tests a connection          "),
p("session( ConnectionArgs )                        returns an OO session to ZQL"),
p("                                                                             "),
p("ls (ConnectionArgs)                                                          "),
p("set (ConnectionArgs, Key, Value)                 set a value                 "),
p("put (ConnectionArgs, Key, Value)                 set a value                 "),
p("get (ConnectionArgs, Key)                        get the value of Key        "),
p("exists (ConnectionArgs, Key)                     does this record/Key exist  "),
p("                                                                             "),
p("create_record (ConnectionArgs )                  create a record             "),
p("delete_record (ConnectionArgs )                  delete a record             "),
p("                                                                             "),
p("has_property (ConnectionArgs, Key, PropertyName) check for a property        "),
p("set_property (C, Key, PropertyName, Value)       set a property              "),
p("delete_property (C, Key, Property)               delete a property           "),
p("add_property (C, Key, PropertyName, Value)       add to a property list      "),  
p("delete_property_name_value (C, Key, P, Value)    delete a property value     "),
p("                                                                             "),
p("print (ConnectionArgs, ID)                       print record with key ID    "),
p("print_all (ConnectionArgs)                       print all records           "),
p("connect (ConnectionArgs)                         connect to the database     "),
p("get (ConnectionArgs, Key)                        get the value of Key        "),

p("                                                                             "),
p(" Example:                                                                    "),
p("C = [{driver,db_riak_driver}, {hostname,'riak@127.0.0.1'},{bucket,<<\"default\">>}]."),
q('zql:set(C, "Name", "Scott").                                                 '),
q('zql:get(C, "Name" ).                                                         '),
p("-----------------------------------------------------------------------------"),
ok.


hello( ) -> p("Hello. System is available").

whichdb( ) -> zql:whichdb( db() ).

lsdb( ) -> zql:ls( db()).

count( ) -> zql:count( db() ).

db( ) -> zql:local().

new( ) -> DB = get_db(),
          Record = DB:create_record( ),
          Id = Record:id(),
          Id.


set(Key,Value) -> zql:set(db(),Key,Value).

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
                               Id = Record:id(),
                               Db = db(),
                               Db,
                               CodeRecord = codeoo:new( Db, Id ),
                               CodeRecord.

add_code( ) ->                 add_code("", "").


