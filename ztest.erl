-module(ztest).
-compile(export_all).
-import(zprint,[println/1,print_number/1]).

test() -> test(riak).

test(riak) -> RiakConnection = [{driver,db_riak_driver},{hostname,'riak@127.0.0.1'},{bucket,<<"default">>}],
              test_zql(RiakConnection).


test_zql(C) ->  println("Number of records in datastore:"),
                Count = zql:count(C),
                print_number(Count),

                zql:delete_all(C,yes_im_sure),                

                zql:set(C, "boy", "Is here"),
                println("\nSaved 'boy' as 'is here'"),

                Value = zql:get(C, "boy"),
                println("got value of boy as : "),               
                println(Value),

                println("Check 'boy' exists :"),
                Exists = zql:exists(C, "boy"),
                println(Exists),

                zql:delete(C,"boy"),
                println("deleted 'boy'"),

                println("Check 'boy' exists :"),
                Exists2 = zql:exists(C, "boy"),
                println(Exists2),

                println("-----------------------"),

                LogEntry = zql:create(C),
                zql:set(C,LogEntry,"type","log").



                