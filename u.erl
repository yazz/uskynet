-module(u).
-compile(export_all).

does_anything_exist_in_db( C ) -> zql:exists(C, "USkynet"). 

create_admin( C ) -> zql:set(C,"USkynet",[]),

                     AdminGroup = zql:create(C),
                     zql:set(C,AdminGroup,"type","group"),
                     zql:set(C,AdminGroup,"name","admin"),

                     AdminUser = zql:create(C),
                     zql:set(C,AdminUser,"type","user"),
                     zql:set(C,AdminUser,"username","root"),
                     zql:set(C,AdminUser,"group","admin").

                               
login(Username, Password) -> ok.