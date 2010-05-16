-module(zql_oo_helper).
-compile(export_all).
-include_lib("zql_all_imports.hrl").


create_oo_session( ConnectionArgs ) -> Session = zql_oo_session:new( ConnectionArgs ),
                                       Session.

