-module(uskynet,[C]).
-compile(export_all).

get(X) -> C = zql:local().

ls() -> zql:ls(C).
