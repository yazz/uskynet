-module(zutils).
-compile(export_all).

to_binary(Value) when is_binary(Value) -> Value;
to_binary(Value) when is_list(Value) -> list_to_binary(Value).

hex_uuid() -> UUID_with_newline = os:cmd("uuidgen"),
              UUID_without_newline = lists:sublist( UUID_with_newline ,1,36),
              UUID_without_newline.

uuid() -> hex_uuid().

remove_newline(Line) -> Length = length(Line),
                        NewLine = lists:sublist( Line, Length - 1),
                        NewLine.

get_timestamp_microseconds() ->
    {Mega,Sec,Micro} = erlang:now(),
    (Mega*1000000+Sec)*1000000+Micro.

to_string(I) when is_integer(I) -> lists:flatten(io_lib:format("~p", [I]));
to_string(S) when is_list(S) -> S;
to_string(S) when is_binary(S) -> binary_to_list(S).


to_integer(I) when is_binary(I) -> to_integer(to_string(I));
to_integer(I) when is_integer(I) -> I;
to_integer(S) when is_list(S) -> 
                                 {I,_}=string:to_integer(to_string(S)),
                                 I.
