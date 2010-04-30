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
