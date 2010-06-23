-module(zql_platform_driver_for_cygwin_dos_shell).
-compile(export_all).

hex_uuid() -> UUID_with_newline = os:cmd("c:/cygwin/bin/uuidgen"),
              UUID_without_newline = lists:sublist( UUID_with_newline ,1,36),
              UUID_without_newline.