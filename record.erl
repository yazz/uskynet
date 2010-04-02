-module(record).
-compile(export_all).

hex_uuid() -> os:cmd("uuidgen").
uuid() -> hex_uuid().

create(UUID) -> C = r:store(UUID, ""),
                ok.

set(UUID, Property, Value) -> r:store(UUID,Property).

new_record() -> UUID = uuid(),
                create(UUID),
                UUID.
