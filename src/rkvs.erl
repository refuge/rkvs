-module(rkvs).

-export([open/2,
         close/1,
         destroy/1,
         contains/2,
         get/2,
         put/3,
         clear/2,
         write_batch/2,
         scan/4,
         clear_range/4,
         fold/4,
         fold_keys/4]).

-include("rkvs.hrl").

open(Name, Options) ->
    Mod = proplists:get_value(backend, Options, rkvs_eleveldb),
    Mod:open(Name, Options).


close(#engine{mod=Mod}=Engine) ->
    Mod:close(Engine).

destroy(#engine{mod=Mod}=Engine) ->
    Mod:destroy(Engine).

contains(#engine{mod=Mod}=Engine, Key) ->
    Mod:contains(Engine, Key).


get(#engine{mod=Mod}=Engine, Key) ->
    Mod:get(Engine, Key).

put(#engine{mod=Mod}=Engine, Key, Value) ->
    Mod:put(Engine, Key, Value).

clear(#engine{mod=Mod}=Engine, Key) ->
    Mod:clear(Engine, Key).

write_batch(#engine{mod=Mod}=Engine, Ops) ->
    Mod:write_batch(Engine, Ops).

scan(#engine{mod=Mod}=Engine, Start, End, Max) ->
    Mod:scan(Engine, Start, End, Max).

clear_range(#engine{mod=Mod}=Engine, Start, End, Max) ->
    Mod:clear_range(Engine, Start, End, Max).

fold_keys(#engine{mod=Mod}=Engine, Fun, Acc0, Opts) ->
    Mod:fold_keys(Engine, Fun, Acc0, Opts).

fold(#engine{mod=Mod}=Engine, Fun, Acc0, Opts) ->
    Mod:fold(Engine, Fun, Acc0, Opts).
