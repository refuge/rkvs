%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @doc hanoidb backend
%%
%% You can pass any options from
%% [hanoidb](https://github.com/bkrestenkra/erocksdb)
%% when opening the database using the db_opts settings.
%%
%% Optionnaly you can pass a db_dir option to set the path of the database.

-module(rkvs_hanoidb).
-behaviour(rkvs_storage_backend).

-include("rkvs.hrl").

-include_lib("hanoidb/include/hanoidb.hrl").


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
         fold_keys/4,
         is_empty/1]).

-import(rkvs_util, [enc/3, dec/3]).

open(Name, Options) ->
    Path = case proplists:get_value(db_dir, Options) of
               undefined -> Name;
               Dir -> filename:join(Dir, Name)
           end,

    DbOpts = proplists:get_value(db_opts, Options, [{sync_strategy, sync}]),
    KeyEncoding = proplists:get_value(key_encoding, Options, raw),
    ValueEncoding = proplists:get_value(value_encoding, Options, term),
    case hanoidb:open(Path, DbOpts) of
        {ok, Ref} ->
            {ok, #engine{name=Name,
                         mod=?MODULE,
                         ref=Ref,
                         key_enc=KeyEncoding,
                         val_enc=ValueEncoding,
                         options=Options}};
        Error ->
            Error
    end.

close(#engine{ref=Ref}) ->
    hanoidb:close(Ref).

destroy(#engine{ref=Ref}) ->
    hanoidb:destroy(Ref).

contains(Engine, Key) ->
    Fun = fun(_K, _Acc) -> true end,
    fold_keys(Engine, Fun, false, [{start_key, Key},
                                   {end_key, Key},
                                   {max, 1}]).


get(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Key) ->
    case hanoidb:get(Ref, enc(key, Key, KE)) of
        {ok, Val} -> dec(value, Val, VE);
        not_found -> {error, not_found};
        Error -> Error
    end.

put(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Key, Value) ->
    hanoidb:put(Ref, enc(key, Key, KE), enc(value, Value, VE)).

clear(#engine{ref=Ref, key_enc=KE}, Key) ->
    hanoidb:delete(Ref, enc(key, Key, KE)).

write_batch(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Ops0) ->
    Ops = lists:reverse(lists:foldl(fun
                    ({put, K, V}, Acc) ->
                        [{put, enc(key, K, KE), enc(value, V, VE)} | Acc];
                    ({delete, K}, Acc) ->
                        [{delete, enc(key, K, KE)} | Acc]
                end, [], Ops0)),

    hanoidb:transact(Ref, Ops).


scan(Engine, Start, End, Max) ->
    AccFun = fun({K, V}, Acc) ->
            [{K, V} | Acc]
    end,
    AccOut = fold(Engine, AccFun, [], [{gte, Start},
                                       {lte, End},
                                       {max, Max}]),
    lists:reverse(AccOut).

clear_range(Engine, Start, End, Max) ->
    AccFun = fun(K, Acc) ->
            [{delete, K} | Acc]
    end,
    Ops = fold_keys(Engine, AccFun, [], [{gte, Start},
                                         {lte, End},
                                         {max, Max}]),
    write_batch(Engine, Ops).


fold_keys(#engine{ref=Ref, key_enc=KE}, Fun, Acc0, Opts) ->
    KeyRange = key_range(Opts, KE),
    WrapperFun = fun(K, _V, Acc1) ->
                         Fun(dec(key, K, KE), Acc1)
                 end,
    hanoidb:fold_range(Ref, WrapperFun, Acc0, KeyRange).


fold(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Fun, Acc0, Opts) ->
    KeyRange = key_range(Opts, KE),
    WrapperFun = fun(K, V, Acc1) ->
                         Fun({dec(key, K, KE), dec(value, V, VE)}, Acc1)
                 end,
    hanoidb:fold_range(Ref, WrapperFun, Acc0, KeyRange).

is_empty(Engine) ->
    case scan(Engine, <<>>, undefined, 1) of
        [] -> true;
        _ -> false
    end.

key_range(Opts, KE) ->
    key_range(Opts, KE, #key_range{}).

key_range([], _KE, KR) ->
    KR;
key_range([{start_key, Start} | Rest], KE, KR) ->
    key_range(Rest, KE, KR#key_range{from_key=enc(key, Start, KE),
                                     from_inclusive=true});
key_range([{end_key, End} | Rest], KE, KR) ->
    key_range(Rest, KE, KR#key_range{to_key=enc(key, End, KE),
                                     to_inclusive=true});
key_range([{gt, Start} | Rest], KE, KR) ->
    key_range(Rest, KE, KR#key_range{from_key=enc(key, Start, KE),
                                     from_inclusive=false});
key_range([{gte, Start} | Rest], KE, KR) ->
    key_range(Rest, KE, KR#key_range{from_key=enc(key, Start, KE),
                                     from_inclusive=true});
key_range([{lt, End} | Rest], KE, KR) ->
    key_range(Rest, KE, KR#key_range{to_key=enc(key, End, KE),
                                     to_inclusive=false});
key_range([{lte, End} | Rest], KE, KR) ->
    key_range(Rest, KE, KR#key_range{to_key=enc(key, End, KE),
                                     to_inclusive=true});
key_range([{max, Max} | Rest], KE, KR) ->
    key_range(Rest, KE, KR#key_range{limit=Max});
key_range([_ | Rest], KE,  KR) ->
    key_range(Rest, KE, KR).
