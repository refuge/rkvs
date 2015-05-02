%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @doc erocksdb backend
%%
%% You can pass any options from
%% [erocksdb](https://github.com/leo-project/erocksdb)
%% when opening the database using the db_opts settings.
%%
%% Optionnaly you can pass a db_dir option to set the path of the database.
-module(rkvs_rocksdb).
-behaviour(rkvs_storage_backend).

-include("rkvs.hrl").

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

    filelib:ensure_dir(filename:join(Path, "dummy")),
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),

    KeyEncoding = proplists:get_value(key_encoding, Options, raw),
    ValueEncoding = proplists:get_value(value_encoding, Options, term),

    case erocksdb:open(Path, DbOpts, []) of
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
    erocksdb:close(Ref).

destroy(#engine{name=Name, options=Options}) ->
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),

    erocksdb:destroy(Name, DbOpts).

contains(Engine, Key) ->
    Fun = fun(_K, _Acc) -> true end,
    fold_keys(Engine, Fun, false, [{start_key, Key},
                                   {end_key, Key},
                                   {max, 1}]).

get(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Key) ->
    case erocksdb:get(Ref, enc(key, Key, KE), []) of
        {ok, Val} -> dec(value, Val, VE);
        not_found -> {error, not_found};
        Error -> Error
    end.

put(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Key, Value) ->
    erocksdb:put(Ref, enc(key, Key, KE), enc(value, Value, VE), [{sync, true}]).

clear(#engine{ref=Ref, key_enc=KE}, Key) ->
    erocksdb:delete(Ref, enc(key, Key, KE), [{sync, true}]).

write_batch(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Ops0) ->
    Ops = lists:reverse(lists:foldl(fun
                    ({put, K, V}, Acc) ->
                        [{put, enc(key, K, KE), enc(value, V, VE)} | Acc];
                    ({delete, K}, Acc) ->
                        [{delete, enc(key, K, KE)} | Acc]
                end, [], Ops0)),
    erocksdb:write(Ref, Ops, [{sync, true}]).

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

fold_keys(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Fun, Acc0, Opts) ->
    FillCache = proplists:get_value(fill_cache, Opts, true),
    {ok, Itr} = erocksdb:iterator(Ref, [{fill_cache, FillCache}], keys_only),
    FoldOpts0 = #fold_options{key_enc=KE, val_enc=VE},
    do_fold(Itr, Fun, Acc0, rkvs_util:fold_options(Opts, FoldOpts0)).


fold(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Fun, Acc0, Opts) ->
    FillCache = proplists:get_value(fill_cache, Opts, true),
    {ok, Itr} = erocksdb:iterator(Ref, [{fill_cache, FillCache}]),
    FoldOpts0 = #fold_options{key_enc=KE, val_enc=VE},
    do_fold(Itr, Fun, Acc0, rkvs_util:fold_options(Opts, FoldOpts0)).


%% private

do_fold(Itr, Fun, Acc0, #fold_options{gt=GT, gte=GTE, key_enc=KE}=Opts) ->
    {Start, Inclusive} = case {GT, GTE} of
                      {nil, nil} -> {first, true};
                      {first, _} -> {first, false};
                      {K, _} when is_binary(K) -> {enc(key, K, KE), false};
                      {_, K} -> {enc(key, K, KE), true}
                  end,
    try
        case erocksdb:iterator_move(Itr, Start) of
            {ok, Start} when Inclusive /= true ->
                fold_loop(erocksdb:iterator_move(Itr, next), Itr, Fun,
                          Acc0, 0, Opts);
            {ok, Start, _V}  when Inclusive /= true ->
                fold_loop(erocksdb:iterator_move(Itr, next), Itr, Fun,
                          Acc0, 0, Opts);
            Next ->
                fold_loop(Next, Itr, Fun, Acc0, 0, Opts)

        end
    after
        erocksdb:iterator_close(Itr)
    end.

fold_loop({error, iterator_closed}, _Itr, _Fun, Acc0, _N, _Opts) ->
    throw({iterator_closed, Acc0});
fold_loop({error, invalid_iterator}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0;
fold_loop({ok, K}=KT, Itr, Fun, Acc0, N0, #fold_options{lt=End}=Opts)
  when End /= nil, K < End ->
    fold_loop1(KT, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K}=KT, Itr, Fun, Acc0, N0, #fold_options{lte=End}=Opts)
         when End =:= nil orelse K < End ->
    fold_loop1(KT, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K}, _Itr, Fun, Acc0, _N,  #fold_options{lt=nil, lte=K}=Opts) ->
    Fun(dec(key, K, Opts#fold_options.key_enc), Acc0);
fold_loop({ok, _K}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0;
fold_loop({ok, K, _V}=KV, Itr, Fun, Acc0, N0, #fold_options{lt=End}=Opts)
  when End /= nil orelse K < End ->
    fold_loop1(KV, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K, _V}=KV, Itr, Fun, Acc0, N0, #fold_options{lte=End}=Opts)
  when End =:= nil orelse K < End ->
    fold_loop1(KV, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K, V}, _Itr, Fun, Acc0, _N, #fold_options{lt=nil, lte=K}=Opts) ->
    #fold_options{key_enc=KE, val_enc=VE}=Opts,
    Fun({dec(key, K, KE), dec(value, V, VE)}, Acc0);
fold_loop({ok, _K, _V}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0.


fold_loop1({ok, K}, Itr, Fun, Acc0, N0, #fold_options{max=Max}=Opts) ->
    Acc = Fun(dec(key, K, Opts#fold_options.key_enc), Acc0),
    N = N0 + 1,
    if ((Max =:=0) orelse (N < Max)) ->
            fold_loop(erocksdb:iterator_move(Itr, next), Itr, Fun, Acc,
                      N, Opts);
        true ->
            Acc
    end;
fold_loop1({ok, K, V}, Itr, Fun, Acc0, N0, #fold_options{max=Max}=Opts) ->
    #fold_options{key_enc=KE, val_enc=VE}=Opts,
    Acc = Fun({enc(key, K, KE), dec(value, V, VE)}, Acc0),
    N = N0 + 1,
    if
        ((Max =:= 0) orelse (N < Max)) ->
            fold_loop(erocksdb:iterator_move(Itr, next), Itr, Fun, Acc, N,
                      Opts);
        true ->
            Acc
    end.

%% @doc Returns true if this backend contains any values; otherwise returns false.
-spec is_empty(engine()) -> boolean() | {error, term()}.
is_empty(#engine{ref=Ref}) ->
    erocksdb:is_empty(Ref).
