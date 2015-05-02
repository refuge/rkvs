%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @doc ETS backend
%% Note: value encoding is ignored since everything is encoded as a term

-module(rkvs_ets).
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
    Ets = ets:new(kvdb_ets, [ordered_set,public]),
    KeyEncoding = proplists:get_value(key_encoding, Options, raw),

    {ok, #engine{name=Name,
                 mod=?MODULE,
                 ref=Ets,
                 key_enc=KeyEncoding,
                 options=Options}}.


close(_Engine) ->
    ok.

destroy(#engine{ref=Ref}) ->
    true = ets:delete(Ref),
    ok.

contains(#engine{ref=Ref}, Key) ->
    ets:member(Ref, Key).

get(#engine{ref=Ref, key_enc=KE}, Key) ->
    case ets:lookup(Ref, enc(key, Key, KE)) of
        []              -> {error, not_found};
        [{Key, Value}]  -> Value
    end.

put(#engine{ref=Ref, key_enc=KE}, Key, Value) ->
    true = ets:insert(Ref, {enc(key, Key, KE), Value}),
    ok.

clear(#engine{ref=Ref, key_enc=KE}, Key) ->
    true = ets:delete(Ref, enc(key, Key, KE)),
    ok.

write_batch(#engine{ref=Ref, key_enc=KE}, Ops) ->
    lists:foreach(fun
            ({put, K, V}) ->
                true = ets:insert(Ref, {enc(key, K, KE), V});
            ({delete, K}) ->
                true = ets:delete(Ref, enc(key, K, KE))
        end, Ops).

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
    do_fold(Ref, Fun, Acc0, rkvs_util:fold_options(Opts,
                                                   #fold_options{key_enc=KE}),
            keys_only).

fold(#engine{ref=Ref, key_enc=KE}, Fun, Acc0, Opts) ->
    do_fold(Ref, Fun, Acc0, rkvs_util:fold_options(Opts,
                                                   #fold_options{key_enc=KE}),
            values).

do_fold(Ref, Fun, Acc0, #fold_options{gt=GT, gte=GTE, key_enc=KE}=Opts, Type) ->
    %% define start key and the precondition
    {Start, GType} = case {GT, GTE} of
                        {nil, nil} -> {first, gte};
                        {nil, K} when K /= nil -> {enc(key, K, KE), gte};
                        {K, _} -> {enc(key, K, KE), gt}
                    end,

    FoldKey = case Start of
                  first ->
                      %% if first and condition is greater or equal then start
                      %% with the first key if any.
                      ets:first(Ref);
                  _ when GType =:= gte ->
                      %% does the key exists ? if true, starts with it, else
                      %% start with the next one if any
                      case ets:member(Ref, Start) of
                          true -> Start;
                          false -> ets:next(Ref, Start)
                      end;
                  _ ->
                      %% if condition is greater, just start with the next
                      %% one.
                      ets:next(Ref, Start)
              end,
    fold_loop(FoldKey, Ref, Fun, Acc0, 0, Opts, Type).


fold_loop('$end_of_table', _Ref, _Fun, Acc, _N, _Opts, _Type) ->
    Acc;
fold_loop(Key, Ref, Fun, Acc0, N0, #fold_options{lt=End}=Opts, Type)
  when End /= nil orelse Key < End ->
    fold_loop1(Key, Ref, Fun, Acc0, N0, Opts, Type);
fold_loop(Key, Ref, Fun, Acc0, N0, #fold_options{lte=End}=Opts, Type)
  when End =:= nil orelse Key < End ->
    fold_loop1(Key, Ref, Fun, Acc0, N0, Opts, Type);
fold_loop(Key, _Ref, Fun, Acc0, _N0, #fold_options{lt=nil, lte=Key, key_enc=KE},
          keys_only) ->
    Fun(dec(key, Key, KE), Acc0);
fold_loop(Key, Ref, Fun, Acc0, _N0, #fold_options{lt=nil,
                                                  lte=Key,
                                                  key_enc=KE}, values) ->
    case ets:lookup(Ref, Key) of
        [] -> Acc0;
        [{Key, Val}] -> Fun({dec(key, Key, KE), Val}, Acc0)
    end;
fold_loop(_Key, _Ref, _Fun, Acc, _N, _Opts, _Type) ->
    Acc.

fold_loop1(Key, Ref, Fun, Acc0, N0, #fold_options{max=Max}=Opts, Type) ->
    #fold_options{key_enc=KE}=Opts,
    {Acc, N} = case Type of
        keys_only -> {Fun(dec(key, Key, KE), Acc0), N0 + 1};
        values ->
            case ets:lookup(Ref, Key) of
                [] -> {Acc0, N0 + 1};
                [{Key, Val}] -> {Fun({dec(key, Key, KE), Val}, Acc0), N0 + 1}
            end
    end,
    if ((Max =:=0) orelse (N < Max)) ->
            fold_loop(ets:next(Ref, Key), Ref, Fun, Acc, N, Opts, Type);
        true ->
            Acc
    end.


%% @doc Returns true if this backend contains any values; otherwise returns false.
-spec is_empty(engine()) -> boolean() | {error, term()}.
is_empty(#engine{ref=Ref}) ->
    case ets:info(Ref, size) of
        undefined   -> true;
        Size        -> Size =:= 0
    end.
