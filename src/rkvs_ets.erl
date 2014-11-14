%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @doc ETS backend
%%
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
         fold_keys/4]).


open(Name, Options) ->
    Ets = ets:new(kvdb_ets, [ordered_set,public]),
    {ok, #engine{name=Name,
                 mod=?MODULE,
                 ref=Ets,
                 options=Options}}.


close(_Engine) ->
    ok.

destroy(#engine{ref=Ref}) ->
    true = ets:delete(Ref),
    ok.

contains(#engine{ref=Ref}, Key) ->
    ets:member(Ref, Key).

get(#engine{ref=Ref}, Key) ->
    case ets:lookup(Ref, Key) of
        [{Key, Value}] -> Value;
        false -> {error, not_found}
    end.

put(#engine{ref=Ref}, Key, Value) ->
    true = ets:insert(Ref, {Key, Value}),
    ok.

clear(#engine{ref=Ref}, Key) ->
    true = ets:delete(Ref, Key),
    ok.

write_batch(#engine{ref=Ref}, Ops) ->
    lists:foreach(fun
            ({put, K, V}) ->
                true = ets:insert(Ref, {K, V});
            ({delete, K}) ->
                true = ets:delete(Ref, K)
        end, Ops).

scan(Engine, Start, End, Max) ->
    AccFun = fun({K, V}, Acc) ->
            [{K, V} | Acc]
    end,
    AccOut = fold(Engine, AccFun, [], [{start_key, Start},
                                       {end_key, End},
                                       {max, Max}]),
    lists:reverse(AccOut).

clear_range(Engine, Start, End, Max) ->
    AccFun = fun(K, Acc) ->
            [{delete, K} | Acc]
    end,
    Ops = fold_keys(Engine, AccFun, [], [{start_key, Start},
                                         {end_key, End},
                                         {max, Max}]),
    write_batch(Engine, Ops).

fold_keys(#engine{ref=Ref}, Fun, Acc0, Opts) ->
    do_fold(Ref, Fun, Acc0, rkvs_util:fold_options(Opts, #fold_options{}),
            keys_only).

fold(#engine{ref=Ref}, Fun, Acc0, Opts) ->
    do_fold(Ref, Fun, Acc0, rkvs_util:fold_options(Opts, #fold_options{}),
            values).

do_fold(Ref, Fun, Acc0, #fold_options{start_key=Start}=Opts, Type) ->
    case Start of
        first ->
            %% if first start with the first key if any
            fold_loop(ets:first(Ref), Ref, Fun, Acc0, 0, Opts, Type);
        _ ->
            %% does the key exist ?, if true, start with it, else start
            %% with the next one if any.
            case ets:member(Ref, Start) of
                true ->
                    fold_loop(Start, Ref, Fun, Acc0, 0, Opts, Type);
                false ->
                    fold_loop(ets:next(Ref, Start), Ref, Fun, Acc0, 0, Opts,
                              Type)
            end
    end.

fold_loop('$end_of_table', _Ref, _Fun, Acc, _N, _Opts, _Type) ->
    Acc;
fold_loop(Key, Ref, Fun, Acc0, N0, #fold_options{end_key=End, max=Max}=Opts,
          Type) when End =:= nil orelse Key < End ->

    {Acc, N} = case Type of
        keys_only -> {Fun(Key, Acc0), N0 + 1};
        values ->
            case ets:lookup(Ref, Key) of
                [] -> {Acc0, N0 + 1};
                [{Key, Val}] -> {Fun({Key, Val}, Acc0), N0 + 1}
            end
    end,
    if ((Max =:=0) orelse (N < Max)) ->
            fold_loop(ets:next(Ref, Key), Ref, Fun, Acc, N, Opts, Type);
        true ->
            Acc
    end;
fold_loop(Key, _Ref, Fun, Acc0, _N0, #fold_options{end_key=Key}, keys_only) ->
    Fun(Key, Acc0);
fold_loop(Key, Ref, Fun, Acc0, _N0, #fold_options{end_key=Key}, values) ->
    case ets:lookup(Ref, Key) of
        [] -> Acc0;
        [{Key, Val}] -> Fun({Key, Val}, Acc0)
    end;
fold_loop(_Key, _Ref, _Fun, Acc, _N, _Opts, _Type) ->
    Acc.
