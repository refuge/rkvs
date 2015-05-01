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
         fold_keys/4,
         is_empty/1]).


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
        []              -> {error, not_found};
        [{Key, Value}]  -> Value
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

fold_keys(#engine{ref=Ref}, Fun, Acc0, Opts) ->
    do_fold(Ref, Fun, Acc0, rkvs_util:fold_options(Opts, #fold_options{}),
            keys_only).

fold(#engine{ref=Ref}, Fun, Acc0, Opts) ->
    do_fold(Ref, Fun, Acc0, rkvs_util:fold_options(Opts, #fold_options{}),
            values).

do_fold(Ref, Fun, Acc0, #fold_options{gt=GT, gte=GTE}=Opts, Type) ->
    %% define start key and the precondition
    {Start, GType} = case {GT, GTE} of
                        {nil, nil} -> {first, gte};
                        {nil, K} when K /= nil -> {K, gte};
                        {K, _} -> {K, gt}
                    end,

    FoldKey = case Start of
                  first when GType =:= gte ->
                      %% if first and condition is greater or equal then start
                      %% with the first key if any.
                      ets:first(Ref);
                  first ->
                      %% if first and condtion is greater then start with the
                      %% next key after first if any.
                      case ets:first(Ref) of
                          'end_of_table' -> Acc0;
                          FirstKey -> ets:next(Ref, FirstKey)
                      end;
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
fold_loop(Key, _Ref, Fun, Acc0, _N0, #fold_options{lt=nil, lte=Key}, keys_only) ->
    Fun(Key, Acc0);
fold_loop(Key, Ref, Fun, Acc0, _N0, #fold_options{lt=nil, lte=Key}, values) ->
    case ets:lookup(Ref, Key) of
        [] -> Acc0;
        [{Key, Val}] -> Fun({Key, Val}, Acc0)
    end;
fold_loop(_Key, _Ref, _Fun, Acc, _N, _Opts, _Type) ->
    Acc.

fold_loop1(Key, Ref, Fun, Acc0, N0, #fold_options{max=Max}=Opts, Type) ->
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
    end.


%% @doc Returns true if this backend contains any values; otherwise returns false.
-spec is_empty(engine()) -> boolean() | {error, term()}.
is_empty(#engine{ref=Ref}) ->
    case ets:info(Ref, size) of
        undefined   -> true;
        Size        -> Size =:= 0
    end.
