%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @doc eleveldb backendi
%%
%% You can pass any options from
%% [eleveldb](https://github.com/basho/eleveldb/blob/develop/src/eleveldb.erl)
%% when opening the database using the db_opts settings.
%%
%% Optionnaly you can pass a db_dir option to set the path of the database.
-module(rkvs_leveldb).
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
    Path = case proplists:get_value(db_dir, Options) of
               undefined -> Name;
               Dir -> filename:join(Dir, Name)
           end,

    filelib:ensure_dir(filename:join(Path, "dummy")),
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),

    case eleveldb:open(Path, DbOpts) of
        {ok, Ref} ->
            {ok, #engine{name=Name,
                         mod=?MODULE,
                         ref=Ref,
                         options=Options}};
        Error ->
            Error
    end.

close(#engine{ref=Ref}) ->
    eleveldb:close(Ref).

destroy(#engine{name=Name, options=Options}) ->
    DbOpts = proplists:get_value(db_opts, Options,
                                 [{create_if_missing, true}]),

    eleveldb:destroy(Name, DbOpts).

contains(Engine, Key) ->
    Fun = fun(_K, _Acc) -> true end,
    fold_keys(Engine, Fun, false, [{start_key, Key},
                                   {end_key, Key},
                                   {max, 1}]).

get(#engine{ref=Ref}, Key) ->
    case eleveldb:get(Ref, Key, []) of
        {ok, Val} -> dec(Val);
        not_found -> {error, not_found};
        Error -> Error
    end.

put(#engine{ref=Ref}, Key, Value) ->
    eleveldb:put(Ref, Key, enc(Value), [{sync, true}]).

clear(#engine{ref=Ref}, Key) ->
    eleveldb:delete(Ref, Key, [{sync, true}]).


write_batch(#engine{ref=Ref}, Ops0) ->
    Ops = lists:reverse(lists:foldl(fun
                    ({put, K, V}, Acc) ->
                        [{put, K, enc(V)} | Acc];
                    (Op, Acc) ->
                        [Op | Acc]
                end, [], Ops0)),

    eleveldb:write(Ref, Ops, [{sync, true}]).

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
    FillCache = proplists:get_value(fill_cache, Opts, false),
    {ok, Itr} = eleveldb:iterator(Ref, [{fill_cache, FillCache}], keys_only),
    do_fold(Itr, Fun, Acc0, rkvs_util:fold_options(Opts, #fold_options{})).


fold(#engine{ref=Ref}, Fun, Acc0, Opts) ->
    FillCache = proplists:get_value(fill_cache, Opts, true),
    {ok, Itr} = eleveldb:iterator(Ref, [{fill_cache, FillCache}]),
    do_fold(Itr, Fun, Acc0, rkvs_util:fold_options(Opts, #fold_options{})).


%% private

do_fold(Itr, Fun, Acc0, #fold_options{gt=Start}=Opts)
  when Start /= nil ->
    try
        case eleveldb:iterator_move(Itr, Start) of
            {error, iterator_closed} -> Acc0;
            {error, invalid_iterator} -> Acc0;
            Next ->
                fold_loop(eleveldb:iterator_move(Itr, prefetch), Itr, Fun,
                          Acc0, 0, Opts)
        end
    after
        eleveldb:iterator_close(Itr)
    end;
do_fold(Itr, Fun, Acc0, #fold_options{gte=Start}=Opts) ->
    try
        fold_loop(eleveldb:iterator_move(Itr, Start), Itr, Fun,
                  Acc0, 0, Opts)
    after
        eleveldb:iterator_close(Itr)
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
fold_loop({ok, K}, _Itr, Fun, Acc0, _N,  #fold_options{lt=nil, lte=K}) ->
    Fun(K, Acc0);
fold_loop({ok, _K}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0;
fold_loop({ok, K, _V}=KV, Itr, Fun, Acc0, N0, #fold_options{lt=End}=Opts)
  when End /= nil orelse K < End ->
    fold_loop1(KV, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K, _V}=KV, Itr, Fun, Acc0, N0, #fold_options{lte=End}=Opts)
  when End =:= nil orelse K < End ->
    fold_loop1(KV, Itr, Fun, Acc0, N0, Opts);
fold_loop({ok, K, V}, _Itr, Fun, Acc0, _N, #fold_options{lt=nil, lte=K}) ->
    Fun({K, dec(V)}, Acc0);
fold_loop({ok, _K, _V}, _Itr, _Fun, Acc0, _N, _Opts) ->
    Acc0.


fold_loop1({ok, K}, Itr, Fun, Acc0, N0, #fold_options{max=Max}=Opts) ->
    Acc = Fun(K, Acc0),
    N = N0 + 1,
    if ((Max =:=0) orelse (N < Max)) ->
            fold_loop(eleveldb:iterator_move(Itr, prefetch), Itr, Fun, Acc,
                      N, Opts);
        true ->
            Acc
    end;
fold_loop1({ok, K, V}, Itr, Fun, Acc0, N0, #fold_options{max=Max}=Opts) ->
    Acc = Fun({K, dec(V)}, Acc0),
    N = N0 + 1,
    if
        ((Max =:= 0) orelse (N < Max)) ->
            fold_loop(eleveldb:iterator_move(Itr, prefetch), Itr, Fun, Acc, N,
                      Opts);
        true ->
            Acc
    end.

enc(T) ->
    term_to_binary(T).

dec(B) ->
    binary_to_term(B).


%% @doc Returns true if this backend contains any values; otherwise returns false.
-spec is_empty(engine()) -> boolean() | {error, term()}.
is_empty(#engine{ref=Ref}) ->
    eleveldb:is_empty(Ref).
