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


open(Name, Options) ->
    Path = case proplists:get_value(db_dir, Options) of
               undefined -> Name;
               Dir -> filename:join(Dir, Name)
           end,

    DbOpts = proplists:get_value(db_opts, Options, [{sync_strategy, sync}]),
    case hanoidb:open(Path, DbOpts) of
        {ok, Ref} ->
            {ok, #engine{name=Name,
                         mod=?MODULE,
                         ref=Ref,
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


get(#engine{ref=Ref}, Key) ->
    case hanoidb:get(Ref, Key) of
        {ok, Val} -> dec(Val);
        not_found -> {error, not_found};
        Error -> Error
    end.

put(#engine{ref=Ref}, Key, Value) ->
    hanoidb:put(Ref, Key, enc(Value)).

clear(#engine{ref=Ref}, Key) ->
    hanoidb:delete(Ref, Key).

write_batch(#engine{ref=Ref}, Ops0) ->
    Ops = lists:reverse(lists:foldl(fun
                    ({put, K, V}, Acc) ->
                        [{put, K, enc(V)} | Acc];
                    (Op, Acc) ->
                        [Op | Acc]
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


fold_keys(#engine{ref=Ref}, Fun, Acc0, Opts) ->
    KeyRange = key_range(Opts),
    WrapperFun = fun(K, _V, Acc1) ->
                         Fun(K, Acc1)
                 end,
    hanoidb:fold_range(Ref, WrapperFun, Acc0, KeyRange).


fold(#engine{ref=Ref}, Fun, Acc0, Opts) ->
    KeyRange = key_range(Opts),
    WrapperFun = fun(K, V, Acc1) ->
                         Fun({K, V}, Acc1)
                 end,
    hanoidb:fold_range(Ref, WrapperFun, Acc0, KeyRange).

is_empty(Engine) ->
    case scan(Engine, <<>>, undefined, 1) of
        [] -> true;
        _ -> false
    end.

enc(T) ->
    term_to_binary(T).

dec(B) ->
    binary_to_term(B).

key_range(Opts) ->
    key_range(Opts, #key_range{}).

key_range([], KR) ->
    KR;
key_range([{start_key, Start} | Rest], KR) ->
    key_range(Rest, KR#key_range{from_key=Start, from_inclusive=true});
key_range([{end_key, End} | Rest], KR) ->
    key_range(Rest, KR#key_range{to_key=End, to_inclusive=true});
key_range([{gt, Start} | Rest], KR) ->
    key_range(Rest, KR#key_range{from_key=Start, from_inclusive=false});
key_range([{gte, Start} | Rest], KR) ->
    key_range(Rest, KR#key_range{from_key=Start, from_inclusive=true});
key_range([{lt, End} | Rest], KR) ->
    key_range(Rest, KR#key_range{to_key=End, to_inclusive=false});
key_range([{lte, End} | Rest], KR) ->
    key_range(Rest, KR#key_range{to_key=End, to_inclusive=true});
key_range([{max, Max} | Rest], KR) ->
    key_range(Rest, KR#key_range{limit=Max});
key_range([_ | Rest], KR) ->
    key_range(Rest, KR).
