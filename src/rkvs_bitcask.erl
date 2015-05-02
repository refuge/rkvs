%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

%% @doc bitcask backend
%%
%% You can pass any options from
%% [bitcask](https://github.com/basho/bitcask)
%% when opening the database using the db_opts settings.
%%
%% Optionnaly you can pass a db_dir option to set the path of the database.
-module(rkvs_bitcask).
-behaviour(rkvs_storage_backend).

-include("rkvs.hrl").
-include_lib("bitcask/include/bitcask.hrl").

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

-record(fold_st, {useracc,
                  cb,
                  key_enc,
                  val_enc,
                  num = 0,
                  max,
                  from = <<>>,
                  from_inclusive = false,
                  to = nil,
                  to_inclusive = false}).


open(Name, Options) ->
    Path = case proplists:get_value(db_dir, Options) of
               undefined -> Name;
               Dir -> filename:join(Dir, Name)
           end,

    DbOpts = proplists:get_value(db_opts, Options, [read_write]),
    KeyEncoding = proplists:get_value(key_encoding, Options, raw),
    ValueEncoding = proplists:get_value(value_encoding, Options, term),
    case catch bitcask:open(Path, DbOpts) of
        {error, _Reason}=Error -> Error;
        {'EXIT', Reason} -> {error, Reason};
        Ref when is_reference(Ref) ->
            {ok, #engine{name=Name,
                         mod=?MODULE,
                         ref=Ref,
                         key_enc=KeyEncoding,
                         val_enc=ValueEncoding,
                         options=Options}}
    end.

close(#engine{ref=Ref}) ->
    case catch bitcask:close(Ref) of
        ok -> ok;
        {error,{invalid_ref, Ref}} -> ok; %% already closed
        {error, Reason} -> {error, Reason};
        {'EXIT', Reason} -> {error, Reason}
    end.

destroy(#engine{name=Name, options=Options}=Engine) ->
    ok = close(Engine),
    Path = case proplists:get_value(db_dir, Options) of
               undefined -> Name;
               Dir -> filename:join(Dir, Name)
           end,
    data_directory_cleanup(Path),
    ok.

contains(Engine, Key) ->
    Fun = fun(_K, _Acc) -> true end,
    fold_keys(Engine, Fun, false, [{start_key, Key},
                                   {end_key, Key},
                                   {max, 1}]).


get(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Key) ->
    case catch bitcask:get(Ref, enc(key, Key, KE)) of
        {ok, Val} -> dec(value, Val, VE);
        not_found -> {error, not_found};
        {error, _Reason}=Error -> Error;
        {'EXIT', Reason} -> {error, Reason}
    end.


put(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Key, Value) ->
    case catch bitcask:put(Ref, enc(key, Key, KE), enc(value, Value, VE)) of
        ok -> ok;
        {error, _Reason}=Error -> Error;
        {'EXIT', Reason} -> {error, Reason}
    end.

clear(#engine{ref=Ref, key_enc=KE}, Key) ->
    case catch bitcask:delete(Ref, enc(key, Key, KE)) of
        ok -> ok;
        {error, _Reason}=Error -> Error;
        {'EXIT', Reason} -> {error, Reason}
    end.

write_batch(Engine, Ops) ->
    do_write_batch(Ops, Engine).


do_write_batch([], _Engine) ->
    ok;
do_write_batch([{put, K, V} | Rest], Engine) ->
    ok = rkvs_bitcask:put(Engine, K, V),
    do_write_batch(Rest, Engine);
do_write_batch([{delete, K} | Rest], Engine) ->
    ok = clear(Engine, K),
    do_write_batch(Rest, Engine).


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


fold_keys(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Fun, Acc, Opts) ->
    FoldAcc = fold_acc(Fun, Acc, KE, VE, Opts),
    case catch bitcask:fold_keys(Ref, fun fold_fun/2, FoldAcc) of
        #fold_st{useracc=UserAcc} -> UserAcc;
        {stop, #fold_st{useracc=UserAcc}} -> UserAcc;
        {error, Reason} -> {error, Reason};
        {'EXIT', Reason} -> {error, Reason};
        _ -> {error, badarg}
    end.

fold(#engine{ref=Ref, key_enc=KE, val_enc=VE}, Fun, Acc, Opts) ->
    FoldAcc = fold_acc(Fun, Acc, KE, VE, Opts),
    case catch bitcask:fold(Ref, fun fold_fun/3, FoldAcc) of
        #fold_st{useracc=UserAcc} -> UserAcc;
        {stop, #fold_st{useracc=UserAcc}} -> UserAcc;
        {error, Reason} -> {error, Reason};
        {'EXIT', Reason} -> {error, Reason};
        _ -> {error, badarg}
    end.

is_empty(#engine{ref=Ref}) ->
    bitcask:is_empty_estimate(Ref).


fold_acc(Fun, Acc, KE, VE, Opts) ->
    fold_acc(Opts, KE, #fold_st{useracc=Acc, cb=Fun, key_enc=KE, val_enc=VE}).

fold_acc([], _KE, St) ->
    St;
fold_acc([{start_key, Start} | Rest], KE, St) ->
    fold_acc(Rest, KE, St#fold_st{from=start_key(Start, KE),
                                  from_inclusive=true});
fold_acc([{end_key, End} | Rest], KE, St) ->
    fold_acc(Rest, KE, St#fold_st{to=enc(key, End, KE),
                                  to_inclusive=true});
fold_acc([{gt, Start} | Rest], KE, St) ->
    fold_acc(Rest, KE, St#fold_st{from=start_key(Start, KE),
                              from_inclusive=false});
fold_acc([{gte, Start} | Rest], KE, St) ->
    fold_acc(Rest, KE, St#fold_st{from=start_key(Start, KE),
                                  from_inclusive=true});
fold_acc([{lt, End} | Rest], KE, St) ->
    fold_acc(Rest, KE, St#fold_st{to=enc(key, End, KE),
                                  to_inclusive=false});
fold_acc([{lte, End} | Rest], KE, St) ->
    fold_acc(Rest, KE, St#fold_st{to=enc(key, End, KE),
                                  to_inclusive=true});
fold_acc([{max, Max} | Rest], KE, St) ->
    fold_acc(Rest, KE, St#fold_st{max=Max});
fold_acc([_ | Rest], KE, St) ->
    fold_acc(Rest, KE, St).



start_key(first, _KE) ->
    <<>>;
start_key(Key, KE) ->
    enc(key, Key, KE).


fold_fun(#bitcask_entry{key=K}, #fold_st{from=Start,
                                         from_inclusive=Inclusive}=Acc) ->
    Res = case Inclusive of
        false when K > Start -> fold_fun1(K, Acc);
        true when K =:= Start orelse K > Start -> fold_fun1(K, Acc);
        _ -> {ok, Acc}
    end,

    case Res of
        {ok, Acc2} -> Acc2;
        {stop, Acc2} -> throw({stop, Acc2})
    end.

fold_fun(K, V, #fold_st{from=Start, from_inclusive=Inclusive}=Acc) ->
    Res = case Inclusive of
        false when K > Start -> fold_fun1(K, V, Acc);
        true when K =:= Start orelse K > Start -> fold_fun1(K, V, Acc);
        _ -> {ok, Acc}
    end,

    case Res of
        {ok, Acc2} -> Acc2;
        {stop, Acc2} -> throw({stop, Acc2})
    end.

fold_fun1(K, #fold_st{to=nil}=St) ->
    do_fold({ok, K}, St);
fold_fun1(K, #fold_st{to=End, to_inclusive=false}=St) when K < End ->
    do_fold({ok, K}, St);
fold_fun1(K, #fold_st{to=K, to_inclusive=true}=St) ->
    {stop, exec_cb(K, St)};
fold_fun1(K, #fold_st{to=End, to_inclusive=true}=St) when K < End ->
    do_fold({ok, K}, St);
fold_fun1(_K, St) ->
    {stop, St}.

fold_fun1(K, V, #fold_st{to=nil}=St) ->
    do_fold({ok, K, V}, St);
fold_fun1(K, V, #fold_st{to=End, to_inclusive=false}=St) when K < End ->
    do_fold({ok, K, V}, St);
fold_fun1(K, V, #fold_st{to=K, to_inclusive=true}=St) ->
    {stop, exec_cb(K, V, St)};
fold_fun1(K, V, #fold_st{to=End, to_inclusive=true}=St) when K < End ->
    do_fold({ok, K, V}, St);
fold_fun1(_K, _V, St) ->
    {stop, St}.


do_fold({ok, K}, #fold_st{max=0}=St) ->
    {ok, exec_cb(K, St)};
do_fold({ok, K}, St) ->
    NewSt = exec_cb(K, St),
    N = NewSt#fold_st.num + 1,
    if
        N < St#fold_st.max -> {ok, NewSt#fold_st{num=N}};
        true -> {stop, NewSt}
    end;
do_fold({ok, K, V}, #fold_st{max=0}=St) ->
    {ok, exec_cb(K, V, St)};
do_fold({ok, K, V}, St) ->
    NewSt = exec_cb(K, V, St),
    N = NewSt#fold_st.num + 1,
    if
        N < St#fold_st.max -> {ok, NewSt#fold_st{num=N}};
        true -> {stop, NewSt}
    end.

exec_cb(K, #fold_st{useracc=Acc, cb=Cb, key_enc=KE}=St) ->
    St#fold_st{useracc=Cb(dec(key, K, KE), Acc)}.

exec_cb(K, V, #fold_st{useracc=Acc, cb=Cb, key_enc=KE, val_enc=VE}=St) ->
    NewAcc = Cb({dec(key, K, KE), dec(value, V, VE)}, Acc),
    St#fold_st{useracc=NewAcc}.

data_directory_cleanup(DirPath) ->
    case file:list_dir(DirPath) of
        {ok, Files} ->
            [file:delete(filename:join([DirPath, File])) ||
             File <- Files],
            file:del_dir(DirPath);
        _ ->
            ignore
    end.
