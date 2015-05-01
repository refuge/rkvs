%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
-module(rkvs_tests).

-include_lib("include/rkvs.hrl").
-include("rkvs_tests.hrl").

-define(setup(F), {setup, fun setup/0, fun teardown/1, F}).
-define(foreach(Fs), {foreach, fun setup/1, fun teardown/1, Fs}).

-define(dbg(Backend,E),
	(fun() ->
		 try (E) of
		     __V ->
			 ?debugFmt(<<"backend = ~p; ~s = ~P">>, [Backend,(??E), __V, 15]),
			 __V
		 catch
		     error:__Err ->
			 io:fwrite(user,
				   "FAIL: backend = ~p; test = ~s~n"
				   "Error = ~p~n"
				   "Trace = ~p~n", [Backend,(??E), __Err,
						    erlang:get_stacktrace()]),
			 error(__Err)
		 end
	  end)()).

backends() ->
    [rkvs_ets, rkvs_leveldb, rkvs_rocksdb, rkvs_hanoidb].

basic_test() ->
    lists:foreach(fun(Backend) ->
                File = ?tempdb(),
                {ok, Engine} = rkvs:open(File, [{backend, Backend}]),
                ?assert(is_record(Engine, engine)),

                ?assertMatch(ok, rkvs:close(Engine)),

                %% test db destroy
                ok = rkvs:destroy(Engine)
        end, backends()),
    ok.


ops_test_funs() ->
    [
        fun should_create_kv/2,
        fun should_get_kv/2,
        fun should_contains_kv/2,
        fun should_return_notfound/2,
        fun should_delete_kv/2,
        fun delete_undefined_return_ok/2,
        fun should_replace_kv/2,
        fun should_scan_all/2,
        fun should_scan_from/2,
        fun should_scan_end/2,
        fun should_scan_max/2,
        fun should_scan_max2/2,
        fun should_fold_lt/2,
        fun should_fold_gt/2,
        fun should_fold_lt_gt/2,
        fun should_fold_lt_gt_max/2,
        fun should_clear_range/2,
        fun should_write_batch/2,
        fun should_write_delete_batch/2
    ].

basic_ops_test_() ->
    RunTests = lists:foldr(fun(Backend, Acc) ->
                [{Backend, Fun} || Fun <- ops_test_funs()] ++ Acc
            end, [], backends()),

    {setup,
     fun() -> ok  end,
     fun(_) -> ok end,
     [{foreachx,
       fun(Backend) ->
                        {ok, Engine} = rkvs:open(?tempdb(), [{backend, Backend}]),
                        Engine
                end,
       fun(_Backend, Engine) ->
                        ok = rkvs:close(Engine),
                        ok = rkvs:destroy(Engine)
                end,
       [{Backend, fun(_, Engine) ->
                                [?_test(?dbg(Backend, Fun(Backend, Engine)))]
                        end} || {Backend, Fun} <- RunTests]
      }]}.

should_create_kv(_Backend, Engine) ->
    ?_assertMatch(ok, rkvs:put(Engine, <<"a">>, 1)).

should_get_kv(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ?_assertMatch(1, rkvs:get(Engine, <<"a">>)).

should_contains_kv(_Backend, Engine) ->
    ?assert(rkvs:contains(Engine, <<"a">>) =:= false),
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ?_assert(rkvs:contains(Engine, <<"a">>)).

should_return_notfound(_Backend, Engine) ->
    ?_assertMatch({error, not_found}, rkvs:get(Engine, <<"a">>)).

should_delete_kv(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ?assertMatch(ok, rkvs:clear(Engine, <<"a">>)),
    ?_assertMatch(false, rkvs:contains(Engine, <<"a">>)).

delete_undefined_return_ok(_Backend, Engine) ->
    ?_assertMatch(ok, rkvs:clear(Engine, <<"a">>)).

should_replace_kv(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"a">>, 2),
    ?_assertMatch(2, rkvs:get(Engine, <<"a">>)).


should_scan_all(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),

    ?_assertMatch([{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
                 rkvs:scan(Engine, first, nil, 0)).


should_scan_from(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),

    ?_assertMatch([{<<"b">>, 2}, {<<"c">>, 3}],
                  rkvs:scan(Engine, <<"b">>, nil, 0)).

should_scan_end(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok =  rkvs:put(Engine, <<"d">>, 4),

    ?_assertMatch([{<<"b">>, 2}, {<<"c">>, 3}],
                    rkvs:scan(Engine, <<"b">>, <<"c">>, 0)).


should_scan_max(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok =  rkvs:put(Engine, <<"d">>, 4),

    ?_assertMatch([{<<"a">>, 1}, {<<"b">>, 2}],
                    rkvs:scan(Engine, first, nil, 2)).


should_scan_max2(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok =  rkvs:put(Engine, <<"d">>, 4),

    ?_assertMatch([{<<"b">>, 2}],
                  rkvs:scan(Engine, <<"b">>, <<"c">>, 1)).


should_fold_lt(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok =  rkvs:put(Engine, <<"d">>, 4),

    AccFun = fun({K, V}, Acc) ->
            [{K, V} | Acc]
    end,

    ?_assertMatch([{<<"b">>, 2}, {<<"c">>, 3}, {<<"d">>, 4}],
                  rkvs:fold(Engine, AccFun, [], [{lt, <<"a">>}])).


should_fold_gt(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok =  rkvs:put(Engine, <<"d">>, 4),

    AccFun = fun({K, V}, Acc) ->
            [{K, V} | Acc]
    end,

    ?_assertMatch([{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
                  rkvs:fold(Engine, AccFun, [], [{gt, <<"d">>}])).

should_fold_lt_gt(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok =  rkvs:put(Engine, <<"d">>, 4),

    AccFun = fun({K, V}, Acc) ->
            [{K, V} | Acc]
    end,

    ?_assertMatch([{<<"b">>, 2}, {<<"c">>, 3}],
                  rkvs:fold(Engine, AccFun, [], [{lt, <<"a">>},
                                                 {gt, <<"d">>}])).

should_fold_lt_gt_max(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok =  rkvs:put(Engine, <<"d">>, 4),

    AccFun = fun({K, V}, Acc) ->
            [{K, V} | Acc]
    end,

    ?_assertMatch([{<<"b">>, 2}],
                  rkvs:fold(Engine, AccFun, [], [{lt, <<"a">>},
                                                 {gt, <<"d">>},
                                                 {max, 1}])).

should_clear_range(_Backend, Engine) ->
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),

    ok = rkvs:clear_range(Engine, first, nil, 0),
    ?_assertMatch([], rkvs:scan(Engine, first, nil, 0)).

should_write_batch(_Backend, Engine) ->
    ok =  rkvs:write_batch(Engine, [{put, <<"a">>, 1},
                                    {put, <<"b">>, 2},
                                    {put, <<"c">>, 3}]),

    ?_assertMatch([{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
                  rkvs:scan(Engine, first, nil, 0)).


should_write_delete_batch(_Backend, Engine) ->
    ok =  rkvs:write_batch(Engine, [{put, <<"a">>, 1},
                                    {put, <<"b">>, 2},
                                    {put, <<"c">>, 3}]),

    ok =  rkvs:write_batch(Engine, [{put, <<"d">>, 4},
                                    {delete, <<"b">>},
                                    {put, <<"e">>, 5}]),

    ?_assertMatch([{<<"a">>, 1}, {<<"c">>, 3}, {<<"d">>, 4}, {<<"e">>, 5}],
                  rkvs:scan(Engine, first, nil, 0)).
