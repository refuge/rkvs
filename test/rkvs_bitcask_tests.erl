%%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%%
-module(rkvs_bitcask_tests).

-include_lib("include/rkvs.hrl").
-include("rkvs_tests.hrl").


basic_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ?assertMatch(ok, rkvs:put(Engine, <<"a">>, 1)),
    ?assertMatch(1, rkvs:get(Engine, <<"a">>)),
    rkvs:destroy(Engine).

contains_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ?assert(rkvs:contains(Engine, <<"a">>) =:= false),
    ?assertMatch({error, not_found}, rkvs:get(Engine, <<"a">>)),
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ?assert(rkvs:contains(Engine, <<"a">>)),
    rkvs:destroy(Engine).

delete_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ?assertMatch(ok, rkvs:clear(Engine, <<"a">>)),
    ?assertMatch(false, rkvs:contains(Engine, <<"a">>)),
    ?assertMatch(ok, rkvs:clear(Engine, <<"a">>)),
    rkvs:destroy(Engine).

update_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"a">>, 2),
    ?assertMatch(2, rkvs:get(Engine, <<"a">>)),
    rkvs:destroy(Engine).

fold_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),

    %% test range scanning
    ?assertMatch([{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
                 rkvs:scan(Engine, first, nil, 0)),

    ?assertMatch([{<<"b">>, 2}, {<<"c">>, 3}],
                 rkvs:scan(Engine, <<"b">>, nil, 0)),


    %% test max
    ok =  rkvs:put(Engine, <<"d">>, 4),

    ?assertMatch([{<<"b">>, 2}, {<<"c">>, 3}],
                 rkvs:scan(Engine, <<"b">>, <<"c">>, 0)),


    ?assertMatch([{<<"a">>, 1}, {<<"b">>, 2}],
                 rkvs:scan(Engine, first, nil, 2)),


    ?assertMatch([{<<"b">>, 2}],
                 rkvs:scan(Engine, <<"b">>, <<"c">>, 1)),


    %% test gt, lt, ge, lte
    AccFun = fun({K, V}, Acc) ->
                     [{K, V} | Acc]
             end,

    ?assertMatch([{<<"b">>, 2}, {<<"c">>, 3}, {<<"d">>, 4}],
                 lists:reverse(rkvs:fold(Engine, AccFun, [], [{gt, <<"a">>}]))),


    ?assertMatch([{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
                 lists:reverse(rkvs:fold(Engine, AccFun, [], [{lt, <<"d">>}]))),

    ?assertMatch([{<<"b">>, 2}, {<<"c">>, 3}],
                 lists:reverse(rkvs:fold(Engine, AccFun, [], [{gt, <<"a">>},
                                                              {lt, <<"d">>}]))),

    ?assertMatch([{<<"b">>, 2}],
                 rkvs:fold(Engine, AccFun, [], [{gt, <<"a">>},
                                                {lt, <<"d">>},
                                                {max, 1}])),

    rkvs:destroy(Engine).

clear_range_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ok =  rkvs:put(Engine, <<"a">>, 1),
    ok =  rkvs:put(Engine, <<"b">>, 2),
    ok =  rkvs:put(Engine, <<"c">>, 3),
    ok = rkvs:clear_range(Engine, first, nil, 0),
    ?assertMatch([], rkvs:scan(Engine, first, nil, 0)),
    rkvs:destroy(Engine).

write_batch_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ok =  rkvs:write_batch(Engine, [{put, <<"a">>, 1},
                                    {put, <<"b">>, 2},
                                    {put, <<"c">>, 3}]),
    ?assertMatch([{<<"a">>, 1}, {<<"b">>, 2}, {<<"c">>, 3}],
                  rkvs:scan(Engine, first, nil, 0)),
    rkvs:destroy(Engine).

write_delete_batch_test() ->
    {ok, Engine} = rkvs:open(?tempdb(), [{backend, rkvs_bitcask}]),
    ok =  rkvs:write_batch(Engine, [{put, <<"a">>, 1},
                                    {put, <<"b">>, 2},
                                    {put, <<"c">>, 3}]),

    ok =  rkvs:write_batch(Engine, [{put, <<"d">>, 4},
                                    {delete, <<"b">>},
                                    {put, <<"e">>, 5}]),

    ?assertMatch([{<<"a">>, 1}, {<<"c">>, 3}, {<<"d">>, 4}, {<<"e">>, 5}],
                  rkvs:scan(Engine, first, nil, 0)),
    rkvs:destroy(Engine).
