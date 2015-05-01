%-*- mode: erlang -*-
%%
%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at http://mozilla.org/MPL/2.0/.

-record(engine, {name,
                 mod,
                 ref,
                 options}).

-record(fold_options, {start_key=first,
                       end_key=nil,
                       gt=nil,
                       gte=nil,
                       lt=nil,
                       lte=nil,
                       max=0}).

-type engine() :: #engine{}.
-export_type([engine/0]).
