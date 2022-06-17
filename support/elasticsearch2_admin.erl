%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc Support function for the admin search query panel.
%% @enddoc

%% Copyright 2022 Driebit BV
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(elasticsearch2_admin).
-author("Driebit <tech@driebit.nl>").

%% API
-export([
    event/2
]).

-include("zotonic.hrl").

%% Previewing the results of a elastic query in the admin edit
event(#postback{message={elastic_query_preview, Opts}}, Context) ->
    DivId = proplists:get_value(div_id, Opts),
    Index = proplists:get_value(index, Opts),
    Id = proplists:get_value(rsc_id, Opts),
    QueryType = list_to_atom(proplists:get_value(query_type, Opts)),

    Q = z_convert:to_binary(z_context:get_q("triggervalue", Context)),

    case jsx:is_json(Q) of
        false ->
            z_render:growl_error("There is an error in your query", Context);
        true ->
            S = z_search:search({QueryType, [{elastic_query, Q}, {index, Index}]}, Context),
            {Html, Context1} = z_template:render_to_iolist({cat, "_admin_query_preview.tpl"}, [{result,S}, {id, Id}], Context),
            z_render:update(DivId, Html, Context1)
    end.
