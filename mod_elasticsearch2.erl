%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc Zotonic module for using Elasticsearch for full text and other searches.
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

-module(mod_elasticsearch2).
-author("Driebit <tech@driebit.nl>").

-mod_title("Elasticsearch2").
-mod_description("Elasticsearch (v7+) integration for Zotonic").
-mod_prio(500).
-mod_schema(1).
-mod_provides([ elasticsearch ]).

-behaviour(gen_server).

-export([
    start_link/1,

    update_rsc/2,
    delete_rsc/2,

    observe_rsc_update_done/2,
    observe_rsc_pivot_done/2,

    observe_edge_insert/2,
    % observe_edge_update/2,
    observe_edge_delete/2,

    observe_search_query/2,
    observe_elasticsearch_put/3,

    manage_schema/2,

    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,

    prepare_index/1
]).

-include_lib("zotonic.hrl").
-include("include/elasticsearch2.hrl").

-record(state, {context}).

start_link(Args) when is_list(Args) ->
    {context, Context} = proplists:lookup(context, Args),
    gen_server:start_link(?MODULE, z_context:new(Context), []).


%% @doc Update a resource in the elastic search index.
-spec update_rsc( m_rsc:resource_id(), z:context() ) -> ok | {error, term()}.
update_rsc(Id, Context) ->
    case z_module_manager:whereis(?MODULE, Context) of
        {ok, Pid} ->
            gen_server:cast(Pid, {update_rsc, Id});
        {error, _} = Error ->
            Error
    end.


%% @doc Update a resource in the elastic search index.
-spec delete_rsc( m_rsc:resource_id(), z:context() ) -> ok | {error, term()}.
delete_rsc(Id, Context) ->
    case z_module_manager:whereis(?MODULE, Context) of
        {ok, Pid} ->
            gen_server:cast(Pid, {delete_rsc, Id});
        {error, _} = Error ->
            Error
    end.

observe_rsc_update_done(#rsc_update_done{ id = Id, action = delete }, Context) ->
    delete_rsc(Id, Context);
observe_rsc_update_done(#rsc_update_done{ id = Id, action = insert }, Context) ->
    update_rsc(Id, Context);
observe_rsc_update_done(#rsc_update_done{ id = _Id, action = _ }, _Context) ->
    ok.

observe_rsc_pivot_done(#rsc_pivot_done{ id = Id }, Context) ->
    update_rsc(Id, Context).

observe_edge_insert(#edge_insert{ subject_id = SubjectId }, Context) ->
    update_rsc(SubjectId, Context).

% observe_edge_update(#edge_update{ subject_id = SubjectId }, Context) ->
%     update_rsc(SubjectId, Context).

observe_edge_delete(#edge_delete{ subject_id = SubjectId }, Context) ->
    update_rsc(SubjectId, Context).

observe_search_query(#search_query{} = Search, Context) ->
    search(Search, Context).

%% @doc If the resource is a keyword, add its title to the suggest completion field.
-spec observe_elasticsearch_put(#elasticsearch_put{}, map(), z:context()) -> map().
observe_elasticsearch_put(#elasticsearch_put{ id = RscId }, Data, Context) when is_integer(RscId) ->
    case m_rsc:is_a(RscId, keyword, Context) of
        true ->
            case elasticsearch2_mapping:default_translation(m_rsc:p(RscId, title, Context), Context) of
                <<>> ->
                    Data;
                Title ->
                    %% Assume good keywords are linked more often than erratic ones.
                    Weight = length(m_edge:subjects(RscId, Context)),
                    Data#{
                        suggest => #{
                            input => Title,
                            weight => Weight
                        }
                    }
            end;
        false ->
            Data
    end;
observe_elasticsearch_put(_, Data, _) ->
    Data.

manage_schema(_Version, _Context) ->
    #datamodel{
        categories = [
            {elastic_query, query, [
                {title, {trans, [
                    {nl, <<"Elasticsearch zoekopdracht">>},
                    {en, <<"Elasticsearch query">>}
                ]}}
            ]}
        ]
    }.


%%%%%%%%%%%%%%%%%%%%%%%%%% gen_server callbacks %%%%%%%%%%%%%%%%%%%%%%%%%%

init(Context) ->
    z_context:lager_md(Context),
    Index = elasticsearch2:index(Context),
    default_config(index, Index, Context),
    {ok, _} = prepare_index(Context),
    {ok, #state{ context = Context }}.

handle_call(Message, _From, State) ->
    {stop, {unknown_call, Message}, State}.

handle_cast({delete_rsc, Id}, State = #state{context = Context}) ->
    elasticsearch2:delete_doc(Id, Context),
    {noreply, State};
handle_cast({update_rsc, Id}, State = #state{context = Context}) ->
    elasticsearch2:put_doc(Id, Context),
    {noreply, State};
handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%% private functions %%%%%%%%%%%%%%%%%%%%%%%%%%


%% @doc Only handle 'elastic' and 'query' search types
search(#search_query{search = {elastic, _Query}} = Search, Context) ->
    elasticsearch2_search:search(Search, Context);
search(#search_query{search = {elastic_suggest, _Query}} = Search, Context) ->
    elasticsearch2_search:search(Search, Context);
search(#search_query{search = {elastic_didyoumean, _Query}} = Search, Context) ->
    elasticsearch2_search:search(Search, Context);
search(#search_query{search = {query, _Query}} = Search, Context) ->
    Options = #elasticsearch_options{ fallback = true },
    elasticsearch2_search:search(Search, Options, Context);
search(_Search, _Context) ->
    undefined.

default_config(Key, Value, Context) ->
    case m_config:get(?MODULE, Key, Context) of
        undefined ->
            m_config:set_value(?MODULE, Key, Value, Context);
        _ ->
            ok
    end.

-spec prepare_index(z:context()) -> elasticsearch2:result().
prepare_index(Context) ->
    {Hash, Mapping} = elasticsearch2_mapping:default_mapping(resource, Context),
    Index = elasticsearch2:index(Context),
    elasticsearch2_index:upgrade(Index, Mapping, Hash, Context).
