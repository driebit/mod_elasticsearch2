%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc Zotonic module for using Elasticsearch for full text and other searches.
%% @end

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
    typed_id/2,
    typed_id_split/1,

    start_link/1,

    status/1,
    flush/1,

    put_doc/3,
    put_doc/4,
    delete_doc/2,
    delete_doc/3,

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

    prepare_index/1,
    delete_recreate_index/1
]).

-include_lib("zotonic.hrl").
-include("include/elasticsearch2.hrl").

% Period during which update commands are buffered.
-define(BULK_DELAY, 1000).

% Max number of batched commands before flush is forced.
-define(BULK_MAX_BATCH, 500).


-record(state, {
        queue :: queue:queue( elasticsearch2:bulkcmd() ),
        timer = undefined :: timer:tref() | undefined,
        bulk_pid = undefined :: undefined | pid(),
        context :: z:context(),
        total_received = 0 :: non_neg_integer(),
        total_error = 0 :: non_neg_integer(),
        total_ok = 0 :: non_neg_integer()
    }).

%% @doc Combine an id and a type into an unique typed id.
-spec typed_id(elasticsearch2:doc_id(), binary()|string()|undefined) -> binary().
typed_id(DocId, Type) ->
    DocId1 = z_convert:to_binary(DocId),
    case z_convert:to_binary(Type) of
        <<>> ->
            DocId1;
        <<"resource">> ->
            DocId1;
        Type1 ->
            <<DocId1/binary, "::", Type1/binary>>
    end.

%% @doc Split a typed id in into the type and the document id.
-spec typed_id_split(binary()) -> {DocId::binary(), Type::binary()}.
typed_id_split(DocId) ->
    case binary:split(DocId, <<"::">>) of
        [ Id, Type ] ->
            {Id, Type};
        [ Id ] ->
            {Id, <<>>}
    end.

start_link(Args) when is_list(Args) ->
    {context, Context} = proplists:lookup(context, Args),
    gen_server:start_link(?MODULE, z_context:new(Context), []).

%% @doc Return information about the queued updates.
-spec status(Context) -> {ok, map()} | {error, term()} when
    Context :: z:context().
status(Context) ->
    case z_module_manager:whereis(?MODULE, Context) of
        {ok, Pid} ->
            gen_server:call(Pid, status);
        {error, _} = Error ->
            Error
    end.

%% @doc Force a flush of all documents (if no flush is running)
-spec flush(Context) -> ok | {error, term()} when
    Context :: z:context().
flush(Context) ->
    case z_module_manager:whereis(?MODULE, Context) of
        {ok, Pid} ->
            Pid ! bulk_flush,
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Put a document to the default index. The put is batched.
-spec put_doc(DocId, Doc, Context) -> ok | {error, term()} when
    DocId :: elasticsearch2:doc_id(),
    Doc :: map() | binary(),
    Context :: z:context().
put_doc(DocId, Doc, Context) ->
    put_doc(elasticsearch2:index(Context), DocId, Doc, Context).

%% @doc Put a document to the given index. The put is batched.
-spec put_doc(Index, DocId, Doc, Context) -> ok | {error, term()} when
    Index :: elasticsearch2:index(),
    DocId :: elasticsearch2:doc_id(),
    Doc :: map() | binary(),
    Context :: z:context().
put_doc(Index, DocId, Doc, Context) ->
    case z_module_manager:whereis(?MODULE, Context) of
        {ok, Pid} ->
            gen_server:cast(Pid, {put_doc, Index, DocId, Doc});
        {error, _} = Error ->
            Error
    end.

%% @doc Delete a document from the default index. The put is batched.
-spec delete_doc(DocId, Context) -> ok | {error, term()} when
    DocId :: elasticsearch2:doc_id(),
    Context :: z:context().
delete_doc(DocId, Context) ->
    delete_doc(elasticsearch2:index(Context), DocId, Context).

%% @doc Delete a document from the given index. The put is batched.
-spec delete_doc(Index, DocId, Context) -> ok | {error, term()} when
    Index :: elasticsearch2:index(),
    DocId :: elasticsearch2:doc_id(),
    Context :: z:context().
delete_doc(Index, DocId, Context) ->
    case z_module_manager:whereis(?MODULE, Context) of
        {ok, Pid} ->
            gen_server:cast(Pid, {delete_doc, Index, DocId});
        {error, _} = Error ->
            Error
    end.

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
    gen_server:cast(self(), prepare_index),
    {ok, #state{
        queue = queue:new(),
        context = Context
    }}.

handle_call(status, _From, State) ->
    Status = #{
        queue_count => queue:len(State#state.queue),
        timer => State#state.timer,
        bulk_pid => State#state.bulk_pid,
        total_received => State#state.total_received,
        total_ok => State#state.total_ok,
        total_error => State#state.total_error
    },
    {reply, {ok, Status}, State};
handle_call(Message, _From, State) ->
    {stop, {unknown_call, Message}, State}.

handle_cast(prepare_index, State = #state{context = Context}) ->
    {ok, _} = prepare_index(Context),
    {noreply, State};

handle_cast({delete_rsc, RscId}, State = #state{ context = Context, queue = Queue, total_received = Total }) ->
    {ok, DeleteCmd} = elasticsearch2:delete_bulkcmd(RscId, Context),
    Q1 = queue:in(DeleteCmd, Queue),
    {noreply, schedule_flush(State#state{ queue = Q1, total_received = Total + 1 })};
handle_cast({update_rsc, RscId}, State = #state{ context = Context, queue = Queue, total_received = Total }) ->
    {Q1, Total1} = case elasticsearch2:put_bulkcmd(RscId, Context) of
        {ok, PutCmd} ->
            {queue:in(PutCmd, Queue), Total + 1};
        {error, _} ->
            {Queue, Total}
    end,
    {noreply, schedule_flush(State#state{ queue = Q1, total_received = Total1 })};

handle_cast({delete_doc, Index, DocId}, State = #state{ context = Context, queue = Queue, total_received = Total }) ->
    {ok, DeleteCmd} = elasticsearch2:delete_bulkcmd(Index, DocId, Context),
    Q1 = queue:in(DeleteCmd, Queue),
    {noreply, schedule_flush(State#state{ queue = Q1, total_received = Total + 1 })};

handle_cast({put_doc, Index, DocId, Doc}, State = #state{ context = Context, queue = Queue, total_received = Total }) ->
    {ok, PutCmd} = elasticsearch2:put_bulkcmd(Index, DocId, Doc, Context),
    Q1 = queue:in(PutCmd, Queue),
    {noreply, schedule_flush(State#state{ queue = Q1, total_received = Total + 1 })};

handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info(bulk_flush, #state{ context = Context, timer = Timer, bulk_pid = undefined, queue = Queue } = State) ->
    State1 = case queue:is_empty(Queue) of
        false ->
            timer:cancel(Timer),
            {Batch, Remaining} = take(?BULK_MAX_BATCH, Queue, []),
            Connection = elasticsearch2:connection(Context),
            lager:info("mod_elasticsearch2: processing batch of ~p items (~p more queued)",
                       [ length(Batch), queue:len(Remaining) ]),
            Self = self(),
            Pid = erlang:spawn(
                fun() ->
                    Result = elasticsearch2_fetch:bulk(Connection, Batch),
                    Self ! {bulk_result, Result}
                end),
            erlang:monitor(process, Pid),
            erlang:garbage_collect(),
            State#state{ bulk_pid = Pid, queue = Remaining, timer = undefined };
        true when Timer =/= undefined ->
            timer:cancel(Timer),
            State#state{ timer = undefined };
        true ->
            State
    end,
    {noreply, State1};
handle_info({'DOWN', _MRef, process, Pid, normal}, #state{ bulk_pid = Pid } = State) ->
    State1 = State#state{ bulk_pid = undefined },
    {noreply, schedule_flush(State1)};
handle_info({'DOWN', _MRef, process, Pid, Reason}, #state{ bulk_pid = Pid } = State) ->
    % TODO: add backoff and retry
    lager:error("mod_elasticsearch2: bulk process is down with ~p", [ Reason ]),
    State1 = State#state{ bulk_pid = undefined },
    {noreply, schedule_flush(State1)};
handle_info({bulk_result, {error, Reason}}, State) ->
    % TODO: add backoff and retry
    lager:error("mod_elasticsearch2: error return from bulk insert: ~p", [ Reason ]),
    {noreply, State};
handle_info({bulk_result, {ok, Result}}, #state{ context = Context, total_ok = TotalOk, total_error = TotalErr } = State) ->
    {Ok, Err} = handle_bulk_result(Result, Context),
    {noreply, State#state{ total_ok = TotalOk + Ok, total_error = TotalErr + Err}};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%% private functions %%%%%%%%%%%%%%%%%%%%%%%%%%

take(0, Q, Acc) ->
    {lists:reverse(Acc), Q};
take(N, Q, Acc) ->
    case queue:out(Q) of
        {empty, Q1} ->
            {lists:reverse(Acc), Q1};
        {{value, H}, Q1} ->
            take(N-1, Q1, [H|Acc])
    end.

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

%% @doc Delete and recreate an empty index.
-spec delete_recreate_index(z:context()) -> elasticsearch2:result().
delete_recreate_index(Context) ->
    {Hash, Mapping} = elasticsearch2_mapping:default_mapping(resource, Context),
    Index = elasticsearch2:index(Context),
    elasticsearch2_index:delete_recreate(Index, Mapping, Hash, Context).

schedule_flush(#state{ timer = Timer, bulk_pid = BulkPid, queue = Queue} = State) ->
    case queue:is_empty(Queue) of
        false ->
            case queue:len(Queue) >= ?BULK_MAX_BATCH of
                true when BulkPid =:= undefined ->
                    self() ! bulk_flush,
                    State;
                true ->
                    State;
                false when Timer =:= undefined ->
                    {ok, TRef} = timer:send_after(?BULK_DELAY, bulk_flush),
                    State#state{ timer = TRef };
                false ->
                    State
            end;
        true when Timer =/= undefined ->
            timer:cancel(Timer),
            State#state{ timer = undefined };
        true ->
            State
    end.

handle_bulk_result(#{ <<"items">> := Items, <<"errors">> := IsErrors }, Context) ->
    lists:foreach(
        fun(Doc) ->
            handle_single_result(Doc, Context)
        end,
        Items),
    Count = length(Items),
    case IsErrors of
        true ->
            ErrorItems = lists:filter(
                fun
                    (#{ <<"index">> := #{ <<"status">> := 200 } }) -> false;
                    (#{ <<"index">> := #{ <<"status">> := 201 } }) -> false;
                    (_) -> true
                end,
                Items),
            % io:format("~p", [ ErrorItems ]),
            lager:error("mod_elasticsearch2: bulk documents could not be indexed: ~p", [ErrorItems]),
            ErrCount = length(ErrorItems),
            {Count - ErrCount, ErrCount};
        false ->
            lager:debug("mod_elasticsearch2: bulk documents ok (~p items)", [ Count ]),
            {Count, 0}
    end.

handle_single_result(#{ <<"delete">> := #{
        <<"_id">> := DocId,
        <<"_index">> := Index,
        <<"status">> := Status
    } = Res }, Context) ->
    z_notifier:notify(#elasticsearch_bulk_result{
            action = delete,
            doc_id = DocId,
            index = elasticsearch2_index:drop_index_hash(Index),
            result = maps:get(<<"result">>, Res, undefined),
            status = Status,
            error = maps:get(<<"error">>, Res, undefined)
        }, Context);
handle_single_result(#{ <<"index">> := #{
        <<"_id">> := DocId,
        <<"_index">> := Index,
        <<"status">> := Status
    } = Res }, Context) ->
    z_notifier:notify(#elasticsearch_bulk_result{
            action = put,
            doc_id = DocId,
            index = elasticsearch2_index:drop_index_hash(Index),
            result = maps:get(<<"result">>, Res, undefined),
            status = Status,
            error = maps:get(<<"error">>, Res, undefined)
        }, Context).
