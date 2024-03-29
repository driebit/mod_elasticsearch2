%% @author Driebit <tech@driebit.nl>
%% @copyright 2022 Driebit BV
%% @doc Support for Zotonic query resources.
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

-module(elasticsearch2_query_rsc).

-export([
    parse/2,
    split_list/1
]).

% -include_lib("zotonic.hrl").

%% @doc Parse a Zotonic query resource text.
parse(QueryId, Context) ->
    Parts = search_query:parse_query_text(
        z_html:unescape(
            m_rsc:p(QueryId, 'query', Context)
        )
    ),
    SplitParts = [{Key, maybe_split_list(Value)} || {Key, Value} <- Parts],
    coalesce(cat_exclude, coalesce(cat, SplitParts)).

maybe_split_list(Id) when is_integer(Id) ->
    [Id];
maybe_split_list(<<"true">>) ->
    true;
maybe_split_list(<<"[", _/binary>> = Binary) ->
    split_list(Binary);
maybe_split_list(Other) ->
    Other.

% Convert an expression like [123,hasdocument]
-spec split_list(Term) -> List when
    Term :: binary() | string() | term(),
    List :: [ term() ].
split_list(<<"[", _/binary>> = Term) ->
    unquote_all(search_parse_list:parse(Term));
split_list([$[|Rest]) ->
    unquote_all(search_parse_list:parse(z_convert:to_binary(Rest)));
split_list(Other) ->
    [Other].

unquote_all(L) when is_list(L) ->
    lists:map(fun unquote_all/1, L);
unquote_all(B) when is_binary(B) ->
    unquot(z_string:trim(B));
unquote_all(T) ->
    T.

unquot(<<C, Rest/binary>>) when C =:= $'; C =:= $"; C =:= $` ->
    binary:replace(Rest, <<C>>, <<>>);
unquot([C | Rest]) when C =:= $'; C =:= $"; C =:= $` ->
    [X || X <- Rest, X =/= C];
unquot(B) ->
    B.

%% @doc Combine multiple occurrences of a key in the proplist into one.
coalesce(Key, Proplist) ->
    case proplists:get_all_values(Key, Proplist) of
        [] ->
            Proplist;
        Values ->
            [{Key, Values} | proplists:delete(Key, Proplist)]
    end.
