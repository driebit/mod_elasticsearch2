mod_elasticsearch2
==================

This [Zotonic](https://github.com/zotonic/zotonic) module gives you more relevant search results
by making resources searchable through  
[Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html).


Configuration
-------------

To configure the Elasticsearch host and port, edit your 
[erlang.config](http://docs.zotonic.com/en/latest/ref/configuration/zotonic-configuration.html)
file:

```erlang
[
    %% ...
    {elasticsearch2_host, [
        {host, <<"elasticsearch">>}, %% Defaults to 127.0.0.1
        {port, 9200}                 %% Defaults to 9200
    ]},
    %% ...
].
```

Or in your site config:

    `mod_elasticsearch2.host`
    `mod_elasticsearch2.port`


Search queries
--------------

When mod_elasticsearch is enabled, it will direct all search operations of the 
‘query’ type to elasticsearch2:

```erlang
z_search:search({query, Args}, Context).
```

For `Args`, you can pass all regular Zotonic [query arguments](http://docs.zotonic.com/en/latest/developer-guide/search.html#query-arguments),
such as:

```erlang
z_search:search({query, [{hasobject, 507}]}, Context).
````

### Query context filters

The `filter` search argument that you know from Zotonic will be used in
Elasticsearch’s [filter context](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-filter-context.html).
To add filters that influence score (ranking), use the `query_context_filter`
instead. The syntax is identical to that of `filter`:

```erlang
z_search:search({query, [{query_context_filter, [["some_field", "value"]]}]}, Context).
```

### Extra query arguments

This module adds some extra query arguments on top of Zotonic’s default ones.

To find documents that have a field, whatever its value (make sure to pass 
`exists` as atom): 

```erlang
{filter, [<<"some_field">>, exists]}
```

To find documents that do not have a field (make sure to pass `missing` as 
atom): 

```erlang
{filter, [<<"some_field">>, missing]}
````

For a [match phrase prefix query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html),
use the `prefix` argument:

```erlang
z_search:search({query, [{prefix, <<"Match this pref">>}]}, Context).
```

To exclude a document:

```erlang
{exclude_document, [Type, Id]}
```

To supply a custom [`function_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html) 
clause, supply one or more `score_function`s. For instance, to rank recent
articles above older ones:

```erlang
z_search:search(
    {query, [
        {text, "Search this"},
        {score_function, #{
            <<"filter">> => [{cat, "article"}],
            <<"exp">> => #{
                <<"publication_start">> => #{
                    <<"scale">> => <<"30d">>
                }
            }
        }}
    ]},
    Context
).
```

Notifications
-------------

### elasticsearch_fields

Observe this foldr notification to change the document fields that are queried.
You can use Elasticsearch [multi_match syntax](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html)
for boosting fields:

```erlang
%% your_site.erl

-export([
    % ...
    observe_elasticsearch_fields/3
]).

observe_elasticsearch_fields({elasticsearch_fields, QueryText}, Fields, Context) ->
    %% QueryText is the search query text

    %% Add or remove fields: 
    [<<"some_field">>, <<"boosted_field^2">>|Fields].   
```

### elasticsearch_put

Observe this notification to change the resource properties before they are
stored in Elasticsearch. For instance, to store their zodiac sign alongside 
person resources:

```erlang
%% your_site.erl

-include_lib("mod_elasticsearch2/include/elasticsearch.hrl").

-export([
    % ...
    observe_elasticsearch_put/3
]).

-spec observe_elasticsearch_put(#elasticsearch_put{}, map(), z:context()) -> map().
observe_elasticsearch_put(#elasticsearch_put{ index = _, type = <<"resource">>, id = Id }, Data, Context) ->
    case m_rsc:is_a(Id, person, Context) of
        true ->
            Data#{ zodiac => calculate_zodiac(Id, Context) };
        false ->
            Data
    end;
observe_elasticsearch_put(#elasticsearch_put{}, Data, Context) ->
    Data.
```

Logging
-------

By default, mod_elasticsearch2 logs outgoing queries at the debug log level. To
see them in your Zotonic console, change the minimum log level to debug:

```erlang
lager:set_loglevel(lager_console_backend, debug).
```

How resources are indexed
-------------------------

Content in all languages is stored in the index, following the 
[one language per field](https://www.elastic.co/guide/en/elasticsearch/guide/current/one-lang-fields.html)
strategy: 

> Each translation is stored in a separate field, which is analyzed according to
> the language it contains. At query time, the user’s language is used to boost
> fields in that particular language.
