mod_elasticsearch2
==================

This [Zotonic](https://github.com/zotonic/zotonic) module gives you more relevant search results
by making resources searchable through  
[Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html).


Configuration
-------------

To configure the Elasticsearch host and port, edit your 
[zotonic.config](http://docs.zotonic.com/en/latest/ref/configuration/zotonic-configuration.html)
file:

```erlang
[
    %% ...
    {elasticsearch2_host, <<"elasticsearch">>},  %% Defaults to 127.0.0.1
    {elasticsearch2_port, 9200},                 %% Defaults to 9200
    %% ...
].
```

Or in your site config:

 * `mod_elasticsearch2.host`
 * `mod_elasticsearch2.port`

Elastic search normally doesn't count hits beyond 10K. To enable or disable counting the real
total number of hits set the config key:

 * `mod_elasticsearch2.track_total_hits`

This config key defaults to `true`.


### Elasticsearch security config

Out of the box Elastic is configured to use TLS and passwords.
Both are not (yet) supported by this library, and also don't have any purpose on localhost.

The following change must be made to `config/elasticsearch.yml`:

```yaml
# Disable security features
xpack.security.enabled: false
xpack.security.enrollment.enabled: false

# Disable encryption for HTTP API client connections, such as Kibana, Logstash, and Agents
xpack.security.http.ssl:
  enabled: false
  keystore.path: certs/http.p12

```


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

### Wildcards

The `text` query term is modified to search for prefix strings by appending `*` operators to the words in the query string.

This is _not_ done if either:

 * the search text contains simple query string operators, especially the `"`; or
 * the config `mod_elasticsearch2.no_automatic_wildcard` is set to a true-ish value.

See https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html for the query string operators.


Buffered put/delete
-------------------

Use the `mod_elasticsearch2:put_doc` and `mod_elasticsearch2:delete_doc` routines to perform bulk put and delete requests.

The requests are buffered for one second, or 500 commands, whatever comes first.

After a command has been executed the following notification is emitted:

```erlang
-record(elasticsearch_bulk_result, {
    action :: put | delete,
    doc_id :: binary(),
    index :: binary(),
    result :: binary(),
    status :: 100..500,
    error :: map() | undefined
}).
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

Type
----

In Elastic 5.x a document was associated with a _type_.

This _type_ has been removed in Elastic 7+.

In this library we still use the _type_, it is stored as `es_type` and it set to `resource` for
all Zotonic resources.

On fetch of a document record the `_source.es_type` is copied to `_type`. This for compatibility with software
written for Elastic 5.x.

Likewise references to `_type` in queries are mapped to `es_type`.

As the document types are often used to distinguish ids between sources (for example Adlib databases) there are routines to combine the type and id:

```erlang
DocId = mod_elasticsearch2:typed_id(Id, Type),
{Id, Type} = mod_elasticsearch2:type_id_split(DocId)
```

The empty type and the `resource` type are not appended to the document id.


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

Development and low disk space
------------------------------

If you happen to be low on disk space then Elastic will become read only.

To disable this, especially on development machines, perform the following two commands:

```shell
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
```

