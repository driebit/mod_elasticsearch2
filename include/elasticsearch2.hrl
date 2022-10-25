-record(elasticsearch_put, {
    index :: elasticsearch2:index(),
    type = <<>> :: binary(),            % <<"resource">> for resources
    id :: elasticsearch2:doc_id()       % integer for resources, otherwise binary
}).

-record(elasticsearch_fields, {
    query :: binary() | map()
}).

%% @doc Search options
%%      fallback: whether to fall back to PostgreSQL for non full-text searches.
-record(elasticsearch_options, {
    fallback = true :: boolean()
}).


%% Event sent after a bulk update or delete has been done.
%% The error is set to 'undefined' for successful updates.
-record(elasticsearch_bulk_result, {
    action :: put | delete,
    doc_id :: binary(),
    index :: binary(),
    result :: binary(),
    status :: 100..500,
    error :: map() | undefined
}).
