%% Example of a 2-phase command.
%% Since 'bind' needs results of 'describe', command requeues itself (with a
%% flag 'stage') when 1st phase is done.
%%
%% > Parse
%% < ParseComplete
%% > Describe
%% < ParameterDescription
%% < RowDescription | NoData
%% --- we need results from Describe to proceed, so, here is 'execute2' --
%% > Bind
%% < BindComplete
%% > Execute
%% < DataRow*
%% < CommandComplete
%% > Close
%% < CloseComplete
%% > Sync
%% < ReadyForQuery
-module(epgsql_cmd_equery2).
-behaviour(epgsql_command).
-export([init/1, execute/2, handle_message/4]).
-export_type([response/0]).

-type response() :: {ok, Count :: non_neg_integer(), Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {ok, Count :: non_neg_integer()}
                  | {ok, Cols :: [epgsql:column()], Rows :: [tuple()]}
                  | {error, epgsql:query_error()}.

-include("epgsql.hrl").
-include("protocol.hrl").

-record(equery2,
        {sql :: iodata(),
         params :: [atom()],
         stage = parse :: parse | execute,
         parameter_descr,
         row_descr,
         decoder}).

init({Sql, Params}) ->
    #equery2{sql = Sql, params = Params}.

%% Phase #1
execute(Sock, #equery2{sql = Sql, stage = parse} = State) ->
    epgsql_sock:send_multi(
      Sock,
      [
       {?PARSE, ["", 0, Sql, 0]},
       {?DESCRIBE, [?PREPARED_STATEMENT, "", 0]},
       {?FLUSH, []}
      ]),
    {ok, Sock, State};

%% Phase #2
execute(Sock, #equery2{params = Params, parameter_descr = ParamDescr,
                       row_descr = Columns, stage = execute} = State) ->
    Codec = epgsql_sock:get_codec(Sock),
    TypedParams = lists:zip(ParamDescr, Params),
    ParamsBin = epgsql_wire:encode_parameters(TypedParams, Codec),
    FormatsBin = epgsql_wire:encode_formats(Columns),
    epgsql_sock:send_multi(
      Sock,
      [
       {?BIND, ["", 0, "", 0, ParamsBin, FormatsBin]},
       {?EXECUTE, ["", 0, <<0:?int32>>]},
       {?CLOSE, [?PREPARED_STATEMENT, "", 0]},
       {?SYNC, []}
      ]),
    {ok, Sock, State}.


handle_message(?PARSE_COMPLETE, <<>>, Sock, _State) ->
    {noaction, Sock};
handle_message(?PARAMETER_DESCRIPTION, Bin, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    Types = epgsql_wire:decode_parameters(Bin, Codec),
    Sock2 = epgsql_sock:notify(Sock, {types, Types}),
    {noaction, Sock2, State#equery2{parameter_descr = Types}};
handle_message(?ROW_DESCRIPTION, <<Count:?int16, Bin/binary>>, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    Columns = epgsql_wire:decode_columns(Count, Bin, Codec),
    Columns2 = [Col#column{format = epgsql_wire:format(Col#column.type, Codec)}
                || Col <- Columns],
    epgsql_sock:notify(Sock, {columns, Columns2}),
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    {requeue, Sock, State#equery2{row_descr = Columns, decoder = Decoder,
                                  stage = execute}};
handle_message(?NO_DATA, <<>>, Sock, State) ->
    Codec = epgsql_sock:get_codec(Sock),
    epgsql_sock:notify(Sock, no_data),
    Columns = [],
    Decoder = epgsql_wire:build_decoder(Columns, Codec),
    {requeue, Sock, State#equery2{row_descr = Columns, decoder = Decoder,
                                  stage = execute}};
handle_message(?BIND_COMPLETE, <<>>, Sock, _State) ->
    {noaction, Sock};
handle_message(?DATA_ROW, <<_Count:?int16, Bin/binary>>,
               Sock, #equery2{decoder = Decoder} = St) ->
    Row = epgsql_wire:decode_data(Bin, Decoder),
    {add_row, Row, Sock, St};
handle_message(?EMPTY_QUERY, _, Sock, State) ->
    {add_result, {ok, [], []}, {complete, empty}, Sock, State};
handle_message(?COMMAND_COMPLETE, Bin, Sock, #equery2{row_descr = Cols} = State) ->
    Complete = epgsql_wire:decode_complete(Bin),
    Rows = epgsql_sock:get_rows(Sock),
    Result = case Complete of
                 {_, Count} when Cols == [] ->
                     {ok, Count};
                 {_, Count} ->
                     {ok, Count, Cols, Rows};
                 _ ->
                     {ok, Cols, Rows}
             end,
    {add_result, Result, {complete, Complete}, Sock, State};
handle_message(?CLOSE_COMPLETE, _, Sock, _State) ->
    {noaction, Sock};
handle_message(?READY_FOR_QUERY, _Status, Sock, _State) ->
    case epgsql_sock:get_results(Sock) of
        [Result] ->
            {finish, Result, done, Sock};
        [] ->
            {finish, done, done, Sock}
    end;
handle_message(?ERROR, Error, Sock, _State) ->
    %% FIXME: if we got error on 'parse' phase, we should return and sync
    Result = {error, Error},
    {add_result, Result, Result, Sock};
handle_message(_, _, _, _) ->
    unknown.
