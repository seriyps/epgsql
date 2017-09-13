%%% Copyright (C) 2009 - Will Glozer.  All rights reserved.
%%% Copyright (C) 2011 - Anton Lebedevich.  All rights reserved.

%%% @doc GenServer holding all connection state (including socket).
%%%
%%% See https://www.postgresql.org/docs/current/static/protocol-flow.html
%%% Commands in PostgreSQL are pipelined: you don't need to wait for reply to
%%% be able to send next command.
%%% Commands are processed (and responses to them are generated) in FIFO order.
%%% eg, if you execute 2 SimpleQuery: #1 and #2, first you get all response
%%% packets for #1 and then all for #2:
%%% > SQuery #1
%%% > SQuery #2
%%% < RowDescription #1
%%% < DataRow #1
%%% < CommandComplete #1
%%% < RowDescription #2
%%% < DataRow #2
%%% < CommandComplete #2
%%%
%%% See epgsql_cmd_connect for network connection and authentication setup


-module(epgsql_sock).

-behavior(gen_server).

-export([start_link/0,
         close/1,
         sync_command/3,
         async_command/4,
         get_parameter/2,
         set_notice_receiver/2,
         get_cmd_status/1,
         cancel/1]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

%% loop callback
-export([on_message/3, on_replication/3]).

%% Comand's APIs
-export([set_net_socket/3, init_replication_state/1, set_attr/3, get_codec/1,
         get_rows/1, get_results/1, notify/2, send/2, send/3, send_multi/2,
         get_parameter_internal/2,
         get_replication_state/1, set_packet_handler/2]).

-export_type([transport/0, pg_sock/0]).

-include("epgsql.hrl").
-include("protocol.hrl").
-include("epgsql_replication.hrl").

-type transport() :: {call, any()}
                   | {cast, pid(), reference()}
                   | {incremental, pid(), reference()}.

-record(state, {mod :: gen_tcp | ssl | undefined,
                sock :: gen_tcp:socket() | ssl:sslsocket() | undefined,
                data = <<>>,
                backend :: {Pid :: integer(), Key :: integer()} | undefined,
                handler = on_message :: on_message | on_replication | undefined,
                codec :: epgsql_binary:codec() | undefined,
                queue = queue:new() :: queue:queue({epgsql_command:command(), any(), transport()}),
                current_cmd :: epgsql_command:command() | undefined,
                current_cmd_state :: any() | undefined,
                current_cmd_transport :: transport() | undefined,
                async :: undefined | atom() | pid(),
                parameters = [] :: [{Key :: binary(), Value :: binary()}],
                rows = [] :: [tuple()],
                results = [],
                sync_required :: boolean() | undefined,
                txstatus :: byte() | undefined,  % $I | $T | $E,
                complete_status :: atom() | {atom(), integer()} | undefined,
                repl :: #repl{} | undefined}).

-opaque pg_sock() :: #state{}.


%% -- client interface --

start_link() ->
    gen_server:start_link(?MODULE, [], []).

close(C) when is_pid(C) ->
    catch gen_server:cast(C, stop),
    ok.

-spec sync_command(epgsql:conection(), epgsql_command:command(), any()) -> any().
sync_command(C, Command, Args) ->
    gen_server:call(C, {command, Command, Args}, 1000).

-spec async_command(epgsql:conection(), cast | incremental,
                    epgsql_command:command(), any()) -> reference().
async_command(C, Transport, Command, Args) ->
    Ref = make_ref(),
    Pid = self(),
    ok = gen_server:cast(C, {{Transport, Pid, Ref}, Command, Args}),
    Ref.

get_parameter(C, Name) ->
    gen_server:call(C, {get_parameter, to_binary(Name)}, infinity).

set_notice_receiver(C, PidOrName) when is_pid(PidOrName);
                                       is_atom(PidOrName) ->
    gen_server:call(C, {set_async_receiver, PidOrName}, infinity).

get_cmd_status(C) ->
    gen_server:call(C, get_cmd_status, infinity).

cancel(S) ->
    gen_server:cast(S, cancel).


%% -- command APIs --

%% send()
%% send_many()

set_net_socket(Mod, Socket, State) ->
    State1 = State#state{mod = Mod, sock = Socket},
    setopts(State1, [{active, true}]),
    State1.

init_replication_state(State) ->
    State#state{repl = #repl{}}.

set_attr(backend, {_Pid, _Key} = Backend, State) ->
    State#state{backend = Backend};
set_attr(async, Async, State) ->
    State#state{async = Async};
set_attr(txstatus, Status, State) ->
    State#state{txstatus = Status};
set_attr(codec, Codec, State) ->
    State#state{codec = Codec};
set_attr(sync_required, Value, State) ->
    State#state{sync_required = Value};
set_attr(replication_state, Value, State) ->
    State#state{repl = Value}.

%% XXX: be careful!
set_packet_handler(Handler, State) ->
    State#state{handler = Handler}.

get_codec(#state{codec = Codec}) ->
    Codec.

get_replication_state(#state{repl = Repl}) ->
    Repl.

get_rows(#state{rows = Rows}) ->
    lists:reverse(Rows).

get_results(#state{results = Results}) ->
    lists:reverse(Results).

get_parameter_internal(Name, #state{parameters = Parameters}) ->
    case lists:keysearch(Name, 1, Parameters) of
        {value, {Name, Value}} -> Value;
        false                  -> undefined
    end.


%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

handle_call({update_type_cache, TypeInfos}, _From, #state{codec = Codec} = State) ->
    Codec2 = epgsql_binary:update_type_cache(TypeInfos, Codec),
    {reply, ok, State#state{codec = Codec2}};

handle_call({get_parameter, Name}, _From, State) ->
    {reply, {ok, get_parameter_internal(Name, State)}, State};

handle_call({set_async_receiver, PidOrName}, _From, #state{async = Previous} = State) ->
    {reply, {ok, Previous}, State#state{async = PidOrName}};

handle_call(get_cmd_status, _From, #state{complete_status = Status} = State) ->
    {reply, {ok, Status}, State};

handle_call({standby_status_update, FlushedLSN, AppliedLSN}, _From,
            #state{handler = on_replication,
                   repl = #repl{last_received_lsn = ReceivedLSN} = Repl} = State) ->
    send(State, ?COPY_DATA, epgsql_wire:encode_standby_status_update(ReceivedLSN, FlushedLSN, AppliedLSN)),
    Repl1 = Repl#repl{last_flushed_lsn = FlushedLSN,
                      last_applied_lsn = AppliedLSN},
    {reply, ok, State#state{repl = Repl1}};
handle_call({command, Command, Args}, From, State) ->
    Transport = {call, From},
    {noreply, command_new(Transport, Command, Args, State)}.

handle_cast({{Method, From, Ref} = Transport, Command, Args}, State)
  when ((Method == cast) or (Method == incremental)),
       is_pid(From),
       is_reference(Ref)  ->
    {noreply, command_new(Transport, Command, Args, State)};

handle_cast(stop, State) ->
    {stop, normal, flush_queue(State, {error, closed})};

handle_cast(cancel, State = #state{backend = {Pid, Key},
                                   sock = TimedOutSock}) ->
    {ok, {Addr, Port}} = case State#state.mod of
                             gen_tcp -> inet:peername(TimedOutSock);
                             ssl -> ssl:peername(TimedOutSock)
                         end,
    SockOpts = [{active, false}, {packet, raw}, binary],
    %% TODO timeout
    {ok, Sock} = gen_tcp:connect(Addr, Port, SockOpts),
    Msg = <<16:?int32, 80877102:?int32, Pid:?int32, Key:?int32>>,
    ok = gen_tcp:send(Sock, Msg),
    gen_tcp:close(Sock),
    {noreply, State}.

handle_info({Closed, Sock}, #state{sock = Sock} = State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    {stop, sock_closed, flush_queue(State#state{sock = undefined}, {error, sock_closed})};

handle_info({Error, Sock, Reason}, #state{sock = Sock} = State)
  when Error == tcp_error; Error == ssl_error ->
    Why = {sock_error, Reason},
    {stop, Why, flush_queue(State, {error, Why})};

handle_info({inet_reply, _, ok}, State) ->
    {noreply, State};

handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, flush_queue(State, {error, Status})};

handle_info({_, Sock, Data2}, #state{data = Data, sock = Sock} = State) ->
    loop(State#state{data = <<Data/binary, Data2/binary>>}).

terminate(_Reason, #state{sock = undefined}) -> ok;
terminate(_Reason, #state{mod = gen_tcp, sock = Sock}) -> gen_tcp:close(Sock);
terminate(_Reason, #state{mod = ssl, sock = Sock}) -> ssl:close(Sock).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- internal functions --

command_new(Transport, Command, Args, State) ->
    CmdState = Command:init(Args),
    command_exec(Transport, Command, CmdState, State).

command_exec(Transport, Command, _, State = #state{sync_required = true})
  when Command /= epgsql_cmd_sync ->
    finish(State#state{current_cmd = Command,
                       current_cmd_transport = Transport},
           {error, sync_required});
command_exec(Transport, Command, CmdState, State) ->
    {ok, State1, CmdState1} = Command:execute(State, CmdState),
    command_enqueue(Transport, Command, CmdState1, State1).

command_enqueue(Transport, Command, CmdState, #state{current_cmd = undefined} = State) ->
    State#state{current_cmd = Command,
                current_cmd_state = CmdState,
                current_cmd_transport = Transport,
                complete_status = undefined};
command_enqueue(Transport, Command, CmdState, #state{queue = Q} = State) ->
    State#state{queue = queue:in({Command, CmdState, Transport}, Q),
                complete_status = undefined}.

command_handle_message(Msg, Payload,
                       #state{current_cmd = Command,
                              current_cmd_state = CmdState} = State) ->
    case Command:handle_message(Msg, Payload, State, CmdState) of
        {add_row, Row, State1, CmdState1} ->
            {noreply, add_row(State1#state{current_cmd_state = CmdState1}, Row)};
        {add_result, Result, Notice, State1, CmdState1} ->
            {noreply,
             add_result(State1#state{current_cmd_state = CmdState1},
                        Notice, Result)};
        {finish, Result, Notice, State1} ->
            {noreply, finish(State1, Notice, Result)};
        {noaction, State1} ->
            {noreply, State1};
        {noaction, State1, CmdState1} ->
            {noreply, State1#state{current_cmd_state = CmdState1}};
        {requeue, State1, CmdState1} ->
            Transport = State1#state.current_cmd_transport,
            {noreply, command_exec(Transport, Command, CmdState1,
                                   State1#state{current_cmd = undefined})};
        {stop, Reason, Response, State1} ->
            {stop, Reason, finish(State1, Response)};
        {sync_required, Why} ->
            %% Protocol error. Finish and flush all pending commands.
            {noreply, sync_required(finish(State#state{sync_required = true}, Why))};
        unknown ->
            {stop, {error, {unexpected_message, Msg, Command, CmdState}}, State}
    end.

command_next(#state{current_cmd = PrevCmd,
                    queue = Q} = State) when PrevCmd =/= undefined ->
    case queue:out(Q) of
        {empty, _} ->
            State#state{current_cmd = undefined,
                        current_cmd_state = undefined,
                        current_cmd_transport = undefined,
                        rows = [],
                        results = []};
        {{value, {Command, CmdState, Transport}}, Q1} ->
            State#state{current_cmd = Command,
                        current_cmd_state = CmdState,
                        current_cmd_transport = Transport,
                        queue = Q1,
                        rows = [],
                        results = []}
    end.


setopts(#state{mod = Mod, sock = Sock}, Opts) ->
    case Mod of
        gen_tcp -> inet:setopts(Sock, Opts);
        ssl     -> ssl:setopts(Sock, Opts)
    end.

send(#state{mod = Mod, sock = Sock}, Data) ->
    do_send(Mod, Sock, epgsql_wire:encode(Data)).

send(#state{mod = Mod, sock = Sock}, Type, Data) ->
    do_send(Mod, Sock, epgsql_wire:encode(Type, Data)).

send_multi(#state{mod = Mod, sock = Sock}, List) ->
    do_send(Mod, Sock, lists:map(fun({Type, Data}) ->
        epgsql_wire:encode(Type, Data)
    end, List)).

do_send(gen_tcp, Sock, Bin) ->
    try erlang:port_command(Sock, Bin) of
        true ->
            ok
    catch
        error:_Error ->
            {error,einval}
    end;

do_send(Mod, Sock, Bin) ->
    Mod:send(Sock, Bin).

loop(#state{data = Data, handler = Handler, repl = Repl} = State) ->
    case epgsql_wire:decode_message(Data) of
        {Type, Payload, Tail} ->
            case ?MODULE:Handler(Type, Payload, State#state{data = Tail}) of
                {noreply, State2} ->
                    loop(State2);
                R = {stop, _Reason2, _State2} ->
                    R
            end;
        _ ->
            %% in replication mode send feedback after each batch of messages
            case (Repl =/= undefined) andalso (Repl#repl.feedback_required) of
                true ->
                    #repl{last_received_lsn = LastReceivedLSN,
                          last_flushed_lsn = LastFlushedLSN,
                          last_applied_lsn = LastAppliedLSN} = Repl,
                    send(State, ?COPY_DATA, epgsql_wire:encode_standby_status_update(
                        LastReceivedLSN, LastFlushedLSN, LastAppliedLSN)),
                    {noreply, State#state{repl = Repl#repl{feedback_required = false}}};
                _ ->
                    {noreply, State}
            end
    end.

finish(State, Result) ->
    finish(State, Result, Result).

finish(State = #state{current_cmd_transport = Transport}, Notice, Result) ->
    case Transport of
        {cast, From, Ref} ->
            From ! {self(), Ref, Result};
        {incremental, From, Ref} ->
            From ! {self(), Ref, Notice};
        {call, From} ->
            gen_server:reply(From, Result)
    end,
    command_next(State).

add_result(#state{results = Results, current_cmd_transport = Transport} = State, Notice, Result) ->
    Results2 = case Transport of
                   {incremental, From, Ref} ->
                       From ! {self(), Ref, Notice},
                       Results;
                   _ ->
                       [Result | Results]
               end,
    State#state{rows = [],
                results = Results2}.

add_row(#state{rows = Rows, current_cmd_transport = Transport} = State, Data) ->
    Rows2 = case Transport of
                {incremental, From, Ref} ->
                    From ! {self(), Ref, {data, Data}},
                    Rows;
                _ ->
                    [Data | Rows]
            end,
    State#state{rows = Rows2}.

notify(#state{current_cmd_transport = {incremental, From, Ref}} = State, Notice) ->
    From ! {self(), Ref, Notice},
    State;
notify(State, _) ->
    State.

%% Send asynchronous messages (notice / notification)
notify_async(#state{async = undefined}, _) ->
    false;
notify_async(#state{async = PidOrName}, Msg) ->
    try PidOrName ! {epgsql, self(), Msg} of
        _ -> true
    catch error:badarg ->
            %% no process registered under this name
            false
    end.

sync_required(#state{current_cmd = epgsql_cmd_sync} = State) ->
    State;
sync_required(#state{current_cmd = undefined} = State) ->
    State#state{sync_required = true};
sync_required(State) ->
    sync_required(finish(State, {error, sync_required})).

flush_queue(#state{current_cmd = undefined} = State, _) ->
    State;
flush_queue(State, Error) ->
    flush_queue(finish(State, Error), Error).

to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L)   -> list_to_binary(L).


%% -- backend message handling --

%% CommandComplete
on_message(?COMMAND_COMPLETE = Msg, Bin, State) ->
    Complete = epgsql_wire:decode_complete(Bin),
    command_handle_message(Msg, Bin, State#state{complete_status = Complete});

%% ReadyForQuery
on_message(?READY_FOR_QUERY = Msg, <<Status:8>> = Bin, State) ->
    command_handle_message(Msg, Bin, State#state{txstatus = Status});

%% Error
on_message(?ERROR = Msg, Err, #state{current_cmd = CurrentCmd} = State) ->
    Reason = epgsql_wire:decode_error(Err),
    case CurrentCmd of
        undefined ->
            %% Message generated by server asynchronously
            {stop, {shutdown, Reason}, State};
        _ ->
            command_handle_message(Msg, Reason, State)
    end;

%% NoticeResponse
on_message(?NOTICE, Data, State) ->
    notify_async(State, {notice, epgsql_wire:decode_error(Data)}),
    {noreply, State};

%% ParameterStatus
on_message(?PARAMETER_STATUS, Data, State) ->
    [Name, Value] = epgsql_wire:decode_strings(Data),
    Parameters2 = lists:keystore(Name, 1, State#state.parameters,
                                 {Name, Value}),
    {noreply, State#state{parameters = Parameters2}};

%% NotificationResponse
on_message(?NOTIFICATION, <<Pid:?int32, Strings/binary>>, State) ->
    {Channel1, Payload1} = case epgsql_wire:decode_strings(Strings) of
        [Channel, Payload] -> {Channel, Payload};
        [Channel]          -> {Channel, <<>>}
    end,
    notify_async(State, {notification, Channel1, Pid, Payload1}),
    {noreply, State};

%% ParseComplete
%% ParameterDescription
%% RowDescription
%% NoData
%% BindComplete
%% CloseComplete
%% DataRow
%% PortalSuspended
%% EmptyQueryResponse
%% CopyData
%% CopyBothResponse
on_message(Msg, Payload, State) ->
    command_handle_message(Msg, Payload, State).


%% CopyData for Replication mode
on_replication(?COPY_DATA, <<?PRIMARY_KEEPALIVE_MESSAGE:8, LSN:?int64, _Timestamp:?int64, ReplyRequired:8>>,
               #state{repl = #repl{last_flushed_lsn = LastFlushedLSN,
                                   last_applied_lsn = LastAppliedLSN} = Repl} = State) ->
    Repl1 =
        case ReplyRequired of
            1 ->
                send(State, ?COPY_DATA,
                     epgsql_wire:encode_standby_status_update(LSN, LastFlushedLSN, LastAppliedLSN)),
                Repl#repl{feedback_required = false,
                          last_received_lsn = LSN};
            _ ->
                Repl#repl{feedback_required = true,
                          last_received_lsn = LSN}
        end,
    {noreply, State#state{repl = Repl1}};

%% CopyData for Replication mode
on_replication(?COPY_DATA, <<?X_LOG_DATA, StartLSN:?int64, EndLSN:?int64,
                             _Timestamp:?int64, WALRecord/binary>>,
               #state{repl = Repl} = State) ->
    Repl1 = handle_xlog_data(StartLSN, EndLSN, WALRecord, Repl),
    {noreply, State#state{repl = Repl1}};
on_replication(?ERROR, Err, State) ->
    Reason = epgsql_wire:decode_error(Err),
    {stop, {error, Reason}, State};
on_replication(M, Data, Sock) when M == ?NOTICE;
                                   M == ?NOTIFICATION;
                                   M == ?PARAMETER_STATUS ->
    on_message(M, Data, Sock).


handle_xlog_data(StartLSN, EndLSN, WALRecord, #repl{cbmodule = undefined,
                                                    receiver = Receiver} = Repl) ->
    %% with async messages
    Receiver ! {epgsql, self(), {x_log_data, StartLSN, EndLSN, WALRecord}},
    Repl#repl{feedback_required = true,
              last_received_lsn = EndLSN};
handle_xlog_data(StartLSN, EndLSN, WALRecord,
                 #repl{cbmodule = CbModule, cbstate = CbState, receiver = undefined} = Repl) ->
    %% with callback method
    {ok, LastFlushedLSN, LastAppliedLSN, NewCbState} =
        CbModule:handle_x_log_data(StartLSN, EndLSN, WALRecord, CbState),
    Repl#repl{feedback_required = true,
              last_received_lsn = EndLSN,
              last_flushed_lsn = LastFlushedLSN,
              last_applied_lsn = LastAppliedLSN,
              cbstate = NewCbState}.
