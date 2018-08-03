-module(rtlang_query).

-author(asif).
-email("asif@tapfreaks.net").

-include("ql2_pb.hrl").
-include("term.hrl").

%%=====================================================
%% An easy way of dogin some complex things
%%=====================================================

-export([next/2, query/1, query/2, query/3, query_test/3]).
-export([stream_poll/2, stream_recv/2, stream_stop/2]).

%%% Fetch next batch
%%% When the response_type is SUCCESS_PARTIAL=3, we can call next to send more data
next({Socket, Token, R}, F) ->
  lists:map(F, R),

  Iolist = ["[2]"],
  Length = iolist_size(Iolist),
  ok = gen_tcp:send(Socket, [<<Token:64/little-unsigned>>, <<Length:32/little-unsigned>>, Iolist]),
  case recv(Socket) of
    {ok, R} ->
      Rterm = jsx:decode(R),
      %proplists:get_value(<<"r">>, Rterm),
      case proplists:get_value(<<"t">>, Rterm) of
        ?RUNTIME_ERROR ->
          {error, proplists:get_value(<<"r">>, Rterm)};
        ?SUCCESS_ATOM ->
          F(proplists:get_value(<<"r">>, Rterm));
        ?SUCCESS_SEQUENCE ->
          {ok, proplists:get_value(<<"r">>, Rterm)};
        ?SUCCESS_PARTIAL ->
          % So we get back a stream, let continous pull query
          next({Socket, Token, proplists:get_value(<<"r">>, Rterm)}, F)
          %Recv = spawn(?MODULE, stream_recv, [Socket, Token]),
          %Pid = spawn(?MODULE, stream_poll, [{Socket, Token}, Recv]),
          %{ok, {pid, Pid}, proplists:get_value(<<"r">>, Rterm)}
      end;
    {error, ErrReason} ->
      {error, ErrReason}
  end.

%%% Build AST from raw query
query(RawQuery) ->
  rtlang_query_builder:make(RawQuery).

%%% Build and Run query when passing Socket
query(Socket, RawQuery) ->
%%  query_test(Socket, RawQuery, [{}]).
  query(Socket, RawQuery, [{}]).

query(Socket, RawQuery, Option) ->
  {A1, A2, A3} = erlang:timestamp(),
  TString = lists:flatten(io_lib:format("~p", [A1+A2+A3])),
  Token = rand:uniform(list_to_integer(TString)),

%%  Query = rtlang_query_builder:make(RawQuery),
  Query = [1,[39,[[15,[[14,[<<"ping">>]],<<"employees">>]],[69,[[2,[0]],[93,[[2,[30,34,33,96]],[170,[[10,[0]],<<"age">>]]]]]]]]],

  %% Query = [?table_list, [ [?DB, [<<"ping">>] ] ] ],
  %% Query = [?TABLE_LIST, [ [?DB, [<<"ping">>] ] ] ],

  Iolist  = jsx:encode(Query), % ["[1,"] ++ [Query] ++ [",{}]"], % list db 

  Length = iolist_size(Iolist),
  %io:format("Query= ~p~n", [Iolist]),
  %io:format("Length: ~p ~n", [Length]),

  case gen_tcp:send(Socket, [<<Token:64/little-unsigned>>, <<Length:32/little-unsigned>>, Iolist]) of
    ok ->
      ok;
    {error, Reason} ->
      io:format("~nError sending query: ~p", [Reason])
  end,

  case recv(Socket) of
    {ok, R} ->
      Rterm = jsx:decode(R),
      %proplists:get_value(<<"r">>, Rterm),
      case proplists:get_value(<<"t">>, Rterm) of
        ?RUNTIME_ERROR ->
          {error, proplists:get_value(<<"r">>, Rterm)};
        ?SUCCESS_ATOM ->
          {ok, proplists:get_value(<<"r">>, Rterm)};
        ?SUCCESS_SEQUENCE ->
          {ok, proplists:get_value(<<"r">>, Rterm)};
        ?SUCCESS_PARTIAL ->
          % So we get back a stream, let continous pull query
	  {cursor, {Socket, Token, proplists:get_value(<<"r">>, Rterm)}};
	 _ ->
	  io:format("Unknow Response : ~n~p", [Rterm])
          %Recv = spawn(?MODULE, stream_recv, [Socket, Token]),
          %Pid = spawn(?MODULE, stream_poll, [{Socket, Token}, Recv]),
          %{ok, {pid, Pid}, proplists:get_value(<<"r">>, Rterm)}
      end
      ;
    {error, ErrReason} ->
      {error, ErrReason}
  end.

query_test (Socket, RawQuery, Option) ->
    Query = rtlang_query_builder:make(RawQuery),
    Finaluery = jsx:encode([?QUERYTYPE_START, Query, Option]),
    io:format("~nWritten Query : ~p", [Query]),
    io:format("~nFinal Query : ~s~n", [Finaluery]),
    query(Socket, RawQuery, Option).
%%%

stream_stop(Socket, Token) ->
  Iolist = ["[3]"],
  Length = iolist_size(Iolist),
  ok = gen_tcp:send(Socket, [<<Token:64/little-unsigned>>, <<Length:32/little-unsigned>>, Iolist])
  .

%% receive data from stream, then pass to other process
stream_recv(Socket, Token) ->
  receive
    R ->
      io:fwrite("Changefeed receive item: ~p ~n",[R])
  end,
  stream_recv(Socket, Token)
  .

%Continues getting data from stream
stream_poll({Socket, Token}, PidCallback) ->
  Iolist = ["[2]"],
  Length = iolist_size(Iolist),

  ok = gen_tcp:send(Socket, [<<Token:64/little-unsigned>>, <<Length:32/little-unsigned>>, Iolist]),
  {ok, R} = recv(Socket),
  Rterm = jsx:decode(R),
  spawn(fun() -> PidCallback ! proplists:get_value(<<"r">>, Rterm) end),
  stream_poll({Socket, Token}, PidCallback)
  .
%% Receive data from Socket
%%Once the query is sent, you can read the response object back from the server. The response object takes the following form:
%%
%% * The 8-byte unique query token
%% * The length of the response, as a 4-byte little-endian integer
%% * The JSON-encoded response
recv(Socket) ->
  case gen_tcp:recv(Socket, 8) of
    {ok, Token} ->
      <<_K:64/little-unsigned>> = Token;
    {error, _Reason} ->
      io:format("Fail to parse token")
  end,

  {_RecvResultCode, ResponseLength} = gen_tcp:recv(Socket, 4),
  <<_Rs:32/little-unsigned>> = ResponseLength,

  {ResultCode, Response} = gen_tcp:recv(Socket, binary:decode_unsigned(ResponseLength, little)),
  case ResultCode of
    ok ->
      {ok, Response};
    error ->
      io:format("Got Error ~s ~n", [Response]),
      {error, Response}
  end.
