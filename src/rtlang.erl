-module(rtlang).

-author(asif).
-email("asif@tapfreaks.net").

%%=====================================================
%% An easy way of dogin some complex things
%%=====================================================

-export([connect/2, close/1, test/0]).
-export([r/1, r/2]).
-export([getField/1]).

%%=====================================================
%% Internal Functions
%%=====================================================

handshake(Sock, _AuthKey) ->
  ok = gen_tcp:send(Sock, binary:encode_unsigned(16#400c2d20, little)),
  ok = gen_tcp:send(Sock, [<<0:32/little-unsigned>>]),
  % Using JSON Protocol
  ok = gen_tcp:send(Sock, [<<16#7e6970c7:32/little-unsigned>>]),

  {ok, Response} = read_until_null(Sock),
  case Response == <<"SUCCESS",0>> of
    true ->
      ok;
    false ->
      io:format("Error: ~s~n", [Response]),
      {error, Response}
  end.

read_until_null(Socket) ->
  read_until_null(Socket, []).

read_until_null(Socket, Acc) ->
  case gen_tcp:recv(Socket, 0) of
    {error, OtherSendError} ->
      io:format("Some other error on socket (~p), closing", [OtherSendError]),
      gen_tcp:close(Socket);
    {ok, Response} ->
      Result = [Acc, Response],
      case is_null_terminated(Response) of
        true -> {ok, iolist_to_binary(Result)};
        false -> read_until_null(Socket, Result)
      end
  end.

is_null_terminated(B) ->
  binary:at(B, iolist_size(B) - 1) == 0.


%%=====================================================
%% External API
%%=====================================================

test () ->
    Conn =  connect("localhost", 28015),
    getField(Conn).

connect (Host, Port) ->
 {ok, Sock} = gen_tcp:connect(Host, Port,
                               [binary, {packet, 0}, {active, false}]),
  handshake(Sock, <<"">>),
  Sock.

close(Sock) ->
  gen_tcp:close(Sock).

%%% RethinkDB API
r(Q) -> rtlang_query:query(Q).
r(Socket, RawQuery) ->
  rtlang_query:query(Socket, RawQuery).

%%=====================================================
%% ReQL Functions
%%=====================================================

getField (Conn) ->

XFun = 
[{db, [<<"ping">>]}, {table,
[<<"employees">>]}, {filter, fun(X) ->
  [
    {'or', [
      {eq, [{field, [X, <<"age">>]}, 34]},
      {eq, [{field, [X, <<"age">>]}, 30]},
      {'or', [
           {eq, [{field, [X, <<"age">>]}, 32]}
      ]}
    ]}
  ]
end}],

r(Conn, []).
