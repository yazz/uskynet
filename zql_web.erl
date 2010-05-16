-module(web).
-compile(export_all).


start(Port) ->
  mochiweb_http:start([{'ip', "127.0.0.1"}, {port, Port},
                       {'loop', fun ?MODULE:loop/1}]).
                           % mochiweb will call loop function for each request

loop(Req) ->
  RawPath = Req:get(raw_path),
  {Path, _, _} = mochiweb_util:urlsplit_path(RawPath),   % get request path

  case Path of                                           % respond based on path
    "/"  -> respond(Req, <<"<p>Hello World!</p>">>);
    "/a" -> respond(Req, <<"<p>Page a</p>">>);
    X    -> respond(Req, X)
%   _    -> respond(Req, <<"<p>Page not found!</p>">>)
  end.

respond(Req, Content) ->
  Req:respond({200, [{<<"Content-Type">>, <<"text/html">>}], Content}).
