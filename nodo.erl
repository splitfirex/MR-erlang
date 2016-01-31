-module(nodo).

-export([init/1, terminate/2, handle_call/3, handle_cast/2,
 handle_info/2, code_change/3,loopDeProcesado/4,remotestart/3]).

-behaviour(gen_server).

handle_call(Other, _From, State) -> {reply, {unknown, Other}, State}.

init({inicial,Parent,N,Valores}) ->
	io:format("Se crea el servidor Nodo N~p : ~p~n",[N,self()]),
	{ok, {Parent,N,Valores}}.

handle_cast({startmap, MapreduceNodo, Fmap}, {Parent,N,Valores}) ->
	spawn(fun()-> loopDeProcesado(MapreduceNodo,N,Valores,Fmap) end),
 	{noreply, {Parent,N,Valores}};

handle_cast(stop, {_Parent,N,_Valores}) ->
    io:format("Finaliza el nodo ~w~n",[N]),
    {stop,normal,terminate};

handle_cast(Request, State) -> io:format("Unexpected request: ~w~n", [Request]),
 {noreply, State}.

loopDeProcesado(Parent,N,Valores,Fmap)->
    try Fmap(N,Valores) of
        [{Clave,Valor}| Resto] -> gen_server:cast(Parent,{Clave,Valor}),
                                  [gen_server:cast(Parent,{C,V}) || {C,V} <- Resto ]
    catch
        _:_ -> io:format("No es posible ejecutar el Map en nodo ~p~n",[N])
    end,
    gen_server:cast(Parent,'end').

remotestart(Parent,N,Valores)->
	io:format("inicializacion remota"),
	{ok,Pid} = gen_server:start(nodo,{inicial,Parent,N,Valores},[]),
	Parent ! {N,Pid}.

handle_info(Message, State) -> io:format("Unexpected message: ~w~n", [Message]), {noreply, State}.
terminate(_Reason, _State) -> io:format("Finaliza un Nodo Map.~n").
code_change(_PreviousVersion, State, _Extra) -> {ok, State}.

%cd("C:/Users/dfarina\/Desktop/erlang").
