-module(reduce).

-export([init/1, terminate/2, handle_call/3, handle_cast/2,
 handle_info/2, code_change/3]).

-behaviour(gen_server).

handle_call(Other, _From, State) -> {reply, {unknown, Other}, State}.

handle_info(Message, State) -> io:format("Unexpected message: ~w~n", [Message]),
 {noreply, State}.

terminate(_Reason, _State) -> io:format("Finaliza nodo Reduce.~n").

code_change(_PreviousVersion, State, _Extra) -> {ok, State}.

init({Parent, Freduce, Clave, Valor}) ->
    {ok, {Parent,Freduce,Clave,Valor}}.

handle_cast({newvalue, Valornuevo}, {Parent,Freduce,Clave,ValorActual}) ->
    {noreply, {Parent,Freduce,Clave,Freduce(Clave,ValorActual,Valornuevo)}};

handle_cast('end', {Parent,_Freduce,Clave,ValorActual}) ->
    gen_server:cast(Parent,{Clave,ValorActual}),
    {stop, normal,terminar};

handle_cast(Request, State) -> io:format("Unexpected request: ~w~n", [Request]),
 {noreply, State}.



