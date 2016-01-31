-module(maestro).

-export([init/1, terminate/2, handle_call/3, handle_cast/2,
 handle_info/2, code_change/3, start/1, mostrarNodosMap/1, mapReduce/3, agregaNodo/2, eliminaNodo/2, agregaNodoRemoto/3]).

-behaviour(gen_server).

handle_call({mapreduce, _Parent, Fmap, Freduce}, _From, {inicial,Dpid}) ->
    S = self(),
    gen_server:start(mprd,{S,Fmap,Freduce,Dpid},[]),
    receive
        Resultado -> tiempo() ,{reply, Resultado, {inicial,Dpid}}
    end;

handle_call(listaNodos,_From,{inicial,Dpid})->
    {reply,Dpid,{inicial,Dpid}};

handle_call({addNodo,Info},_From,{inicial,Dpid})->
    case list_max(orddict:fetch_keys(Dpid)) of
        vacio -> Nnodo = 1;
        {ok, Val} -> Nnodo = Val+1
    end,
    {ok, NodoPid} = gen_server:start(nodo,{inicial,self(),Nnodo,Info},[]),
    {reply,{ok,{Nnodo,NodoPid}},{inicial,orddict:append(Nnodo,NodoPid,Dpid)}};

handle_call({addNodoRemoto,{Info,Host}},_From,{inicial,Dpid})->
    case list_max(orddict:fetch_keys(Dpid)) of
        vacio -> Nnodo = 1;
        {ok, Val} -> Nnodo = Val+1
    end,
    S = self(),
    spawn(Host,nodo,remotestart,[S,Nnodo,Info]),
	receive
		{Nnodo, NodoPid} -> {reply,{ok,{Nnodo,NodoPid}},{inicial,orddict:append(Nnodo,NodoPid,Dpid)}};
		_Any ->  {reply,error,{inicial,Dpid}}
	end;

handle_call({delNodo,N},_From,{inicial,Dpid})->
    case orddict:find(N,Dpid) of
        {ok,[Pid]} -> gen_server:cast(Pid,stop), {reply,{ok,Pid},{inicial,orddict:erase(N,Dpid)}};
        error ->  {reply,error,{inicial,Dpid}}
    end;

handle_call(Other, _From, State) -> io:format("estado ~w",[State]), {reply, {unknown, Other}, State}.


init({Info,N}) when N > 0 -> {ok, {inicial,crea_nodos(Info,N,round(length(Info) / N),orddict:new())}};
init(_) -> {ok, {inicial,orddict:new()}}.

crea_nodos(_L,0,_Nelem,Dpid) -> Dpid;
crea_nodos(L,N,Nelem,Dpid) ->
    {Valores, Resto} = lists:split(Nelem,L),
    case N =:= 1 of
        true ->
            {ok, Npid} = gen_server:start(nodo,{inicial,self(),N,L},[]),
            crea_nodos(Resto,N-1,Nelem,orddict:append(N,Npid,Dpid));
        false ->
            {ok, Npid} = gen_server:start(nodo,{inicial,self(),N,Valores},[]),
            crea_nodos(Resto,N-1,Nelem,orddict:append(N,Npid,Dpid))
    end.

handle_cast(Request, State) -> io:format("Unexpected request: ~w~n", [Request]), {noreply, State}.

handle_info(Message, State) -> io:format("Unexpected message: ~w~n", [Message]), {noreply, State}.

terminate(_Reason, _State) -> io:format("Nodo maestro finalizado.~n").

code_change(_PreviousVersion, State, _Extra) -> {ok, State}.

%Metodos de interfaz

start({Info,N}) -> gen_server:start_link(?MODULE, {Info,N}, []).

mostrarNodosMap(PidServer)->
    gen_server:call(PidServer,listaNodos).

mapReduce(PidServer,Fmap,Freduce)->
    gen_server:call(PidServer,{mapreduce,self(),Fmap,Freduce},5000).

agregaNodo(PidServer,Info)->
    gen_server:call(PidServer,{addNodo,Info}).

agregaNodoRemoto(PidServer,Host,Info)->
    gen_server:call(PidServer,{addNodoRemoto,{Info,Host}}).

eliminaNodo(PidServer,Nnodo)->
    gen_server:call(PidServer,{delNodo,Nnodo}).

%Funciones Auxiliares

list_max([]   ) -> vacio;
list_max([H|T]) -> {ok, list_max(H, T)}.

list_max(X, []   )            -> X;
list_max(X, [H|T]) when X < H -> list_max(H, T);
list_max(X, [_|T])            -> list_max(X, T).


tiempo() ->
    {H, M, S} = time(),
    io:format('Tiempo Fin : ~2..0b:~2..0b:~2..0b~n', [H, M, S]).

% {ok,PidServer} = maestro:start({[300,22,3,4,1,2223,4,5,22234,5,534,34,5,6456,77,567,3453,45,6546,7568,567,333,66,5,3,4,5,6,7,22,4,5],2}).
% maestro:mapReduce(PidServer,Fmap,Freduce).
% maestro:agregaNodo(PidServer,[2,3,4,1,3,4,4,5,6,7,2,23,4,5,6]).
% maestro:agregaNodoRemoto(PidServer,'d1@daniel-VirtualBox',[2,3,4,1,3,4,4,5,6,7,2,23,4,5,6]).
% maestro:mostrarNodosMap(PidServer).
% maestro:eliminaNodo(PidServer,2).
% cd("C:/Users/dfarina/Desktop/mapreduce").
% Fmap = fun (_C,V) -> Maximos = [{max,X} || X <- V, X >=300 ], Minimos = [{min,X} || X <- V, X <300 ], Maximos ++ Minimos end.
% Freduce = fun (C,VA,V) when C=:=min andalso VA >= V -> V; (C,VA,V) when C=:=min andalso VA < V -> VA; (C,VA,V) when C=:=max andalso VA >= V -> VA; (C,VA,V) when C=:=max andalso VA < V -> V end.
