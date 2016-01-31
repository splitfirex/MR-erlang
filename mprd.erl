-module(mprd).

-export([init/1, terminate/2, handle_call/3, handle_cast/2,
     handle_info/2, code_change/3]).

init({Parent,Fmap,Freduce,Dpid}) ->
    [ startMap(X,Fmap) || {_K,[X]} <- orddict:to_list(Dpid) ],
    tiempo(),
    io:format("Procesando mapReduce proceso : ~w~n",[self()]),
    {ok, {esperamap,Parent,{Freduce, orddict:new(),length(Dpid)}}}.

handle_cast({Clave,Valor}, {esperamap,Parent,{Freduce,D,N}}) ->
    S = self(),
    case orddict:find(Clave,D) of
                {ok, [DictValor]} -> gen_server:cast(DictValor,{newvalue, Valor}),
                    {noreply, {esperamap,Parent,{Freduce,D,N}}};
                error           -> {ok, PIDReduce} = gen_server:start(reduce,{S, Freduce, Clave, Valor},[]),
                    {noreply, {esperamap,Parent,{Freduce,orddict:append(Clave,PIDReduce,D),N}}}
    end;
handle_cast({Clave,Valor}, {esperareduce,Parent,{Result,N}}) when N > 1 ->
    {noreply, {esperareduce,Parent,{Result++[{Clave,Valor}],N-1}}};
handle_cast({Clave,Valor}, {esperareduce,Parent,{Result,1}}) ->
    Parent ! Result ++ [{Clave,Valor}],
    {stop, normal, terminar};
handle_cast('end', {esperamap,Parent,{Freduce,D,N}}) when N > 1 ->
    {noreply, {esperamap,Parent,{Freduce,D,N-1}}};
handle_cast('end', {esperamap,Parent,{_Freduce,D,1}})  ->
    [ gen_server:cast(PID,'end') || {_X,[PID]} <- orddict:to_list(D) ],
    {noreply, {esperareduce,Parent,{[],length(orddict:fetch_keys(D))}}};
handle_cast(Request, State) -> io:format("Unexpected request: ~w ~w ~n", [Request,State]),
 {noreply, State}.

handle_call(Other, _From, State) -> {reply, {unknown, Other}, State}.
handle_info(Message, State) -> io:format("Unexpected message: ~w~n", [Message]), {noreply, State}.
terminate(_Reason, _State) -> io:format("Proceso MapReduce Finalizado.~n").
code_change(_PreviousVersion, State, _Extra) -> {ok, State}.

%INTERFACES

tiempo() ->
    {H, M, S} = time(),
    io:format('Tiempo inicio : ~2..0b:~2..0b:~2..0b~n', [H, M, S]).

startMap(PidServer,Fmap)->
    gen_server:cast(PidServer,{startmap, self(), Fmap}).