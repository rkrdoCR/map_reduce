-module(mr_matrix).

-export([map_task/2, reduce_task/1,start_mm/0]).

start_mm() -> 
    {ok, Data} = file:consult("mm_input.txt"),
    M = length(Data),
    R = trunc(math:sqrt(M)),
    Map_func = fun(Block) ->
        spmv_multiply(Block) 
    end,
    Red_func = fun (Block) -> 
        sum_row(Block)
    end,
    map_reduce(Data, M, R, Map_func, Red_func).

map_reduce(Data, M, R, Map_func, Red_func) ->
    Reduce_processes = repeat_exec(R,
				   fun (_) ->
					   spawn(mr_matrix, reduce_task,
						 [Red_func])
				   end),
    io:format("Reduce processes ~w are started~n",
	      [Reduce_processes]),
    Map_processes = repeat_exec(M,
				fun (_) ->
					spawn(mr_matrix, map_task,
					      [Reduce_processes, Map_func])
				end),
    io:format("Map processes ~w are started~n~n",
	      [Map_processes]),
    Extract_func = fun (N) ->
			   Extracted_line = lists:nth(N + 1, Data),
			   Map_proc = find_mapper(Map_processes),
			%    io:format("-Send ~w to map process ~w~n~n",
			% 	     [Extracted_line, Map_proc]),
			   Map_proc ! {map, Extracted_line}
		   end,
    repeat_exec(M, Extract_func),
    timer:sleep(2000),
    All_results = repeat_exec(length(Reduce_processes),
			      fun (N) ->
				      collect(lists:nth(N + 1,
							Reduce_processes))
			      end),
    % lists:flatten(All_results).
    file:write_file("results/mm_reduceResult.dat", io_lib:format("~p.~n", [lists:flatten(All_results)]), [append]),
    io:format("~n**Finished processing ~w chuncks with ~w mappers and ~w reducers.~n**Find the outputs in the results folder~n", [length(Data), M, R]).
    
sum_row(Block) ->
    {R, M} = Block,
    io:format("Block Results: Row: ~w Value: ~w~n", [R, summary([], M)]),
    Block.

spmv_multiply(Block) ->
    {R, V, M} = Block,
    ValRowCol = [
        {X, {index_of(L, M), index_of(X, L)}} || X <- lists:flatten(M), L <- M, X > 0, index_of(X, L) /= not_found
    ],
    Val = [Val || {Val, {_,_}} <- ValRowCol],
    R_ptr = [
        index_of(first_nonzero(L), Val) || L <- M, index_of(first_nonzero(L), Val) /= not_found
    ],
    S = [
        lists:sublist(Val, lists:nth(P, R_ptr), lists:nth(P+1, R_ptr)-lists:nth(P, R_ptr)) || P <- lists:seq(1, length(R_ptr)), P+1 =< length(R_ptr)
    ],
    Segments = lists:append(S, [lists:sublist(Val, lists:last(R_ptr), length(Val) + 1 - lists:last(R_ptr))]),
    MV = [
        {Row, Value * lists:nth(Column, V)} || Z <- lists:flatten([Seg || Seg <- Segments]), {Value, {Row, Column}} <- ValRowCol, Z == Value
    ],
    {R, MV}.

index_of(Item, List) -> index_of(Item, List, 1).

index_of(_, [], _)  -> not_found;
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index+1).

first_nonzero([]) -> 0;
first_nonzero([H|_]) when H > 0 -> H;
first_nonzero([_|T]) -> first_nonzero(T).

summary(List,[]) -> 
    List;
summary(List,[Head | Tail]) ->
    {X, Y} = Head,
    Result = lists:keyfind(X,1,List),
    if 
        Result /= false ->
            {_,Z}=Result,
            summary(lists:keystore(X,1 ,List, {X, Y + Z}) , Tail);
        true ->
            summary(lists:append(List,[{X,Y}]) , Tail)
        end.

map_task(Reduce_processes, MapFun) ->
    receive
        {map, Data} ->
	    IntermediateResults = MapFun(Data),
	    file:write_file("results/mm_mapResult.dat", io_lib:format("~p.~n", [IntermediateResults]), [append]),
	    {K,V} = IntermediateResults,
        Reducer_proc = find_reducer(Reduce_processes, K),
		Reducer_proc ! {reduce, {K, V}},
	    map_task(Reduce_processes, MapFun)
    end.

% % finds all the tuples for a given key
% find([], _) -> [];
% find(_, []) -> [];
% find(Pairs, Keys) -> find(Pairs, Keys, Pairs, []).

% find(_, [], _, Results) -> Results;
% find([], [_ | Keys], OriginalPairs, Results) ->
%     find(OriginalPairs, Keys, OriginalPairs, Results);
% find([{Key, _} = Pair | Pairs], [Key | _] = Keys,
%      OriginalPairs, Results) ->
%     find(Pairs, Keys, OriginalPairs, [Pair | Results]);
% find([_ | Pairs], Keys, OriginalPairs, Results) ->
%     find(Pairs, Keys, OriginalPairs, Results).

reduce_task(ReduceFun) ->
    receive
      {reduce, {K, V}} ->
	%   io:format("--Id ~w send to reduce process: ~w~n~n",
	% 	    [V, self()]),
        % Value = lists:append([get(K)], [ReduceFun({K,V})]),
        % Value = [get(K)] ++ [ReduceFun({K,V})],
	  put(K, ReduceFun({K, V})),
	  reduce_task(ReduceFun);
      {collect, PPid} ->
	  PPid ! {result, get()}, reduce_task(ReduceFun)
    end.

repeat_exec(N, Func) ->
    lists:map(Func, lists:seq(0, N - 1)).

find_reducer(Processes, Key) ->
    Index = erlang:phash(Key, length(Processes)),
    lists:nth(Index, Processes).

find_mapper(Processes) ->
    case rand:uniform(length(Processes)) of
      0 -> find_mapper(Processes);
      N -> lists:nth(N, Processes)
    end.

collect(Reduce_proc) ->
    Reduce_proc ! {collect, self()},
    receive {result, Result} -> Result end.