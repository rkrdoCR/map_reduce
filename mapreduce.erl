-module(mapreduce).

-export([map_task/2, reduce_task/1, start_tuples/3]).

start_tuples(M, R, ChunkSize) ->
    % delete results folder content
    file:delete("results/chuncks.dat"),
    file:delete("results/mapResult.dat"),
    file:delete("results/reduceResult.dat"),
    timer:sleep(1000),

    % read the source file and load it into Lines variable
    {ok, Lines} = file:consult("tuplas.dat"),
    AllChunks = createChuncks(Lines, ChunkSize),
    % io:format("The chuncks list is: ~n~w~n~n", [AllChunks]),
    file:write_file("results/chuncks.dat", io_lib:format("~p.~n", [AllChunks]), [append]),

    % map function definition
    Map_func = fun (Chunck) ->
		       [{K, V1 + V2} || {K, V1, V2} <- Chunck]
	       end,

    % reduce function definition
    Red_func = fun (MList, P) -> reduce(MList, P) end,
    
    % call map reduce function
    map_reduce(M, R, Map_func, Red_func, AllChunks).

map_reduce(M, R, M_func, R_func, AllChunks) ->
    Reduce_processes = repeat_exec(R,
				   fun (_) ->
					   spawn(mapreduce, reduce_task,
						 [R_func])
				   end),
    io:format("Reduce processes ~w are started~n",
	      [Reduce_processes]),
    Map_processes = repeat_exec(M,
				fun (_) ->
					spawn(mapreduce, map_task,
					      [Reduce_processes, M_func])
				end),
    io:format("Map processes ~w are started~n~n",
	      [Map_processes]),
    Extract_func = fun (N) ->
			   Extracted_line = lists:nth(N + 1, AllChunks),
			   Map_proc = find_mapper(Map_processes),
			   io:format("-Send ~w tuples to map process ~w~n~n",
				     [length(Extracted_line), Map_proc]),
			   Map_proc ! {map, Extracted_line}
		   end,
    repeat_exec(length(AllChunks), Extract_func),
    timer:sleep(2000),
    All_results = repeat_exec(length(Reduce_processes),
			      fun (N) ->
				      collect(lists:nth(N + 1,
							Reduce_processes))
			      end),
    % lists:flatten(All_results).
    file:write_file("results/reduceResult.dat", io_lib:format("~p.~n", [All_results]), [append]),
    io:format("**Finished processing ~w chuncks with ~w mappers and ~w reducers.~n**Find the outputs in the results folder~n", [length(AllChunks), M, R]).

reduce(List, P) ->
    if P == undefined ->
	   Total = lists:sum([V || {_, V} <- List]) + 0;
       true -> Total = lists:sum([V || {_, V} <- List]) + P
    end,
    Total.

createChuncks(List, Len) ->
    LeaderLength = case length(List) rem Len of
		     0 -> 0;
		     N -> Len - N
		   end,
    Leader = lists:duplicate(LeaderLength, undefined),
    createChuncks(Leader ++ lists:reverse(List), [], 0,
		  Len).

% creates a list of chuncks
createChuncks([], Acc, _, _) -> Acc;
createChuncks([H | T], Acc, Pos, Max) when Pos == Max ->
    createChuncks(T, [[H] | Acc], 1, Max);
createChuncks([H | T], [HAcc | TAcc], Pos, Max) ->
    createChuncks(T, [[H | HAcc] | TAcc], Pos + 1, Max);
createChuncks([H | T], [], Pos, Max) ->
    createChuncks(T, [[H]], Pos + 1, Max).

% the map process
map_task(Reduce_processes, MapFun) ->
    receive
      {map, Data} ->
	  IntermediateResults = MapFun(Data),
	  GroupedResults = [{KK, find(IntermediateResults, [KK])}
			    || KK
				   <- maps:keys(maps:from_list(IntermediateResults))],
	  io:format("*Map function produce: ~w intermediate results~n~n",
		    [length(GroupedResults)]),
	  file:write_file("results/mapResult.dat", io_lib:format("~p.~n", [GroupedResults]), [append]),
	  lists:foreach(fun ({K, V}) ->
				Reducer_proc = find_reducer(Reduce_processes,
							    K),
				Reducer_proc ! {reduce, {K, V}}
			end,
			GroupedResults),
	  map_task(Reduce_processes, MapFun)
    end.

% finds all the tuples for a given key
find([], _) -> [];
find(_, []) -> [];
find(Pairs, Keys) -> find(Pairs, Keys, Pairs, []).

find(_, [], _, Results) -> Results;
find([], [_ | Keys], OriginalPairs, Results) ->
    find(OriginalPairs, Keys, OriginalPairs, Results);
find([{Key, _} = Pair | Pairs], [Key | _] = Keys,
     OriginalPairs, Results) ->
    find(Pairs, Keys, OriginalPairs, [Pair | Results]);
find([_ | Pairs], Keys, OriginalPairs, Results) ->
    find(Pairs, Keys, OriginalPairs, Results).

% The reducer process
reduce_task(ReduceFun) ->
    receive
      {reduce, {K, V}} ->
	  io:format("--Id ~w send to reduce process: ~w~n~n",
		    [K, self()]),
	  put(K, ReduceFun(V, get(K))),
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
