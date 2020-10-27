-module(mr_matrix).

-export([start_mm/0]).

start_mm() -> 
    {ok, Data} = file:consult("mm_input.txt"),
    mapreduce(Data).

mapreduce(Data)->
    spmv_multiply(Data).
    

spmv_multiply(Data) ->
    T = lists:nth(1,Data),
    {R, V, M} = T,
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