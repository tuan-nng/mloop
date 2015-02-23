fun threat (x,y) (x',y') = let 
  val distance = x' -  x
  in
    y = y' orelse y = y' - distance orelse y = y' + distance
end

fun conflict pos = List.exists (threat pos)

fun randInt ()= let
  val t = Time.now()
  val b = Time.toMilliseconds t
  val bucket = b mod 10
in
  IntInf.toString bucket
end

fun codeBoard ([],str) = str
  | codeBoard ((_,row)::t, "") = codeBoard(t, Int.toString row)
  | codeBoard ((_,row)::t, str) = codeBoard(t, (Int.toString row) ^ "-" ^ str)


fun fillQueen (qs,n,col,fc) = let
  fun put row = if row > n then fc ()	
	else if conflict (col,row) qs then put (row + 1)
	else if col = n then (MapContext.emit(randInt (), codeBoard((col,row)::qs, "")); put (row + 1))
	else fillQueen((col,row)::qs,n,col+1, fn () => put (row+1))
 in 
     put 1
 end

fun append (row,nil) = [(1,row)]
  | append (row, (col,x)::t) = (col + 1, row) :: (col,x)::t

fun parseBoard value = let 
  val splitter = String.tokens(fn c => c = #"-")
  fun toInt s = Option.getOpt (Int.fromString s, 0)
  val values = map toInt (splitter value)
  in foldl append nil values
end

fun mloop_reduce (key:string)=
    while (ReduceContext.nextValue()) do
        ReduceContext.emit("", ReduceContext.getInputValue())


fun mloop_map (key,value) = let 
  val board = parseBoard value
  val col = 1 + List.length board
in
  fillQueen(board, 17, col, fn () => ())
end
