fun split (str,c) = String.tokens (fn ch => ch = c) str 

fun tokens str = String.tokens (fn c => Char.isSpace c) str 

fun join (l,separator) = let
  fun j ([],sep,result) = result
    | j ([h],sep,result) = result ^ h
    | j (h::t, sep, result) = j (t,sep,result ^ h ^ sep)
  in
  j(l,separator,"")
end

val maxInt = Option.getOpt(Int.maxInt,1000000000)

(* info = EDGES|DISTANCE_FROM_SOURCE|COLOR| *)
fun unpackInfo info = let 
  val tmp = split (info,#"|")
  val valid2 = if List.length tmp = 3 then true else raise Fail("Invalid info.")
  val e::d::c::_ = tmp
in 
  (e,d,c)
end 

(* str = ID    EDGES|DISTANCE_FROM_SOURCE|COLOR| *)
fun unpack str = let
  val tmp = tokens str
  val valid = if List.length tmp = 2 then true else raise Fail("Invalid key-value.")
  val k::v::_ = tmp
in
  (k, unpackInfo v)
end

fun pack {id=k:string,edges=e:string list,distance=d:int,color=c:string} = (join (e,",")) ^ "|" ^ (Int.toString d) ^ "|" ^ c
fun newNode (identity,e, dist,c) = {id=identity,edges=e,distance=dist,color=c}

fun printNode (id, edges, dist, color) = MapContext.emit (id, edges ^ "|" ^ dist ^ "|" ^ color)
fun reduceNode (id, edges, dist, color) = ReduceContext.emit (id, edges ^ "|" ^ dist ^ "|" ^ color)

fun mloop_map (key,value) = let
  val (id,(edges,dist,color)) = unpack value
  fun increaseDist dist = case (Int.fromString dist) of SOME t => Int.toString(t+1) | NONE => "Integer.MAX_VALUE"
  fun parseEdges e = if e = "NULL" then [] else split (e,#",")
 in
  if color = "GRAY" then (
	map (fn k => printNode (k,"NULL",(increaseDist dist),color)) (parseEdges edges);
	printNode (id, edges,dist,"BLACK")
	)
   else 
  printNode (id,edges,dist,color)
end

fun mloop_reduce (key:string) = let
  val dist = ref "Integer.MAX_VALUE"
  val colour = ref "WHITE"
  val edge = ref "NULL"
  fun colorInt color = case color of "WHITE" => 0 | "GRAY" => 1 | "BLACK" => 2 | _ => ~1
  fun maxColor (c1, c2) = let
	  val v1 = colorInt c1
	  val v2 = colorInt c2
    in
	  if v1 < v2 then c2 else c1
  end
  fun minDist (d1, d2) = let 
	val v1 = case (Int.fromString d1) of SOME t => t | NONE => maxInt
	val v2 = case (Int.fromString d2) of SOME t => t | NONE => maxInt
  in
	if v1 < v2 then d1 else d2
  end
  fun update (edges:string,distance:string,color:string) = (if not (edges = "NULL") then edge:=edges else ();
            dist:= minDist (!dist, distance);
            colour := maxColor(!colour,color))
 in
    while (ReduceContext.nextValue()) do
        update (unpackInfo (ReduceContext.getInputValue()));
    reduceNode (key,!edge,!dist,!colour)
end
  




