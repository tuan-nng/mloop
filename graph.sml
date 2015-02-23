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

fun unpack str = let
  val k::v::_ = tokens str
  val e::d::c::_ = split (v,#"|")
  val dist = case (Int.fromString d) of SOME t => t | NONE => maxInt
  val edge_list = if e = "NULL" then [] else split (e,#",")
in
  {id=k,edges=edge_list,distance=dist,color=c}
end

fun pack {id=k:string,edges=e:string list,distance=d:int,color=c:string} = (join (e,",")) ^ "|" ^ (Int.toString d) ^ "|" ^ c
fun newNode (identity,e, dist,c) = {id=identity,edges=e,distance=dist,color=c}

fun printNode node = MapContext.emit ((#id node), pack node)
fun reduceNode node = ReduceContext.emit ((#id node), pack node)

fun mloop_map (key,value) = let
  val {id=i,edges=e,distance=dist,color=c} = unpack value
 in
  if c = "GRAY" then (
	map (fn id => printNode (newNode(id,["NULL"], dist + 1, c))) e;
	printNode (newNode(i, e, dist, "BLACK"))
	)
   else 
  printNode {id=i,edges=e,distance=dist,color=c}
end

fun maxColor (_, "BLACK") = "BLACK"
  | maxColor ("BLACK", _ ) = "BLACK"
  | maxColor ("GRAY", _) = "GRAY"
  | maxColor (_, "GRAY") = "GRAY"
  | maxColor (_,_) = "WHITE"

fun mloop_reduce (key:string) = let
  val dist = ref maxInt
  val colour = ref "WHITE"
  val edge = ref [] : string list ref
  fun update node = (if not ((#edges node) = []) then edge:=(#edges node) else ();
            if (#distance node) < (!dist) then dist:=(#distance node) else ();
            colour := maxColor(!colour,#color node))
 in
    while (ReduceContext.nextValue()) do
        update (unpack (key ^ " " ^ (ReduceContext.getInputValue())));
    reduceNode {id=key,edges=(!edge),distance=(!dist),color=(!colour)}
end

  




