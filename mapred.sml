val getInputValueMapContext = _import "mapContext_getInputValue" public: MLton.Pointer.t -> MLton.Pointer.t;
val getInputKeyMapContext = _import "mapContext_getInputKey" public: MLton.Pointer.t -> MLton.Pointer.t;

val emitData = _import "context_emit" public : MLton.Pointer.t * string * string -> unit;
val emitBatch = _import "context_emitBatch" public : MLton.Pointer.t * string array * string array * int-> unit;

val getInputValueReduceContext = _import "reduceContext_getInputValue" public : MLton.Pointer.t -> MLton.Pointer.t;
val getValueSetReduceContext = _import "reduceContext_getValueSet" public : MLton.Pointer.t -> MLton.Pointer.t;
val nextValueReduceContext = _import "reduceContext_nextValue" public : MLton.Pointer.t -> bool;
val getInputKeyReduceContext = _import "reduceContext_getInputKey" public : MLton.Pointer.t -> MLton.Pointer.t;


fun strlen p =
   let
      fun loop i =
         if 0w0 = MLton.Pointer.getWord8 (p, i) then i else loop (i + 1)
   in
      loop 0
   end

fun fetchCString p =
   CharVector.tabulate (strlen p, fn i =>
                        Byte.byteToChar (MLton.Pointer.getWord8 (p, i)))

fun cstring content = content ^ "\000"

fun convertToList (valueSet:MLton.Pointer.t, size:int):string list = 
    let
        fun parseData (data:string list, p:MLton.Pointer.t, len:int, offset:int):string list = if offset < len then 
            let val v = fetchCString (MLton.Pointer.getPointer(p,offset)) in parseData(v::data, p, len, offset + 1) end
            else data
    in
        parseData (nil, valueSet, size, 0)
    end

structure Context = struct
    local 
        val batchSize = 1500
        val k = Array.array(batchSize,"")
        val v = Array.array(batchSize,"")
        val len = ref 0
    in
        fun flushAll (address: MLton.Pointer.t) = 
                if (!len) > 0 then 
                    (emitBatch(address, k, v, !len); len := 0)
                else ()
        fun emit (address:MLton.Pointer.t, key:string, value:string) = 
            let 
                val ckey = cstring key
                val cvalue = cstring value
            in
                Array.update (k,!len,ckey);
                Array.update (v,!len,cvalue);
                len := (!len) + 1;
                if (!len + 1) >= batchSize then flushAll (address)
                else ()
            end
        fun reset () = len := 0
    end 
end
structure ControlledValue = struct
  datatype value = Content of {set: MLton.Pointer.t -> unit, get: unit -> MLton.Pointer.t}
  exception InvalidInit of string
  fun mk m (Content t) = m t
  fun newValue (override:bool) = let 
        val init = ref MLton.Pointer.null
        val valid = ref false
    in 
          Content {set = fn add:MLton.Pointer.t => if override orelse (not (!valid)) then (init := add;valid := true) else raise InvalidInit "Already initialized.",
                    get = fn () => !init}
    end
end

structure MapContext = struct
    open ControlledValue
    val addr = newValue (false)
    fun getAddress () = mk#get addr ()
    fun setAddress (add:MLton.Pointer.t) = mk#set addr add

    fun getInputValue ():string = fetchCString (getInputValueMapContext (getAddress ()))
    fun getInputKey ():string = fetchCString (getInputKeyMapContext(getAddress()))

    fun emit (key:string, value:string) = emitData  (getAddress (), cstring key, cstring value)

(*
    fun emit (key:string, value:string) = Context.emit (getAddress (), key, value)

    fun flushAll () = Context.flushAll (getAddress ())
     *)      
end

structure ReduceContext = struct
    open ControlledValue
    val addr = newValue (true)

    fun getAddress () = mk#get addr ()
    fun setAddress (add:MLton.Pointer.t) = mk#set addr add

    fun getInputValue ():string = fetchCString (getInputValueReduceContext (getAddress ()))
    fun getInputKey ():string = fetchCString (getInputKeyReduceContext (getAddress ()))
    fun nextValue ():bool = nextValueReduceContext (getAddress ())

    
    fun getValueSet ():string list = let
        fun push (l) = if (nextValue ()) then push((getInputValue ())::l)
		else l
      in 
        push []
      end 
      
    fun emit (key:string, value:string) = emitData  (getAddress (), cstring key, cstring value)

(*
    fun emit (key:string, value:string) = Context.emit (getAddress (), key, value)
    fun flushAll () = Context.flushAll (getAddress ())
*)
end