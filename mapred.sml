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

    fun emit (key:string, value:string) = emitData  (getAddress (), key, value)       
        
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
    (*
    fun getValueSet ():string list = let
        val res = ref nil : string list ref
        fun push (value:string, nil) = [value]
            | push (value:string, l) = value::l
      in 
        while (nextValue ()) do 
            res := push (getInputValue (), !res);
        !res
      end
      *)

    fun emit (key:string, value:string) = emitData  (getAddress (), key, value)
    

end