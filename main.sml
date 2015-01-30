
fun mloop_map (key:string, value:string) = 
    let
        val splitter = String.tokens(fn c => c = #" ")
        val words = splitter value
        
    in   
        map (fn word => MapContext.emit (word,"1")) words;
        ()
    end

fun mloop_reduce (key:string, values:string list)=
    let
        fun fromString str = let 
            val x = Int.fromString str
            in 
                Option.getOpt(x,0)
            end
        val sum = foldl (fn (str:string, s:int) => s + (fromString str)) 0 values
    in
        ReduceContext.emit(key, Int.toString(sum))
    end

fun mloop_combine (key:string, values:string list) = 
    mloop_reduce (key, values)

fun init_map addr = (MapContext.setAddress addr; Context.reset ())
fun init_reduce (addr:MLton.Pointer.t) = (ReduceContext.setAddress addr; Context.reset ())

fun convertToList (valueSet:MLton.Pointer.t, size:int):string list = 
    let
        fun parseData (data:string list, p:MLton.Pointer.t, len:int, offset:int):string list = if offset < len then 
            let val v = fetchCString (MLton.Pointer.getPointer(p,offset)) in parseData(v::data, p, len, offset + 1) end
            else data
    in 
        parseData (nil, valueSet, size, 0)
    end

fun mloop_map_ (key:MLton.Pointer.t, value:MLton.Pointer.t) = mloop_map (fetchCString key, fetchCString value)
(*    
(mloop_map (fetchCString key, fetchCString value);MapContext.flushAll ()) 
*)


fun mloop_combine_ (address:MLton.Pointer.t, key:MLton.Pointer.t, valueSet:MLton.Pointer.t, size:int) = 
    let 
        val _ = ReduceContext.setAddress address
    in 
        mloop_combine (fetchCString (key), convertToList(valueSet, size))
    end

fun mloop_reduce_ (key:MLton.Pointer.t, values:MLton.Pointer.t, len:int) = 
    let 
        val k = fetchCString (key)
        val v = convertToList (values,len)
    in 
        mloop_reduce (k, v)
    end

(*
fun mloop_reduce_ (key:MLton.Pointer.t) = 
    let 
        val k = fetchCString (key)
        val v = ReduceContext.getValueSet ()
    in 
        mloop_reduce (k, v)
    end
*)
val m_init = _export "init_map": (MLton.Pointer.t  -> unit) -> unit;
val _ = m_init (fn addr => init_map addr)


val r_init = _export "init_reduce": (MLton.Pointer.t  -> unit) -> unit;
val _ = r_init (fn addr => init_reduce addr)


val m = _export "mloop_map_": (MLton.Pointer.t * MLton.Pointer.t  -> unit) -> unit;
val _ = m (fn (key,value) => mloop_map_ (key,value))


val r = _export "mloop_reduce_": (MLton.Pointer.t * MLton.Pointer.t * int-> unit) -> unit;
val _ = r (fn (key,values,len) => mloop_reduce_ (key,values,len))


(*
val r = _export "mloop_reduce_": (MLton.Pointer.t  -> unit) -> unit;
val _ = r (fn (k) => mloop_reduce_ (k))
*)

val c = _export "mloop_combine_": (MLton.Pointer.t * MLton.Pointer.t * MLton.Pointer.t * int  -> unit) -> unit;
val _ = c (fn (address, key,valueSet,size) => mloop_combine_ (address, key,valueSet,size))
(*
val m_f = _export "map_flush": (unit  -> unit) -> unit;
val _ = m_f (fn () => MapContext.flushAll ())

val r_f = _export "reduce_flush": (unit  -> unit) -> unit;
val _ = r_f (fn () => ReduceContext.flushAll ())
*)

val runTask = _import "runTask" public: bool*bool*bool -> int;
val v = runTask (false, true, false)
