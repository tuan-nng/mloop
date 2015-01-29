(*
fun mloop_map (key:string, value:string) = 
    let
        val splitter = String.tokens(fn c => c = #" ")
        val words = splitter value
        
    in   
        map (fn word => MapContext.emit (word,"1")) words;
        ()
    end
*)
fun mloop_map () =                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
    let
        val line = MapContext.getInputValue ()
            val splitter = String.tokens(fn c => c = #" ")
            val words = splitter line

        in   
            map (fn word => MapContext.emit (word,"1")) words;
            ()
        end                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           

    fun mloop_reduce_ () = 
        let
            fun fromString str = let 
                val x = Int.fromString str
                in 
                    Option.getOpt(x,0)
            end
        val sum = foldl (fn (str:string, s:int) => s + (fromString str)) 0 (ReduceContext.getValueSet ())
    in
        ReduceContext.emit((ReduceContext.getInputKey ()), Int.toString(sum))
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

fun init_map addr = MapContext.setAddress addr
fun init_reduce (addr:MLton.Pointer.t) = ReduceContext.setAddress addr

fun convertToList (valueSet:MLton.Pointer.t, size:int):string list = 
    let
        fun parseData (data:string list, p:MLton.Pointer.t, len:int, offset:int):string list = if offset < len then 
            let val v = fetchCString (MLton.Pointer.getPointer(p,offset)) in parseData(v::data, p, len, offset + 1) end
            else data
    in 
        parseData (nil, valueSet, size, 0)
    end
(*
fun mloop_map_ (key:MLton.Pointer.t, value:MLton.Pointer.t) = 
    (mloop_map (fetchCString key, fetchCString value);MapContext.flushAll ()) 
*)

fun mloop_map_ () = 
    (mloop_map (); MapContext.flushAll ())

fun mloop_combine_ (address:MLton.Pointer.t, key:MLton.Pointer.t, valueSet:MLton.Pointer.t, size:int) = 
    let 
        val _ = ReduceContext.setAddress address
    in 
        mloop_combine (fetchCString (key), convertToList(valueSet, size))
    end
(*
fun mloop_reduce_ (key:MLton.Pointer.t, values:MLton.Pointer.t, len:int) = 
    let 
        val k = fetchCString (key)
        val v = convertToList (values,len)
    in 
        (mloop_reduce (k, v); ReduceContext.flushAll ())
    end
*)
val m_init = _export "init_map": (MLton.Pointer.t  -> unit) -> unit;
val _ = m_init (fn addr => init_map addr)


val r_init = _export "init_reduce": (MLton.Pointer.t  -> unit) -> unit;
val _ = r_init (fn addr => init_reduce addr)

(*
val m = _export "mloop_map_": (MLton.Pointer.t * MLton.Pointer.t  -> unit) -> unit;
val _ = m (fn (key,value) => mloop_map_ (key,value))

val r = _export "mloop_reduce_": (MLton.Pointer.t * MLton.Pointer.t * int-> unit) -> unit;
val _ = r (fn (key,values,len) => mloop_reduce_ (key,values,len))
*)

val m = _export "mloop_map_": (unit  -> unit) -> unit;
val _ = m (fn () => mloop_map_ ())


val r = _export "mloop_reduce_": (unit  -> unit) -> unit;
val _ = r (fn () => mloop_reduce_ ())


val c = _export "mloop_combine_": (MLton.Pointer.t * MLton.Pointer.t * MLton.Pointer.t * int  -> unit) -> unit;
val _ = c (fn (address, key,valueSet,size) => mloop_combine_ (address, key,valueSet,size))

val runTask = _import "runTask" public: bool*bool*bool -> int;
val v = runTask (false, true, false)
