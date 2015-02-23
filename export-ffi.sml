fun init_map addr = MapContext.setAddress addr
fun init_reduce (addr:MLton.Pointer.t) = ReduceContext.setAddress addr


fun mloop_map_ (key:MLton.Pointer.t, value:MLton.Pointer.t) = mloop_map (fetchCString key, fetchCString value)

fun mloop_combine_ (address:MLton.Pointer.t, key:MLton.Pointer.t) = 
    let 
        val _ = ReduceContext.setAddress address
    in 
        mloop_combine (fetchCString (key))
    end

fun mloop_reduce_ (key:MLton.Pointer.t) = mloop_reduce (fetchCString (key))

val m_init = _export "init_map": (MLton.Pointer.t  -> unit) -> unit;
val _ = m_init (fn addr => init_map addr)


val r_init = _export "init_reduce": (MLton.Pointer.t  -> unit) -> unit;
val _ = r_init (fn addr => init_reduce addr)


val m = _export "mloop_map_": (MLton.Pointer.t * MLton.Pointer.t  -> unit) -> unit;
val _ = m (fn (key,value) => mloop_map_ (key,value))

val r = _export "mloop_reduce_": (MLton.Pointer.t  -> unit) -> unit;
val _ = r (fn (k) => mloop_reduce_ (k))


val c = _export "mloop_combine_": (MLton.Pointer.t * MLton.Pointer.t  -> unit) -> unit;
val _ = c (fn (address, key) => mloop_combine_ (address, key))

val v = runTask (false, false, false)