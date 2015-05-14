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

(* Export map, reduce and combiner *)
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

(* Export reader, writer *)
val r = _export "reader_init": (MLton.Pointer.t * Int64.int * Int64.int -> unit) -> unit;
val _ = r (fn (file,offset,length) => Reader.init (file,offset,length))

val s = _export "reader_updateOffset_bytesConsumed": (Int64.int * Int64.int-> unit) -> unit;
val _ = s (fn (off,bytes) => Reader.updateOffset_bytesConsumed (off,bytes))

val setup = _export "reader_setup": (unit -> unit) -> unit;
val _ = setup (fn () => setUpReader())

val r = _export "reader_nextVal": (unit -> bool) -> unit;
val _ = r (fn () => mloop_read ())

val w = _export "writer_init": (MLton.Pointer.t -> unit) -> unit;
val _ = w (fn wr => Writer.init wr)

val mw = _export "mloop_write": (MLton.Pointer.t * MLton.Pointer.t -> unit) -> unit;
val _ = mw (fn (k,v) => mloop_write (k,v))

val v = runTask (useCombiner, useReader, useWriter)
