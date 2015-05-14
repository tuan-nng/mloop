fun setUpReader () = let
    val offset = Reader.getOffset()
    val reader = Reader.get()
    in
        if offset = 0 then () 
        else (seekHdfs(reader, offset - 1); Reader.readLine ();())
    end

(*
fun readNext () = let 
    val start = Reader.getOffsetNow ()
    val endOffset = (Reader.length()) + (Reader.getOffset())
    fun append (result, i) =
        let                
            val line = Reader.readLine ()
            val offNow = Reader.getOffsetNow ()
            val bytes_read = Reader.getBytes_read() 
            val end_of_file = bytes_read = 0
            val end_of_split = offNow >= endOffset
        in 
            case (end_of_file, end_of_split, i = 1) of
                (true,_,_) => if i = 3 then (result,false) else (result,true)
                | (false,true,_) => (result ^ line ^ "\n", true) 
                | (false,false,true) => (result ^ line ^ "\n", true)
                | _ => append (result ^ line ^ "\n", i - 1)
             
        end

    val (value,hasNext) = if start < endOffset then append("",3) else ("",false)
    in
        (*print ((Int64.toString start) ^ ":" ^ value);*)
        if hasNext then setKeyValue (Reader.get(),cstring (Int64.toString start),cstring value)
        else ();
        hasNext
    end
*)
fun mloop_read () = let 
    val start = Reader.getOffsetNow ()
    val endOffset = (Reader.length()) + (Reader.getOffset())
    fun getLine () = (Reader.readLine (),true)

    val (value,hasNext) = if start < endOffset then getLine() else ("",false)
    in
        if hasNext then setKeyValue (Reader.get(),cstring (Int64.toString start),cstring value)
        else ();
        hasNext
    end

fun mloop_write (key,value) = Writer.emit (fetchCString key, fetchCString value)

val useReader = true
val useWriter = true
