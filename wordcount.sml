
fun mloop_map (key:string, value:string) = 
    let
        val splitter = String.tokens(fn c => Char.isSpace c)
        val words = splitter value
        
    in   
        map (fn word => MapContext.emit (word,"1")) words;
        ()
    end

fun mloop_reduce (key:string)=
    let
        fun fromString str = let 
            val x = Int.fromString str
            in 
                Option.getOpt(x,0)
            end
        val sum = ref 0
    in
        while (ReduceContext.nextValue()) do
            sum := (!sum) + (fromString (ReduceContext.getInputValue()));
        ReduceContext.emit(key, Int.toString(!sum))
    end

fun mloop_combine (key:string) = 
    mloop_reduce (key)

val useCombiner = true
