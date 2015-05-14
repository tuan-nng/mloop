
fun mloop_map (key:string, value:string) = 
    let
        val splitter = String.tokens(fn c => Char.isSpace c)
        val words = splitter value
        fun toInt number = let 
            val x = IntInf.fromString number
            in 
                Option.getOpt(x,0)
            end
        fun sum (from:IntInf.int,to:IntInf.int, result:IntInf.int) = if from > to then result
            else sum (from + 1, to, result + from);
        val v = sum (toInt (List.nth(words,0)), toInt (List.nth(words,1)), 0);
    in   
        MapContext.emit ("", IntInf.toString v)
    end

fun mloop_reduce (key:string)=
    let
        fun fromString str = let 
            val x = IntInf.fromString str
            in 
                Option.getOpt(x,0)
            end
        val sum = ref 0 : IntInf.int ref
    in
        while (ReduceContext.nextValue()) do
            sum := (!sum) + (fromString (ReduceContext.getInputValue()));
        ReduceContext.emit(key, IntInf.toString(!sum))
    end



