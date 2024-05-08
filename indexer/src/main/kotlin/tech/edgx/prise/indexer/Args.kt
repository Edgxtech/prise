package tech.edgx.prise.indexer

import java.util.*

data class Args(private val params: Map<String, String>) {

    fun getArg(param: String): String? {
        check(params.containsKey(param)) { "No parameter: $param" }
        return params[param]
    }

    fun hasArg(arg: String): Boolean {
        return params.containsKey(arg)
    }

    companion object {
        fun parse(args: Array<String>): Args {
            val map = paramMap<String, String>()
            map.putAll(System.getenv())
            var i = 0
            while (i < args.size) {
                var argName = args[i]
                if (argName.startsWith("-")) argName = argName.substring(1)
                map[argName] = args[++i]
                i++
            }
            return Args(map)
        }

        fun <K, V> paramMap(): MutableMap<K, V> {
            return LinkedHashMap(16, 0.75f, false)
        }
    }
}
