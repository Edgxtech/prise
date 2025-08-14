package tech.edgx.prise.indexer.model

enum class DexEnum(val code: Int, val nativeName: String, val friendlyName: String, val abbrev: String) {
    WINGRIDERS(0, "wingriders","Wingriders", "Wingriders"),
    SUNDAESWAP(1, "sundaeswap","SundaeSwap", "SundaeSwap"),
    MINSWAP(2, "minswap","MinSwap", "MinSwap"),
    MINSWAPV2(3, "minswapv2","MinSwapV2", "MinswapV2"),
    SATURNSWAP(4, "saturnswap", "Saturnswap", "Saturnswap");

    companion object {
        fun fromId(id: Int): DexEnum = values().find { it.code == id }
            ?: throw IllegalArgumentException("No DexEnum for id $id")
        fun fromName(name: String): DexEnum = values().find { it.nativeName == name }
            ?: throw IllegalArgumentException("No DexEnum for name $name")
    }
}