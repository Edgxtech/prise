package tech.edgx.prise.indexer.model

enum class DexEnum(val code: Int, val nativeName: String, val friendlyName: String, val abbrev: String) {
    WINGRIDERS(0, "wingriders","Wingriders", "Wingriders"),
    SUNDAESWAP(1, "sundaeswap","SundaeSwap", "SundaeSwap"),
    MINSWAP(2,"minswap","MinSwap", "MinSwap"),
    MINSWAPV2(3, "minswapv2","MinSwapV2", "MinswapV2" ),
    SATURNSWAP(4, "saturnswap", "Saturnswap", "Saturnswap")
}