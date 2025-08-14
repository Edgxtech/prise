package tech.edgx.prise.indexer.model

data class RefreshableView(
    val cronSchedule: String, // Cron for synced mode, e.g., "0 */15 * * *"
    val bootstrapCronSchedule: String // Cron for bootstrap mode, e.g., "0 */5 * * *"
)