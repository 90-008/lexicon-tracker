<script lang="ts">
    interface Props {
        status: "connecting" | "connected" | "disconnected" | "error";
    }

    let { status }: Props = $props();

    const statusConfig = {
        connected: {
            text: "stream live",
            classes:
                "bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200 border-green-200 dark:border-green-800",
        },
        connecting: {
            text: "stream connecting",
            classes:
                "bg-yellow-100 dark:bg-yellow-900 text-yellow-800 dark:text-yellow-200 border-yellow-200 dark:border-yellow-800",
        },
        error: {
            text: "stream errored",
            classes:
                "bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-200 border-red-200 dark:border-red-800",
        },
        disconnected: {
            text: "stream offline",
            classes:
                "bg-gray-100 dark:bg-gray-900 text-gray-800 dark:text-gray-200 border-gray-200 dark:border-gray-800",
        },
    };

    const config = $derived(statusConfig[status]);
</script>

<div class="flex flex-row items-center gap-2 wsbadge {config.classes}">
    <!-- connecting spinner -->
    {#if status === "connecting"}
        <div
            class="animate-spin rounded-full h-4 w-4 border-b-2 border-yellow-800 dark:border-yellow-200"
        ></div>
    {/if}
    <!-- status text -->
    <span>{config.text}</span>
</div>
