<script lang="ts">
    interface Props {
        status: "connecting" | "connected" | "disconnected" | "error";
    }

    let { status }: Props = $props();

    const statusConfig = {
        connected: {
            text: "stream live",
            classes: "bg-green-100 text-green-800 border-green-200",
        },
        connecting: {
            text: "stream connecting",
            classes: "bg-yellow-100 text-yellow-800 border-yellow-200",
        },
        error: {
            text: "stream errored",
            classes: "bg-red-100 text-red-800 border-red-200",
        },
        disconnected: {
            text: "stream offline",
            classes: "bg-gray-100 text-gray-800 border-gray-200",
        },
    };

    const config = $derived(statusConfig[status]);
</script>

<div class="flex flex-row items-center gap-2 wsbadge {config.classes}">
    <!-- connecting spinner -->
    {#if status === "connecting"}
        <div
            class="animate-spin rounded-full h-4 w-4 border-b-2 border-yellow-800"
        ></div>
    {/if}
    <!-- status text -->
    <span>{config.text}</span>
</div>
