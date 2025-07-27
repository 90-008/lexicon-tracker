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

<span class="wsbadge {config.classes}">
    {config.text}
</span>

<style lang="postcss">
    @reference "../../app.css";
    .wsbadge {
        @apply text-sm font-semibold mt-1.5 px-2.5 py-0.5 rounded-full border;
    }
</style>
