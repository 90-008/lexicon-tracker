<script lang="ts">
    import type { EventRecord } from "$lib/db.js";

    interface Props {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        data: any;
    }
    let { data }: Props = $props();

    let events: EventRecord[] = $state(data.events);
    let totalEvents = $state(data.totalEvents);
    let error: string | null = $state(null);

    const loadData = async () => {
        try {
            error = null;

            const response = await fetch("/api/events");
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            events = data.events;
            totalEvents = data.totalEvents;
        } catch (err) {
            error =
                err instanceof Error
                    ? err.message
                    : "an unknown error occurred";
            console.error("error loading data:", err);
        }
    };

    const formatNumber = (num: number): string => {
        return num.toLocaleString();
    };

    const formatTimestamp = (timestamp: number): string => {
        return new Date(timestamp / 1000).toLocaleString();
    };
</script>

<svelte:head>
    <title>bluesky event tracker</title>
    <meta name="description" content="tracks bluesky events by collection" />
</svelte:head>

<div class="max-w-[60vw] mx-auto p-2">
    <header class="text-center mb-8">
        <h1 class="text-4xl font-bold mb-2 text-gray-900">
            bluesky event tracker
        </h1>
        <p class="text-gray-600">
            tracking of bluesky events by collection from the jetstream
        </p>
    </header>

    <div class="mx-auto w-fit grid grid-cols-1 md:grid-cols-2 mb-8">
        <div
            class="bg-gradient-to-r from-blue-50 to-blue-100 p-6 rounded-lg border border-blue-200"
        >
            <h3 class="text-base font-medium text-blue-700 mb-2">
                total events
            </h3>
            <p class="text-3xl font-bold text-blue-900">
                {formatNumber(totalEvents)}
            </p>
        </div>
        <div
            class="bg-gradient-to-r from-green-50 to-green-100 p-6 rounded-lg border border-green-200"
        >
            <h3 class="text-base font-medium text-green-700 mb-2">
                unique collections
            </h3>
            <p class="text-3xl font-bold text-green-900">
                {formatNumber(events.length)}
            </p>
        </div>
    </div>

    <div class="text-center mb-8">
        <button
            onclick={loadData}
            class="bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-white px-6 py-3 rounded-lg font-medium transition-colors"
        >
            refresh
        </button>
    </div>

    {#if error}
        <div
            class="bg-red-100 border border-red-300 text-red-700 px-4 py-3 rounded-lg mb-6"
        >
            <p>Error: {error}</p>
        </div>
    {/if}

    {#if events.length > 0}
        <div class="mb-8">
            <h2 class="text-2xl font-bold mb-6 text-gray-900">
                events by collection
            </h2>
            <div class="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
                {#each events as event, index (event.nsid)}
                    <div
                        class="bg-white border border-gray-200 rounded-lg p-6 hover:shadow-lg transition-shadow duration-200 hover:-translate-y-1 transform"
                    >
                        <div class="flex justify-between items-start mb-3">
                            <div
                                class="text-sm font-bold text-blue-600 bg-blue-100 px-3 py-1 rounded-full"
                            >
                                #{index + 1}
                            </div>
                        </div>
                        <div
                            class="font-mono text-sm text-gray-700 mb-2 break-all leading-relaxed"
                        >
                            {event.nsid}
                        </div>
                        <div class="text-lg font-bold text-green-600">
                            {formatNumber(event.count)} created
                        </div>
                        <div class="text-lg font-bold text-red-600 mb-3">
                            {formatNumber(event.deleted_count)} deleted
                        </div>
                        <div class="text-xs text-gray-500">
                            last: {formatTimestamp(event.timestamp)}
                        </div>
                    </div>
                {/each}
            </div>
        </div>
    {:else}
        <div class="text-center py-12 bg-gray-50 rounded-lg">
            <div class="text-gray-400 text-4xl mb-4">📊</div>
            <p class="text-gray-600">no events tracked yet.</p>
        </div>
    {/if}
</div>
