<script lang="ts">
    import { dev } from "$app/environment";
    import type { EventRecord } from "$lib/types";
    import { onMount } from "svelte";

    let events: EventRecord[] = $state([]);
    let all: EventRecord = $derived(
        events.reduce(
            (acc, event) => {
                return {
                    nsid: "*",
                    last_seen:
                        acc.last_seen > event.last_seen
                            ? acc.last_seen
                            : event.last_seen,
                    count: acc.count + event.count,
                    deleted_count: acc.deleted_count + event.deleted_count,
                };
            },
            {
                nsid: "*",
                last_seen: 0,
                count: 0,
                deleted_count: 0,
            },
        ),
    );
    let error: string | null = $state(null);
    let dontShowBsky = $state(false);

    const loadData = async () => {
        try {
            error = null;

            const response = dev
                ? await fetch("http://localhost:3000/events")
                : await fetch("/api/events");
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();
            events = data.events;
        } catch (err) {
            error =
                err instanceof Error
                    ? err.message
                    : "an unknown error occurred";
            console.error("error loading data:", err);
        }
    };

    onMount(() => {
        loadData();
    });

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

<div class="md:max-w-[60vw] mx-auto p-2">
    <header class="text-center mb-8">
        <h1 class="text-4xl font-bold mb-2 text-gray-900">
            bluesky event tracker
        </h1>
        <p class="text-gray-600">
            tracking of bluesky events by collection from the jetstream
        </p>
    </header>

    <div class="mx-auto w-fit grid grid-cols-2 md:grid-cols-3 gap-5 mb-8">
        <div
            class="bg-gradient-to-r from-green-50 to-green-100 p-3 md:p-6 rounded-lg border border-green-200"
        >
            <h3 class="text-base font-medium text-green-700 mb-2">
                total creation
            </h3>
            <p class="text-3xl font-bold text-green-900">
                {// eslint-disable-next-line @typescript-eslint/no-non-null-asserted-optional-chain
                formatNumber(all.count)}
            </p>
        </div>
        <div
            class="bg-gradient-to-r from-red-50 to-red-100 p-3 md:p-6 rounded-lg border border-red-200"
        >
            <h3 class="text-base font-medium text-red-700 mb-2">
                total deletion
            </h3>
            <p class="text-3xl font-bold text-red-900">
                {// eslint-disable-next-line @typescript-eslint/no-non-null-asserted-optional-chain
                formatNumber(all.deleted_count)}
            </p>
        </div>
        <div
            class="bg-gradient-to-r from-orange-50 to-orange-100 p-3 md:p-6 rounded-lg border border-orange-200"
        >
            <h3 class="text-base font-medium text-orange-700 mb-2">
                unique collections
            </h3>
            <p class="text-3xl font-bold text-orange-900">
                {formatNumber(events.length)}
            </p>
        </div>
    </div>

    <div class="w-fit flex flex-col items-center mx-auto mb-8">
        <button
            onclick={loadData}
            class="w-fit bg-blue-600 hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed text-white px-6 py-3 rounded-lg font-medium transition-colors"
        >
            refresh
        </button>
        <!-- svelte-ignore a11y_click_events_have_key_events -->
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <button
            onclick={() => (dontShowBsky = !dontShowBsky)}
            class="mt-2 bg-yellow-100 hover:bg-yellow-200 px-2 py-1 rounded-full"
        >
            <input bind:checked={dontShowBsky} type="checkbox" />
            <span class="ml-1"> don't show app.bsky.* </span>
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
            <div class="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                {#each events.filter((e) => {
                    return dontShowBsky ? !e.nsid.startsWith("app.bsky.") : true;
                }) as event, index (event.nsid)}
                    <div
                        class="mx-auto md:mx-0 bg-white border border-gray-200 rounded-lg md:p-6 hover:shadow-lg transition-shadow duration-200 hover:-translate-y-1 transform"
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
                            last: {formatTimestamp(event.last_seen)}
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
