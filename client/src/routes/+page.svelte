<script lang="ts">
    import { dev } from "$app/environment";
    import type { EventRecord, NsidCount } from "$lib/types";
    import { onMount, onDestroy } from "svelte";
    import { writable } from "svelte/store";
    import { PUBLIC_API_URL } from "$env/static/public";
    import { fetchEvents } from "$lib/api";
    import { createRegexFilter } from "$lib/filter";
    import StatsCard from "$lib/components/StatsCard.svelte";
    import StatusBadge from "$lib/components/StatusBadge.svelte";
    import EventCard from "$lib/components/EventCard.svelte";
    import FilterControls from "$lib/components/FilterControls.svelte";

    const events = writable(new Map<string, EventRecord>());
    let eventsList: NsidCount[] = $state([]);
    events.subscribe((value) => {
        eventsList = value
            .entries()
            .map(([nsid, event]) => ({
                nsid,
                ...event,
            }))
            .toArray();
        eventsList.sort((a, b) => b.count - a.count);
    });
    let per_second = $state(0);

    let all: EventRecord = $derived(
        eventsList.reduce(
            (acc, event) => {
                return {
                    last_seen:
                        acc.last_seen > event.last_seen
                            ? acc.last_seen
                            : event.last_seen,
                    count: acc.count + event.count,
                    deleted_count: acc.deleted_count + event.deleted_count,
                };
            },
            {
                last_seen: 0,
                count: 0,
                deleted_count: 0,
            },
        ),
    );
    let error: string | null = $state(null);
    let filterRegex = $state("");
    let dontShowBsky = $state(false);

    let websocket: WebSocket | null = null;
    let isStreamOpen = $state(false);
    let websocketStatus = $state<
        "connecting" | "connected" | "disconnected" | "error"
    >("disconnected");
    const connectToStream = async () => {
        if (isStreamOpen) return;
        websocketStatus = "connecting";
        websocket = new WebSocket(
            `${dev ? "ws" : "wss"}://${PUBLIC_API_URL}/stream_events`,
        );
        websocket.binaryType = "arraybuffer";
        websocket.onopen = () => {
            console.log("ws opened");
            isStreamOpen = true;
            websocketStatus = "connected";
        };
        websocket.onmessage = async (event) => {
            const jsonData = JSON.parse(event.data);

            if (jsonData.per_second > 0) {
                per_second = jsonData.per_second;
            }
            events.update((map) => {
                for (const [nsid, event] of Object.entries(jsonData.events)) {
                    map.set(nsid, event as EventRecord);
                }
                return map;
            });
        };
        websocket.onerror = (error) => {
            console.error("ws error:", error);
            websocketStatus = "error";
        };
        websocket.onclose = () => {
            console.log("ws closed");
            isStreamOpen = false;
            websocketStatus = "disconnected";
        };
    };

    const loadData = async () => {
        try {
            error = null;
            const data = await fetchEvents();
            per_second = data.per_second;
            events.update((map) => {
                for (const [nsid, event] of Object.entries(data.events)) {
                    map.set(nsid, event);
                }
                return map;
            });
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
        connectToStream();
    });

    onDestroy(() => {
        // Close WebSocket connection
        if (websocket) {
            websocket.close();
        }
    });

    const filterEvents = (events: NsidCount[]) => {
        let filtered = events;

        // Apply regex filter
        if (filterRegex) {
            const regex = createRegexFilter(filterRegex);
            if (regex) {
                filtered = filtered.filter((e) => regex.test(e.nsid));
            }
        }

        // Apply app.bsky filter
        if (dontShowBsky) {
            filtered = filtered.filter((e) => !e.nsid.startsWith("app.bsky."));
        }

        return filtered;
    };
</script>

<svelte:head>
    <title>lexicon tracker</title>
    <meta
        name="description"
        content="tracks bluesky jetstream events by collection"
    />
</svelte:head>

<div class="md:max-w-[61vw] mx-auto p-2">
    <header class="text-center mb-8">
        <h1 class="text-4xl font-bold mb-2 text-gray-900">lexicon tracker</h1>
        <p class="text-lg text-gray-600">
            tracks lexicons seen on the jetstream
        </p>
    </header>

    <div
        class="min-w-fit grid grid-cols-2 xl:grid-cols-4 gap-2 2xl:gap-6 2xl:mx-16 mb-8"
    >
        <StatsCard
            title="total creation"
            value={all.count}
            colorScheme="green"
        />
        <StatsCard
            title="total deletion"
            value={all.deleted_count}
            colorScheme="red"
        />
        <StatsCard
            title="per second"
            value={per_second}
            colorScheme="turqoise"
        />
        <StatsCard
            title="unique collections"
            value={eventsList.length}
            colorScheme="orange"
        />
    </div>

    {#if error}
        <div
            class="bg-red-100 border border-red-300 text-red-700 px-4 py-3 rounded-lg mb-6"
        >
            <p>Error: {error}</p>
        </div>
    {/if}

    {#if eventsList.length > 0}
        <div class="mb-8">
            <div class="flex flex-wrap items-center gap-3 mb-3">
                <h2 class="text-2xl font-bold text-gray-900">seen lexicons</h2>
                <StatusBadge status={websocketStatus} />
            </div>
            <FilterControls
                {filterRegex}
                {dontShowBsky}
                onFilterChange={(value) => (filterRegex = value)}
                onBskyToggle={() => (dontShowBsky = !dontShowBsky)}
            />
            <div
                class="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4 gap-4"
            >
                {#each filterEvents(eventsList) as event, index (event.nsid)}
                    <EventCard {event} {index} />
                {/each}
            </div>
        </div>
    {:else}
        <div class="text-center py-12 bg-gray-50 rounded-lg">
            <div class="text-gray-400 text-4xl mb-4">📊</div>
            <p class="text-gray-600">no events tracked yet. try refreshing?</p>
        </div>
    {/if}
</div>

<footer class="py-2 border-t border-gray-200 text-center">
    <p class="text-gray-600 text-sm">
        source code <a
            href="https://tangled.sh/@poor.dog/nsid-tracker"
            target="_blank"
            rel="noopener noreferrer"
            class="text-blue-600 hover:text-blue-800 underline"
            >@poor.dog/nsid-tracker</a
        >
    </p>
</footer>
