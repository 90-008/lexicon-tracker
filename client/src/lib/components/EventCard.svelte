<script lang="ts">
    import type { EventRecord } from "$lib/types";
    import { onMount, onDestroy } from "svelte";

    interface Props {
        nsid: string;
        event: EventRecord;
        index: number;
        formatNumber: (num: number) => string;
        formatTimestamp: (timestamp: number) => string;
    }

    let { nsid, event, index, formatNumber, formatTimestamp }: Props = $props();

    // Border animation state
    let borderThickness = $state(0);
    let lastEventTime = $state(0);
    let lastCount = $state(0);
    let lastDeletedCount = $state(0);
    let decayTimer: ReturnType<typeof setTimeout> | null = null;
    let isAnimating = $state(false);

    // Constants for border behavior
    const MAX_BORDER_THICKNESS = 7; // Maximum border thickness in pixels
    const INITIAL_THICKNESS_ADD = 2; // How much thickness to add for first/slow events
    const RAPID_SUCCESSION_THRESHOLD = 50; // ms - events faster than this are considered rapid
    const DECAY_RATE = 0.1; // How much thickness to remove per decay tick
    const DECAY_INTERVAL = 45; // ms between decay ticks

    // Track when event data changes
    $effect(() => {
        const currentTime = Date.now();
        const hasChanged =
            event.count !== lastCount ||
            event.deleted_count !== lastDeletedCount;

        if (hasChanged && (lastCount > 0 || lastDeletedCount > 0)) {
            const timeSinceLastUpdate = currentTime - lastEventTime;

            // Calculate how much thickness to add based on timing
            let thicknessToAdd;
            if (timeSinceLastUpdate < RAPID_SUCCESSION_THRESHOLD) {
                // Rapid succession - add less thickness with a decay factor
                const rapidnessFactor = Math.max(
                    0.1,
                    timeSinceLastUpdate / RAPID_SUCCESSION_THRESHOLD,
                );
                thicknessToAdd = INITIAL_THICKNESS_ADD * rapidnessFactor * 0.5;
            } else {
                // Normal/slow event - add full thickness
                thicknessToAdd = INITIAL_THICKNESS_ADD;
            }

            // Add thickness but cap at maximum
            borderThickness = Math.min(
                MAX_BORDER_THICKNESS,
                borderThickness + thicknessToAdd,
            );
            isAnimating = true;
            lastEventTime = currentTime;

            // Start/restart continuous decay
            if (decayTimer) {
                clearTimeout(decayTimer);
            }
            decayTimer = setTimeout(startDecay, DECAY_INTERVAL);
        }

        lastCount = event.count;
        lastDeletedCount = event.deleted_count;
    });

    const startDecay = () => {
        if (borderThickness <= 0) {
            isAnimating = false;
            decayTimer = null;
            return;
        }

        borderThickness = Math.max(0, borderThickness - DECAY_RATE);

        if (borderThickness > 0) {
            decayTimer = setTimeout(startDecay, DECAY_INTERVAL);
        } else {
            isAnimating = false;
            decayTimer = null;
        }
    };

    onMount(() => {
        // Initialize with current values to avoid triggering animation on mount
        lastCount = event.count;
        lastDeletedCount = event.deleted_count;
        lastEventTime = Date.now();

        // Start continuous decay immediately
        decayTimer = setTimeout(startDecay, DECAY_INTERVAL);
    });

    onDestroy(() => {
        // Clean up decay timer
        if (decayTimer) {
            clearTimeout(decayTimer);
        }
    });
</script>

<div
    class="mx-auto md:mx-0 bg-white border border-gray-200 rounded-lg p-2 md:p-6 hover:shadow-lg transition-all duration-200 hover:-translate-y-1 transform"
    class:has-activity={isAnimating}
    style="--border-thickness: {borderThickness}px"
>
    <div class="flex justify-between items-start mb-3">
        <div
            class="text-sm font-bold text-blue-600 bg-blue-100 px-3 py-1 rounded-full"
        >
            #{index + 1}
        </div>
    </div>
    <div class="font-mono text-sm text-gray-700 mb-2 break-all leading-relaxed">
        {nsid}
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

<style>
    .has-activity {
        position: relative;
        transition: all 0.2s ease-out;
    }

    .has-activity::before {
        content: "";
        position: absolute;
        top: calc(-1 * var(--border-thickness));
        left: calc(-1 * var(--border-thickness));
        right: calc(-1 * var(--border-thickness));
        bottom: calc(-1 * var(--border-thickness));
        border: var(--border-thickness) solid rgba(59, 130, 246, 0.8);
        border-radius: calc(0.5rem + var(--border-thickness));
        pointer-events: none;
        transition: all 0.3s ease-out;
    }
</style>
