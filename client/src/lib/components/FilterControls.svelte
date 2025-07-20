<script lang="ts">
    interface Props {
        filterRegex: string;
        dontShowBsky: boolean;
        onFilterChange: (value: string) => void;
        onBskyToggle: () => void;
    }

    let { filterRegex, dontShowBsky, onFilterChange, onBskyToggle }: Props =
        $props();
</script>

<div class="flex flex-wrap items-center gap-3 mb-6">
    <div
        class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-blue-100 hover:bg-blue-200 border-blue-300"
    >
        <label for="filter-regex" class="text-blue-800 mr-1"> filter: </label>
        <input
            id="filter-regex"
            value={filterRegex}
            oninput={(e) =>
                onFilterChange((e.target as HTMLInputElement).value)}
            type="text"
            placeholder="regex..."
            class="bg-blue-50 text-blue-900 placeholder-blue-400 border border-blue-200 rounded-full px-1 outline-none focus:bg-white focus:border-blue-400 min-w-0 w-24"
        />
    </div>
    <!-- svelte-ignore a11y_click_events_have_key_events -->
    <!-- svelte-ignore a11y_no_static_element_interactions -->
    <button
        onclick={onBskyToggle}
        class="wsbadge !mt-0 !font-normal bg-yellow-100 hover:bg-yellow-200 border-yellow-300"
    >
        <input checked={dontShowBsky} type="checkbox" />
        <span class="ml-0.5"> don't show app.bsky.* </span>
    </button>
</div>

<style lang="postcss">
    @reference "../../app.css";
    .wsbadge {
        @apply text-sm font-semibold mt-1.5 px-2.5 py-0.5 rounded-full border;
    }
</style>
