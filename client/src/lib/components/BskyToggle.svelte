<script lang="ts">
    interface Props {
        dontShowBsky: boolean;
        onBskyToggle: () => void;
    }

    let { dontShowBsky, onBskyToggle }: Props = $props();
</script>

<!-- svelte-ignore a11y_click_events_have_key_events -->
<!-- svelte-ignore a11y_no_static_element_interactions -->
<button
    onclick={onBskyToggle}
    class="wsbadge !mt-0 !font-normal bg-yellow-100 hover:bg-yellow-200 border-yellow-300"
>
    <input checked={dontShowBsky} type="checkbox" />
    <span class="ml-0.5"> hide app.bsky.* </span>
</button>

<style lang="postcss">
    @reference "../../app.css";
    .wsbadge {
        @apply text-sm font-semibold mt-1.5 px-2.5 py-0.5 rounded-full border;
    }
</style>
