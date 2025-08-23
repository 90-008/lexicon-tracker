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
    class="wsbadge !mt-0 !font-normal bg-blue-100 dark:bg-blue-900 hover:bg-blue-200 dark:hover:bg-blue-800 border-blue-300 dark:border-blue-700"
>
    <input checked={dontShowBsky} type="checkbox" />
    <span class="ml-0.5 text-black dark:text-gray-200"> hide app.bsky.* </span>
</button>
