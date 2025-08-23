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
    class="wsbadge !mt-0 !font-normal bg-blue-100 hover:bg-blue-200 border-blue-300"
>
    <input checked={dontShowBsky} type="checkbox" />
    <span class="ml-0.5"> hide app.bsky.* </span>
</button>
