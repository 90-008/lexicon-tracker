<script lang="ts">
    interface Props {
        refreshRate: string;
        onRefreshChange: (value: string) => void;
    }

    let { refreshRate, onRefreshChange }: Props = $props();
</script>

<div
    class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-lime-100 dark:bg-lime-900 dark:hover:bg-lime-800 hover:bg-lime-200 border-lime-300 dark:border-lime-700"
>
    <label for="refresh-rate" class="text-lime-800 dark:text-lime-200 mr-1"
        >refresh:</label
    >
    <input
        id="refresh-rate"
        value={refreshRate}
        oninput={(e) => {
            const el = e.target as HTMLInputElement;
            if (!el.validity.valid) el.value = el.value.replace(/\D+/g, "");
            onRefreshChange(el.value);
        }}
        type="text"
        inputmode="numeric"
        pattern="[0-9]*"
        min="0"
        placeholder="real-time"
        class="bg-green-50 dark:bg-green-900 text-lime-900 dark:text-lime-200 placeholder-lime-600 dark:placeholder-lime-400 border border-lime-200 dark:border-lime-700 rounded-full px-1 outline-none focus:border-lime-400 min-w-0 w-20"
    />
    <span class="text-lime-800 dark:text-lime-200">s</span>
</div>
