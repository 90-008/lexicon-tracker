<script lang="ts">
    interface Props {
        refreshRate: string;
        onRefreshChange: (value: string) => void;
    }

    let { refreshRate, onRefreshChange }: Props = $props();
</script>

<div
    class="wsbadge !pl-2 !px-1 !mt-0 !font-normal bg-green-100 hover:bg-green-200 border-green-300"
>
    <label for="refresh-rate" class="text-green-800 mr-1">refresh:</label>
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
        class="bg-green-50 text-green-900 placeholder-green-400 border border-green-200 rounded-full px-1 outline-none focus:bg-white focus:border-green-400 min-w-0 w-20"
    />
    <span class="text-green-700">s</span>
</div>
