export const formatNumber = (num: number): string => {
  return num.toLocaleString();
};

const isValidDate = (d: Date) => d instanceof Date && !isNaN(d.getTime());
export const formatTimestamp = (timestamp: number): string => {
  const date = new Date(timestamp * 1000);
  return isValidDate(date)
    ? date.toLocaleString()
    : new Date(timestamp / 1000).toLocaleString();
};
